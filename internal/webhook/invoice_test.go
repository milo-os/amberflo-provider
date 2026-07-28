/*
Copyright 2026 Datum Technology Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.
*/

package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
	"go.miloapis.com/amberflo-provider/internal/invoice"
)

// stubAmberflo is a minimal amberflo.Client for webhook tests.
type stubAmberflo struct {
	invoices    []amberflo.CustomerProductInvoice
	byKey       map[string]amberflo.CustomerProductInvoice
	listErr     error
	getErr      error
	getCalls    int
	listCalls   int
}

func (s *stubAmberflo) EnsureCustomer(context.Context, amberflo.DesiredCustomer) (amberflo.Customer, error) {
	return amberflo.Customer{}, nil
}
func (s *stubAmberflo) DisableCustomer(context.Context, string) error { return nil }
func (s *stubAmberflo) GetCustomer(context.Context, string) (amberflo.Customer, error) {
	return amberflo.Customer{}, nil
}
func (s *stubAmberflo) EnsureMeter(context.Context, amberflo.DesiredMeter) (amberflo.Meter, error) {
	return amberflo.Meter{}, nil
}
func (s *stubAmberflo) DeleteMeter(context.Context, string) error { return nil }
func (s *stubAmberflo) GetMeter(context.Context, string) (amberflo.Meter, error) {
	return amberflo.Meter{}, nil
}
func (s *stubAmberflo) SubmitUsage(context.Context, []amberflo.UsageRecord) error { return nil }
func (s *stubAmberflo) ListInvoices(context.Context, string) ([]amberflo.CustomerProductInvoice, error) {
	s.listCalls++
	return s.invoices, s.listErr
}
func (s *stubAmberflo) GetLatestInvoice(context.Context, string) (amberflo.CustomerProductInvoice, error) {
	return amberflo.CustomerProductInvoice{}, nil
}
func (s *stubAmberflo) GetInvoice(_ context.Context, key amberflo.InvoiceKey) (amberflo.CustomerProductInvoice, error) {
	s.getCalls++
	if s.getErr != nil {
		return amberflo.CustomerProductInvoice{}, s.getErr
	}
	if s.byKey != nil {
		if inv, ok := s.byKey[amberflo.FormatInvoiceKey(key)]; ok {
			return inv, nil
		}
	}
	if len(s.invoices) == 1 {
		return s.invoices[0], nil
	}
	return amberflo.CustomerProductInvoice{}, amberflo.ErrInvoiceNotFound
}
func (s *stubAmberflo) ListPaymentSettings(context.Context) ([]amberflo.PaymentSetting, error) {
	return nil, nil
}
func (s *stubAmberflo) ListPaymentMethodSwitches(context.Context, string) ([]amberflo.PaymentMethodSwitch, error) {
	return nil, nil
}
func (s *stubAmberflo) SchedulePaymentMethodSwitch(_ context.Context, sw amberflo.PaymentMethodSwitch) (amberflo.PaymentMethodSwitch, error) {
	return sw, nil
}

func newWebhookScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := billingv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return s
}

func TestInvoiceHandler_UnauthorizedWithoutSecret(t *testing.T) {
	t.Parallel()
	h := &InvoiceHandler{Secret: "expected-secret"}
	req := httptest.NewRequest(http.MethodPost, Endpoint, bytes.NewReader([]byte(`{"customerId":"x"}`)))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rr.Code)
	}
}

func TestInvoiceHandler_UnauthorizedWrongSecret(t *testing.T) {
	t.Parallel()
	h := &InvoiceHandler{Secret: "expected-secret"}
	req := httptest.NewRequest(http.MethodPost, Endpoint, bytes.NewReader([]byte(`{"customerId":"x"}`)))
	req.Header.Set(DefaultSecretHeader, "wrong-secret")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rr.Code)
	}
}

func TestInvoiceHandler_UnauthorizedWhenSecretUnconfigured(t *testing.T) {
	t.Parallel()
	h := &InvoiceHandler{Secret: ""}
	req := httptest.NewRequest(http.MethodPost, Endpoint, bytes.NewReader([]byte(`{"customerId":"x"}`)))
	req.Header.Set(DefaultSecretHeader, "anything")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 when handler secret empty", rr.Code)
	}
}

func TestInvoiceHandler_OKWithReadyProductInvoicesEnvelope(t *testing.T) {
	t.Parallel()

	scheme := newWebhookScheme(t)
	accountUID := types.UID("eda5be25-f145-4448-b85c-6dc6dce1a2ac")
	account := &billingv1alpha1.BillingAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "acct-ok",
			Namespace: "default",
			UID:       accountUID,
		},
		Spec: billingv1alpha1.BillingAccountSpec{CurrencyCode: "USD"},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&billingv1alpha1.Invoice{}).
		WithObjects(account).
		Build()

	start := time.Date(2022, 2, 6, 0, 0, 0, 0, time.UTC).Unix()
	end := time.Date(2022, 3, 6, 0, 0, 0, 0, time.UTC).Unix()
	key := amberflo.InvoiceKey{
		CustomerID:    string(accountUID),
		ProductID:     "1",
		ProductPlanID: "2b08a8f9-f70e-40e2-b595-06b6483c5d91",
		Year:          2022,
		Month:         2,
		Day:           6,
	}
	inv := amberflo.CustomerProductInvoice{
		InvoiceURI:                "payments/invoices/account_id=3/customer_id=" + string(accountUID),
		InvoiceStartTimeInSeconds: start,
		InvoiceEndTimeInSeconds:   end,
		TotalBill:                 amberflo.ProductPlanBill{TotalPrice: 10},
		PaymentStatus:             amberflo.PaymentStatusSettled,
		InvoiceKey:                key,
	}
	stub := &stubAmberflo{
		byKey: map[string]amberflo.CustomerProductInvoice{
			amberflo.FormatInvoiceKey(key): inv,
		},
	}

	h := &InvoiceHandler{
		Client:         c,
		AmberfloClient: stub,
		Syncer:         &invoice.Syncer{Client: c, Now: func() time.Time { return time.Date(2022, 3, 1, 0, 0, 0, 0, time.UTC) }},
		Secret:         "shared-secret",
	}

	body := []byte(`{
		"eventId": "1bf92cf0-1d99-11ed-90a2-4fb51c754f3b",
		"data": {
			"invoiceUri": "payments/invoices/account_id=3/customer_id=` + string(accountUID) + `",
			"customerId": "` + string(accountUID) + `",
			"productId": "1",
			"productPlanId": "2b08a8f9-f70e-40e2-b595-06b6483c5d91",
			"year": 2022,
			"month": 2,
			"day": 6,
			"invoiceStartTimeInSeconds": ` + jsonNumber(start) + `,
			"invoiceEndTimeInSeconds": ` + jsonNumber(end) + `,
			"totalBill": {"totalPrice": 10},
			"paymentStatus": "SETTLED"
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, Endpoint, bytes.NewReader(body))
	req.Header.Set(DefaultSecretHeader, "shared-secret")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	if stub.getCalls != 1 {
		t.Errorf("GetInvoice calls = %d, want 1", stub.getCalls)
	}
	if stub.listCalls != 0 {
		t.Errorf("ListInvoices calls = %d, want 0 when keyed Get succeeds", stub.listCalls)
	}

	var got billingv1alpha1.Invoice
	nn := types.NamespacedName{Name: "acct-ok-2022-02", Namespace: "default"}
	if err := c.Get(context.Background(), nn, &got); err != nil {
		t.Fatalf("expected Invoice created by webhook: %v", err)
	}
	if got.Status.Phase != billingv1alpha1.InvoicePhasePaid {
		t.Errorf("phase = %q, want Paid", got.Status.Phase)
	}
}

func TestInvoiceHandler_FallsBackToListWithoutKey(t *testing.T) {
	t.Parallel()

	scheme := newWebhookScheme(t)
	accountUID := types.UID("cust-uid-list")
	account := &billingv1alpha1.BillingAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "acct-list",
			Namespace: "default",
			UID:       accountUID,
		},
		Spec: billingv1alpha1.BillingAccountSpec{CurrencyCode: "USD"},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&billingv1alpha1.Invoice{}).
		WithObjects(account).
		Build()

	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).Unix()
	end := time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC).Unix()
	stub := &stubAmberflo{
		invoices: []amberflo.CustomerProductInvoice{{
			InvoiceURI:                "https://amberflo.example/inv/may",
			InvoiceStartTimeInSeconds: start,
			InvoiceEndTimeInSeconds:   end,
			TotalBill:                 amberflo.ProductPlanBill{TotalPrice: 10},
			PaymentStatus:             amberflo.PaymentStatusSettled,
			InvoiceKey: amberflo.InvoiceKey{
				CustomerID: string(accountUID),
				ProductID:  "1",
				Year:       2026,
				Month:      5,
				Day:        1,
			},
		}},
	}

	h := &InvoiceHandler{
		Client:         c,
		AmberfloClient: stub,
		Syncer:         &invoice.Syncer{Client: c, Now: func() time.Time { return time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC) }},
		Secret:         "shared-secret",
	}

	body, _ := json.Marshal(map[string]string{"customerId": string(accountUID)})
	req := httptest.NewRequest(http.MethodPost, Endpoint, bytes.NewReader(body))
	req.Header.Set(DefaultSecretHeader, "shared-secret")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	if stub.listCalls != 1 {
		t.Errorf("ListInvoices calls = %d, want 1", stub.listCalls)
	}
}

func TestParseInvoiceReadyEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantCust   string
		wantURI    string
		wantKey    bool
		wantErr    bool
	}{
		{
			name: "amberflo envelope with key fields",
			body: `{
				"eventId":"e1",
				"data":{
					"invoiceUri":"payments/invoices/x",
					"customerId":"c1",
					"productPlanId":"plan",
					"year":2022,"month":2,"day":6
				}
			}`,
			wantCust: "c1",
			wantURI:  "payments/invoices/x",
			wantKey:  true,
		},
		{name: "top-level customerId", body: `{"customerId":"c2"}`, wantCust: "c2"},
		{name: "empty body", body: ``, wantErr: true},
		{name: "missing", body: `{"event":"invoice.ready"}`, wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseInvoiceReadyEvent([]byte(tt.body))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseInvoiceReadyEvent: %v", err)
			}
			if got.CustomerID != tt.wantCust {
				t.Errorf("customerId = %q, want %q", got.CustomerID, tt.wantCust)
			}
			if got.InvoiceURI != tt.wantURI {
				t.Errorf("invoiceUri = %q, want %q", got.InvoiceURI, tt.wantURI)
			}
			if tt.wantKey && !got.Key.Complete() {
				t.Errorf("expected complete invoice key, got %+v", got.Key)
			}
		})
	}
}

func jsonNumber(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}
