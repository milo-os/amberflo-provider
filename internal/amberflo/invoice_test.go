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

package amberflo

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestListInvoices_DecodesWirePayload(t *testing.T) {
	c, f := newTestClient(t)
	f.seedInvoices("cust-1", []CustomerProductInvoice{
		{
			InvoiceURI:                "https://amberflo.example/inv/1",
			InvoiceStartTimeInSeconds: 1711929600,
			InvoiceEndTimeInSeconds:   1714521600,
			GracePeriodInHours:        24,
			TotalBill:                 ProductPlanBill{TotalPrice: 99.5},
			InvoicePriceStatus:        "price_finalized",
			PaymentStatus:             PaymentStatusSettled,
			InvoiceKey: InvoiceKey{
				CustomerID: "cust-1",
				ProductID:  "1",
				Year:       2024,
				Month:      4,
				Day:        1,
			},
		},
		{
			InvoiceURI:                "https://amberflo.example/inv/2",
			InvoiceStartTimeInSeconds: 1714521600,
			InvoiceEndTimeInSeconds:   1717200000,
			TotalBill:                 ProductPlanBill{TotalPrice: 10},
			PaymentStatus:             PaymentStatusPrePayment,
			InvoiceKey: InvoiceKey{
				CustomerID: "cust-1",
				ProductID:  "1",
				Year:       2024,
				Month:      5,
				Day:        1,
			},
		},
	})

	got, err := c.ListInvoices(context.Background(), "cust-1")
	if err != nil {
		t.Fatalf("ListInvoices: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].PaymentStatus != PaymentStatusSettled {
		t.Errorf("first paymentStatus = %q", got[0].PaymentStatus)
	}
	if got[0].TotalBill.TotalPrice != 99.5 {
		t.Errorf("first totalPrice = %v", got[0].TotalBill.TotalPrice)
	}
	if got[0].InvoiceKey.CustomerID != "cust-1" || got[0].InvoiceKey.Month != 4 {
		t.Errorf("first invoiceKey = %+v", got[0].InvoiceKey)
	}
	if got[1].PaymentStatus != PaymentStatusPrePayment {
		t.Errorf("second paymentStatus = %q", got[1].PaymentStatus)
	}

	reqs := f.requestsCopy()
	found := false
	for _, r := range reqs {
		if r.Method == http.MethodGet &&
			strings.Contains(r.Path, "/payments/billing/customer-product-invoice") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected ListInvoices GET to be recorded")
	}
}

func TestListInvoices_EmptyAndNull(t *testing.T) {
	c, f := newTestClient(t)

	got, err := c.ListInvoices(context.Background(), "nobody")
	if err != nil {
		t.Fatalf("ListInvoices empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty list, got %d", len(got))
	}

	// Explicit null body.
	f.armInvoiceNull = true
	got, err = c.ListInvoices(context.Background(), "nobody")
	if err != nil {
		t.Fatalf("ListInvoices null: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("null body should decode as empty, got %d", len(got))
	}
}

func TestListInvoices_EmptyCustomerIDIsPermanent(t *testing.T) {
	c, _ := newTestClient(t)
	_, err := c.ListInvoices(context.Background(), "")
	if err == nil || !IsPermanent(err) {
		t.Fatalf("expected PermanentError, got %v", err)
	}
}

func TestGetLatestInvoice_DecodesAndNotFound(t *testing.T) {
	c, f := newTestClient(t)

	_, err := c.GetLatestInvoice(context.Background(), "missing")
	if !errors.Is(err, ErrInvoiceNotFound) {
		t.Fatalf("expected ErrInvoiceNotFound, got %v", err)
	}

	f.seedLatestInvoice("cust-2", CustomerProductInvoice{
		InvoiceURI:                "https://amberflo.example/latest",
		InvoiceStartTimeInSeconds: 1711929600,
		InvoiceEndTimeInSeconds:   1714521600,
		TotalBill:                 ProductPlanBill{TotalPrice: 1.25},
		PaymentStatus:             PaymentStatusPending,
		InvoiceKey:                InvoiceKey{CustomerID: "cust-2", ProductID: "1", Year: 2024, Month: 4},
	})

	got, err := c.GetLatestInvoice(context.Background(), "cust-2")
	if err != nil {
		t.Fatalf("GetLatestInvoice: %v", err)
	}
	if got.PaymentStatus != PaymentStatusPending {
		t.Errorf("paymentStatus = %q", got.PaymentStatus)
	}
	if got.TotalBill.TotalPrice != 1.25 {
		t.Errorf("totalPrice = %v", got.TotalBill.TotalPrice)
	}
	if len(got.Raw) == 0 {
		t.Error("expected Raw body to be attached on GetLatestInvoice")
	}
}

func TestGetInvoice_ByKey(t *testing.T) {
	c, f := newTestClient(t)
	key := InvoiceKey{
		CustomerID:    "cust-3",
		ProductID:     "1",
		ProductPlanID: "plan-a",
		Year:          2022,
		Month:         2,
		Day:           6,
	}
	f.seedInvoices("cust-3", []CustomerProductInvoice{{
		InvoiceURI:                "payments/invoices/x",
		InvoiceStartTimeInSeconds: 1644105600,
		InvoiceEndTimeInSeconds:   1646524800,
		PaymentStatus:             PaymentStatusSettled,
		TotalBill:                 ProductPlanBill{TotalPrice: 42},
		InvoiceKey:                key,
	}})

	got, err := c.GetInvoice(context.Background(), key)
	if err != nil {
		t.Fatalf("GetInvoice: %v", err)
	}
	if got.PaymentStatus != PaymentStatusSettled {
		t.Errorf("paymentStatus = %q", got.PaymentStatus)
	}
	if got.TotalBill.TotalPrice != 42 {
		t.Errorf("totalPrice = %v", got.TotalBill.TotalPrice)
	}

	var found bool
	for _, r := range f.requestsCopy() {
		if r.Method == http.MethodGet && r.Path == "/payments/billing/customer-product-invoice" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected GetInvoice GET without /all, requests=%+v", f.requestsCopy())
	}

	_, err = c.GetInvoice(context.Background(), InvoiceKey{
		CustomerID: "cust-3", ProductPlanID: "missing", Year: 2022, Month: 2, Day: 6,
	})
	if !errors.Is(err, ErrInvoiceNotFound) {
		t.Errorf("missing key err = %v, want ErrInvoiceNotFound", err)
	}
}

func TestInvoiceGetPath(t *testing.T) {
	path := invoiceGetPath(InvoiceKey{
		CustomerID: "c", ProductPlanID: "p", Year: 2022, Month: 2, Day: 6,
	})
	for _, want := range []string{
		"/payments/billing/customer-product-invoice?",
		"customerId=c",
		"productPlanId=p",
		"year=2022",
		"month=2",
		"day=6",
		"withPaymentStatus=true",
	} {
		if !strings.Contains(path, want) {
			t.Errorf("path %q missing %q", path, want)
		}
	}
	if strings.Contains(path, "/all") {
		t.Errorf("get-by-key path must not use /all: %q", path)
	}
}

func TestInvoiceListPath(t *testing.T) {
	path := invoiceListPath("cust-x", false)
	for _, want := range []string{
		"customerId=cust-x",
		"productId=1",
		"fromCache=true",
		"withPaymentStatus=true",
	} {
		if !strings.Contains(path, want) {
			t.Errorf("path %q missing %q", path, want)
		}
	}
	if strings.Contains(path, "latest=") {
		t.Errorf("non-latest path should not set latest: %q", path)
	}

	latest := invoiceListPath("cust-x", true)
	if !strings.Contains(latest, "latest=true") {
		t.Errorf("latest path missing latest=true: %q", latest)
	}
}

func TestFormatInvoiceKey(t *testing.T) {
	key := InvoiceKey{CustomerID: "c", ProductID: "1", Year: 2026, Month: 3, Day: 1}
	raw := FormatInvoiceKey(key)
	var decoded InvoiceKey
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded != key {
		t.Errorf("round-trip = %+v, want %+v", decoded, key)
	}
}
