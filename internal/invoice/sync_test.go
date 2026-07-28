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

package invoice

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
)

func newInvoiceScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := billingv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return s
}

func newTestAccount() *billingv1alpha1.BillingAccount {
	return &billingv1alpha1.BillingAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "acct-1",
			Namespace: "default",
			UID:       types.UID("uid-acct-1"),
		},
		Spec: billingv1alpha1.BillingAccountSpec{
			CurrencyCode: "USD",
		},
	}
}

func sampleAmberfloInvoice(status amberflo.PaymentStatus, total float64) amberflo.CustomerProductInvoice {
	start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).Unix()
	end := time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC).Unix()
	return amberflo.CustomerProductInvoice{
		InvoiceURI:                "https://amberflo.example/inv/1",
		InvoiceStartTimeInSeconds: start,
		InvoiceEndTimeInSeconds:   end,
		GracePeriodInHours:        24,
		TotalBill:                 amberflo.ProductPlanBill{TotalPrice: total},
		PaymentStatus:             status,
		InvoiceKey: amberflo.InvoiceKey{
			CustomerID: "uid-acct-1",
			ProductID:  "1",
			Year:       2026,
			Month:      3,
			Day:        1,
		},
	}
}

func TestSyncer_Upsert_CreatesInvoice(t *testing.T) {
	scheme := newInvoiceScheme(t)
	account := newTestAccount()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&billingv1alpha1.Invoice{}).
		WithObjects(account).
		Build()

	// Mid-period so PRE_PAYMENT stays Open (not past due).
	fixedNow := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	syncer := &Syncer{
		Client: c,
		Now:    func() time.Time { return fixedNow },
	}

	inv := sampleAmberfloInvoice(amberflo.PaymentStatusPrePayment, 42.5)
	if err := syncer.Upsert(context.Background(), account, inv); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	var got billingv1alpha1.Invoice
	key := types.NamespacedName{Name: "acct-1-2026-03", Namespace: "default"}
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("Get Invoice: %v", err)
	}

	if got.Name != "acct-1-2026-03" {
		t.Errorf("name = %q, want acct-1-2026-03", got.Name)
	}
	if got.Spec.BillingAccountRef.Name != "acct-1" {
		t.Errorf("billingAccountRef = %q", got.Spec.BillingAccountRef.Name)
	}
	if got.Annotations[InvoiceKeyAnnotation] == "" {
		t.Errorf("missing %s annotation", InvoiceKeyAnnotation)
	}
	wantKey := amberflo.FormatInvoiceKey(inv.InvoiceKey)
	if got.Annotations[InvoiceKeyAnnotation] != wantKey {
		t.Errorf("invoiceKey annotation = %q, want %q", got.Annotations[InvoiceKeyAnnotation], wantKey)
	}
	if len(got.OwnerReferences) != 1 {
		t.Fatalf("ownerRefs len = %d, want 1", len(got.OwnerReferences))
	}
	or := got.OwnerReferences[0]
	if or.Name != account.Name || or.UID != account.UID {
		t.Errorf("ownerRef = %+v, want name/uid of account", or)
	}
	if or.Controller == nil || *or.Controller {
		t.Errorf("ownerRef.Controller should be false, got %v", or.Controller)
	}
	if or.BlockOwnerDeletion != nil && *or.BlockOwnerDeletion {
		t.Errorf("ownerRef.BlockOwnerDeletion should be false so BA deletion is not blocked")
	}
	if got.Status.Phase != billingv1alpha1.InvoicePhaseOpen {
		t.Errorf("phase = %q, want Open", got.Status.Phase)
	}
	if got.Status.Total != "42.50" || got.Status.AmountDue != "42.50" || got.Status.AmountPaid != "0.00" {
		t.Errorf("money status total=%s paid=%s due=%s", got.Status.Total, got.Status.AmountPaid, got.Status.AmountDue)
	}
	if got.Status.CurrencyCode != "USD" {
		t.Errorf("currency = %q", got.Status.CurrencyCode)
	}
	if got.Status.DocumentURI != inv.InvoiceURI {
		t.Errorf("documentUri = %q", got.Status.DocumentURI)
	}
}

func TestSyncer_Upsert_UpdateIsIdempotent(t *testing.T) {
	scheme := newInvoiceScheme(t)
	account := newTestAccount()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&billingv1alpha1.Invoice{}).
		WithObjects(account).
		Build()

	fixedNow := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	syncer := &Syncer{
		Client: c,
		Now:    func() time.Time { return fixedNow },
	}

	open := sampleAmberfloInvoice(amberflo.PaymentStatusPrePayment, 42.5)
	if err := syncer.Upsert(context.Background(), account, open); err != nil {
		t.Fatalf("first Upsert: %v", err)
	}

	paid := sampleAmberfloInvoice(amberflo.PaymentStatusSettled, 42.5)
	paid.PaymentCreatedInSeconds = time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC).Unix()
	if err := syncer.Upsert(context.Background(), account, paid); err != nil {
		t.Fatalf("second Upsert: %v", err)
	}

	var list billingv1alpha1.InvoiceList
	if err := c.List(context.Background(), &list, client.InNamespace("default")); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected exactly 1 Invoice after idempotent upsert, got %d", len(list.Items))
	}

	got := list.Items[0]
	if got.Name != "acct-1-2026-03" {
		t.Errorf("name changed to %q", got.Name)
	}
	if got.Status.Phase != billingv1alpha1.InvoicePhasePaid {
		t.Errorf("phase after update = %q, want Paid", got.Status.Phase)
	}
	if got.Status.AmountPaid != "42.50" || got.Status.AmountDue != "0.00" {
		t.Errorf("paid money paid=%s due=%s", got.Status.AmountPaid, got.Status.AmountDue)
	}
	if got.Status.PaidAt == nil {
		t.Fatal("PaidAt should be set for Paid phase")
	}
	if got.Annotations[InvoiceKeyAnnotation] == "" {
		t.Error("invoiceKey annotation lost on update")
	}
	// Spec is immutable — period must still match the create-time values.
	wantStart := metav1.NewTime(time.Unix(open.InvoiceStartTimeInSeconds, 0).UTC())
	if !got.Spec.Period.Start.Equal(&wantStart) {
		t.Errorf("spec.period.start mutated: got %v want %v", got.Spec.Period.Start, wantStart)
	}
}


func TestSyncer_Upsert_RejectsNilAccount(t *testing.T) {
	syncer := &Syncer{Client: fake.NewClientBuilder().WithScheme(newInvoiceScheme(t)).Build()}
	err := syncer.Upsert(context.Background(), nil, sampleAmberfloInvoice(amberflo.PaymentStatusSettled, 1))
	if err == nil {
		t.Fatal("expected error for nil account")
	}
}

func TestSyncer_Upsert_RejectsMissingStartTime(t *testing.T) {
	account := newTestAccount()
	syncer := &Syncer{
		Client: fake.NewClientBuilder().
			WithScheme(newInvoiceScheme(t)).
			WithStatusSubresource(&billingv1alpha1.Invoice{}).
			WithObjects(account).
			Build(),
	}
	inv := sampleAmberfloInvoice(amberflo.PaymentStatusSettled, 1)
	inv.InvoiceStartTimeInSeconds = 0
	err := syncer.Upsert(context.Background(), account, inv)
	if err == nil {
		t.Fatal("expected error for missing start time")
	}
}
