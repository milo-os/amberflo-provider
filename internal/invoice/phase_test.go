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
	"testing"
	"time"

	"github.com/go-logr/logr"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
)

func TestMapPhase(t *testing.T) {
	t.Parallel()

	// Fixed "now" so past-due cases are deterministic.
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	// Invoice ended 2026-06-30 with 24h grace → due 2026-07-01 00:00 UTC.
	pastDueEnd := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC).Unix()
	// Invoice ends in the future relative to now.
	futureEnd := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC).Unix()

	tests := []struct {
		name string
		inv  amberflo.CustomerProductInvoice
		want billingv1alpha1.InvoicePhase
	}{
		{
			name: "SETTLED maps to Paid",
			inv:  amberflo.CustomerProductInvoice{PaymentStatus: amberflo.PaymentStatusSettled},
			want: billingv1alpha1.InvoicePhasePaid,
		},
		{
			name: "NOT_NEEDED maps to Paid",
			inv:  amberflo.CustomerProductInvoice{PaymentStatus: amberflo.PaymentStatusNotNeeded},
			want: billingv1alpha1.InvoicePhasePaid,
		},
		{
			name: "FAILED maps to PastDue",
			inv:  amberflo.CustomerProductInvoice{PaymentStatus: amberflo.PaymentStatusFailed},
			want: billingv1alpha1.InvoicePhasePastDue,
		},
		{
			name: "PRE_PAYMENT before due maps to Open",
			inv: amberflo.CustomerProductInvoice{
				PaymentStatus:           amberflo.PaymentStatusPrePayment,
				InvoiceEndTimeInSeconds: futureEnd,
			},
			want: billingv1alpha1.InvoicePhaseOpen,
		},
		{
			name: "PENDING before due maps to Open",
			inv: amberflo.CustomerProductInvoice{
				PaymentStatus:           amberflo.PaymentStatusPending,
				InvoiceEndTimeInSeconds: futureEnd,
			},
			want: billingv1alpha1.InvoicePhaseOpen,
		},
		{
			name: "REQUIRES_ACTION before due maps to Open",
			inv: amberflo.CustomerProductInvoice{
				PaymentStatus:           amberflo.PaymentStatusRequiresAction,
				InvoiceEndTimeInSeconds: futureEnd,
			},
			want: billingv1alpha1.InvoicePhaseOpen,
		},
		{
			name: "PRE_PAYMENT past due maps to PastDue",
			inv: amberflo.CustomerProductInvoice{
				PaymentStatus:           amberflo.PaymentStatusPrePayment,
				InvoiceEndTimeInSeconds: pastDueEnd,
				GracePeriodInHours:      24,
			},
			want: billingv1alpha1.InvoicePhasePastDue,
		},
		{
			name: "UNKNOWN before due maps to Open (void-ish default)",
			inv: amberflo.CustomerProductInvoice{
				PaymentStatus:           amberflo.PaymentStatusUnknown,
				InvoiceEndTimeInSeconds: futureEnd,
			},
			want: billingv1alpha1.InvoicePhaseOpen,
		},
		{
			name: "empty paymentStatus before due maps to Open",
			inv: amberflo.CustomerProductInvoice{
				PaymentStatus:           "",
				InvoiceEndTimeInSeconds: futureEnd,
			},
			want: billingv1alpha1.InvoicePhaseOpen,
		},
		{
			name: "UNKNOWN past due maps to PastDue",
			inv: amberflo.CustomerProductInvoice{
				PaymentStatus:           amberflo.PaymentStatusUnknown,
				InvoiceEndTimeInSeconds: pastDueEnd,
				GracePeriodInHours:      24,
			},
			want: billingv1alpha1.InvoicePhasePastDue,
		},
		{
			name: "unrecognized paymentStatus defaults to Open",
			inv: amberflo.CustomerProductInvoice{
				PaymentStatus:           amberflo.PaymentStatus("VOIDED"),
				InvoiceEndTimeInSeconds: futureEnd,
			},
			want: billingv1alpha1.InvoicePhaseOpen,
		},
		{
			name: "SETTLED ignores past due date",
			inv: amberflo.CustomerProductInvoice{
				PaymentStatus:           amberflo.PaymentStatusSettled,
				InvoiceEndTimeInSeconds: pastDueEnd,
				GracePeriodInHours:      24,
			},
			want: billingv1alpha1.InvoicePhasePaid,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := MapPhase(tt.inv, now, logr.Discard())
			if got != tt.want {
				t.Errorf("MapPhase() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInvoiceName(t *testing.T) {
	t.Parallel()
	// 2026-03-15 00:00:00 UTC
	start := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC).Unix()
	if got := InvoiceName("acct-1", start); got != "acct-1-2026-03" {
		t.Errorf("InvoiceName = %q, want acct-1-2026-03", got)
	}
}

func TestAmounts(t *testing.T) {
	t.Parallel()

	total, paid, due := Amounts(42.5, billingv1alpha1.InvoicePhasePaid)
	if total != "42.50" || paid != "42.50" || due != "0.00" {
		t.Errorf("Paid amounts = (%s,%s,%s), want (42.50,42.50,0.00)", total, paid, due)
	}

	total, paid, due = Amounts(42.5, billingv1alpha1.InvoicePhaseOpen)
	if total != "42.50" || paid != "0.00" || due != "42.50" {
		t.Errorf("Open amounts = (%s,%s,%s), want (42.50,0.00,42.50)", total, paid, due)
	}

	total, paid, due = Amounts(10, billingv1alpha1.InvoicePhasePastDue)
	if total != "10.00" || paid != "0.00" || due != "10.00" {
		t.Errorf("PastDue amounts = (%s,%s,%s), want (10.00,0.00,10.00)", total, paid, due)
	}
}

func TestDueTime(t *testing.T) {
	t.Parallel()

	if got := DueTime(amberflo.CustomerProductInvoice{}); got != nil {
		t.Errorf("DueTime with no end = %v, want nil", got)
	}

	end := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC).Unix()
	got := DueTime(amberflo.CustomerProductInvoice{
		InvoiceEndTimeInSeconds: end,
		GracePeriodInHours:      48,
	})
	if got == nil {
		t.Fatal("DueTime returned nil")
	}
	want := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
	if !got.UTC().Equal(want) {
		t.Errorf("DueTime = %v, want %v", got.UTC(), want)
	}
}
