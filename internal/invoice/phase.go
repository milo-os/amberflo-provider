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
	"fmt"
	"time"

	"github.com/go-logr/logr"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
)

// MapPhase translates Amberflo payment/price status into a Milo Invoice
// phase. Primary signal is paymentStatus; invoicePriceStatus and due-date
// are secondary.
//
// Mapping (from design/invoicing.md):
//
//	SETTLED, NOT_NEEDED                         → Paid
//	FAILED                                      → PastDue
//	PRE_PAYMENT, PENDING, REQUIRES_ACTION, UNKNOWN → Open
//	invoicePriceStatus=price_locked (unpaid)    → Open
//	past dueDate with unpaid paymentStatus      → PastDue
//	unknown paymentStatus                       → Open (+ warn log)
func MapPhase(inv amberflo.CustomerProductInvoice, now time.Time, log logr.Logger) billingv1alpha1.InvoicePhase {
	switch inv.PaymentStatus {
	case amberflo.PaymentStatusSettled, amberflo.PaymentStatusNotNeeded:
		return billingv1alpha1.InvoicePhasePaid
	case amberflo.PaymentStatusFailed:
		return billingv1alpha1.InvoicePhasePastDue
	case amberflo.PaymentStatusPrePayment,
		amberflo.PaymentStatusPending,
		amberflo.PaymentStatusRequiresAction:
		if pastDue(inv, now) {
			return billingv1alpha1.InvoicePhasePastDue
		}
		return billingv1alpha1.InvoicePhaseOpen
	case amberflo.PaymentStatusUnknown, "":
		if log.GetSink() != nil {
			log.Info("unknown Amberflo paymentStatus; defaulting Invoice phase to Open",
				"paymentStatus", string(inv.PaymentStatus),
				"invoicePriceStatus", inv.InvoicePriceStatus,
			)
		}
		if pastDue(inv, now) {
			return billingv1alpha1.InvoicePhasePastDue
		}
		// price_locked without a settled payment stays Open.
		return billingv1alpha1.InvoicePhaseOpen
	default:
		if log.GetSink() != nil {
			log.Info("unrecognized Amberflo paymentStatus; defaulting Invoice phase to Open",
				"paymentStatus", string(inv.PaymentStatus),
			)
		}
		if pastDue(inv, now) {
			return billingv1alpha1.InvoicePhasePastDue
		}
		return billingv1alpha1.InvoicePhaseOpen
	}
}

// pastDue reports whether now is after invoice end + grace period and the
// invoice is not already paid.
func pastDue(inv amberflo.CustomerProductInvoice, now time.Time) bool {
	if inv.InvoiceEndTimeInSeconds <= 0 {
		return false
	}
	due := time.Unix(inv.InvoiceEndTimeInSeconds, 0).UTC().
		Add(time.Duration(inv.GracePeriodInHours) * time.Hour)
	return now.UTC().After(due)
}

// DueTime returns the payment due instant (invoice end + grace), or nil
// when the invoice has no end timestamp.
func DueTime(inv amberflo.CustomerProductInvoice) *time.Time {
	if inv.InvoiceEndTimeInSeconds <= 0 {
		return nil
	}
	t := time.Unix(inv.InvoiceEndTimeInSeconds, 0).UTC().
		Add(time.Duration(inv.GracePeriodInHours) * time.Hour)
	return &t
}

// FormatDecimal renders a float64 as a decimal string suitable for
// Invoice status money fields (no scientific notation).
func FormatDecimal(v float64) string {
	return fmt.Sprintf("%.2f", v)
}

// Amounts derives total / amountPaid / amountDue from phase and Amberflo
// totals. Paid invoices report amountDue=0 and amountPaid=total; open /
// past-due report amountPaid=0 and amountDue=total.
func Amounts(total float64, phase billingv1alpha1.InvoicePhase) (totalStr, paidStr, dueStr string) {
	totalStr = FormatDecimal(total)
	switch phase {
	case billingv1alpha1.InvoicePhasePaid:
		return totalStr, totalStr, FormatDecimal(0)
	default:
		return totalStr, FormatDecimal(0), totalStr
	}
}

// InvoiceName builds the deterministic Milo Invoice name
// `<billing-account>-<YYYY>-<MM>` from the Amberflo invoice start time.
func InvoiceName(accountName string, startSeconds int64) string {
	t := time.Unix(startSeconds, 0).UTC()
	return fmt.Sprintf("%s-%04d-%02d", accountName, t.Year(), int(t.Month()))
}
