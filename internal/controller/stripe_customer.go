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

package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"
	stripev1alpha1 "go.miloapis.com/stripe-provider/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
)

// Amberflo Stripe integration trait keys. Lowercase forms match Amberflo's
// Stripe docs.
const (
	traitStripeID            = "stripeid"
	traitPaymentProviderName = "paymentprovidername"

	paymentProviderStripe = "stripe"

	// stripePaymentSwitchFallbackID is used as targetPaymentId when
	// Amberflo's billing-settings/list is empty. Staging with an Active
	// Stripe connector accepts this literal; the API normalizes
	// targetPaymentType to "stripe".
	stripePaymentSwitchFallbackID = "stripe"

	// paymentSwitchMinFutureSkew is added when the computed period start
	// is not strictly in the future. Amberflo rejects switchTimeInSeconds
	// that are in the past or equal to "now".
	paymentSwitchMinFutureSkew = 60 * time.Second
)

// resolveStripeCustomerID returns the Stripe customer id for the account's
// default payment method when DefaultPaymentMethodReady=True.
//
// Resolution path:
//
//	BA.spec.defaultPaymentMethodRef.name
//	  → PaymentMethod (must be Active)
//	  → StripePaymentMethod with the same name
//	  → status.stripeCustomerId
//
// Returns ("", nil) when the gate is not met or the Stripe id is not yet
// available — callers must preserve any previously synced Stripe ExtraTraits
// and skip scheduling a payment-method switch in that case.
func (r *BillingAccountReconciler) resolveStripeCustomerID(
	ctx context.Context,
	account *billingv1alpha1.BillingAccount,
) (string, error) {
	if !apimeta.IsStatusConditionTrue(account.Status.Conditions, billingv1alpha1.BillingAccountConditionDefaultPaymentMethodReady) {
		return "", nil
	}
	if account.Spec.DefaultPaymentMethodRef == nil || account.Spec.DefaultPaymentMethodRef.Name == "" {
		return "", nil
	}

	pmName := account.Spec.DefaultPaymentMethodRef.Name
	ns := account.Namespace

	var pm billingv1alpha1.PaymentMethod
	if err := r.Get(ctx, types.NamespacedName{Name: pmName, Namespace: ns}, &pm); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("get PaymentMethod %s/%s: %w", ns, pmName, err)
	}
	if pm.Status.Phase != billingv1alpha1.PaymentMethodPhaseActive {
		return "", nil
	}

	var spm stripev1alpha1.StripePaymentMethod
	if err := r.Get(ctx, types.NamespacedName{Name: pmName, Namespace: ns}, &spm); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("get StripePaymentMethod %s/%s: %w", ns, pmName, err)
	}
	if spm.Status.StripeCustomerID == "" {
		return "", nil
	}
	return spm.Status.StripeCustomerID, nil
}

// stripeExtraTraitsForAccount builds the Amberflo ExtraTraits map that
// links a customer to Stripe for Amberflo's native charging integration.
// The payment-method effective time is scheduled separately via
// SchedulePaymentMethodSwitch (switchTimeInSeconds), not as a trait.
func stripeExtraTraitsForAccount(stripeCustomerID string) map[string]string {
	if stripeCustomerID == "" {
		return nil
	}
	return map[string]string{
		traitStripeID:            stripeCustomerID,
		traitPaymentProviderName: paymentProviderStripe,
	}
}

// stripeTraitsFromExisting copies previously synced Stripe ExtraTraits so a
// transient DefaultPaymentMethodReady=false / SPM lag does not strip
// stripeid from Amberflo on the next EnsureCustomer PUT.
func stripeTraitsFromExisting(traits map[string]string) map[string]string {
	if len(traits) == 0 {
		return nil
	}
	id := traits[traitStripeID]
	if id == "" {
		id = traits["stripeId"]
	}
	if id == "" {
		return nil
	}
	out := map[string]string{
		traitStripeID:            id,
		traitPaymentProviderName: paymentProviderStripe,
	}
	if v := traits[traitPaymentProviderName]; v != "" {
		out[traitPaymentProviderName] = v
	}
	return out
}

// scheduleStripePaymentSwitch ensures Amberflo will charge through Stripe
// by calling POST /customers/payment-method/switch.
//
// targetPaymentType/Id prefer an entry from GET /payments/billing-settings/list
// when present. Amberflo's Connectors "Stripe" Active state does not always
// populate that list; in that case we fall back to type/id "stripe", which
// the switch API accepts once Stripe is connected.
//
// switchTimeInSeconds prefers the current billing-period start when that
// instant is still in the future. Otherwise Amberflo requires a future
// timestamp, so we schedule ASAP (now + paymentSwitchMinFutureSkew).
//
// Idempotent: skips when a matching switch already exists for the same
// Stripe customer id and switch time.
func (r *BillingAccountReconciler) scheduleStripePaymentSwitch(
	ctx context.Context,
	logger logr.Logger,
	account *billingv1alpha1.BillingAccount,
	customerID string,
	stripeCustomerID string,
	now time.Time,
) error {
	if stripeCustomerID == "" || customerID == "" {
		return nil
	}

	targetType, targetID, err := r.resolveStripePaymentSwitchTarget(ctx)
	if err != nil {
		return err
	}

	switchAt := switchTimeUnix(now, account.Spec.PaymentTerms)
	existing, err := r.AmberfloClient.ListPaymentMethodSwitches(ctx, customerID)
	if err != nil {
		return fmt.Errorf("list Amberflo payment method switches: %w", err)
	}
	if amberflo.HasMatchingPaymentMethodSwitch(existing, stripeCustomerID, switchAt) {
		logger.V(1).Info("Amberflo Stripe payment switch already scheduled",
			"stripeCustomerId", stripeCustomerID,
			"switchTimeInSeconds", switchAt,
			"targetPaymentType", targetType,
			"targetPaymentId", targetID,
		)
		return nil
	}

	sw := amberflo.PaymentMethodSwitch{
		CustomerID:               customerID,
		TargetPaymentType:        targetType,
		TargetPaymentID:          targetID,
		TargetCustomerIdentifier: stripeCustomerID,
		SwitchTimeInSeconds:      switchAt,
	}

	got, err := r.AmberfloClient.SchedulePaymentMethodSwitch(ctx, sw)
	if err != nil {
		return fmt.Errorf("schedule Amberflo payment method switch: %w", err)
	}
	logger.Info("scheduled Amberflo Stripe payment method switch",
		"switch", amberflo.FormatPaymentMethodSwitch(got),
	)
	return nil
}

// resolveStripePaymentSwitchTarget picks targetPaymentType/Id for a Stripe
// switch. Prefers billing-settings/list (or an explicit config pin); falls
// back to the Connectors-compatible "stripe"/"stripe" pair when the list
// has no Stripe entry.
func (r *BillingAccountReconciler) resolveStripePaymentSwitchTarget(ctx context.Context) (targetType, targetID string, err error) {
	settings, err := r.AmberfloClient.ListPaymentSettings(ctx)
	if err != nil {
		return "", "", fmt.Errorf("list Amberflo payment settings: %w", err)
	}
	preferID := r.StripePaymentSettingID
	if setting, ok := amberflo.FindPaymentSettingBySystem(settings, amberflo.BillingSystemStripe, preferID); ok {
		targetType = setting.BillingSystem
		if targetType == "" {
			targetType = paymentProviderStripe
		}
		return targetType, setting.ID, nil
	}
	if preferID != "" {
		// Operator pinned an id that is not present/Stripe-matched.
		return "", "", &amberflo.PermanentError{Err: fmt.Errorf(
			"Amberflo Stripe payment setting id %q not found in billing-settings/list",
			preferID,
		)}
	}
	return paymentProviderStripe, stripePaymentSwitchFallbackID, nil
}

// switchTimeUnix returns the Unix timestamp Amberflo should evaluate for a
// payment-method switch. Prefer the current period start when it is still
// strictly in the future; otherwise schedule ASAP.
func switchTimeUnix(now time.Time, terms *billingv1alpha1.PaymentTerms) int64 {
	now = now.UTC()
	start := periodStartUTC(now, terms)
	if start.After(now) {
		return start.Unix()
	}
	return now.Add(paymentSwitchMinFutureSkew).Unix()
}

// monthStartUTC returns midnight UTC on the first day of now's calendar month.
func monthStartUTC(now time.Time) time.Time {
	t := now.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// invoiceDayInMonth returns midnight UTC on day-of-month within year/month,
// clamped to the month's last day (e.g. day 31 in February → 28/29).
func invoiceDayInMonth(year int, month time.Month, day int) time.Time {
	lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
	if day > lastDay {
		day = lastDay
	}
	if day < 1 {
		day = 1
	}
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// periodStartUTC derives the billing-period start used when that instant is
// still in the future (see switchTimeUnix). When payment terms carry an
// invoice day of month, the period start is that day in the current period
// (clamped to the month's last day). If today's date is still before this
// month's invoice day, the current period started on the previous month's
// invoice day. Otherwise it falls back to calendar month start.
func periodStartUTC(now time.Time, terms *billingv1alpha1.PaymentTerms) time.Time {
	now = now.UTC()
	if terms == nil || terms.InvoiceDayOfMonth <= 0 {
		return monthStartUTC(now)
	}
	day := int(terms.InvoiceDayOfMonth)
	start := invoiceDayInMonth(now.Year(), now.Month(), day)
	if now.Before(start) {
		prev := now.AddDate(0, -1, 0)
		start = invoiceDayInMonth(prev.Year(), prev.Month(), day)
	}
	return start
}
