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
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"
	stripev1alpha1 "go.miloapis.com/stripe-provider/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
)

func newStripeTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := billingv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("billing scheme: %v", err)
	}
	if err := stripev1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("stripe scheme: %v", err)
	}
	return s
}

func TestResolveStripeCustomerID_GatedWhenDefaultPMNotReady(t *testing.T) {
	t.Parallel()
	scheme := newStripeTestScheme(t)

	account := &billingv1alpha1.BillingAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "ba-1", Namespace: "default", UID: "uid-1"},
		Spec: billingv1alpha1.BillingAccountSpec{
			CurrencyCode:            "USD",
			DefaultPaymentMethodRef: &billingv1alpha1.DefaultPaymentMethodRef{Name: "pm-1"},
		},
	}
	pm := &billingv1alpha1.PaymentMethod{
		ObjectMeta: metav1.ObjectMeta{Name: "pm-1", Namespace: "default"},
		Spec: billingv1alpha1.PaymentMethodSpec{
			BillingAccountRef: billingv1alpha1.BillingAccountRef{Name: "ba-1"},
			DisplayName:       "Card",
		},
		Status: billingv1alpha1.PaymentMethodStatus{Phase: billingv1alpha1.PaymentMethodPhaseActive},
	}
	spm := &stripev1alpha1.StripePaymentMethod{
		ObjectMeta: metav1.ObjectMeta{Name: "pm-1", Namespace: "default"},
		Spec: stripev1alpha1.StripePaymentMethodSpec{
			PaymentMethodRef: stripev1alpha1.PaymentMethodLocalRef{Name: "pm-1"},
		},
		Status: stripev1alpha1.StripePaymentMethodStatus{StripeCustomerID: "cus_should_not_resolve"},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(account, pm, spm).Build()
	r := &BillingAccountReconciler{Client: c}

	got, err := r.resolveStripeCustomerID(context.Background(), account)
	if err != nil {
		t.Fatalf("resolveStripeCustomerID: %v", err)
	}
	if got != "" {
		t.Errorf("expected empty stripe customer id when gate is false, got %q", got)
	}
}

func TestResolveStripeCustomerID_SucceedsWhenPMActiveAndSPMHasID(t *testing.T) {
	t.Parallel()
	scheme := newStripeTestScheme(t)

	account := &billingv1alpha1.BillingAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "ba-2", Namespace: "default", UID: "uid-2"},
		Spec: billingv1alpha1.BillingAccountSpec{
			CurrencyCode:            "USD",
			DefaultPaymentMethodRef: &billingv1alpha1.DefaultPaymentMethodRef{Name: "pm-ready"},
		},
		Status: billingv1alpha1.BillingAccountStatus{
			Conditions: []metav1.Condition{{
				Type:   billingv1alpha1.BillingAccountConditionDefaultPaymentMethodReady,
				Status: metav1.ConditionTrue,
				Reason: "Ready",
			}},
		},
	}
	pm := &billingv1alpha1.PaymentMethod{
		ObjectMeta: metav1.ObjectMeta{Name: "pm-ready", Namespace: "default"},
		Spec: billingv1alpha1.PaymentMethodSpec{
			BillingAccountRef: billingv1alpha1.BillingAccountRef{Name: "ba-2"},
			DisplayName:       "Card",
		},
		Status: billingv1alpha1.PaymentMethodStatus{Phase: billingv1alpha1.PaymentMethodPhaseActive},
	}
	spm := &stripev1alpha1.StripePaymentMethod{
		ObjectMeta: metav1.ObjectMeta{Name: "pm-ready", Namespace: "default"},
		Spec: stripev1alpha1.StripePaymentMethodSpec{
			PaymentMethodRef: stripev1alpha1.PaymentMethodLocalRef{Name: "pm-ready"},
		},
		Status: stripev1alpha1.StripePaymentMethodStatus{StripeCustomerID: "cus_abc123"},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(account, pm, spm).Build()
	r := &BillingAccountReconciler{Client: c}

	got, err := r.resolveStripeCustomerID(context.Background(), account)
	if err != nil {
		t.Fatalf("resolveStripeCustomerID: %v", err)
	}
	if got != "cus_abc123" {
		t.Errorf("stripe customer id = %q, want cus_abc123", got)
	}
}

func TestResolveStripeCustomerID_SkipsWhenPMNotActive(t *testing.T) {
	t.Parallel()
	scheme := newStripeTestScheme(t)

	account := &billingv1alpha1.BillingAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "ba-3", Namespace: "default", UID: "uid-3"},
		Spec: billingv1alpha1.BillingAccountSpec{
			CurrencyCode:            "USD",
			DefaultPaymentMethodRef: &billingv1alpha1.DefaultPaymentMethodRef{Name: "pm-pending"},
		},
		Status: billingv1alpha1.BillingAccountStatus{
			Conditions: []metav1.Condition{{
				Type:   billingv1alpha1.BillingAccountConditionDefaultPaymentMethodReady,
				Status: metav1.ConditionTrue,
				Reason: "Ready",
			}},
		},
	}
	pm := &billingv1alpha1.PaymentMethod{
		ObjectMeta: metav1.ObjectMeta{Name: "pm-pending", Namespace: "default"},
		Spec: billingv1alpha1.PaymentMethodSpec{
			BillingAccountRef: billingv1alpha1.BillingAccountRef{Name: "ba-3"},
			DisplayName:       "Card",
		},
		Status: billingv1alpha1.PaymentMethodStatus{Phase: billingv1alpha1.PaymentMethodPhasePending},
	}
	spm := &stripev1alpha1.StripePaymentMethod{
		ObjectMeta: metav1.ObjectMeta{Name: "pm-pending", Namespace: "default"},
		Spec: stripev1alpha1.StripePaymentMethodSpec{
			PaymentMethodRef: stripev1alpha1.PaymentMethodLocalRef{Name: "pm-pending"},
		},
		Status: stripev1alpha1.StripePaymentMethodStatus{StripeCustomerID: "cus_ignored"},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(account, pm, spm).Build()
	r := &BillingAccountReconciler{Client: c}

	got, err := r.resolveStripeCustomerID(context.Background(), account)
	if err != nil {
		t.Fatalf("resolveStripeCustomerID: %v", err)
	}
	if got != "" {
		t.Errorf("expected empty id when PM not Active, got %q", got)
	}
}

func TestStripeExtraTraitsForAccount(t *testing.T) {
	t.Parallel()

	if got := stripeExtraTraitsForAccount(""); got != nil {
		t.Errorf("empty stripe id should yield nil traits, got %v", got)
	}

	got := stripeExtraTraitsForAccount("cus_xyz")
	if got[traitStripeID] != "cus_xyz" {
		t.Errorf("stripeid = %q", got[traitStripeID])
	}
	if got[traitPaymentProviderName] != paymentProviderStripe {
		t.Errorf("paymentprovidername = %q", got[traitPaymentProviderName])
	}
	if _, ok := got["payment.stripeIdEffectiveTimeInSeconds"]; ok {
		t.Error("effective-time trait must not be set; use payment-method switch API")
	}
}

func TestPeriodStartUTC(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 2, 15, 8, 0, 0, 0, time.UTC)

	got := periodStartUTC(now, nil)
	want := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("nil terms = %v, want %v", got, want)
	}

	got = periodStartUTC(now, &billingv1alpha1.PaymentTerms{InvoiceDayOfMonth: 31})
	// Feb 15 is still before the Feb 28 clamp of day-31, so the current
	// period started on the previous month's invoice day (Jan 31).
	want = time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("day=31 before this month's clamp = %v, want %v", got, want)
	}

	// Invoice day later this month → current period started last month.
	now = time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	got = periodStartUTC(now, &billingv1alpha1.PaymentTerms{InvoiceDayOfMonth: 20})
	want = time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("future invoice day this month = %v, want %v", got, want)
	}

	// Invoice day already passed this month → this month's day.
	now = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	got = periodStartUTC(now, &billingv1alpha1.PaymentTerms{InvoiceDayOfMonth: 20})
	want = time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("past invoice day this month = %v, want %v", got, want)
	}
}

func TestStripeTraitsFromExisting(t *testing.T) {
	t.Parallel()
	if got := stripeTraitsFromExisting(nil); got != nil {
		t.Errorf("nil traits => nil, got %v", got)
	}
	if got := stripeTraitsFromExisting(map[string]string{"currencyCode": "USD"}); got != nil {
		t.Errorf("no stripeid => nil, got %v", got)
	}
	got := stripeTraitsFromExisting(map[string]string{
		traitStripeID:            "cus_keep",
		traitPaymentProviderName: paymentProviderStripe,
		"currencyCode":           "USD",
	})
	if got[traitStripeID] != "cus_keep" {
		t.Errorf("preserved stripeid = %q", got[traitStripeID])
	}
	if got[traitPaymentProviderName] != paymentProviderStripe {
		t.Errorf("preserved paymentprovidername = %q", got[traitPaymentProviderName])
	}
	if _, ok := got["currencyCode"]; ok {
		t.Error("must not copy non-Stripe traits into ExtraTraits")
	}

	got = stripeTraitsFromExisting(map[string]string{"stripeId": "cus_camel"})
	if got[traitStripeID] != "cus_camel" {
		t.Errorf("stripeId alias = %q", got[traitStripeID])
	}
}

func TestScheduleStripePaymentSwitch_MissingSettingIsPermanent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	account := &billingv1alpha1.BillingAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "ba", Namespace: "ns", UID: types.UID("cust-3")},
	}
	r := &BillingAccountReconciler{AmberfloClient: &stripeSwitchStub{}}
	err := r.scheduleStripePaymentSwitch(context.Background(), logr.Discard(), account, "cust-3", "cus_x", now)
	if !amberflo.IsPermanent(err) {
		t.Fatalf("expected PermanentError, got %v", err)
	}
}

func TestScheduleStripePaymentSwitch_Idempotent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	switchAt := periodStartUTC(now, &billingv1alpha1.PaymentTerms{InvoiceDayOfMonth: 1}).Unix()
	account := &billingv1alpha1.BillingAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "ba", Namespace: "ns", UID: types.UID("cust-1")},
		Spec: billingv1alpha1.BillingAccountSpec{
			PaymentTerms: &billingv1alpha1.PaymentTerms{InvoiceDayOfMonth: 1},
		},
	}

	stub := &stripeSwitchStub{
		settings: []amberflo.PaymentSetting{{
			ID: "ps-stripe", BillingSystem: "Stripe",
		}},
		switches: []amberflo.PaymentMethodSwitch{{
			CustomerID:               "cust-1",
			TargetCustomerIdentifier: "cus_abc",
			SwitchTimeInSeconds:      switchAt,
		}},
	}
	r := &BillingAccountReconciler{AmberfloClient: stub}
	if err := r.scheduleStripePaymentSwitch(context.Background(), logr.Discard(), account, "cust-1", "cus_abc", now); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if stub.scheduleCalls != 0 {
		t.Errorf("expected no Schedule call when switch exists, got %d", stub.scheduleCalls)
	}
}

func TestScheduleStripePaymentSwitch_Creates(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	account := &billingv1alpha1.BillingAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "ba", Namespace: "ns", UID: types.UID("cust-2")},
		Spec: billingv1alpha1.BillingAccountSpec{
			PaymentTerms: &billingv1alpha1.PaymentTerms{InvoiceDayOfMonth: 10},
		},
	}
	stub := &stripeSwitchStub{
		settings: []amberflo.PaymentSetting{{
			ID: "ps-stripe", BillingSystem: "Stripe",
		}},
	}
	r := &BillingAccountReconciler{AmberfloClient: stub}
	if err := r.scheduleStripePaymentSwitch(context.Background(), logr.Discard(), account, "cust-2", "cus_new", now); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if stub.scheduleCalls != 1 {
		t.Fatalf("Schedule calls = %d, want 1", stub.scheduleCalls)
	}
	wantAt := periodStartUTC(now, account.Spec.PaymentTerms).Unix()
	if stub.lastSwitch.SwitchTimeInSeconds != wantAt {
		t.Errorf("switchTimeInSeconds = %d, want %d", stub.lastSwitch.SwitchTimeInSeconds, wantAt)
	}
	if stub.lastSwitch.TargetPaymentID != "ps-stripe" {
		t.Errorf("targetPaymentId = %q", stub.lastSwitch.TargetPaymentID)
	}
	if stub.lastSwitch.TargetCustomerIdentifier != "cus_new" {
		t.Errorf("targetCustomerIdentifier = %q", stub.lastSwitch.TargetCustomerIdentifier)
	}
}

// stripeSwitchStub implements only the Amberflo methods used by
// scheduleStripePaymentSwitch; other Client methods panic if called.
type stripeSwitchStub struct {
	settings      []amberflo.PaymentSetting
	switches      []amberflo.PaymentMethodSwitch
	scheduleCalls int
	lastSwitch    amberflo.PaymentMethodSwitch
	amberflo.Client
}

func (s *stripeSwitchStub) ListPaymentSettings(context.Context) ([]amberflo.PaymentSetting, error) {
	return s.settings, nil
}
func (s *stripeSwitchStub) ListPaymentMethodSwitches(context.Context, string) ([]amberflo.PaymentMethodSwitch, error) {
	return s.switches, nil
}
func (s *stripeSwitchStub) SchedulePaymentMethodSwitch(_ context.Context, sw amberflo.PaymentMethodSwitch) (amberflo.PaymentMethodSwitch, error) {
	s.scheduleCalls++
	s.lastSwitch = sw
	s.switches = append(s.switches, sw)
	return sw, nil
}
