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

import "testing"

func TestFindPaymentSettingBySystem(t *testing.T) {
	t.Parallel()
	settings := []PaymentSetting{
		{ID: "ps-aws", BillingSystem: "AWSMarketplace"},
		{ID: "ps-stripe", BillingSystem: "Stripe"},
	}

	got, ok := FindPaymentSettingBySystem(settings, BillingSystemStripe, "")
	if !ok || got.ID != "ps-stripe" {
		t.Fatalf("FindPaymentSettingBySystem Stripe = %+v ok=%v", got, ok)
	}

	got, ok = FindPaymentSettingBySystem(settings, BillingSystemStripe, "ps-aws")
	if !ok || got.ID != "ps-stripe" {
		t.Fatalf("preferID for non-Stripe setting must be ignored: got %+v ok=%v", got, ok)
	}

	got, ok = FindPaymentSettingBySystem(settings, BillingSystemStripe, "ps-stripe")
	if !ok || got.ID != "ps-stripe" {
		t.Fatalf("preferID for Stripe setting: got %+v ok=%v", got, ok)
	}

	_, ok = FindPaymentSettingBySystem(settings, "PayPal", "")
	if ok {
		t.Fatal("expected no match for PayPal")
	}
}

func TestHasMatchingPaymentMethodSwitch(t *testing.T) {
	t.Parallel()
	switches := []PaymentMethodSwitch{{
		TargetCustomerIdentifier: "cus_a",
		SwitchTimeInSeconds:      100,
	}}
	if !HasMatchingPaymentMethodSwitch(switches, "cus_a", 100) {
		t.Fatal("expected match")
	}
	if HasMatchingPaymentMethodSwitch(switches, "cus_a", 101) {
		t.Fatal("different time should not match")
	}
	if HasMatchingPaymentMethodSwitch(switches, "cus_b", 100) {
		t.Fatal("different customer should not match")
	}
}
