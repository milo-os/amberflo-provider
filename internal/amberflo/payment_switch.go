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
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// BillingSystemStripe is the Amberflo billingSystem / payment-type value for
// Stripe. ListPaymentSettings returns the account's canonical casing; callers
// should prefer the value from that list when scheduling a switch.
const BillingSystemStripe = "Stripe"

// PaymentSetting is one Amberflo payment-provider connection from
// GET /payments/billing-settings/list.
type PaymentSetting struct {
	ID            string `json:"id"`
	Name          string `json:"name,omitempty"`
	BillingSystem string `json:"billingSystem,omitempty"`
	AccountID     string `json:"accountId,omitempty"`
}

// PaymentMethodSwitch schedules (or describes) a customer payment-provider
// transition. See Amberflo POST /customers/payment-method/switch.
type PaymentMethodSwitch struct {
	CustomerID               string `json:"customerId"`
	SourcePaymentType        string `json:"sourcePaymentType,omitempty"`
	SourcePaymentID          string `json:"sourcePaymentId,omitempty"`
	TargetPaymentType        string `json:"targetPaymentType"`
	TargetPaymentID          string `json:"targetPaymentId"`
	TargetCustomerIdentifier string `json:"targetCustomerIdentifier"`
	SwitchTimeInSeconds      int64  `json:"switchTimeInSeconds"`
	CreatedTimeInSeconds     int64  `json:"createdTimeInSeconds,omitempty"`
	UpdatedTimeInSeconds     int64  `json:"updatedTimeInSeconds,omitempty"`
}

// ListPaymentSettings returns the Amberflo account's configured payment
// provider connections (Stripe, AWS Marketplace, …).
func (c *client) ListPaymentSettings(ctx context.Context) ([]PaymentSetting, error) {
	var out []PaymentSetting
	_, body, err := c.doJSON(ctx, http.MethodGet, "/payments/billing-settings/list", nil, &out)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 || string(body) == "null" {
		return nil, nil
	}
	return out, nil
}

// FindPaymentSettingBySystem returns the first payment setting whose
// billingSystem matches want (case-insensitive). When preferID is non-empty
// and present in the list with a matching billingSystem, that setting is
// returned instead. A preferID for a different billingSystem is ignored.
func FindPaymentSettingBySystem(settings []PaymentSetting, want, preferID string) (PaymentSetting, bool) {
	want = strings.TrimSpace(want)
	matchesSystem := func(s PaymentSetting) bool {
		if want == "" {
			return false
		}
		if strings.EqualFold(s.BillingSystem, want) {
			return true
		}
		return strings.Contains(strings.ToLower(s.BillingSystem), strings.ToLower(want))
	}

	if preferID != "" {
		for i := range settings {
			if settings[i].ID == preferID && matchesSystem(settings[i]) {
				return settings[i], true
			}
		}
	}
	for i := range settings {
		if matchesSystem(settings[i]) {
			return settings[i], true
		}
	}
	return PaymentSetting{}, false
}

// ListPaymentMethodSwitches returns scheduled/completed payment-method
// switches for customerID.
func (c *client) ListPaymentMethodSwitches(ctx context.Context, customerID string) ([]PaymentMethodSwitch, error) {
	if customerID == "" {
		return nil, &PermanentError{Err: errors.New("customerID is required")}
	}
	q := url.Values{}
	q.Set("customerId", customerID)
	path := "/customers/payment-method/switch?" + q.Encode()
	var out []PaymentMethodSwitch
	_, body, err := c.doJSON(ctx, http.MethodGet, path, nil, &out)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 || string(body) == "null" {
		return nil, nil
	}
	return out, nil
}

// SchedulePaymentMethodSwitch creates a payment-provider switch scheduled
// at switch.SwitchTimeInSeconds (must align to a billing-period boundary
// Amberflo can evaluate against invoice start times).
func (c *client) SchedulePaymentMethodSwitch(ctx context.Context, sw PaymentMethodSwitch) (PaymentMethodSwitch, error) {
	if sw.CustomerID == "" {
		return PaymentMethodSwitch{}, &PermanentError{Err: errors.New("PaymentMethodSwitch.CustomerID is required")}
	}
	if sw.TargetPaymentType == "" || sw.TargetPaymentID == "" {
		return PaymentMethodSwitch{}, &PermanentError{Err: errors.New("PaymentMethodSwitch targetPaymentType and targetPaymentId are required")}
	}
	if sw.TargetCustomerIdentifier == "" {
		return PaymentMethodSwitch{}, &PermanentError{Err: errors.New("PaymentMethodSwitch.targetCustomerIdentifier is required")}
	}
	if sw.SwitchTimeInSeconds <= 0 {
		return PaymentMethodSwitch{}, &PermanentError{Err: errors.New("PaymentMethodSwitch.switchTimeInSeconds is required")}
	}

	var got PaymentMethodSwitch
	_, body, err := c.doJSON(ctx, http.MethodPost, "/customers/payment-method/switch", sw, &got)
	if err != nil {
		return PaymentMethodSwitch{}, err
	}
	if got.CustomerID == "" {
		got = sw
	}
	_ = body
	return got, nil
}

// HasMatchingPaymentMethodSwitch reports whether switches already contains
// a switch to the same Stripe customer id at the same switch time.
func HasMatchingPaymentMethodSwitch(switches []PaymentMethodSwitch, targetCustomerID string, switchTime int64) bool {
	for i := range switches {
		s := &switches[i]
		if s.TargetCustomerIdentifier == targetCustomerID && s.SwitchTimeInSeconds == switchTime {
			return true
		}
	}
	return false
}

// FormatPaymentMethodSwitch is a short debug label for logs/events.
func FormatPaymentMethodSwitch(sw PaymentMethodSwitch) string {
	return fmt.Sprintf("customer=%s target=%s/%s stripeCustomer=%s at=%d",
		sw.CustomerID, sw.TargetPaymentType, sw.TargetPaymentID, sw.TargetCustomerIdentifier, sw.SwitchTimeInSeconds)
}
