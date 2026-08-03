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
	"net/url"
	"time"
)

const (
	customerPricingPath     = "/payments/pricing/amberflo/customer-pricing"
	customerPricingListPath = "/payments/pricing/amberflo/customer-pricing/list"
)

// DesiredCustomerPlan is the controller-facing representation of an
// Amberflo customer↔product-plan assignment.
type DesiredCustomerPlan struct {
	// CustomerID is the Amberflo customer id (= BillingAccount.UID).
	CustomerID string
	// ProductPlanID is the Amberflo product plan id (= Offer.UID).
	ProductPlanID string
	// ProductID defaults to "1".
	ProductID string
	// StartTime is when the assignment becomes effective. Zero means now.
	StartTime time.Time
}

// CustomerPlan is the provider-facing view of a customer-pricing relation.
type CustomerPlan struct {
	CustomerID         string
	ProductPlanID      string
	ProductID          string
	RelationID         string
	StartTimeInSeconds int64
	EndTimeInSeconds   int64
	Raw                json.RawMessage
}

// Active reports whether the assignment has no end time (or end is in the future).
func (cp CustomerPlan) Active(now time.Time) bool {
	if cp.EndTimeInSeconds <= 0 {
		return true
	}
	return time.Unix(cp.EndTimeInSeconds, 0).After(now)
}

// wireCustomerProductPlan mirrors Amberflo's customer-pricing payload
// (matches github.com/amberflo/metering-go CustomerProductPlan).
type wireCustomerProductPlan struct {
	ProductID            string `json:"productId"`
	ProductPlanID        string `json:"productPlanId"`
	CustomerID           string `json:"customerId"`
	StartTimeInSeconds   int64  `json:"startTimeInSeconds"`
	EndTimeInSeconds     int64  `json:"endTimeInSeconds,omitempty"`
	RelationID           string `json:"relationId,omitempty"`
	CreatedTimeInSeconds int64  `json:"createdTimeInSeconds,omitempty"`
}

// ListCustomerPlans returns every customer-pricing assignment for customerID.
func (c *client) ListCustomerPlans(ctx context.Context, customerID string) ([]CustomerPlan, error) {
	if customerID == "" {
		return nil, &PermanentError{Err: errors.New("customerID is required")}
	}
	path := customerPricingListPath + "?customerId=" + url.QueryEscape(customerID)
	var wire []wireCustomerProductPlan
	_, body, err := c.doJSON(ctx, http.MethodGet, path, nil, &wire)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 || string(body) == "null" {
		return nil, nil
	}
	out := make([]CustomerPlan, 0, len(wire))
	for _, w := range wire {
		out = append(out, customerPlanFromWire(w, nil))
	}
	return out, nil
}

// EnsureCustomerPlan assigns the customer to the product plan. When the
// customer already has an active assignment to a different plan, that
// assignment is cancelled first (swap).
func (c *client) EnsureCustomerPlan(ctx context.Context, desired DesiredCustomerPlan) (CustomerPlan, error) {
	if desired.CustomerID == "" {
		return CustomerPlan{}, &PermanentError{Err: errors.New("DesiredCustomerPlan.CustomerID is required")}
	}
	if desired.ProductPlanID == "" {
		return CustomerPlan{}, &PermanentError{Err: errors.New("DesiredCustomerPlan.ProductPlanID is required")}
	}
	productID := desired.ProductID
	if productID == "" {
		productID = defaultProductPlanPID
	}
	now := c.now().UTC()
	start := desired.StartTime
	if start.IsZero() {
		start = now
	}

	existing, err := c.ListCustomerPlans(ctx, desired.CustomerID)
	if err != nil {
		return CustomerPlan{}, err
	}

	var activeMatch *CustomerPlan
	for i := range existing {
		cp := &existing[i]
		if !cp.Active(now) {
			continue
		}
		if cp.ProductPlanID == desired.ProductPlanID {
			activeMatch = cp
			continue
		}
		// Cancel stray active assignments so the customer has exactly
		// one active product plan (mirrors Milo's one-BE-per-BA rule).
		if err := c.CancelCustomerPlan(ctx, desired.CustomerID, cp.ProductPlanID); err != nil {
			return CustomerPlan{}, err
		}
	}
	if activeMatch != nil {
		return *activeMatch, nil
	}

	payload := wireCustomerProductPlan{
		ProductID:          productID,
		ProductPlanID:      desired.ProductPlanID,
		CustomerID:         desired.CustomerID,
		StartTimeInSeconds: start.Unix(),
	}
	var got wireCustomerProductPlan
	_, body, err := c.doJSON(ctx, http.MethodPost, customerPricingPath, payload, &got)
	if err != nil {
		return CustomerPlan{}, err
	}
	if got.CustomerID == "" {
		got = payload
	}
	return customerPlanFromWire(got, body), nil
}

// CancelCustomerPlan ends the customer's assignment to productPlanID by
// posting an update with endTimeInSeconds=now. Missing assignments succeed.
func (c *client) CancelCustomerPlan(ctx context.Context, customerID, productPlanID string) error {
	if customerID == "" {
		return &PermanentError{Err: errors.New("customerID is required")}
	}
	if productPlanID == "" {
		return &PermanentError{Err: errors.New("productPlanID is required")}
	}

	now := c.now().UTC()
	existing, err := c.ListCustomerPlans(ctx, customerID)
	if err != nil {
		return err
	}

	found := false
	for _, cp := range existing {
		if cp.ProductPlanID != productPlanID {
			continue
		}
		if !cp.Active(now) {
			found = true
			continue
		}
		found = true
		productID := cp.ProductID
		if productID == "" {
			productID = defaultProductPlanPID
		}
		start := cp.StartTimeInSeconds
		if start <= 0 {
			start = now.Unix()
		}
		payload := wireCustomerProductPlan{
			ProductID:          productID,
			ProductPlanID:      productPlanID,
			CustomerID:         customerID,
			StartTimeInSeconds: start,
			EndTimeInSeconds:   now.Unix(),
			RelationID:         cp.RelationID,
		}
		_, _, err := c.doJSON(ctx, http.MethodPost, customerPricingPath, payload, nil)
		if err != nil {
			return err
		}
	}
	if !found {
		// Desired end state is "no active assignment"; already true.
		return nil
	}
	return nil
}

func customerPlanFromWire(w wireCustomerProductPlan, raw []byte) CustomerPlan {
	var rawCopy json.RawMessage
	if len(raw) > 0 {
		rawCopy = append(json.RawMessage(nil), raw...)
	}
	return CustomerPlan{
		CustomerID:         w.CustomerID,
		ProductPlanID:      w.ProductPlanID,
		ProductID:          w.ProductID,
		RelationID:         w.RelationID,
		StartTimeInSeconds: w.StartTimeInSeconds,
		EndTimeInSeconds:   w.EndTimeInSeconds,
		Raw:                rawCopy,
	}
}
