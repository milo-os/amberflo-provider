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
	"net/http"
	"testing"
	"time"
)

func TestEnsureCustomerPlan_CreatesWhenAbsent(t *testing.T) {
	c, f := newTestClient(t, func(co *ClientOptions) {
		co.now = func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }
	})
	got, err := c.EnsureCustomerPlan(context.Background(), DesiredCustomerPlan{
		CustomerID:    "ba-uid",
		ProductPlanID: "offer-uid",
	})
	if err != nil {
		t.Fatalf("EnsureCustomerPlan: %v", err)
	}
	if got.CustomerID != "ba-uid" || got.ProductPlanID != "offer-uid" {
		t.Errorf("got=%+v", got)
	}
	if got.StartTimeInSeconds != 1_700_000_000 {
		t.Errorf("start=%d", got.StartTimeInSeconds)
	}
	counts := methodCounts(f.requestsCopy())
	if counts[http.MethodGet] < 1 || counts[http.MethodPost] < 1 {
		t.Errorf("expected GET+POST, got %v", counts)
	}
}

func TestEnsureCustomerPlan_NoopWhenActive(t *testing.T) {
	c, f := newTestClient(t, func(co *ClientOptions) {
		co.now = func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }
	})
	desired := DesiredCustomerPlan{CustomerID: "ba-uid", ProductPlanID: "offer-uid"}
	if _, err := c.EnsureCustomerPlan(context.Background(), desired); err != nil {
		t.Fatalf("create: %v", err)
	}
	before := len(f.requestsCopy())
	if _, err := c.EnsureCustomerPlan(context.Background(), desired); err != nil {
		t.Fatalf("second: %v", err)
	}
	posts := 0
	for _, r := range f.requestsCopy()[before:] {
		if r.Method == http.MethodPost && r.Path == customerPricingPath {
			posts++
		}
	}
	if posts != 0 {
		t.Errorf("expected no assign POST on noop, got %d", posts)
	}
}

func TestEnsureCustomerPlan_SwapsOnPlanChange(t *testing.T) {
	c, f := newTestClient(t, func(co *ClientOptions) {
		co.now = func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }
	})
	if _, err := c.EnsureCustomerPlan(context.Background(), DesiredCustomerPlan{
		CustomerID: "ba-uid", ProductPlanID: "offer-a",
	}); err != nil {
		t.Fatalf("assign a: %v", err)
	}
	if _, err := c.EnsureCustomerPlan(context.Background(), DesiredCustomerPlan{
		CustomerID: "ba-uid", ProductPlanID: "offer-b",
	}); err != nil {
		t.Fatalf("assign b: %v", err)
	}
	f.mu.Lock()
	plans := f.customerPlans["ba-uid"]
	f.mu.Unlock()
	var activeB, endedA bool
	for _, p := range plans {
		switch p.ProductPlanID {
		case "offer-a":
			if p.EndTimeInSeconds > 0 {
				endedA = true
			}
		case "offer-b":
			if p.EndTimeInSeconds == 0 {
				activeB = true
			}
		}
	}
	if !endedA || !activeB {
		t.Fatalf("swap incomplete: plans=%+v endedA=%v activeB=%v", plans, endedA, activeB)
	}
}

func TestCancelCustomerPlan_EndsAssignment(t *testing.T) {
	c, f := newTestClient(t, func(co *ClientOptions) {
		co.now = func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }
	})
	if _, err := c.EnsureCustomerPlan(context.Background(), DesiredCustomerPlan{
		CustomerID: "ba-uid", ProductPlanID: "offer-uid",
	}); err != nil {
		t.Fatalf("assign: %v", err)
	}
	if err := c.CancelCustomerPlan(context.Background(), "ba-uid", "offer-uid"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	f.mu.Lock()
	plans := f.customerPlans["ba-uid"]
	f.mu.Unlock()
	if len(plans) != 1 || plans[0].EndTimeInSeconds != 1_700_000_000 {
		t.Fatalf("expected ended plan, got %+v", plans)
	}
}

func TestCancelCustomerPlan_ToleratesMissing(t *testing.T) {
	c, _ := newTestClient(t)
	if err := c.CancelCustomerPlan(context.Background(), "ba-uid", "missing"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
}

func TestEnsureCustomerPlan_EmptyIDsPermanent(t *testing.T) {
	c, _ := newTestClient(t)
	if _, err := c.EnsureCustomerPlan(context.Background(), DesiredCustomerPlan{}); !IsPermanent(err) {
		t.Fatalf("expected permanent, got %v", err)
	}
}
