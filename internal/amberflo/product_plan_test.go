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
	"net/http"
	"strings"
	"testing"
)

func flatRate(v float64) DesiredPlanRate {
	return DesiredPlanRate{Flat: &v}
}

func baseDesiredProductPlan() DesiredProductPlan {
	return DesiredProductPlan{
		ID:   "offer-uid-1",
		Name: "Compute Allocated v1",
		Items: []DesiredPlanItem{
			{
				ID:           "cpu-allocated",
				Label:        "CPU Allocated",
				ChargeType:   PlanChargeTypeUsage,
				MeterAPIName: "meter-uid-cpu",
				Rates:        []DesiredPlanRate{flatRate(0.025)},
			},
		},
	}
}

func TestEnsureProductPlan_CreatesFlatUsage(t *testing.T) {
	c, f := newTestClient(t)
	got, err := c.EnsureProductPlan(context.Background(), baseDesiredProductPlan())
	if err != nil {
		t.Fatalf("EnsureProductPlan: %v", err)
	}
	if got.ID != "offer-uid-1" {
		t.Errorf("ID=%q", got.ID)
	}
	if got.Name != "Compute Allocated v1" {
		t.Errorf("Name=%q", got.Name)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.productItems["meter-uid-cpu"]; !ok {
		t.Fatalf("expected product item for meter")
	}
	priceID := productItemPriceID("offer-uid-1", "cpu-allocated")
	price, ok := f.itemPrices[priceID]
	if !ok {
		t.Fatalf("expected product item price %s", priceID)
	}
	var leaf wireLeafNode
	if err := json.Unmarshal(price.Price, &leaf); err != nil {
		t.Fatalf("decode price: %v", err)
	}
	if leaf.Type != priceMachineLeafNode {
		t.Errorf("type=%q", leaf.Type)
	}
	if !leaf.AllowPartialBatch {
		t.Errorf("expected allowPartialBatch")
	}
	if len(leaf.Tiers) != 1 || leaf.Tiers[0].PricePerBatch != 0.025 {
		t.Errorf("tiers=%+v", leaf.Tiers)
	}
	plan, ok := f.productPlans["offer-uid-1"]
	if !ok {
		t.Fatalf("expected product plan stored")
	}
	if plan.ProductItemPriceIdsMap["meter-uid-cpu"] != priceID {
		t.Errorf("price map=%v", plan.ProductItemPriceIdsMap)
	}
}

func TestEnsureProductPlan_TieredGraduated(t *testing.T) {
	c, _ := newTestClient(t)
	desired := DesiredProductPlan{
		ID:   "offer-tiered",
		Name: "Data Transfer",
		Items: []DesiredPlanItem{{
			ID:           "egress",
			ChargeType:   PlanChargeTypeUsage,
			MeterAPIName: "meter-egress",
			Rates: []DesiredPlanRate{{
				Tiers: []DesiredPriceTier{
					{StartAfterUnit: 0, BatchSize: 1, PricePerBatch: 0.12, AllowPartialBatch: true},
					{StartAfterUnit: 100, BatchSize: 1, PricePerBatch: 0.09, AllowPartialBatch: true},
				},
			}},
		}},
	}
	if _, err := c.EnsureProductPlan(context.Background(), desired); err != nil {
		t.Fatalf("EnsureProductPlan: %v", err)
	}
}

func TestEnsureProductPlan_DimensionMatch(t *testing.T) {
	c, f := newTestClient(t)
	desired := DesiredProductPlan{
		ID:   "offer-dim",
		Name: "AI",
		Items: []DesiredPlanItem{{
			ID:           "tokens",
			ChargeType:   PlanChargeTypeUsage,
			MeterAPIName: "meter-tokens",
			Rates: []DesiredPlanRate{
				{Match: &DimensionFilter{Dimension: "model", Value: "sonnet"}, Flat: floatPtr(0.000003)},
				{Match: &DimensionFilter{Dimension: "model", Value: "opus"}, Flat: floatPtr(0.000015)},
			},
		}},
	}
	if _, err := c.EnsureProductPlan(context.Background(), desired); err != nil {
		t.Fatalf("EnsureProductPlan: %v", err)
	}
	priceID := productItemPriceID("offer-dim", "tokens")
	f.mu.Lock()
	price := f.itemPrices[priceID]
	f.mu.Unlock()
	var matrix wireDimensionMatrixNode
	if err := json.Unmarshal(price.Price, &matrix); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if matrix.Type != priceMachineDimMatrix {
		t.Errorf("type=%q", matrix.Type)
	}
	if len(matrix.DimensionKeys) != 1 || matrix.DimensionKeys[0] != "model" {
		t.Errorf("keys=%v", matrix.DimensionKeys)
	}
	if len(matrix.DimensionsPrices) != 2 {
		t.Errorf("dims=%d", len(matrix.DimensionsPrices))
	}
}

func TestEnsureProductPlan_UnmatchedWithMatchRejected(t *testing.T) {
	c, _ := newTestClient(t)
	flat := 0.01
	desired := DesiredProductPlan{
		ID:   "offer-bad-default",
		Name: "Bad",
		Items: []DesiredPlanItem{{
			ID:           "tokens",
			ChargeType:   PlanChargeTypeUsage,
			MeterAPIName: "meter-tokens",
			Rates: []DesiredPlanRate{
				{Match: &DimensionFilter{Dimension: "model", Value: "sonnet"}, Flat: floatPtr(0.000003)},
				{Flat: &flat}, // unmatched catch-all — not representable in Amberflo
			},
		}},
	}
	_, err := c.EnsureProductPlan(context.Background(), desired)
	if err == nil {
		t.Fatal("expected permanent error for unmatched rate alongside Match")
	}
	if !IsPermanent(err) {
		t.Fatalf("want permanent error, got %v", err)
	}
	if !strings.Contains(err.Error(), "unmatched catch-all") || !strings.Contains(err.Error(), "other") {
		t.Fatalf("want catch-all / sentinel guidance in error, got %v", err)
	}
}

func TestEnsureProductPlan_OneTimeAndRecurringFees(t *testing.T) {
	c, f := newTestClient(t)
	desired := DesiredProductPlan{
		ID:   "offer-fees",
		Name: "With Fees",
		Items: []DesiredPlanItem{
			{ID: "setup", Label: "Setup", ChargeType: PlanChargeTypeOneTime, Amount: 10},
			{ID: "platform", Label: "Platform", ChargeType: PlanChargeTypeRecurring, Amount: 5},
		},
	}
	if _, err := c.EnsureProductPlan(context.Background(), desired); err != nil {
		t.Fatalf("EnsureProductPlan: %v", err)
	}
	f.mu.Lock()
	plan := f.productPlans["offer-fees"]
	f.mu.Unlock()
	if plan.FeeMap["setup"].Cost != 10 || !plan.FeeMap["setup"].IsOneTimeFee {
		t.Errorf("setup fee=%+v", plan.FeeMap["setup"])
	}
	if plan.FeeMap["platform"].Cost != 5 || plan.FeeMap["platform"].IsOneTimeFee {
		t.Errorf("platform fee=%+v", plan.FeeMap["platform"])
	}
}

func TestEnsureProductPlan_NoopWhenEqual(t *testing.T) {
	c, f := newTestClient(t)
	desired := baseDesiredProductPlan()
	if _, err := c.EnsureProductPlan(context.Background(), desired); err != nil {
		t.Fatalf("create: %v", err)
	}
	before := len(f.requestsCopy())
	if _, err := c.EnsureProductPlan(context.Background(), desired); err != nil {
		t.Fatalf("second: %v", err)
	}
	after := f.requestsCopy()
	// Second call: GET plan + GET item + GET price; no POST plan.
	posts := 0
	for _, r := range after[before:] {
		if r.Method == http.MethodPost && r.Path == productPlansPath {
			posts++
		}
	}
	if posts != 0 {
		t.Errorf("expected no plan POST on noop, got %d", posts)
	}
}

func TestDeleteProductPlan_ToleratesNotFound(t *testing.T) {
	c, _ := newTestClient(t)
	if err := c.DeleteProductPlan(context.Background(), "missing"); err != nil {
		t.Fatalf("DeleteProductPlan: %v", err)
	}
}

func TestDeleteProductPlan_RemovesExisting(t *testing.T) {
	c, f := newTestClient(t)
	if _, err := c.EnsureProductPlan(context.Background(), baseDesiredProductPlan()); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.DeleteProductPlan(context.Background(), "offer-uid-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	f.mu.Lock()
	_, ok := f.productPlans["offer-uid-1"]
	f.mu.Unlock()
	if ok {
		t.Fatal("plan still present")
	}
	counts := methodCounts(f.requestsCopy())
	if counts[http.MethodDelete] != 1 {
		t.Errorf("expected 1 DELETE, got %#v", counts)
	}
}

func TestEnsureProductPlan_EmptyIDPermanent(t *testing.T) {
	c, _ := newTestClient(t)
	_, err := c.EnsureProductPlan(context.Background(), DesiredProductPlan{})
	if !IsPermanent(err) {
		t.Fatalf("expected permanent, got %v", err)
	}
}

func floatPtr(v float64) *float64 { return &v }
