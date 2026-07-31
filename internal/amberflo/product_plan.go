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
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
)

// Amberflo account-pricing paths used by the product-plan client.
const (
	productItemsPath      = "/payments/pricing/amberflo/account-pricing/product-items"
	productItemPricePath  = "/payments/pricing/amberflo/account-pricing/product-item-price"
	productPlansPath      = "/payments/pricing/amberflo/account-pricing/product-plans"
	defaultProductPlanPID = "1"
	priceMachineLeafNode  = "LeafNode"
	priceMachineDimMatrix = "DimensionMatrixNode"
	lockingStatusClose    = "close_to_changes"
)

// PlanChargeType distinguishes Usage vs fixed plan items.
type PlanChargeType string

const (
	PlanChargeTypeUsage     PlanChargeType = "usage"
	PlanChargeTypeOneTime   PlanChargeType = "one_time"
	PlanChargeTypeRecurring PlanChargeType = "recurring"
)

// DesiredProductPlan is the controller-facing representation of an
// Amberflo Product Plan the Offer reconciler wants to exist.
type DesiredProductPlan struct {
	// ID is the Amberflo product plan id. For amberflo-provider this is
	// always string(Offer.UID).
	ID string
	// Name is the human-readable productPlanName.
	Name string
	// Description is optional plan description text.
	Description string
	// ProductID defaults to "1" (Amberflo's built-in default product).
	ProductID string
	// Currency defaults to "USD".
	Currency string
	// Items are product-item prices (Usage) and plan fees (OneTime /
	// Recurring) derived from Offer.spec.servicePricings.
	Items []DesiredPlanItem
}

// DesiredPlanItem is one Usage price or fixed fee on a product plan.
type DesiredPlanItem struct {
	// ID is a stable identifier within the plan (ServicePricing snapshot
	// name). Used to build Amberflo product-item-price and fee ids.
	ID string
	// Label is the human-readable display name.
	Label string
	// ChargeType selects the Amberflo encoding (usage price vs fee).
	ChargeType PlanChargeType

	// MeterAPIName is the Amberflo meterApiName for Usage items
	// (= string(MeterDefinition.UID)). Required when ChargeType=usage.
	MeterAPIName string
	// Rates are Usage rate entries. Exactly one of Flat or Tiers is set
	// per entry; Match optionally scopes the rate to a dimension value.
	Rates []DesiredPlanRate

	// Amount is the fixed USD amount for OneTime / Recurring fees.
	Amount float64
}

// DesiredPlanRate is a single Usage rate (flat or graduated tiers).
type DesiredPlanRate struct {
	// Match optionally restricts this rate to a dimension value.
	Match *DimensionFilter
	// Flat is a single per-unit price. Mutually exclusive with Tiers.
	Flat *float64
	// Tiers are graduated LeafNode tiers. Mutually exclusive with Flat.
	Tiers []DesiredPriceTier
}

// DimensionFilter selects a rate by meter dimension value.
type DimensionFilter struct {
	Dimension string
	Value     string
}

// DesiredPriceTier is one graduated LeafNode tier.
type DesiredPriceTier struct {
	StartAfterUnit    int64
	BatchSize         int64
	PricePerBatch     float64
	AllowPartialBatch bool
}

// ProductPlan is the provider-facing view of an Amberflo product plan.
type ProductPlan struct {
	ID          string
	Name        string
	ProductID   string
	Currency    string
	Description string
	Raw         json.RawMessage
}

// wireProductItem mirrors Amberflo's product-item payload.
type wireProductItem struct {
	ID           string `json:"id"`
	Name         string `json:"name,omitempty"`
	MeterAPIName string `json:"meterApiName,omitempty"`
	ProductID    string `json:"productId,omitempty"`
}

// wirePriceTier is one LeafNode tier on the wire.
type wirePriceTier struct {
	StartAfterUnit int64   `json:"startAfterUnit"`
	BatchSize      int64   `json:"batchSize"`
	PricePerBatch  float64 `json:"pricePerBatch"`
}

// wireLeafNode is an Amberflo LeafNode price machine.
type wireLeafNode struct {
	Type              string          `json:"type"`
	AllowPartialBatch bool            `json:"allowPartialBatch"`
	Tiers             []wirePriceTier `json:"tiers"`
}

// wireDimensionPrice is one DimensionMatrixNode entry.
type wireDimensionPrice struct {
	DimensionValues []string     `json:"dimensionValues"`
	LeafNode        wireLeafNode `json:"leafNode"`
}

// wireDimensionMatrixNode is an Amberflo DimensionMatrixNode price machine.
type wireDimensionMatrixNode struct {
	Type             string               `json:"type"`
	DimensionKeys    []string             `json:"dimensionKeys"`
	DimensionsPrices []wireDimensionPrice `json:"dimensionsPrices"`
}

// wireProductItemPrice mirrors Amberflo's product-item-price payload.
type wireProductItemPrice struct {
	ID                   string          `json:"id,omitempty"`
	ProductItemID        string          `json:"productItemId"`
	ProductItemPriceName string          `json:"productItemPriceName,omitempty"`
	Price                json.RawMessage `json:"price"`
	LockingStatus        string          `json:"lockingStatus,omitempty"`
}

// wirePlanFee mirrors a ProductPlan feeMap entry for OneTime / Recurring.
type wirePlanFee struct {
	Name         string  `json:"name,omitempty"`
	Cost         float64 `json:"cost"`
	IsOneTimeFee bool    `json:"isOneTimeFee"`
}

// wireBillingPeriod is the plan billing cadence.
type wireBillingPeriod struct {
	Interval       string `json:"interval"`
	IntervalsCount int    `json:"intervalsCount"`
}

// wireProductPlan mirrors Amberflo's product-plan payload.
type wireProductPlan struct {
	ID                     string            `json:"id,omitempty"`
	ProductID              string            `json:"productId,omitempty"`
	ProductPlanName        string            `json:"productPlanName,omitempty"`
	Description            string            `json:"description,omitempty"`
	BillingPeriod          wireBillingPeriod `json:"billingPeriod"`
	ProductItemPriceIdsMap map[string]string `json:"productItemPriceIdsMap,omitempty"`
	FeeMap                 map[string]wirePlanFee `json:"feeMap,omitempty"`
	LockingStatus          string            `json:"lockingStatus,omitempty"`
	PlanCurrency           string            `json:"planCurrency,omitempty"`
}

// GetProductPlan fetches a product plan by id.
func (c *client) GetProductPlan(ctx context.Context, id string) (ProductPlan, error) {
	if id == "" {
		return ProductPlan{}, &PermanentError{Err: errors.New("product plan id is required")}
	}
	path := productPlansPath + "/" + url.PathEscape(id)
	var wp wireProductPlan
	_, body, err := c.doJSON(ctx, http.MethodGet, path, nil, &wp)
	if err != nil {
		var perm *PermanentError
		if errors.As(err, &perm) && perm.StatusCode == http.StatusNotFound {
			return ProductPlan{}, fmt.Errorf("%w: %s", ErrProductPlanNotFound, id)
		}
		return ProductPlan{}, err
	}
	if wp.ID == "" {
		return ProductPlan{}, fmt.Errorf("%w: %s", ErrProductPlanNotFound, id)
	}
	return productPlanFromWire(wp, body), nil
}

// EnsureProductPlan creates or updates the product plan so Amberflo matches
// DesiredProductPlan. Usage items become product items + item prices;
// OneTime/Recurring items become feeMap entries.
func (c *client) EnsureProductPlan(ctx context.Context, desired DesiredProductPlan) (ProductPlan, error) {
	if desired.ID == "" {
		return ProductPlan{}, &PermanentError{Err: errors.New("DesiredProductPlan.ID is required")}
	}
	if desired.Name == "" {
		desired.Name = desired.ID
	}
	productID := desired.ProductID
	if productID == "" {
		productID = defaultProductPlanPID
	}
	currency := desired.Currency
	if currency == "" {
		currency = "USD"
	}

	priceIDs := map[string]string{}
	feeMap := map[string]wirePlanFee{}

	for i := range desired.Items {
		item := &desired.Items[i]
		if item.ID == "" {
			return ProductPlan{}, &PermanentError{Err: fmt.Errorf("DesiredPlanItem[%d].ID is required", i)}
		}
		switch item.ChargeType {
		case PlanChargeTypeUsage:
			if item.MeterAPIName == "" {
				return ProductPlan{}, &PermanentError{Err: fmt.Errorf("DesiredPlanItem %q: MeterAPIName is required for usage", item.ID)}
			}
			if err := c.ensureProductItem(ctx, wireProductItem{
				ID:           item.MeterAPIName,
				Name:         firstNonEmpty(item.Label, item.MeterAPIName),
				MeterAPIName: item.MeterAPIName,
				ProductID:    productID,
			}); err != nil {
				return ProductPlan{}, err
			}
			priceID := productItemPriceID(desired.ID, item.ID)
			price, err := buildWirePrice(item.Rates)
			if err != nil {
				return ProductPlan{}, &PermanentError{Err: fmt.Errorf("DesiredPlanItem %q: %w", item.ID, err)}
			}
			priceRaw, err := json.Marshal(price)
			if err != nil {
				return ProductPlan{}, &PermanentError{Err: fmt.Errorf("encode price machine: %w", err)}
			}
			if err := c.ensureProductItemPrice(ctx, wireProductItemPrice{
				ID:                   priceID,
				ProductItemID:        item.MeterAPIName,
				ProductItemPriceName: firstNonEmpty(item.Label, item.ID),
				Price:                priceRaw,
				LockingStatus:        lockingStatusClose,
			}); err != nil {
				return ProductPlan{}, err
			}
			priceIDs[item.MeterAPIName] = priceID
		case PlanChargeTypeOneTime, PlanChargeTypeRecurring:
			feeMap[item.ID] = wirePlanFee{
				Name:         firstNonEmpty(item.Label, item.ID),
				Cost:         item.Amount,
				IsOneTimeFee: item.ChargeType == PlanChargeTypeOneTime,
			}
		default:
			return ProductPlan{}, &PermanentError{Err: fmt.Errorf("DesiredPlanItem %q: unsupported ChargeType %q", item.ID, item.ChargeType)}
		}
	}

	want := wireProductPlan{
		ID:              desired.ID,
		ProductID:       productID,
		ProductPlanName: desired.Name,
		Description:     desired.Description,
		BillingPeriod: wireBillingPeriod{
			Interval:       "month",
			IntervalsCount: 1,
		},
		ProductItemPriceIdsMap: priceIDs,
		FeeMap:                 feeMap,
		LockingStatus:          lockingStatusClose,
		PlanCurrency:           currency,
	}

	existing, err := c.GetProductPlan(ctx, desired.ID)
	switch {
	case errors.Is(err, ErrProductPlanNotFound):
		return c.putProductPlan(ctx, http.MethodPost, want)
	case err != nil:
		return ProductPlan{}, err
	}

	var existingWire wireProductPlan
	_ = json.Unmarshal(existing.Raw, &existingWire)
	if !productPlanNeedsUpdate(existingWire, want) {
		return existing, nil
	}
	return c.putProductPlan(ctx, http.MethodPost, want)
}

// DeleteProductPlan removes a product plan by id. 404 is success.
func (c *client) DeleteProductPlan(ctx context.Context, id string) error {
	if id == "" {
		return &PermanentError{Err: errors.New("product plan id is required")}
	}
	path := productPlansPath + "/" + url.PathEscape(id)
	_, _, err := c.doJSON(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		var perm *PermanentError
		if errors.As(err, &perm) && perm.StatusCode == http.StatusNotFound {
			return nil
		}
		return err
	}
	return nil
}

func (c *client) ensureProductItem(ctx context.Context, item wireProductItem) error {
	path := productItemsPath + "/" + url.PathEscape(item.ID)
	_, _, err := c.doJSON(ctx, http.MethodGet, path, nil, &wireProductItem{})
	if err == nil {
		return nil
	}
	var perm *PermanentError
	if !errors.As(err, &perm) || perm.StatusCode != http.StatusNotFound {
		return err
	}
	_, _, err = c.doJSON(ctx, http.MethodPost, productItemsPath, item, nil)
	return err
}

func (c *client) ensureProductItemPrice(ctx context.Context, price wireProductItemPrice) error {
	path := productItemPricePath + "/" + url.PathEscape(price.ID)
	var existing wireProductItemPrice
	_, _, err := c.doJSON(ctx, http.MethodGet, path, nil, &existing)
	switch err {
	case nil:
		if existing.ProductItemID == price.ProductItemID &&
			existing.ProductItemPriceName == price.ProductItemPriceName &&
			jsonEqual(existing.Price, price.Price) {
			return nil
		}
		_, _, err = c.doJSON(ctx, http.MethodPost, productItemPricePath, price, nil)
		return err
	default:
		var perm *PermanentError
		if errors.As(err, &perm) && perm.StatusCode == http.StatusNotFound {
			_, _, err = c.doJSON(ctx, http.MethodPost, productItemPricePath, price, nil)
			return err
		}
		return err
	}
}

func (c *client) putProductPlan(ctx context.Context, method string, wp wireProductPlan) (ProductPlan, error) {
	var got wireProductPlan
	_, body, err := c.doJSON(ctx, method, productPlansPath, wp, &got)
	if err != nil {
		return ProductPlan{}, err
	}
	if got.ID == "" {
		got = wp
	}
	return productPlanFromWire(got, body), nil
}

func productPlanFromWire(wp wireProductPlan, raw []byte) ProductPlan {
	var rawCopy json.RawMessage
	if len(raw) > 0 {
		rawCopy = append(json.RawMessage(nil), raw...)
	}
	return ProductPlan{
		ID:          wp.ID,
		Name:        wp.ProductPlanName,
		ProductID:   wp.ProductID,
		Currency:    wp.PlanCurrency,
		Description: wp.Description,
		Raw:         rawCopy,
	}
}

func productPlanNeedsUpdate(existing, want wireProductPlan) bool {
	if existing.ProductPlanName != want.ProductPlanName {
		return true
	}
	if existing.Description != want.Description {
		return true
	}
	if existing.PlanCurrency != want.PlanCurrency && want.PlanCurrency != "" {
		return true
	}
	if existing.LockingStatus != want.LockingStatus && want.LockingStatus != "" {
		return true
	}
	if !reflect.DeepEqual(normalizeStringMap(existing.ProductItemPriceIdsMap), normalizeStringMap(want.ProductItemPriceIdsMap)) {
		return true
	}
	return !reflect.DeepEqual(normalizeFeeMap(existing.FeeMap), normalizeFeeMap(want.FeeMap))
}

func normalizeStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func normalizeFeeMap(in map[string]wirePlanFee) map[string]wirePlanFee {
	if len(in) == 0 {
		return map[string]wirePlanFee{}
	}
	out := make(map[string]wirePlanFee, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// buildWirePrice renders DesiredPlanRate entries into an Amberflo price
// machine. Rates with Match become a DimensionMatrixNode; otherwise a
// single LeafNode. Graduated tiers use startAfterUnit/batchSize/pricePerBatch.
func buildWirePrice(rates []DesiredPlanRate) (any, error) {
	if len(rates) == 0 {
		return nil, errors.New("at least one rate is required")
	}

	hasMatch := false
	for _, r := range rates {
		if r.Match != nil {
			hasMatch = true
			break
		}
	}
	if !hasMatch {
		if len(rates) != 1 {
			return nil, errors.New("multiple unmatched rates are not supported; use Match filters")
		}
		leaf, err := leafFromRate(rates[0])
		if err != nil {
			return nil, err
		}
		return leaf, nil
	}

	dimKey := ""
	dims := make([]wireDimensionPrice, 0, len(rates))
	for _, r := range rates {
		if r.Match == nil {
			// Default catch-all: Amberflo DimensionMatrixNode drops unmatched
			// usage, so we skip encoding an unmatched default here. Callers
			// that need a default should emit an explicit Match.
			continue
		}
		if dimKey == "" {
			dimKey = r.Match.Dimension
		} else if dimKey != r.Match.Dimension {
			return nil, fmt.Errorf("mixed match dimensions %q and %q are not supported", dimKey, r.Match.Dimension)
		}
		leaf, err := leafFromRate(r)
		if err != nil {
			return nil, err
		}
		dims = append(dims, wireDimensionPrice{
			DimensionValues: []string{r.Match.Value},
			LeafNode:        leaf,
		})
	}
	if dimKey == "" || len(dims) == 0 {
		return nil, errors.New("matched rates required for DimensionMatrixNode")
	}
	sort.SliceStable(dims, func(i, j int) bool {
		return dims[i].DimensionValues[0] < dims[j].DimensionValues[0]
	})
	return wireDimensionMatrixNode{
		Type:             priceMachineDimMatrix,
		DimensionKeys:    []string{dimKey},
		DimensionsPrices: dims,
	}, nil
}

func leafFromRate(r DesiredPlanRate) (wireLeafNode, error) {
	hasFlat := r.Flat != nil
	hasTiers := len(r.Tiers) > 0
	if hasFlat == hasTiers {
		return wireLeafNode{}, errors.New("exactly one of Flat or Tiers must be set")
	}
	leaf := wireLeafNode{
		Type:              priceMachineLeafNode,
		AllowPartialBatch: true,
	}
	if hasFlat {
		leaf.Tiers = []wirePriceTier{{
			StartAfterUnit: 0,
			BatchSize:      1,
			PricePerBatch:  *r.Flat,
		}}
		return leaf, nil
	}
	leaf.Tiers = make([]wirePriceTier, 0, len(r.Tiers))
	for _, t := range r.Tiers {
		batch := t.BatchSize
		if batch <= 0 {
			batch = 1
		}
		leaf.Tiers = append(leaf.Tiers, wirePriceTier{
			StartAfterUnit: t.StartAfterUnit,
			BatchSize:      batch,
			PricePerBatch:  t.PricePerBatch,
		})
		if t.AllowPartialBatch {
			leaf.AllowPartialBatch = true
		}
	}
	return leaf, nil
}

func productItemPriceID(planID, itemID string) string {
	return planID + "--" + itemID
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func jsonEqual(a, b json.RawMessage) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return string(a) == string(b)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return string(a) == string(b)
	}
	return reflect.DeepEqual(av, bv)
}

// ParseDecimalFloat parses a decimal USD string into float64.
func ParseDecimalFloat(s string) (float64, error) {
	if s == "" {
		return 0, errors.New("empty decimal")
	}
	return strconv.ParseFloat(s, 64)
}
