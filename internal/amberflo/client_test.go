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
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeServer is a package-local, minimal Amberflo stand-in used across
// this package's tests. Tests configure it inline per case rather than
// through an admin API — closures over `fail*` fields let them inject
// 503/429/400 responses for the next N requests.
type fakeServer struct {
	srv    *httptest.Server
	apiKey string

	mu sync.Mutex

	customers     map[string]*storedWireCustomer
	meters        map[string]*storedWireMeter
	invoices      map[string][]CustomerProductInvoice
	latest        map[string]*CustomerProductInvoice
	productItems  map[string]*wireProductItem
	itemPrices    map[string]*wireProductItemPrice
	productPlans  map[string]*wireProductPlan
	customerPlans map[string][]wireCustomerProductPlan
	requests      []recordedRequest

	// failure injection
	failStatus     int
	failCount      int
	failRetryAfter int

	// armInvoiceNull makes the next invoice list response a JSON null.
	armInvoiceNull bool
}

// storedWireCustomer mirrors the wireCustomer shape for tests.
type storedWireCustomer struct {
	CustomerId    string            `json:"customerId"`
	CustomerName  string            `json:"customerName"`
	CustomerEmail string            `json:"customerEmail,omitempty"`
	Traits        map[string]string `json:"traits,omitempty"`
	Enabled       bool              `json:"enabled"`
	UpdateTime    int64             `json:"updateTime,omitempty"`
	CreateTime    int64             `json:"createTime,omitempty"`
}

// storedWireMeter mirrors the wireMeter shape for tests. Matches the
// fields the client writes and reads; anything else is discarded on
// decode.
type storedWireMeter struct {
	ID                    string   `json:"id,omitempty"`
	Label                 string   `json:"label,omitempty"`
	MeterAPIName          string   `json:"meterApiName"`
	MeterType             string   `json:"meterType,omitempty"`
	AggregationDimensions []string `json:"aggregationDimensions"`
	Unit                  string   `json:"unit,omitempty"`
	Dimensions            []string `json:"dimensions"`
	UseInBilling          bool     `json:"useInBilling"`
	LockingStatus         string   `json:"lockingStatus"`
}

type recordedRequest struct {
	Method  string
	Path    string
	Headers http.Header
	Body    []byte
}

func newFake(t *testing.T) *fakeServer {
	t.Helper()
	f := &fakeServer{
		apiKey:        "unit-test-key",
		customers:     map[string]*storedWireCustomer{},
		meters:        map[string]*storedWireMeter{},
		invoices:      map[string][]CustomerProductInvoice{},
		latest:        map[string]*CustomerProductInvoice{},
		productItems:  map[string]*wireProductItem{},
		itemPrices:    map[string]*wireProductItemPrice{},
		productPlans:  map[string]*wireProductPlan{},
		customerPlans: map[string][]wireCustomerProductPlan{},
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeServer) URL() string    { return f.srv.URL }
func (f *fakeServer) APIKey() string { return f.apiKey }

// seed installs a customer as if it had already been created. Tests call
// this to cover the "customer already exists" branch of EnsureCustomer.
func (f *fakeServer) seed(c storedWireCustomer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := c
	if cp.Traits != nil {
		cp.Traits = cloneStringMap(cp.Traits)
	}
	f.customers[c.CustomerId] = &cp
}

// seedInvoices installs a customer-product invoice list for ListInvoices.
func (f *fakeServer) seedInvoices(customerID string, invs []CustomerProductInvoice) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]CustomerProductInvoice, len(invs))
	copy(cp, invs)
	f.invoices[customerID] = cp
}

// seedLatestInvoice installs the payload returned by GetLatestInvoice.
func (f *fakeServer) seedLatestInvoice(customerID string, inv CustomerProductInvoice) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := inv
	f.latest[customerID] = &cp
}

// seedMeter installs a meter as if it had already been created. Used by
// meter_test.go to cover the "meter already exists" and drift branches
// of EnsureMeter.
func (f *fakeServer) seedMeter(m storedWireMeter) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := m
	cp.Dimensions = append([]string(nil), m.Dimensions...)
	cp.AggregationDimensions = append([]string(nil), m.AggregationDimensions...)
	f.meters[m.MeterAPIName] = &cp
}

// fetchMeter returns a deep copy of a stored meter or false. Used by
// assertions that need to read the fake's post-write state.
func (f *fakeServer) fetchMeter(apiName string) (storedWireMeter, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.meters[apiName]
	if !ok {
		return storedWireMeter{}, false
	}
	out := *m
	out.Dimensions = append([]string(nil), m.Dimensions...)
	out.AggregationDimensions = append([]string(nil), m.AggregationDimensions...)
	return out, true
}

// armFailures queues the next `count` requests to return `status`. When
// retryAfter > 0 it is sent as a Retry-After header.
func (f *fakeServer) armFailures(status, count, retryAfter int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failStatus = status
	f.failCount = count
	f.failRetryAfter = retryAfter
}

// requestsCopy returns a snapshot of recorded requests.
func (f *fakeServer) requestsCopy() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]recordedRequest, len(f.requests))
	copy(out, f.requests)
	return out
}

func (f *fakeServer) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()

	f.mu.Lock()
	f.requests = append(f.requests, recordedRequest{
		Method:  r.Method,
		Path:    r.URL.Path,
		Headers: r.Header.Clone(),
		Body:    append([]byte(nil), body...),
	})
	if f.failCount > 0 {
		status := f.failStatus
		retryAfter := f.failRetryAfter
		f.failCount--
		f.mu.Unlock()
		if retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		}
		http.Error(w, "armed failure", status)
		return
	}
	f.mu.Unlock()

	if r.Header.Get("X-API-KEY") != f.apiKey {
		http.Error(w, "forbidden", http.StatusUnauthorized)
		return
	}

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/customers/payment-method/switch":
		writeJSON(w, http.StatusOK, []PaymentMethodSwitch{})

	case r.Method == http.MethodPost && r.URL.Path == "/customers/payment-method/switch":
		var sw PaymentMethodSwitch
		_ = json.Unmarshal(body, &sw)
		writeJSON(w, http.StatusOK, sw)

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/customers/"):
		id := strings.TrimPrefix(r.URL.Path, "/customers/")
		f.mu.Lock()
		c, ok := f.customers[id]
		f.mu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, c)

	case (r.Method == http.MethodPost || r.Method == http.MethodPut) &&
		r.URL.Path == "/customers":
		var in storedWireCustomer
		if err := json.Unmarshal(body, &in); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if in.CustomerId == "" {
			http.Error(w, "customerId required", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		stored := &storedWireCustomer{
			CustomerId:    in.CustomerId,
			CustomerName:  in.CustomerName,
			CustomerEmail: in.CustomerEmail,
			Traits:        cloneStringMap(in.Traits),
			Enabled:       in.Enabled,
			UpdateTime:    time.Now().UnixMilli(),
		}
		f.customers[in.CustomerId] = stored
		out := *stored
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, out)

	case r.Method == http.MethodGet && r.URL.Path == "/meters":
		// List-with-filter: the live API returns [] when no match.
		// The client only ever uses ?meterApiName=<x>, so that's all we
		// support. Requests without the filter return every meter (rare
		// in tests; helpful for debugging).
		filter := r.URL.Query().Get("meterApiName")
		f.mu.Lock()
		out := make([]storedWireMeter, 0, len(f.meters))
		for _, m := range f.meters {
			if filter != "" && m.MeterAPIName != filter {
				continue
			}
			// Copy to avoid lock-scope aliasing.
			cp := *m
			cp.Dimensions = append([]string(nil), m.Dimensions...)
			cp.AggregationDimensions = append([]string(nil), m.AggregationDimensions...)
			out = append(out, cp)
		}
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, out)

	case r.Method == http.MethodPost && r.URL.Path == "/meters":
		// POST must NOT carry an id. The live API silently ignores any
		// caller-supplied id and mints its own UUID; we match that
		// behaviour so tests catch accidental id-on-POST bugs.
		var in storedWireMeter
		if err := json.Unmarshal(body, &in); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if in.MeterAPIName == "" {
			http.Error(w, "meterApiName required", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		if existing, ok := f.meters[in.MeterAPIName]; ok {
			f.mu.Unlock()
			// Live API behaviour: POST over an existing meterApiName
			// 400s with "already exists".
			http.Error(w,
				`{"errorMessage":"Invalid request: Meter already exists with 'meterApiName': `+existing.MeterAPIName+`"}`,
				http.StatusBadRequest)
			return
		}
		id := "fake-id-" + in.MeterAPIName
		// Default lockingStatus to "open" if the caller omitted it —
		// matches the live API's default. The provider always sends
		// "close_to_changes" now, so this branch is mostly defensive.
		locking := in.LockingStatus
		if locking == "" {
			locking = "open"
		}
		stored := &storedWireMeter{
			ID:                    id,
			Label:                 in.Label,
			MeterAPIName:          in.MeterAPIName,
			MeterType:             in.MeterType,
			AggregationDimensions: append([]string(nil), in.AggregationDimensions...),
			Unit:                  in.Unit,
			Dimensions:            append([]string(nil), in.Dimensions...),
			UseInBilling:          in.UseInBilling,
			LockingStatus:         locking,
		}
		f.meters[in.MeterAPIName] = stored
		out := *stored
		out.Dimensions = append([]string(nil), stored.Dimensions...)
		out.AggregationDimensions = append([]string(nil), stored.AggregationDimensions...)
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, out)

	case r.Method == http.MethodPut && r.URL.Path == "/meters":
		// PUT requires both server id and meterApiName in the body.
		// Live API 400s with "already exists" when id is missing —
		// that same error is what tripped the earlier e2e run.
		var in storedWireMeter
		if err := json.Unmarshal(body, &in); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if in.MeterAPIName == "" {
			http.Error(w, "meterApiName required", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		existing, ok := f.meters[in.MeterAPIName]
		if !ok {
			f.mu.Unlock()
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if in.ID == "" || in.ID != existing.ID {
			f.mu.Unlock()
			// Match the live-API error body so tests can inspect it.
			http.Error(w,
				`{"errorMessage":"Invalid request: Meter already exists with 'meterApiName': `+existing.MeterAPIName+`"}`,
				http.StatusBadRequest)
			return
		}
		// Enforce the one-way lockingStatus lifecycle: open →
		// close_to_changes | close_to_deletions | deprecated;
		// close_to_changes → deprecated only; deprecated → deprecated
		// (idempotent). Any backwards transition the live API rejects
		// with 400.
		if in.LockingStatus != "" && !lockingTransitionAllowed(existing.LockingStatus, in.LockingStatus) {
			f.mu.Unlock()
			http.Error(w,
				`{"errorMessage":"Invalid lockingStatus transition from `+existing.LockingStatus+` to `+in.LockingStatus+`"}`,
				http.StatusBadRequest)
			return
		}
		existing.Label = in.Label
		existing.MeterType = in.MeterType
		existing.Unit = in.Unit
		existing.Dimensions = append([]string(nil), in.Dimensions...)
		existing.AggregationDimensions = append([]string(nil), in.AggregationDimensions...)
		existing.UseInBilling = in.UseInBilling
		if in.LockingStatus != "" {
			existing.LockingStatus = in.LockingStatus
		}
		out := *existing
		out.Dimensions = append([]string(nil), existing.Dimensions...)
		out.AggregationDimensions = append([]string(nil), existing.AggregationDimensions...)
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, out)

	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/meters/"):
		// DELETE is keyed by server id. If a caller passes
		// meterApiName the live API returns 200 with no effect — we
		// mirror that. Meters must be in `deprecated` to delete;
		// otherwise 400 (live-API behaviour).
		id := strings.TrimPrefix(r.URL.Path, "/meters/")
		f.mu.Lock()
		var apiName string
		for _, m := range f.meters {
			if m.ID == id {
				apiName = m.MeterAPIName
				break
			}
		}
		if apiName == "" {
			f.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		m := f.meters[apiName]
		if m.LockingStatus != "deprecated" {
			status := m.LockingStatus
			f.mu.Unlock()
			http.Error(w,
				`{"errorMessage":"'lockingStatus' `+status+` prevents meter from being deleted."}`,
				http.StatusBadRequest)
			return
		}
		delete(f.meters, apiName)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)

	default:
		if f.servePricing(w, r, body) {
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/payments/billing-settings/list" {
			writeJSON(w, http.StatusOK, []PaymentSetting{})
			return
		}
		if r.Method == http.MethodGet &&
			strings.HasPrefix(r.URL.Path, "/payments/billing/customer-product-invoice") {
			customerID := r.URL.Query().Get("customerId")
			if r.URL.Query().Get("latest") == "true" {
				f.mu.Lock()
				inv, ok := f.latest[customerID]
				f.mu.Unlock()
				if !ok || inv == nil {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				writeJSON(w, http.StatusOK, inv)
				return
			}
			// Get-by-key (no /all suffix).
			if !strings.HasSuffix(r.URL.Path, "/all") &&
				r.URL.Query().Get("productPlanId") != "" {
				f.mu.Lock()
				out := f.invoices[customerID]
				f.mu.Unlock()
				planID := r.URL.Query().Get("productPlanId")
				year := r.URL.Query().Get("year")
				month := r.URL.Query().Get("month")
				day := r.URL.Query().Get("day")
				for i := range out {
					inv := out[i]
					if inv.InvoiceKey.ProductPlanID == planID &&
						strconv.FormatInt(inv.InvoiceKey.Year, 10) == year &&
						strconv.FormatInt(inv.InvoiceKey.Month, 10) == month &&
						strconv.FormatInt(inv.InvoiceKey.Day, 10) == day {
						writeJSON(w, http.StatusOK, inv)
						return
					}
				}
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			f.mu.Lock()
			nullBody := f.armInvoiceNull
			if nullBody {
				f.armInvoiceNull = false
			}
			out := f.invoices[customerID]
			f.mu.Unlock()
			if nullBody {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("null"))
				return
			}
			if out == nil {
				out = []CustomerProductInvoice{}
			}
			writeJSON(w, http.StatusOK, out)
			return
		}
		http.Error(w, "not implemented", http.StatusNotImplemented)
	}
}

// servePricing handles product-item, product-item-price, product-plan, and
// customer-pricing routes used by EnsureProductPlan / EnsureCustomerPlan.
func (f *fakeServer) servePricing(w http.ResponseWriter, r *http.Request, body []byte) bool {
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, productItemsPath+"/"):
		id := strings.TrimPrefix(r.URL.Path, productItemsPath+"/")
		f.mu.Lock()
		item, ok := f.productItems[id]
		f.mu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return true
		}
		writeJSON(w, http.StatusOK, item)
		return true

	case r.Method == http.MethodPost && r.URL.Path == productItemsPath:
		var in wireProductItem
		if err := json.Unmarshal(body, &in); err != nil || in.ID == "" {
			http.Error(w, "bad product item", http.StatusBadRequest)
			return true
		}
		f.mu.Lock()
		cp := in
		f.productItems[in.ID] = &cp
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, in)
		return true

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, productItemPricePath+"/"):
		id := strings.TrimPrefix(r.URL.Path, productItemPricePath+"/")
		f.mu.Lock()
		price, ok := f.itemPrices[id]
		f.mu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return true
		}
		writeJSON(w, http.StatusOK, price)
		return true

	case r.Method == http.MethodPost && r.URL.Path == productItemPricePath:
		var in wireProductItemPrice
		if err := json.Unmarshal(body, &in); err != nil || in.ID == "" {
			http.Error(w, "bad product item price", http.StatusBadRequest)
			return true
		}
		f.mu.Lock()
		cp := in
		if len(in.Price) > 0 {
			cp.Price = append(json.RawMessage(nil), in.Price...)
		}
		f.itemPrices[in.ID] = &cp
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, in)
		return true

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, productPlansPath+"/"):
		id := strings.TrimPrefix(r.URL.Path, productPlansPath+"/")
		f.mu.Lock()
		plan, ok := f.productPlans[id]
		f.mu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return true
		}
		writeJSON(w, http.StatusOK, plan)
		return true

	case r.Method == http.MethodPost && r.URL.Path == productPlansPath:
		var in wireProductPlan
		if err := json.Unmarshal(body, &in); err != nil || in.ID == "" {
			http.Error(w, "bad product plan", http.StatusBadRequest)
			return true
		}
		f.mu.Lock()
		cp := in
		if in.ProductItemPriceIdsMap != nil {
			cp.ProductItemPriceIdsMap = maps.Clone(in.ProductItemPriceIdsMap)
		}
		if in.FeeMap != nil {
			cp.FeeMap = maps.Clone(in.FeeMap)
		}
		f.productPlans[in.ID] = &cp
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, in)
		return true

	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, productPlansPath+"/"):
		id := strings.TrimPrefix(r.URL.Path, productPlansPath+"/")
		f.mu.Lock()
		delete(f.productPlans, id)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return true

	case r.Method == http.MethodGet && r.URL.Path == customerPricingListPath:
		customerID := r.URL.Query().Get("customerId")
		f.mu.Lock()
		out := append([]wireCustomerProductPlan(nil), f.customerPlans[customerID]...)
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, out)
		return true

	case r.Method == http.MethodPost && r.URL.Path == customerPricingPath:
		var in wireCustomerProductPlan
		if err := json.Unmarshal(body, &in); err != nil || in.CustomerID == "" || in.ProductPlanID == "" {
			http.Error(w, "bad customer plan", http.StatusBadRequest)
			return true
		}
		f.mu.Lock()
		plans := f.customerPlans[in.CustomerID]
		replaced := false
		for i := range plans {
			if plans[i].ProductPlanID == in.ProductPlanID {
				plans[i] = in
				replaced = true
				break
			}
		}
		if !replaced {
			if in.RelationID == "" {
				in.RelationID = "rel-" + in.CustomerID + "-" + in.ProductPlanID
			}
			plans = append(plans, in)
		}
		f.customerPlans[in.CustomerID] = plans
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, in)
		return true
	}
	return false
}

// lockingTransitionAllowed reports whether the one-way Amberflo
// lockingStatus state machine permits `from` → `to`. Empty `to` means
// the caller did not set the field, which the fake treats as "keep
// the current value" in the PUT handler.
func lockingTransitionAllowed(from, to string) bool {
	if from == to {
		return true
	}
	switch from {
	case "", "open":
		// open → anything is fine.
		return true
	case "close_to_changes":
		return to == "deprecated"
	case "close_to_deletions":
		return to == "deprecated"
	case "deprecated":
		// Terminal: only self-loop is allowed.
		return false
	default:
		// Unknown source state — be permissive in the fake so unknown
		// real-API additions don't break the tests.
		return true
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// newTestClient returns a Client pointed at a fresh fake. Retries use an
// instant-sleep to keep tests deterministic.
func newTestClient(t *testing.T, opts ...func(*ClientOptions)) (Client, *fakeServer) {
	t.Helper()
	f := newFake(t)

	co := ClientOptions{
		BaseURL:         f.URL(),
		APIKey:          f.APIKey(),
		RateLimitPerSec: 1000,
		RetryAttempts:   4,
		sleep: func(ctx context.Context, _ time.Duration) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return nil
		},
		jitter: func(d time.Duration) time.Duration { return d },
	}
	for _, o := range opts {
		o(&co)
	}
	c, err := NewClient(co)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c, f
}

func TestNewClient_RejectsEmptyAPIKey(t *testing.T) {
	_, err := NewClient(ClientOptions{BaseURL: "https://example.com", APIKey: ""})
	if err == nil {
		t.Fatalf("expected error for empty APIKey")
	}
	if !strings.Contains(err.Error(), "APIKey") {
		t.Errorf("error should mention APIKey: %v", err)
	}
}

func TestNewClient_RejectsInvalidBaseURL(t *testing.T) {
	_, err := NewClient(ClientOptions{BaseURL: "not-a-url", APIKey: "x"})
	if err == nil {
		t.Fatalf("expected error for invalid BaseURL")
	}
}

func TestNewClient_DefaultsBaseURL(t *testing.T) {
	c, err := NewClient(ClientOptions{APIKey: "x"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestClient_AuthHeaderPresentOnEveryCall(t *testing.T) {
	c, f := newTestClient(t)
	ctx := context.Background()
	f.seed(storedWireCustomer{CustomerId: "acct-1", CustomerName: "acct-1", Enabled: true})
	if _, err := c.GetCustomer(ctx, "acct-1"); err != nil {
		t.Fatalf("GetCustomer: %v", err)
	}
	reqs := f.requestsCopy()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 recorded request, got %d", len(reqs))
	}
	got := reqs[0].Headers.Get("X-Api-Key")
	if got != f.APIKey() {
		t.Errorf("X-API-KEY header missing/wrong: %v", reqs[0].Headers)
	}
}

func TestClient_RejectsMissingAPIKeyAtServer(t *testing.T) {
	f := newFake(t)
	c, err := NewClient(ClientOptions{
		BaseURL:       f.URL(),
		APIKey:        "wrong-key",
		RetryAttempts: 1,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = c.GetCustomer(context.Background(), "any")
	if err == nil {
		t.Fatalf("expected error for bad API key")
	}
	if !IsPermanent(err) {
		t.Errorf("expected permanent error for 401, got %v", err)
	}
}

func TestClient_RespectsRetryAfter(t *testing.T) {
	var sleeps []time.Duration
	c, f := newTestClient(t, func(co *ClientOptions) {
		co.sleep = func(ctx context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return ctx.Err()
		}
	})
	f.armFailures(429, 1, 1)

	if _, err := c.GetCustomer(context.Background(), "anything"); err != nil && !errors.Is(err, ErrCustomerNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sleeps) == 0 {
		t.Fatalf("expected at least one sleep")
	}
	if sleeps[0] < time.Second {
		t.Errorf("expected first retry delay >= 1s for Retry-After, got %v", sleeps[0])
	}
}

func TestClient_NetworkErrorIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()

	c, err := NewClient(ClientOptions{
		BaseURL:       url,
		APIKey:        "k",
		RetryAttempts: 2,
		sleep:         func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = c.GetCustomer(context.Background(), "x")
	if err == nil {
		t.Fatalf("expected network error")
	}
	if !IsTransient(err) {
		t.Errorf("expected TransientError, got %T: %v", err, err)
	}
}
