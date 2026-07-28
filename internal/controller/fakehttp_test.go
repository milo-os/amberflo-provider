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
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
)

// recordingFakeServer is a minimal Amberflo stand-in used by the envtest
// controller suite. It implements just enough of the /customers surface
// for the reconciler to drive end-to-end tests without reaching a real
// API. It is intentionally self-contained — the earlier fake sub-package
// is gone; tests build inline fakes like this one.
type recordingFakeServer struct {
	srv    *httptest.Server
	apiKey string

	mu         sync.Mutex
	customers  map[string]*storedCustomer
	meters     map[string]*storedMeter
	requests   []recordedRequest
	failStatus int
	failCount  int
}

// storedCustomer mirrors the Amberflo customer wire shape the client uses.
type storedCustomer struct {
	CustomerId    string            `json:"customerId"`
	CustomerName  string            `json:"customerName"`
	CustomerEmail string            `json:"customerEmail,omitempty"`
	Enabled       bool              `json:"enabled"`
	Traits        map[string]string `json:"traits,omitempty"`
}

// storedMeter mirrors the Amberflo meter wire shape the client writes. We
// only persist the fields the reconciler reads back; anything else is
// discarded on decode. The fake treats meterApiName as the primary key
// (matching Amberflo's caller-supplied id semantics).
type storedMeter struct {
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

// recordedRequest captures a request for post-hoc inspection by tests.
type recordedRequest struct {
	Method string
	Path   string
	Body   []byte
}

// newRecordingFakeServer returns a started fake.
func newRecordingFakeServer() *recordingFakeServer {
	f := &recordingFakeServer{
		apiKey:    "envtest-api-key",
		customers: map[string]*storedCustomer{},
		meters:    map[string]*storedMeter{},
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}

// URL returns the base URL of the fake server.
func (f *recordingFakeServer) URL() string { return f.srv.URL }

// APIKey returns the accepted API key value for X-API-KEY.
func (f *recordingFakeServer) APIKey() string { return f.apiKey }

// Close tears down the fake server.
func (f *recordingFakeServer) Close() { f.srv.Close() }

// Reset clears state between tests.
func (f *recordingFakeServer) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.customers = map[string]*storedCustomer{}
	f.meters = map[string]*storedMeter{}
	f.requests = nil
	f.failStatus = 0
	f.failCount = 0
}

// ArmFailures causes the next `count` API requests to return `status`.
func (f *recordingFakeServer) ArmFailures(status, count int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failStatus = status
	f.failCount = count
}

// FetchCustomer returns a deep copy of a stored customer or false.
func (f *recordingFakeServer) FetchCustomer(id string) (storedCustomer, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.customers[id]
	if !ok {
		return storedCustomer{}, false
	}
	return *c, true
}

// FetchMeter returns a deep copy of a stored meter or false.
func (f *recordingFakeServer) FetchMeter(apiName string) (storedMeter, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.meters[apiName]
	if !ok {
		return storedMeter{}, false
	}
	out := *m
	out.Dimensions = append([]string(nil), m.Dimensions...)
	out.AggregationDimensions = append([]string(nil), m.AggregationDimensions...)
	return out, true
}

// DeleteMeter removes a meter from the fake's store. Returns whether the
// meter existed. Test-only helper used to seed a "prior 404" before a
// reconciler run.
func (f *recordingFakeServer) DeleteMeter(apiName string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.meters[apiName]
	delete(f.meters, apiName)
	return ok
}

// Requests returns a snapshot of all requests the fake has served.
func (f *recordingFakeServer) Requests() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]recordedRequest, len(f.requests))
	copy(out, f.requests)
	return out
}

func (f *recordingFakeServer) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()

	f.mu.Lock()
	f.requests = append(f.requests, recordedRequest{
		Method: r.Method,
		Path:   r.URL.Path,
		Body:   append([]byte(nil), body...),
	})
	// Apply armed failure count first so auth/normal paths don't mask it.
	if f.failCount > 0 {
		status := f.failStatus
		f.failCount--
		f.mu.Unlock()
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
		writeJSON(w, http.StatusOK, []any{})

	case r.Method == http.MethodPost && r.URL.Path == "/customers/payment-method/switch":
		writeJSON(w, http.StatusOK, map[string]any{})

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
		(r.URL.Path == "/customers"):
		var in storedCustomer
		if err := json.Unmarshal(body, &in); err != nil {
			http.Error(w, fmt.Sprintf("bad json: %v", err), http.StatusBadRequest)
			return
		}
		if in.CustomerId == "" {
			http.Error(w, "customerId required", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.customers[in.CustomerId] = &storedCustomer{
			CustomerId:    in.CustomerId,
			CustomerName:  in.CustomerName,
			CustomerEmail: in.CustomerEmail,
			Enabled:       in.Enabled,
			Traits:        cloneMap(in.Traits),
		}
		out := *f.customers[in.CustomerId]
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, out)

	case r.Method == http.MethodGet && r.URL.Path == "/meters":
		// List-with-filter — matches the live API. Empty result => [].
		filter := r.URL.Query().Get("meterApiName")
		f.mu.Lock()
		out := make([]storedMeter, 0, len(f.meters))
		for _, m := range f.meters {
			if filter != "" && m.MeterAPIName != filter {
				continue
			}
			cp := *m
			cp.Dimensions = append([]string(nil), m.Dimensions...)
			cp.AggregationDimensions = append([]string(nil), m.AggregationDimensions...)
			out = append(out, cp)
		}
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, out)

	case r.Method == http.MethodPost && r.URL.Path == "/meters":
		// POST creates a new record and stamps a server id. Duplicate
		// meterApiName returns 400 to mirror the live API.
		var in storedMeter
		if err := json.Unmarshal(body, &in); err != nil {
			http.Error(w, fmt.Sprintf("bad json: %v", err), http.StatusBadRequest)
			return
		}
		if in.MeterAPIName == "" {
			http.Error(w, "meterApiName required", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		if _, ok := f.meters[in.MeterAPIName]; ok {
			f.mu.Unlock()
			http.Error(w, `{"errorMessage":"Invalid request: Meter already exists"}`, http.StatusBadRequest)
			return
		}
		locking := in.LockingStatus
		if locking == "" {
			locking = "open"
		}
		f.meters[in.MeterAPIName] = &storedMeter{
			ID:                    "fake-id-" + in.MeterAPIName,
			Label:                 in.Label,
			MeterAPIName:          in.MeterAPIName,
			MeterType:             in.MeterType,
			AggregationDimensions: append([]string(nil), in.AggregationDimensions...),
			Unit:                  in.Unit,
			Dimensions:            append([]string(nil), in.Dimensions...),
			UseInBilling:          in.UseInBilling,
			LockingStatus:         locking,
		}
		out := *f.meters[in.MeterAPIName]
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, out)

	case r.Method == http.MethodPut && r.URL.Path == "/meters":
		// PUT requires the server id plus meterApiName in the body.
		var in storedMeter
		if err := json.Unmarshal(body, &in); err != nil {
			http.Error(w, fmt.Sprintf("bad json: %v", err), http.StatusBadRequest)
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
			http.Error(w, `{"errorMessage":"Invalid request: Meter already exists"}`, http.StatusBadRequest)
			return
		}
		// Enforce one-way lockingStatus transitions (mirrors the live
		// API). A PUT with an unset lockingStatus is treated as
		// "don't change it".
		if in.LockingStatus != "" && !fakeLockingTransitionAllowed(existing.LockingStatus, in.LockingStatus) {
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
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, out)

	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/meters/"):
		// DELETE is keyed by server id. Unknown id => 200 no-op (live
		// API behaviour). Target must be in `deprecated` to delete.
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
		// Invoice list — empty by default so BA reconcile invoice fallback
		// is a no-op in tests that don't seed invoices.
		if r.Method == http.MethodGet && r.URL.Path == "/payments/billing-settings/list" {
			writeJSON(w, http.StatusOK, []any{})
			return
		}
		if r.Method == http.MethodGet &&
			strings.HasPrefix(r.URL.Path, "/payments/billing/customer-product-invoice") {
			writeJSON(w, http.StatusOK, []any{})
			return
		}
		http.Error(w, "not implemented", http.StatusNotImplemented)
	}
}

// fakeLockingTransitionAllowed mirrors the live-API lifecycle rules:
// open → anything; close_to_changes|close_to_deletions → deprecated;
// deprecated is terminal (self-loop only). Unknown source states are
// permissive to avoid breaking tests on future API additions.
func fakeLockingTransitionAllowed(from, to string) bool {
	if from == to {
		return true
	}
	switch from {
	case "", "open":
		return true
	case "close_to_changes", "close_to_deletions":
		return to == "deprecated"
	case "deprecated":
		return false
	default:
		return true
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func cloneMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	maps.Copy(out, in)
	return out
}
