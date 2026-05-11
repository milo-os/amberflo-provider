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
	"testing"
)

// stubClient is a minimal Client implementation that returns fixed values.
type stubClient struct {
	ensureErr         error
	disableErr        error
	getErr            error
	ensureMeterErr    error
	deleteMeterErr    error
	getMeterErr       error
}

func (s *stubClient) EnsureCustomer(ctx context.Context, _ DesiredCustomer) (Customer, error) {
	return Customer{}, s.ensureErr
}
func (s *stubClient) DisableCustomer(ctx context.Context, _ string) error { return s.disableErr }
func (s *stubClient) GetCustomer(ctx context.Context, _ string) (Customer, error) {
	return Customer{}, s.getErr
}
func (s *stubClient) EnsureMeter(ctx context.Context, _ DesiredMeter) (Meter, error) {
	return Meter{}, s.ensureMeterErr
}
func (s *stubClient) DeleteMeter(ctx context.Context, _ string) error { return s.deleteMeterErr }
func (s *stubClient) GetMeter(ctx context.Context, _ string) (Meter, error) {
	return Meter{}, s.getMeterErr
}
func (s *stubClient) SubmitUsage(ctx context.Context, _ UsageRecord) error { return nil }

func TestInstrumentedClient_DelegatesAndInstruments(t *testing.T) {
	stub := &stubClient{}
	wrapped := NewInstrumentedClient(stub)
	// Calling again should be idempotent: no double-wrap.
	if NewInstrumentedClient(wrapped) != wrapped {
		t.Errorf("double-wrap should be a no-op")
	}

	ctx := context.Background()
	if _, err := wrapped.EnsureCustomer(ctx, DesiredCustomer{ID: "x"}); err != nil {
		t.Errorf("EnsureCustomer passthrough: %v", err)
	}
	if err := wrapped.DisableCustomer(ctx, "x"); err != nil {
		t.Errorf("DisableCustomer passthrough: %v", err)
	}
	if _, err := wrapped.GetCustomer(ctx, "x"); err != nil {
		t.Errorf("GetCustomer passthrough: %v", err)
	}

	// Exercise error paths.
	stub.ensureErr = &PermanentError{Err: errors.New("e"), StatusCode: 400}
	if _, err := wrapped.EnsureCustomer(ctx, DesiredCustomer{ID: "x"}); err == nil {
		t.Errorf("expected error pass-through")
	}

	stub.disableErr = &TransientError{Err: errors.New("t"), StatusCode: 503}
	if err := wrapped.DisableCustomer(ctx, "x"); err == nil {
		t.Errorf("expected error pass-through")
	}

	stub.getErr = fmt.Errorf("%w: id", ErrCustomerNotFound)
	if _, err := wrapped.GetCustomer(ctx, "x"); err == nil {
		t.Errorf("expected error pass-through")
	}

	if _, err := wrapped.EnsureMeter(ctx, DesiredMeter{APIName: "x"}); err != nil {
		t.Errorf("EnsureMeter passthrough: %v", err)
	}
	if err := wrapped.DeleteMeter(ctx, "x"); err != nil {
		t.Errorf("DeleteMeter passthrough: %v", err)
	}
	if _, err := wrapped.GetMeter(ctx, "x"); err != nil {
		t.Errorf("GetMeter passthrough: %v", err)
	}

	stub.ensureMeterErr = &PermanentError{Err: errors.New("e"), StatusCode: 400}
	if _, err := wrapped.EnsureMeter(ctx, DesiredMeter{APIName: "x"}); err == nil {
		t.Errorf("expected EnsureMeter error pass-through")
	}
	stub.deleteMeterErr = &TransientError{Err: errors.New("t"), StatusCode: 503}
	if err := wrapped.DeleteMeter(ctx, "x"); err == nil {
		t.Errorf("expected DeleteMeter error pass-through")
	}
	stub.getMeterErr = fmt.Errorf("%w: id", ErrMeterNotFound)
	if _, err := wrapped.GetMeter(ctx, "x"); err == nil {
		t.Errorf("expected GetMeter error pass-through")
	}
}

func TestClassifyForMetrics(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantStat string
		wantCls  string
	}{
		{"success", nil, "success", "2xx"},
		{"not_found",
			fmt.Errorf("%w: foo", ErrCustomerNotFound),
			"not_found", "4xx"},
		{"permanent_4xx",
			&PermanentError{Err: errors.New("e"), StatusCode: 404},
			"permanent", "4xx"},
		{"permanent_5xx",
			&PermanentError{Err: errors.New("e"), StatusCode: 503},
			"permanent", "5xx"},
		{"permanent_unknown",
			&PermanentError{Err: errors.New("e"), StatusCode: 0},
			"permanent", "unknown"},
		{"transient_network",
			&TransientError{Err: errors.New("dial")},
			"transient", "network"},
		{"transient_5xx",
			&TransientError{Err: errors.New("e"), StatusCode: 503},
			"transient", "5xx"},
		{"transient_4xx",
			&TransientError{Err: errors.New("e"), StatusCode: 429},
			"transient", "4xx"},
		{"transient_unknown",
			&TransientError{Err: errors.New("e"), StatusCode: 301},
			"transient", "unknown"},
		{"unknown_generic",
			errors.New("random"),
			"unknown", "unknown"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, c := classifyForMetrics(tc.err)
			if s != tc.wantStat {
				t.Errorf("status=%q want %q", s, tc.wantStat)
			}
			if c != tc.wantCls {
				t.Errorf("class=%q want %q", c, tc.wantCls)
			}
		})
	}
}
