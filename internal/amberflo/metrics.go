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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Prometheus instruments exported from this package.
//
// Metrics register against controller-runtime's default registry (the
// same one `/metrics` is served from) so operators see amberflo_provider_*
// series alongside controller_runtime_* without extra wiring.
//
// Register is called exactly once; the sync.Once guard keeps re-running
// the manager in tests safe.

var (
	registerOnce sync.Once

	amberfloRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "amberflo_provider_amberflo_request_duration_seconds",
			Help:    "Duration of requests to the Amberflo API, by operation and HTTP status class.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation", "status"},
	)

	amberfloRequestTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "amberflo_provider_amberflo_request_total",
			Help: "Total Amberflo API calls, by operation and HTTP status class.",
		},
		[]string{"operation", "status_class"},
	)
)

func init() {
	registerOnce.Do(func() {
		ctrlmetrics.Registry.MustRegister(
			amberfloRequestDuration,
			amberfloRequestTotal,
		)
	})
}

// InstrumentedClient wraps a Client and records Prometheus metrics for
// each operation. Instrumentation happens at the interface boundary so
// the underlying Client implementation (client.go) stays free of metrics
// coupling.
type InstrumentedClient struct {
	Client Client
}

// NewInstrumentedClient wraps c. If c is already an *InstrumentedClient
// the wrapper is returned as-is to avoid double accounting.
func NewInstrumentedClient(c Client) Client {
	if _, ok := c.(*InstrumentedClient); ok {
		return c
	}
	return &InstrumentedClient{Client: c}
}

// EnsureCustomer forwards to the wrapped client while recording metrics.
func (ic *InstrumentedClient) EnsureCustomer(ctx context.Context, desired DesiredCustomer) (Customer, error) {
	start := time.Now()
	out, err := ic.Client.EnsureCustomer(ctx, desired)
	recordOp("EnsureCustomer", start, err)
	return out, err
}

// DisableCustomer forwards to the wrapped client while recording metrics.
func (ic *InstrumentedClient) DisableCustomer(ctx context.Context, customerID string) error {
	start := time.Now()
	err := ic.Client.DisableCustomer(ctx, customerID)
	recordOp("DisableCustomer", start, err)
	return err
}

// GetCustomer forwards to the wrapped client while recording metrics.
func (ic *InstrumentedClient) GetCustomer(ctx context.Context, customerID string) (Customer, error) {
	start := time.Now()
	out, err := ic.Client.GetCustomer(ctx, customerID)
	recordOp("GetCustomer", start, err)
	return out, err
}

// EnsureMeter forwards to the wrapped client while recording metrics.
func (ic *InstrumentedClient) EnsureMeter(ctx context.Context, desired DesiredMeter) (Meter, error) {
	start := time.Now()
	out, err := ic.Client.EnsureMeter(ctx, desired)
	recordOp("EnsureMeter", start, err)
	return out, err
}

// DeleteMeter forwards to the wrapped client while recording metrics.
func (ic *InstrumentedClient) DeleteMeter(ctx context.Context, meterAPIName string) error {
	start := time.Now()
	err := ic.Client.DeleteMeter(ctx, meterAPIName)
	recordOp("DeleteMeter", start, err)
	return err
}

// GetMeter forwards to the wrapped client while recording metrics.
func (ic *InstrumentedClient) GetMeter(ctx context.Context, meterAPIName string) (Meter, error) {
	start := time.Now()
	out, err := ic.Client.GetMeter(ctx, meterAPIName)
	recordOp("GetMeter", start, err)
	return out, err
}

// SubmitUsage forwards to the wrapped client while recording metrics.
func (ic *InstrumentedClient) SubmitUsage(ctx context.Context, record UsageRecord) error {
	start := time.Now()
	err := ic.Client.SubmitUsage(ctx, record)
	recordOp("SubmitUsage", start, err)
	return err
}

// recordOp records a single outbound Amberflo call. status captures the
// classified outcome (success, transient, permanent) in lieu of a raw
// HTTP status — the Client does not expose per-request status codes at
// this layer, and status_class (2xx/4xx/5xx) is inferred from the error
// classifier.
func recordOp(op string, start time.Time, err error) {
	elapsed := time.Since(start).Seconds()
	status, class := classifyForMetrics(err)
	amberfloRequestDuration.WithLabelValues(op, status).Observe(elapsed)
	amberfloRequestTotal.WithLabelValues(op, class).Inc()
}

// classifyForMetrics maps an error returned by the Client into two label
// values: a fine-grained `status` (success|not_found|transient|permanent|
// unknown) and a coarse `status_class` (2xx|4xx|5xx|network).
func classifyForMetrics(err error) (status, class string) {
	if err == nil {
		return "success", "2xx"
	}
	if errors.Is(err, ErrCustomerNotFound) || errors.Is(err, ErrMeterNotFound) {
		return "not_found", "4xx"
	}
	var pe *PermanentError
	if errors.As(err, &pe) {
		switch {
		case pe.StatusCode >= 400 && pe.StatusCode < 500:
			return "permanent", "4xx"
		case pe.StatusCode >= 500:
			return "permanent", "5xx"
		default:
			return "permanent", "unknown"
		}
	}
	var te *TransientError
	if errors.As(err, &te) {
		switch {
		case te.StatusCode == 0:
			return "transient", "network"
		case te.StatusCode >= 500:
			return "transient", "5xx"
		case te.StatusCode >= 400:
			return "transient", "4xx"
		default:
			return "transient", "unknown"
		}
	}
	return "unknown", "unknown"
}
