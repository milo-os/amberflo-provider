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

package submission

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Prometheus instruments for the submission consumer.
//
// Registered against controller-runtime's default registry (the same one
// /metrics is served from) so operators see billing_* series alongside
// controller_runtime_* without extra wiring.
//
// The sync.Once guard keeps re-running the manager in tests safe.

var (
	submissionOnce sync.Once

	// submissionRequestsTotal counts Amberflo ingest API submission
	// attempts by outcome. Labels:
	//   status="success"   — 2xx response, event acked.
	//   status="transient" — 5xx/429/network, event nacked for retry.
	//   status="permanent" — 4xx (non-429), event acked and dropped.
	submissionRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "billing_submission_requests_total",
			Help: "Total Amberflo ingest API submission attempts, by outcome.",
		},
		[]string{"status"},
	)

	// pipelineOldestUnsubmittedSeconds tracks the age of the oldest
	// fetched-but-unacked event in the current submission batch.
	// Resets to 0 when no messages are in-flight (after each batch).
	// A sustained non-zero value indicates the consumer is stalled.
	pipelineOldestUnsubmittedSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "billing_pipeline_oldest_unsubmitted_seconds",
			Help: "Age in seconds of the oldest fetched-but-unacked event in the current submission batch. Resets to 0 when no messages are in flight.",
		},
	)
)

func init() {
	submissionOnce.Do(func() {
		ctrlmetrics.Registry.MustRegister(
			submissionRequestsTotal,
			pipelineOldestUnsubmittedSeconds,
		)
	})
}

// recordSubmission increments the submission counter with the given status
// label. Valid labels: "success", "transient", "permanent".
func recordSubmission(status string) {
	submissionRequestsTotal.WithLabelValues(status).Inc()
}

// setOldestUnsubmittedAge sets the age gauge to the given number of seconds.
// Pass 0 after a batch drains to signal no in-flight messages.
func setOldestUnsubmittedAge(seconds float64) {
	pipelineOldestUnsubmittedSeconds.Set(seconds)
}
