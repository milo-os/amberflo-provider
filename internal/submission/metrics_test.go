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
	"testing"

	dto "github.com/prometheus/client_model/go"
)

// TestSubmissionMetrics_CounterIncrement verifies that counter increments
// are reflected in the in-process Prometheus registry.
func TestSubmissionMetrics_CounterIncrement(t *testing.T) {
	beforeSuccess := readCounter(t, "success")
	beforeTransient := readCounter(t, "transient")
	beforePermanent := readCounter(t, "permanent")

	recordSubmission("success")
	recordSubmission("transient")
	recordSubmission("transient")
	recordSubmission("permanent")

	if got := readCounter(t, "success") - beforeSuccess; got != 1 {
		t.Errorf("success counter: expected 1 increment, got %.0f", got)
	}
	if got := readCounter(t, "transient") - beforeTransient; got != 2 {
		t.Errorf("transient counter: expected 2 increments, got %.0f", got)
	}
	if got := readCounter(t, "permanent") - beforePermanent; got != 1 {
		t.Errorf("permanent counter: expected 1 increment, got %.0f", got)
	}
}

// TestSubmissionMetrics_GaugeSetAndReset verifies gauge updates and reset.
func TestSubmissionMetrics_GaugeSetAndReset(t *testing.T) {
	setOldestUnsubmittedAge(42.5)
	if got := readGauge(t); got != 42.5 {
		t.Errorf("gauge after set: got %v want 42.5", got)
	}

	setOldestUnsubmittedAge(0)
	if got := readGauge(t); got != 0 {
		t.Errorf("gauge after reset: got %v want 0", got)
	}
}

// readCounter reads the current counter value for the given status label.
func readCounter(t *testing.T, status string) float64 {
	t.Helper()
	var m dto.Metric
	if err := submissionRequestsTotal.WithLabelValues(status).Write(&m); err != nil {
		t.Fatalf("readCounter(%q): %v", status, err)
	}
	if m.Counter == nil {
		return 0
	}
	return m.Counter.GetValue()
}

// readGauge reads the current value of the oldest unsubmitted seconds gauge.
func readGauge(t *testing.T) float64 {
	t.Helper()
	var m dto.Metric
	if err := pipelineOldestUnsubmittedSeconds.Write(&m); err != nil {
		t.Fatalf("readGauge: %v", err)
	}
	if m.Gauge == nil {
		return 0
	}
	return m.Gauge.GetValue()
}
