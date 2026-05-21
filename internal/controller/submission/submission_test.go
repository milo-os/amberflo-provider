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
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/apimachinery/pkg/types"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
)

// fakeMsg implements jetstream.Msg for testing processMessage without a
// real NATS server.
type fakeMsg struct {
	data     []byte
	subject  string
	acked    bool
	naked    bool
	nakDelay time.Duration
}

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Timestamp: time.Now()}, nil
}
func (m *fakeMsg) Data() []byte                       { return m.data }
func (m *fakeMsg) Headers() natsgo.Header             { return nil }
func (m *fakeMsg) Subject() string                    { return m.subject }
func (m *fakeMsg) Reply() string                      { return "" }
func (m *fakeMsg) Ack() error                         { m.acked = true; return nil }
func (m *fakeMsg) DoubleAck(_ context.Context) error  { m.acked = true; return nil }
func (m *fakeMsg) Nak() error                         { m.naked = true; return nil }
func (m *fakeMsg) NakWithDelay(d time.Duration) error { m.naked = true; m.nakDelay = d; return nil }
func (m *fakeMsg) InProgress() error                  { return nil }
func (m *fakeMsg) Term() error                        { return nil }
func (m *fakeMsg) TermWithReason(_ string) error      { return nil }

// fakeIngestClient implements amberflo.IngestClient for testing.
type fakeIngestClient struct {
	submitErr error
	received  []amberflo.UsageRecord
}

func (f *fakeIngestClient) SubmitUsage(_ context.Context, records []amberflo.UsageRecord) error {
	f.received = append(f.received, records...)
	return f.submitErr
}

// fakeCacheSync implements cacheSync for testing, always returning true.
type fakeCacheSync struct{}

func (f *fakeCacheSync) WaitForCacheSync(_ context.Context) bool { return true }

// buildCloudEventJSON builds a minimal valid CloudEvent JSON payload.
func buildCloudEventJSON(t *testing.T, id, ceType, billingAccountRef string, value int64, dims map[string]string) []byte {
	t.Helper()
	data := eventData{
		Value:      fmt.Sprintf("%d", value),
		Dimensions: dims,
	}
	rawData, _ := json.Marshal(data)
	ce := map[string]interface{}{
		"specversion":       "1.0",
		"id":                id,
		"type":              ceType,
		"source":            "test-source",
		"time":              time.Now().UTC().Format(time.RFC3339Nano),
		"datacontenttype":   "application/json",
		"billingaccountref": billingAccountRef,
		"data":              json.RawMessage(rawData),
	}
	b, err := json.Marshal(ce)
	if err != nil {
		t.Fatalf("buildCloudEventJSON: %v", err)
	}
	return b
}

// newTestConsumer returns a SubmissionConsumer backed by the provided caches.
func newTestConsumer(t *testing.T, ingestClient amberflo.IngestClient, meterCache *MeterDefinitionCache, baCache *BillingAccountCache) *SubmissionConsumer {
	t.Helper()
	return &SubmissionConsumer{
		Cache:               &fakeCacheSync{},
		IngestClient:        ingestClient,
		BillingAccountCache: baCache,
		MeterCache:          meterCache,
		Logger:              logr.Discard(),
		FetchBatch:          1,
		RetryAfter:          5 * time.Second,
		AckWait:             30 * time.Second,
		FetchTimeout:        5 * time.Second,
	}
}

// meterCacheWith returns a MeterDefinitionCache pre-populated with the given entries.
func meterCacheWith(entries map[string]types.UID) *MeterDefinitionCache {
	return &MeterDefinitionCache{uidByMeterName: entries}
}

// baCacheWith returns a BillingAccountCache pre-populated with the given entries.
func baCacheWith(entries map[string]types.UID) *BillingAccountCache {
	return &BillingAccountCache{uidByName: entries}
}

// TestProcessMessage_HappyPath verifies that a well-formed event is acked
// and the ingest client receives the correct UsageRecord.
func TestProcessMessage_HappyPath(t *testing.T) {
	meterName := "compute.miloapis.com/cpu"
	meterUID := types.UID("meter-uid-abc")
	baUID := types.UID("billing-account-uid-abc")

	ingest := &fakeIngestClient{}
	consumer := newTestConsumer(t, ingest,
		meterCacheWith(map[string]types.UID{meterName: meterUID}),
		baCacheWith(map[string]types.UID{"acct-xyz": baUID}),
	)

	msg := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-001", meterName, "acct-xyz", 100, map[string]string{"region": "us-east-1"}),
		subject: "billing.usage.proj-1.valid",
	}

	consumer.processMessages(context.Background(), []jetstream.Msg{msg})

	if !msg.acked {
		t.Error("expected message to be acked")
	}
	if msg.naked {
		t.Error("expected message NOT to be nacked")
	}
	if len(ingest.received) != 1 {
		t.Fatalf("expected 1 usage record submitted, got %d", len(ingest.received))
	}
	got := ingest.received[0]
	if got.CustomerID != string(baUID) {
		t.Errorf("CustomerID: got %q want %q", got.CustomerID, string(baUID))
	}
	if got.MeterAPIName != string(meterUID) {
		t.Errorf("MeterAPIName: got %q want %q", got.MeterAPIName, string(meterUID))
	}
	if got.MeterValue != 100 {
		t.Errorf("MeterValue: got %d want 100", got.MeterValue)
	}
	if got.UniqueID == "" {
		t.Error("expected non-empty UniqueID")
	}
}

// TestProcessMessage_MalformedCloudEvent verifies that a malformed CloudEvent
// is acked (discarded) without calling the ingest client.
func TestProcessMessage_MalformedCloudEvent(t *testing.T) {
	ingest := &fakeIngestClient{}
	consumer := newTestConsumer(t, ingest,
		meterCacheWith(map[string]types.UID{}),
		baCacheWith(map[string]types.UID{}),
	)

	msg := &fakeMsg{
		data:    []byte("this is not json"),
		subject: "billing.usage.proj-1.valid",
	}

	consumer.processMessages(context.Background(), []jetstream.Msg{msg})

	if !msg.acked {
		t.Error("expected malformed event to be acked (discarded)")
	}
	if len(ingest.received) != 0 {
		t.Error("expected no ingest call for malformed event")
	}
}

// TestProcessMessage_BillingAccountCacheMiss verifies that when the
// BillingAccount UID cannot be resolved, the message is nacked for retry.
func TestProcessMessage_BillingAccountCacheMiss(t *testing.T) {
	meterName := "compute.miloapis.com/cpu"
	meterUID := types.UID("meter-uid-abc")

	ingest := &fakeIngestClient{}
	consumer := newTestConsumer(t, ingest,
		meterCacheWith(map[string]types.UID{meterName: meterUID}),
		baCacheWith(map[string]types.UID{}), // empty — BA not yet indexed
	)

	msg := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-ba-miss", meterName, "acct-xyz", 10, nil),
		subject: "billing.usage.proj-1.valid",
	}

	consumer.processMessages(context.Background(), []jetstream.Msg{msg})

	if !msg.naked {
		t.Error("expected message to be nacked on billing account cache miss")
	}
	if len(ingest.received) != 0 {
		t.Error("expected no ingest call on billing account cache miss")
	}
}

// TestProcessMessage_MeterDefinitionCacheMiss verifies that when no
// MeterDefinition UID can be resolved, the message is nacked for retry.
func TestProcessMessage_MeterDefinitionCacheMiss(t *testing.T) {
	baUID := types.UID("billing-account-uid-abc")

	ingest := &fakeIngestClient{}
	consumer := newTestConsumer(t, ingest,
		meterCacheWith(map[string]types.UID{}), // empty — meter not indexed
		baCacheWith(map[string]types.UID{"acct-xyz": baUID}),
	)

	msg := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-002", "unknown.meter/type", "acct-xyz", 5, nil),
		subject: "billing.usage.proj-1.valid",
	}

	consumer.processMessages(context.Background(), []jetstream.Msg{msg})

	if !msg.naked {
		t.Error("expected message to be nacked on cache miss")
	}
	if len(ingest.received) != 0 {
		t.Error("expected no ingest call on cache miss")
	}
}

// TestProcessMessage_TransientIngestError verifies that a transient ingest
// error results in the message being nacked.
func TestProcessMessage_TransientIngestError(t *testing.T) {
	meterName := "compute.miloapis.com/mem"
	meterUID := types.UID("meter-uid-def")
	baUID := types.UID("billing-account-uid-def")

	ingest := &fakeIngestClient{
		submitErr: &amberflo.TransientError{Err: fmt.Errorf("503 service unavailable"), StatusCode: 503},
	}
	consumer := newTestConsumer(t, ingest,
		meterCacheWith(map[string]types.UID{meterName: meterUID}),
		baCacheWith(map[string]types.UID{"acct-xyz": baUID}),
	)

	msg := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-003", meterName, "acct-xyz", 50, nil),
		subject: "billing.usage.proj-1.valid",
	}

	consumer.processMessages(context.Background(), []jetstream.Msg{msg})

	if !msg.naked {
		t.Error("expected message to be nacked on transient ingest error")
	}
	if msg.acked {
		t.Error("expected message NOT to be acked on transient error")
	}
}

// TestProcessMessage_PermanentIngestError verifies that a permanent ingest
// error results in the message being acked (discarded after logging).
func TestProcessMessage_PermanentIngestError(t *testing.T) {
	meterName := "compute.miloapis.com/storage"
	meterUID := types.UID("meter-uid-ghi")
	baUID := types.UID("billing-account-uid-ghi")

	ingest := &fakeIngestClient{
		submitErr: &amberflo.PermanentError{Err: fmt.Errorf("invalid record"), StatusCode: 400},
	}
	consumer := newTestConsumer(t, ingest,
		meterCacheWith(map[string]types.UID{meterName: meterUID}),
		baCacheWith(map[string]types.UID{"acct-xyz": baUID}),
	)

	msg := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-004", meterName, "acct-xyz", 200, nil),
		subject: "billing.usage.proj-1.valid",
	}

	consumer.processMessages(context.Background(), []jetstream.Msg{msg})

	if !msg.acked {
		t.Error("expected message to be acked on permanent ingest error (discard)")
	}
	if msg.naked {
		t.Error("expected message NOT to be nacked on permanent error")
	}
}

// TestProcessMessage_MalformedEventDataValue verifies that an event whose
// data.value field is not a valid int64 string is acked (discarded) without
// calling the ingest client.
func TestProcessMessage_MalformedEventDataValue(t *testing.T) {
	meterName := "compute.miloapis.com/cpu"
	meterUID := types.UID("meter-uid-abc")
	baUID := types.UID("billing-account-uid-abc")

	ingest := &fakeIngestClient{}
	consumer := newTestConsumer(t, ingest,
		meterCacheWith(map[string]types.UID{meterName: meterUID}),
		baCacheWith(map[string]types.UID{"acct-xyz": baUID}),
	)

	// Build a CloudEvent with a non-integer data.value.
	raw := map[string]interface{}{
		"specversion":       "1.0",
		"id":                "evt-bad-value",
		"type":              meterName,
		"source":            "test",
		"time":              time.Now().UTC().Format(time.RFC3339Nano),
		"datacontenttype":   "application/json",
		"billingaccountref": "acct-xyz",
		"data":              json.RawMessage(`{"value":"not-a-number"}`),
	}
	data, _ := json.Marshal(raw)
	msg := &fakeMsg{data: data, subject: "billing.usage.proj-1.valid"}

	consumer.processMessages(context.Background(), []jetstream.Msg{msg})

	if !msg.acked {
		t.Error("expected message with non-integer value to be acked (discarded)")
	}
	if msg.naked {
		t.Error("expected message NOT to be nacked for malformed value")
	}
	if len(ingest.received) != 0 {
		t.Error("expected no ingest call for event with malformed data.value")
	}
}

// TestProcessMessage_MissingBillingAccountRef verifies that an event without
// the billingaccountref extension is acked (discarded) as a permanent error.
func TestProcessMessage_MissingBillingAccountRef(t *testing.T) {
	ingest := &fakeIngestClient{}
	consumer := newTestConsumer(t, ingest,
		meterCacheWith(map[string]types.UID{}),
		baCacheWith(map[string]types.UID{}),
	)

	// CloudEvent without billingaccountref extension.
	ce := map[string]interface{}{
		"specversion":     "1.0",
		"id":              "evt-005",
		"type":            "compute.miloapis.com/cpu",
		"source":          "test",
		"time":            time.Now().UTC().Format(time.RFC3339Nano),
		"datacontenttype": "application/json",
		"data":            json.RawMessage(`{"value":"10"}`),
	}
	data, _ := json.Marshal(ce)

	msg := &fakeMsg{data: data, subject: "billing.usage.proj-1.valid"}

	consumer.processMessages(context.Background(), []jetstream.Msg{msg})

	if !msg.acked {
		t.Error("expected message without billingaccountref to be acked (discarded)")
	}
	if len(ingest.received) != 0 {
		t.Error("expected no ingest call for event missing billingaccountref")
	}
}

// fakeBatchIngestClient lets us mock SubmitUsage for specific lists of records.
type fakeBatchIngestClient struct {
	submitFunc func(ctx context.Context, records []amberflo.UsageRecord) error
	received   []amberflo.UsageRecord
}

func (f *fakeBatchIngestClient) SubmitUsage(ctx context.Context, records []amberflo.UsageRecord) error {
	f.received = append(f.received, records...)
	return f.submitFunc(ctx, records)
}

func TestProcessMessages_HappyPath_Batch(t *testing.T) {
	meterName := "compute.miloapis.com/cpu"
	meterUID := types.UID("meter-uid-abc")
	baUID := types.UID("billing-account-uid-abc")

	calledBatch := false
	ingest := &fakeBatchIngestClient{
		submitFunc: func(ctx context.Context, records []amberflo.UsageRecord) error {
			if len(records) == 2 {
				calledBatch = true
			}
			return nil
		},
	}
	consumer := newTestConsumer(t, ingest,
		meterCacheWith(map[string]types.UID{meterName: meterUID}),
		baCacheWith(map[string]types.UID{"acct-xyz": baUID}),
	)

	msg1 := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-001", meterName, "acct-xyz", 100, nil),
		subject: "billing.usage.proj-1.valid",
	}
	msg2 := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-002", meterName, "acct-xyz", 200, nil),
		subject: "billing.usage.proj-1.valid",
	}

	consumer.processMessages(context.Background(), []jetstream.Msg{msg1, msg2})

	if !calledBatch {
		t.Error("expected SubmitUsage to be called as a batch of 2")
	}
	if !msg1.acked || !msg2.acked {
		t.Error("expected both messages to be acked")
	}
	if len(ingest.received) != 2 {
		t.Fatalf("expected 2 usage records received, got %d", len(ingest.received))
	}
}

func TestProcessMessages_TransientValidationAndSuccess(t *testing.T) {
	meterName := "compute.miloapis.com/cpu"
	meterUID := types.UID("meter-uid-abc")
	baUID := types.UID("billing-account-uid-abc")

	ingest := &fakeBatchIngestClient{
		submitFunc: func(ctx context.Context, records []amberflo.UsageRecord) error {
			return nil
		},
	}
	consumer := newTestConsumer(t, ingest,
		meterCacheWith(map[string]types.UID{meterName: meterUID}),
		baCacheWith(map[string]types.UID{"acct-xyz": baUID}),
	)

	// Valid message
	msgValid := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-valid", meterName, "acct-xyz", 100, nil),
		subject: "billing.usage.proj-1.valid",
	}
	// Malformed (permanent validation error)
	msgMalformed := &fakeMsg{
		data:    []byte("not json"),
		subject: "billing.usage.proj-1.valid",
	}
	// Cache miss (transient validation error)
	msgCacheMiss := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-miss", "unknown-meter", "acct-xyz", 100, nil),
		subject: "billing.usage.proj-1.valid",
	}

	consumer.processMessages(context.Background(), []jetstream.Msg{msgValid, msgMalformed, msgCacheMiss})

	if !msgValid.acked {
		t.Error("expected valid message to be acked")
	}
	if !msgMalformed.acked {
		t.Error("expected malformed message to be acked (discarded)")
	}
	if !msgCacheMiss.naked {
		t.Error("expected cache miss message to be nacked (transient)")
	}

	if len(ingest.received) != 1 {
		t.Fatalf("expected 1 record to be submitted, got %d", len(ingest.received))
	}
	if ingest.received[0].UniqueID == "" {
		t.Error("expected non-empty UniqueID for valid message")
	}
}

func TestProcessMessages_BatchPermanentErrorFallback(t *testing.T) {
	meterName := "compute.miloapis.com/cpu"
	meterUID := types.UID("meter-uid-abc")
	baUID := types.UID("billing-account-uid-abc")

	// We want to simulate Amberflo rejecting the batch of 2,
	// but succeeding when we send them individually except for the bad one.
	calls := 0
	ingest := &fakeBatchIngestClient{
		submitFunc: func(ctx context.Context, records []amberflo.UsageRecord) error {
			calls++
			if len(records) == 2 {
				// Reject the batch with a permanent error
				return &amberflo.PermanentError{Err: fmt.Errorf("bad batch"), StatusCode: 400}
			}
			// When processing individually, reject the record with MeterValue = 999
			if records[0].MeterValue == 999 {
				return &amberflo.PermanentError{Err: fmt.Errorf("bad value"), StatusCode: 400}
			}
			return nil
		},
	}
	consumer := newTestConsumer(t, ingest,
		meterCacheWith(map[string]types.UID{meterName: meterUID}),
		baCacheWith(map[string]types.UID{"acct-xyz": baUID}),
	)

	msgGood := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-good", meterName, "acct-xyz", 100, nil),
		subject: "billing.usage.proj-1.valid",
	}
	msgBad := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-bad", meterName, "acct-xyz", 999, nil),
		subject: "billing.usage.proj-1.valid",
	}

	consumer.processMessages(context.Background(), []jetstream.Msg{msgGood, msgBad})

	// Total calls: 1 (batch) + 2 (fallback) = 3 calls
	if calls != 3 {
		t.Errorf("expected 3 SubmitUsage calls, got %d", calls)
	}

	if !msgGood.acked {
		t.Error("expected good message to be acked")
	}
	if !msgBad.acked {
		t.Error("expected bad message to be acked (discarded) after permanent individual error")
	}
}

func TestProcessMessages_BatchTransientError(t *testing.T) {
	meterName := "compute.miloapis.com/cpu"
	meterUID := types.UID("meter-uid-abc")
	baUID := types.UID("billing-account-uid-abc")

	ingest := &fakeBatchIngestClient{
		submitFunc: func(ctx context.Context, records []amberflo.UsageRecord) error {
			return &amberflo.TransientError{Err: fmt.Errorf("503 service unavailable"), StatusCode: 503}
		},
	}
	consumer := newTestConsumer(t, ingest,
		meterCacheWith(map[string]types.UID{meterName: meterUID}),
		baCacheWith(map[string]types.UID{"acct-xyz": baUID}),
	)

	msg1 := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-001", meterName, "acct-xyz", 100, nil),
		subject: "billing.usage.proj-1.valid",
	}
	msg2 := &fakeMsg{
		data:    buildCloudEventJSON(t, "evt-002", meterName, "acct-xyz", 200, nil),
		subject: "billing.usage.proj-1.valid",
	}

	consumer.processMessages(context.Background(), []jetstream.Msg{msg1, msg2})

	if !msg1.naked || !msg2.naked {
		t.Error("expected both messages to be nacked on transient batch error")
	}
}
