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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ingestTestServer is a minimal httptest server for IngestClient tests.
type ingestTestServer struct {
	srv        *httptest.Server
	apiKey     string
	statusCode int
	retryAfter string // set to test Retry-After header handling
	received   []json.RawMessage
}

func newIngestTestServer(statusCode int) *ingestTestServer {
	s := &ingestTestServer{
		apiKey:     "test-ingest-key",
		statusCode: statusCode,
	}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-KEY") != s.apiKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(r.Body)
		s.received = append(s.received, json.RawMessage(body))
		if s.retryAfter != "" {
			w.Header().Set("Retry-After", s.retryAfter)
		}
		w.WriteHeader(s.statusCode)
	}))
	return s
}

func (s *ingestTestServer) Close()      { s.srv.Close() }
func (s *ingestTestServer) URL() string { return s.srv.URL }

func newIngestClientForTest(t *testing.T, baseURL, apiKey string) IngestClient {
	t.Helper()
	c, err := NewClient(ClientOptions{
		BaseURL:         baseURL,
		APIKey:          apiKey,
		RateLimitPerSec: 1000,
		RetryAttempts:   1,
		sleep: func(_ context.Context, _ time.Duration) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func sampleRecord() UsageRecord {
	return UsageRecord{
		CustomerID:    "cust-123",
		MeterAPIName:  "meter-456",
		MeterValue:    42,
		UniqueID:      "01J2Q4MJFVWBR6PQBXDQSTH00K",
		UTCTimeMillis: 1700000000000,
		Dimensions:    map[string]string{"region": "us-east-1"},
	}
}

func TestNewIngestClient_RequiresAPIKey(t *testing.T) {
	_, err := NewClient(ClientOptions{
		BaseURL: "https://ingest.amberflo.io",
		APIKey:  "",
	})
	if err == nil {
		t.Fatal("expected error for empty APIKey")
	}
}

func TestNewIngestClient_InvalidBaseURL(t *testing.T) {
	_, err := NewClient(ClientOptions{
		BaseURL: "not-a-url",
		APIKey:  "key",
	})
	if err == nil {
		t.Fatal("expected error for invalid BaseURL")
	}
}

func TestIngestClient_SubmitUsage_Success(t *testing.T) {
	s := newIngestTestServer(http.StatusOK)
	defer s.Close()

	c := newIngestClientForTest(t, s.URL(), s.apiKey)
	rec := sampleRecord()

	if err := c.SubmitUsage(context.Background(), rec); err != nil {
		t.Fatalf("SubmitUsage: unexpected error: %v", err)
	}

	if len(s.received) != 1 {
		t.Fatalf("expected 1 request to ingest server, got %d", len(s.received))
	}

	// Verify the JSON body is a one-element array with the expected fields.
	var records []wireUsageRecord
	if err := json.Unmarshal(s.received[0], &records); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record in array, got %d", len(records))
	}
	got := records[0]
	if got.CustomerID != rec.CustomerID {
		t.Errorf("customerId: got %q want %q", got.CustomerID, rec.CustomerID)
	}
	if got.MeterAPIName != rec.MeterAPIName {
		t.Errorf("meterApiName: got %q want %q", got.MeterAPIName, rec.MeterAPIName)
	}
	if got.MeterValue != rec.MeterValue {
		t.Errorf("meterValue: got %d want %d", got.MeterValue, rec.MeterValue)
	}
	if got.UniqueID != rec.UniqueID {
		t.Errorf("uniqueId: got %q want %q", got.UniqueID, rec.UniqueID)
	}
	if got.UTCTimeMillis != rec.UTCTimeMillis {
		t.Errorf("utcTimeMillis: got %d want %d", got.UTCTimeMillis, rec.UTCTimeMillis)
	}
}

func TestIngestClient_SubmitUsage_500IsTransient(t *testing.T) {
	s := newIngestTestServer(http.StatusInternalServerError)
	defer s.Close()

	c := newIngestClientForTest(t, s.URL(), s.apiKey)

	err := c.SubmitUsage(context.Background(), sampleRecord())
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if !IsTransient(err) {
		t.Errorf("expected TransientError, got %T: %v", err, err)
	}
}

func TestIngestClient_SubmitUsage_400IsPermanent(t *testing.T) {
	s := newIngestTestServer(http.StatusBadRequest)
	defer s.Close()

	c := newIngestClientForTest(t, s.URL(), s.apiKey)

	err := c.SubmitUsage(context.Background(), sampleRecord())
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	if !IsPermanent(err) {
		t.Errorf("expected PermanentError, got %T: %v", err, err)
	}
}

func TestIngestClient_SubmitUsage_429IsTransient(t *testing.T) {
	s := newIngestTestServer(http.StatusTooManyRequests)
	s.retryAfter = "1"
	defer s.Close()

	c := newIngestClientForTest(t, s.URL(), s.apiKey)

	err := c.SubmitUsage(context.Background(), sampleRecord())
	if err == nil {
		t.Fatal("expected error for 429 response")
	}
	if !IsTransient(err) {
		t.Errorf("expected TransientError for 429, got %T: %v", err, err)
	}
}

func TestIngestClient_SubmitUsage_NetworkErrorIsTransient(t *testing.T) {
	// Point the client at a server that refuses connections.
	c := newIngestClientForTest(t, "http://127.0.0.1:1", "test-key")

	err := c.SubmitUsage(context.Background(), sampleRecord())
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
	if !IsTransient(err) {
		t.Errorf("expected TransientError for network error, got %T: %v", err, err)
	}
}

func TestIngestClient_SubmitUsage_NoDimensionsOmitted(t *testing.T) {
	s := newIngestTestServer(http.StatusOK)
	defer s.Close()

	c := newIngestClientForTest(t, s.URL(), s.apiKey)

	rec := sampleRecord()
	rec.Dimensions = nil

	if err := c.SubmitUsage(context.Background(), rec); err != nil {
		t.Fatalf("SubmitUsage: %v", err)
	}

	var records []wireUsageRecord
	_ = json.Unmarshal(s.received[0], &records)
	if records[0].Dimensions != nil {
		t.Errorf("expected nil Dimensions to be omitted, got %v", records[0].Dimensions)
	}
}
