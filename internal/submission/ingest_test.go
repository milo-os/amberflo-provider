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
	"time"

	"github.com/oklog/ulid/v2"
)

func TestUlidFromEventID_DeterministicForSameID(t *testing.T) {
	ceID := "test-event-id-12345"
	ceTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	first, err := ulidFromEventID(ceID, ceTime)
	if err != nil {
		t.Fatalf("ulidFromEventID first call: %v", err)
	}

	second, err := ulidFromEventID(ceID, ceTime)
	if err != nil {
		t.Fatalf("ulidFromEventID second call: %v", err)
	}

	if first != second {
		t.Errorf("same ceID produced different ULIDs: %q vs %q", first, second)
	}
}

func TestUlidFromEventID_ResultIsValidULID(t *testing.T) {
	ceID := "my-cloud-event-id"
	ceTime := time.Now()

	result, err := ulidFromEventID(ceID, ceTime)
	if err != nil {
		t.Fatalf("ulidFromEventID: %v", err)
	}

	if len(result) != 26 {
		t.Errorf("expected ULID length 26, got %d: %q", len(result), result)
	}

	// Verify it parses as a valid ULID.
	if _, parseErr := ulid.ParseStrict(result); parseErr != nil {
		t.Errorf("ulidFromEventID result %q is not a valid ULID: %v", result, parseErr)
	}
}

func TestUlidFromEventID_DifferentIDsProduceDifferentULIDs(t *testing.T) {
	ceTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	id1, err := ulidFromEventID("event-id-alpha", ceTime)
	if err != nil {
		t.Fatalf("ulidFromEventID alpha: %v", err)
	}

	id2, err := ulidFromEventID("event-id-beta", ceTime)
	if err != nil {
		t.Fatalf("ulidFromEventID beta: %v", err)
	}

	if id1 == id2 {
		t.Errorf("different ceIDs produced identical ULIDs: %q", id1)
	}
}

func TestUlidFromEventID_TimestampEmbedded(t *testing.T) {
	ceID := "event-with-time"
	ceTime := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	result, err := ulidFromEventID(ceID, ceTime)
	if err != nil {
		t.Fatalf("ulidFromEventID: %v", err)
	}

	parsed, err := ulid.ParseStrict(result)
	if err != nil {
		t.Fatalf("parse ULID: %v", err)
	}

	// The ULID's embedded timestamp should match ceTime to millisecond precision.
	embedded := time.UnixMilli(int64(parsed.Time()))
	wantMS := ceTime.UnixMilli()
	gotMS := embedded.UnixMilli()
	if gotMS != wantMS {
		t.Errorf("ULID timestamp: got %d ms want %d ms", gotMS, wantMS)
	}
}
