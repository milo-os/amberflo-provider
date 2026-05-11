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
	"bytes"
	"crypto/sha256"
	"time"

	"github.com/oklog/ulid/v2"
)

// ulidFromEventID derives a deterministic ULID from a CloudEvent ID.
//
// Algorithm:
//  1. sha256(ceID) → [32]byte
//  2. Take first 10 bytes as entropy (satisfies ulid.New's 10-byte entropy reader)
//  3. Encode as ULID with timestamp = ceTime.UnixMilli()
//
// The same ceID always produces the same ULID, guaranteeing no double-billing
// when the same event is retried after a transient failure.
func ulidFromEventID(ceID string, ceTime time.Time) (string, error) {
	sum := sha256.Sum256([]byte(ceID))
	entropy := bytes.NewReader(sum[:10])
	ms := ulid.Timestamp(ceTime)
	id, err := ulid.New(ms, entropy)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}
