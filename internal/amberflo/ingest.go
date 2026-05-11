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
	"net/http"
)

// IngestClient is the subset of Client used by SubmissionConsumer. Defined
// as a narrow interface so unit tests can substitute a fake without
// implementing the full Client contract.
type IngestClient interface {
	SubmitUsage(ctx context.Context, record UsageRecord) error
}

// UsageRecord is a single metered usage event to submit to Amberflo.
type UsageRecord struct {
	CustomerID    string
	MeterAPIName  string
	MeterValue    int64
	UniqueID      string
	UTCTimeMillis int64
	Dimensions    map[string]string
}

// wireUsageRecord is the JSON shape sent to the Amberflo ingest endpoint.
// Field names follow Amberflo's camelCase API convention (consistent with
// wireMeter in meter.go).
//
// REQUIRES SANDBOX CONFIRMATION: "meterValue" may be "ingestionValue".
// Only the json tag needs changing — no logic change required.
type wireUsageRecord struct {
	CustomerID    string            `json:"customerId"`
	MeterAPIName  string            `json:"meterApiName"`
	MeterValue    int64             `json:"meterValue"`
	UniqueID      string            `json:"uniqueId"`
	UTCTimeMillis int64             `json:"utcTimeMillis"`
	Dimensions    map[string]string `json:"dimensions,omitempty"`
}

// SubmitUsage posts a single usage record to the Amberflo ingest API.
func (c *client) SubmitUsage(ctx context.Context, record UsageRecord) error {
	w := wireUsageRecord{
		CustomerID:    record.CustomerID,
		MeterAPIName:  record.MeterAPIName,
		MeterValue:    record.MeterValue,
		UniqueID:      record.UniqueID,
		UTCTimeMillis: record.UTCTimeMillis,
		Dimensions:    record.Dimensions,
	}
	_, _, err := c.doJSON(ctx, http.MethodPost, "/ingest", []wireUsageRecord{w}, nil)
	return err
}
