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

// Package submission provides the SubmissionConsumer, a manager.Runnable
// that dequeues validated CloudEvents from billing.usage.*.valid via a
// durable NATS JetStream pull consumer and submits them to the Amberflo
// ingest API.
package submission

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/go-logr/logr"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
)

// cacheSync is the subset of cache.Cache used by SubmissionConsumer.
type cacheSync interface {
	WaitForCacheSync(ctx context.Context) bool
}

const (
	// billingUsageStream is the JetStream stream that carries validated
	// and attributed usage CloudEvents from the billing pipeline.
	billingUsageStream = "billing-usage"

	// durableConsumerName is the durable pull consumer name. The name is
	// stable across restarts; JetStream persists the ack sequence so the
	// consumer resumes from the last acked message after a pod restart.
	durableConsumerName = "amberflo-usage-submitter"

	// filterSubject matches all validated events across all projects.
	filterSubject = "billing.usage.*.valid"

	// ackWait is how long the server waits for an ack before redelivering.
	ackWait = 30 * time.Second

	// ConsumerFetchTimeout is the per-Fetch call timeout. Using 5 seconds
	// keeps the consumer responsive to context cancellation.
	ConsumerFetchTimeout = 5 * time.Second

	// billingAccountRefExtension is the CloudEvent extension attribute
	// name carrying the BillingAccount name set by the attribution stage.
	billingAccountRefExtension = "billingaccountref"
)

// eventData is the payload of a validated usage CloudEvent published by
// the billing pipeline. The Value field uses string encoding to preserve
// int64 range across JSON serialisers.
type eventData struct {
	// Value is the usage quantity, string-encoded int64.
	Value string `json:"value"`
	// Dimensions are the optional attribute key/value pairs from the
	// original usage event.
	Dimensions map[string]string `json:"dimensions,omitempty"`
}

// SubmissionConsumer is a manager.Runnable that dequeues validated CloudEvents
// from billing.usage.*.valid and submits them to the Amberflo ingest API.
//
// The consumer is registered in main.go only when NATSConfig is non-nil;
// existing deployments without NATS are unaffected.
type SubmissionConsumer struct {
	// Cache is used to wait for the informer caches to sync before
	// processing starts.
	Cache cacheSync

	// NC is the shared NATS connection.
	NC *natsgo.Conn

	// IngestClient is the Amberflo ingest API client.
	IngestClient amberflo.IngestClient

	// BillingAccountCache maps BillingAccount names to their UIDs.
	// The UID is the Amberflo customerId.
	BillingAccountCache *BillingAccountCache

	// MeterCache maps MeterDefinition spec.meterName values to their UIDs.
	// The UID is the Amberflo meterApiName.
	MeterCache *MeterDefinitionCache

	// Logger is the structured logger.
	Logger logr.Logger

	// FetchBatch is the number of messages to fetch per pull. Defaults to 1.
	FetchBatch int
}

// Start implements manager.Runnable. It blocks until ctx is cancelled,
// continuously pulling and processing messages from JetStream.
func (c *SubmissionConsumer) Start(ctx context.Context) error {
	if c.FetchBatch <= 0 {
		c.FetchBatch = 1
	}

	// Wait for the informer cache to be populated so the BillingAccount and
	// MeterDefinition caches are ready before we touch any messages.
	if !c.Cache.WaitForCacheSync(ctx) {
		return fmt.Errorf("submission consumer: cache sync failed or context cancelled")
	}

	js, err := jetstream.New(c.NC)
	if err != nil {
		return fmt.Errorf("submission consumer: create jetstream context: %w", err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, billingUsageStream, jetstream.ConsumerConfig{
		Durable:       durableConsumerName,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxAckPending: -1,
		AckWait:       ackWait,
	})
	if err != nil {
		return fmt.Errorf("submission consumer: create/update durable consumer %q: %w", durableConsumerName, err)
	}

	c.Logger.Info("submission consumer started",
		"stream", billingUsageStream,
		"consumer", durableConsumerName,
		"fetchBatch", c.FetchBatch,
	)

	for {
		if ctx.Err() != nil {
			break
		}

		msgs, err := cons.Fetch(c.FetchBatch, jetstream.FetchMaxWait(ConsumerFetchTimeout))
		if err != nil {
			// Fetch timeout is expected when the stream is idle; continue.
			c.Logger.V(1).Info("fetch returned error (may be idle timeout)", "err", err)
			continue
		}

		// Track the oldest message in this batch to report processing lag metrics.
		var oldestTimestamp time.Time
		for msg := range msgs.Messages() {
			meta, metaErr := msg.Metadata()
			if metaErr == nil && !meta.Timestamp.IsZero() {
				// Update the batch-local oldest timestamp.
				if oldestTimestamp.IsZero() || meta.Timestamp.Before(oldestTimestamp) {
					oldestTimestamp = meta.Timestamp
				}
				// Update the "age" gauge with the current message's duration in the stream.
				setOldestUnsubmittedAge(time.Since(meta.Timestamp).Seconds())
			}

			// Process the individual message: resolve IDs and submit to Amberflo.
			if processErr := c.processMessage(ctx, msg); processErr != nil {
				c.Logger.Error(processErr, "processMessage error; nacking",
					"subject", msg.Subject(),
				)
				// Negative Acknowledgement (Nak) ensures the message is redelivered
				// and not lost if processing failed (e.g., transient network issues).
				if nakErr := msg.Nak(); nakErr != nil {
					c.Logger.Error(nakErr, "failed to nack message", "subject", msg.Subject())
				}
			}
		}

		if fetchErr := msgs.Error(); fetchErr != nil {
			c.Logger.V(1).Info("message batch error", "err", fetchErr)
		}

		// Reset the age gauge after each batch drains.
		setOldestUnsubmittedAge(0)
	}

	setOldestUnsubmittedAge(0)
	c.Logger.Info("submission consumer stopped")
	return nil
}

// processMessage processes a single JetStream message: resolves the Amberflo
// customer and meter IDs from the informer caches, submits the usage record,
// and acks or nacks based on the outcome.
func (c *SubmissionConsumer) processMessage(ctx context.Context, msg jetstream.Msg) error {
	// Deserialise the CloudEvent from the NATS message body.
	var ce cloudevents.Event
	if err := json.Unmarshal(msg.Data(), &ce); err != nil {
		c.Logger.Error(err, "malformed CloudEvent; discarding",
			"subject", msg.Subject(),
		)
		recordSubmission("permanent")
		return msg.Ack()
	}

	// Extract the billing account name from the CloudEvent extension.
	exts := ce.Extensions()
	billingAccountRef, ok := exts[billingAccountRefExtension]
	if !ok || billingAccountRef == nil {
		c.Logger.Error(nil, "CloudEvent missing billingaccountref extension; discarding",
			"eventID", ce.ID(),
		)
		recordSubmission("permanent")
		return msg.Ack()
	}
	baName := fmt.Sprintf("%v", billingAccountRef)

	// Resolve the BillingAccount UID — this is the Amberflo customerId.
	baUID, ok := c.BillingAccountCache.GetUID(baName)
	if !ok {
		c.Logger.Info("BillingAccount not found in cache; nacking for retry",
			"billingAccountRef", baName,
			"eventID", ce.ID(),
		)
		return msg.Nak()
	}
	customerID := string(baUID)

	// Resolve the MeterDefinition UID — this is the Amberflo meterApiName.
	meterUID, ok := c.MeterCache.GetUID(ce.Type())
	if !ok {
		c.Logger.Info("MeterDefinition not found for meterName; nacking for retry",
			"meterName", ce.Type(),
			"eventID", ce.ID(),
		)
		return msg.Nak()
	}
	meterAPIName := string(meterUID)

	// Deserialise the event payload.
	var data eventData
	if err := ce.DataAs(&data); err != nil {
		c.Logger.Error(err, "failed to decode CloudEvent data; discarding",
			"eventID", ce.ID(),
		)
		recordSubmission("permanent")
		return msg.Ack()
	}

	// Parse the string-encoded int64 value.
	var meterValue int64
	if _, err := fmt.Sscanf(data.Value, "%d", &meterValue); err != nil {
		c.Logger.Error(err, "CloudEvent data.value is not a valid int64; discarding",
			"eventID", ce.ID(),
			"value", data.Value,
		)
		recordSubmission("permanent")
		return msg.Ack()
	}

	// Derive the deterministic ULID idempotency key.
	uniqueID, err := ulidFromEventID(ce.ID(), ce.Time())
	if err != nil {
		c.Logger.Error(err, "failed to derive ULID from CloudEvent ID; discarding",
			"eventID", ce.ID(),
		)
		recordSubmission("permanent")
		return msg.Ack()
	}

	record := amberflo.UsageRecord{
		CustomerID:    customerID,
		MeterAPIName:  meterAPIName,
		MeterValue:    meterValue,
		UniqueID:      uniqueID,
		UTCTimeMillis: ce.Time().UnixMilli(),
		Dimensions:    data.Dimensions,
	}

	submitErr := c.IngestClient.SubmitUsage(ctx, record)
	if submitErr == nil {
		recordSubmission("success")
		return msg.Ack()
	}

	if amberflo.IsPermanent(submitErr) {
		c.Logger.Error(submitErr, "permanent Amberflo ingest error; discarding event",
			"eventID", ce.ID(),
			"customerID", customerID,
			"meterAPIName", meterAPIName,
		)
		recordSubmission("permanent")
		return msg.Ack()
	}

	// Transient (5xx/429/network): nack for redelivery.
	c.Logger.V(1).Info("transient Amberflo ingest error; nacking for retry",
		"eventID", ce.ID(),
		"err", submitErr,
	)
	recordSubmission("transient")
	return msg.Nak()
}
