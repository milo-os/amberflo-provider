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

package webhook

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
	"go.miloapis.com/amberflo-provider/internal/controller"
	"go.miloapis.com/amberflo-provider/internal/invoice"
)

const (
	// Endpoint is the path registered on the controller-runtime webhook
	// server for Amberflo invoice events.
	Endpoint = "/amberflo/invoices"

	// DefaultSecretHeader is the HTTP header Amberflo adds when a
	// ready-product-invoices webhook is registered with authHeader
	// ["X-Auth", "<secret>"]. See Amberflo Invoice Ready Webhook docs.
	DefaultSecretHeader = "X-Auth"

	// TopicReadyProductInvoices is Amberflo's invoice-ready event topic.
	TopicReadyProductInvoices = "ready-product-invoices"

	maxBodyBytes = 1 << 18
)

var log = ctrl.Log.WithName("amberflo-invoice-webhook")

// InvoiceHandler receives Amberflo ready-product-invoices events, verifies
// the shared secret (X-Auth by default), resolves the BillingAccount by
// UID, refreshes invoice detail from Amberflo, and upserts Milo Invoice
// resources.
type InvoiceHandler struct {
	Client         client.Client
	AmberfloClient amberflo.Client
	Syncer         *invoice.Syncer

	// Secret is the expected shared-secret value. Compared with
	// constant-time equality against the configured request header.
	Secret string

	// SecretHeader is the request header that carries the shared secret.
	// Defaults to DefaultSecretHeader (X-Auth) when empty.
	SecretHeader string
}

// SetupWithManager registers the handler on the manager's webhook server.
func (h *InvoiceHandler) SetupWithManager(mgr ctrl.Manager) error {
	if h.Client == nil {
		h.Client = mgr.GetClient()
	}
	if h.Syncer == nil {
		h.Syncer = &invoice.Syncer{Client: h.Client, Log: log}
	}
	mgr.GetWebhookServer().Register(Endpoint, h)
	return nil
}

// ServeHTTP implements http.Handler.
func (h *InvoiceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !h.authorize(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		log.Error(err, "reading webhook body")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	event, err := parseInvoiceReadyEvent(body)
	if err != nil {
		log.Info("webhook payload unusable", "err", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if event.CustomerID == "" {
		log.Info("webhook payload missing customerId")
		http.Error(w, "customerId required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if err := h.handle(ctx, event); err != nil {
		log.Error(err, "handling Amberflo invoice webhook",
			"customerID", event.CustomerID,
			"invoiceUri", event.InvoiceURI,
		)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func (h *InvoiceHandler) authorize(r *http.Request) bool {
	if h.Secret == "" {
		// Misconfiguration: refuse rather than accept unverified traffic.
		return false
	}
	header := h.SecretHeader
	if header == "" {
		header = DefaultSecretHeader
	}
	got := strings.TrimSpace(r.Header.Get(header))
	return subtle.ConstantTimeCompare([]byte(got), []byte(h.Secret)) == 1
}

func (h *InvoiceHandler) handle(ctx context.Context, event invoiceReadyEvent) error {
	account, err := h.lookupBillingAccount(ctx, event.CustomerID)
	if err != nil {
		return err
	}
	if account == nil {
		// Unknown customer — acknowledge so Amberflo does not retry forever
		// for deleted accounts, but log for operators.
		log.Info("no BillingAccount for Amberflo customerId; ignoring",
			"customerID", event.CustomerID,
			"invoiceUri", event.InvoiceURI,
		)
		return nil
	}

	invoices, err := h.resolveInvoices(ctx, event)
	if err != nil {
		return err
	}
	for i := range invoices {
		if err := h.Syncer.Upsert(ctx, account, invoices[i]); err != nil {
			return fmt.Errorf("sync invoice for %s: %w", event.CustomerID, err)
		}
	}
	return nil
}

// resolveInvoices prefers a keyed GetInvoice (Amberflo docs: payload data
// matches the Customer Invoice API; call it to verify status), then an
// embedded payload invoice, then a full ListInvoices fallback.
func (h *InvoiceHandler) resolveInvoices(
	ctx context.Context,
	event invoiceReadyEvent,
) ([]amberflo.CustomerProductInvoice, error) {
	if event.Key.Complete() {
		inv, err := h.AmberfloClient.GetInvoice(ctx, event.Key)
		if err == nil {
			return []amberflo.CustomerProductInvoice{inv}, nil
		}
		if !errors.Is(err, amberflo.ErrInvoiceNotFound) && !amberflo.IsPermanent(err) {
			return nil, fmt.Errorf("get Amberflo invoice %s: %w", amberflo.FormatInvoiceKey(event.Key), err)
		}
		log.Info("GetInvoice missed; falling back",
			"customerID", event.CustomerID,
			"invoiceKey", amberflo.FormatInvoiceKey(event.Key),
			"err", err,
		)
	}

	if event.Embedded != nil && event.Embedded.InvoiceStartTimeInSeconds > 0 {
		inv := *event.Embedded
		if inv.InvoiceKey.CustomerID == "" {
			inv.InvoiceKey = event.Key
		}
		if inv.InvoiceURI == "" {
			inv.InvoiceURI = event.InvoiceURI
		}
		return []amberflo.CustomerProductInvoice{inv}, nil
	}

	invoices, err := h.AmberfloClient.ListInvoices(ctx, event.CustomerID)
	if err != nil {
		return nil, fmt.Errorf("list Amberflo invoices for %s: %w", event.CustomerID, err)
	}
	return invoices, nil
}

func (h *InvoiceHandler) lookupBillingAccount(ctx context.Context, customerID string) (*billingv1alpha1.BillingAccount, error) {
	var list billingv1alpha1.BillingAccountList
	if err := h.Client.List(ctx, &list, client.MatchingFields{controller.BillingAccountUIDField: customerID}); err != nil {
		// Index may be unavailable in unit tests; fall back to list+filter.
		log.V(1).Info("BillingAccount uid index lookup failed; falling back to list",
			"err", err.Error())
		if listErr := h.Client.List(ctx, &list); listErr != nil {
			return nil, fmt.Errorf("list BillingAccounts: %w", listErr)
		}
		for i := range list.Items {
			if string(list.Items[i].UID) == customerID {
				return &list.Items[i], nil
			}
		}
		return nil, nil
	}
	if len(list.Items) == 0 {
		return nil, nil
	}
	return &list.Items[0], nil
}

// invoiceReadyEvent is the subset of Amberflo's ready-product-invoices
// payload we need. Per Amberflo docs, `data` mirrors the Customer Invoice
// API response and includes invoiceUri plus the composite invoice key.
type invoiceReadyEvent struct {
	CustomerID string
	InvoiceURI string
	Key        amberflo.InvoiceKey
	Embedded   *amberflo.CustomerProductInvoice
}

func parseInvoiceReadyEvent(body []byte) (invoiceReadyEvent, error) {
	if len(body) == 0 {
		return invoiceReadyEvent{}, fmt.Errorf("empty body")
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return invoiceReadyEvent{}, err
	}

	// Prefer nested data (Amberflo envelope); fall back to top-level.
	dataRaw, hasData := top["data"]
	if !hasData {
		dataRaw = body
	}

	var data map[string]json.RawMessage
	if err := json.Unmarshal(dataRaw, &data); err != nil {
		return invoiceReadyEvent{}, fmt.Errorf("decode data: %w", err)
	}

	event := invoiceReadyEvent{}
	event.CustomerID = stringField(data, "customerId", "customerID", "customer_id")
	if event.CustomerID == "" {
		event.CustomerID = stringField(top, "customerId", "customerID", "customer_id")
	}
	event.InvoiceURI = stringField(data, "invoiceUri", "invoiceURI")

	event.Key = parseInvoiceKey(data)
	if event.Key.CustomerID == "" {
		event.Key.CustomerID = event.CustomerID
	}

	// Full invoice payload may be embedded in data (same shape as GET invoice).
	var embedded amberflo.CustomerProductInvoice
	if err := json.Unmarshal(dataRaw, &embedded); err == nil && embedded.InvoiceStartTimeInSeconds > 0 {
		event.Embedded = &embedded
		if event.CustomerID == "" && embedded.InvoiceKey.CustomerID != "" {
			event.CustomerID = embedded.InvoiceKey.CustomerID
		}
		if event.InvoiceURI == "" {
			event.InvoiceURI = embedded.InvoiceURI
		}
		if !event.Key.Complete() && embedded.InvoiceKey.ProductPlanID != "" {
			event.Key = embedded.InvoiceKey
			if event.Key.CustomerID == "" {
				event.Key.CustomerID = event.CustomerID
			}
		}
	}

	if event.CustomerID == "" {
		return invoiceReadyEvent{}, fmt.Errorf("customerId not found")
	}
	return event, nil
}

func parseInvoiceKey(data map[string]json.RawMessage) amberflo.InvoiceKey {
	var key amberflo.InvoiceKey
	if raw, ok := data["invoiceKey"]; ok {
		_ = json.Unmarshal(raw, &key)
	}
	if key.CustomerID == "" {
		key.CustomerID = stringField(data, "customerId", "customerID")
	}
	if key.ProductID == "" {
		key.ProductID = stringField(data, "productId")
	}
	if key.ProductPlanID == "" {
		key.ProductPlanID = stringField(data, "productPlanId")
	}
	if key.Year == 0 {
		key.Year = int64Field(data, "year")
	}
	if key.Month == 0 {
		key.Month = int64Field(data, "month")
	}
	if key.Day == 0 {
		key.Day = int64Field(data, "day")
	}
	return key
}

func stringField(m map[string]json.RawMessage, keys ...string) string {
	for _, k := range keys {
		raw, ok := m[k]
		if !ok {
			continue
		}
		var s string
		if err := json.Unmarshal(raw, &s); err == nil && s != "" {
			return s
		}
	}
	return ""
}

func int64Field(m map[string]json.RawMessage, key string) int64 {
	raw, ok := m[key]
	if !ok {
		return 0
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return int64(f)
	}
	return 0
}
