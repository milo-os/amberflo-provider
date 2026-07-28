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
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

// Default Amberflo product id used for customer-product invoices. Product
// "1" is Amberflo's built-in default product for single-product tenants.
const defaultInvoiceProductID = "1"

// PaymentStatus is Amberflo's invoice payment lifecycle vocabulary.
type PaymentStatus string

const (
	PaymentStatusPrePayment     PaymentStatus = "PRE_PAYMENT"
	PaymentStatusRequiresAction PaymentStatus = "REQUIRES_ACTION"
	PaymentStatusPending        PaymentStatus = "PENDING"
	PaymentStatusFailed         PaymentStatus = "FAILED"
	PaymentStatusSettled        PaymentStatus = "SETTLED"
	PaymentStatusNotNeeded      PaymentStatus = "NOT_NEEDED"
	PaymentStatusUnknown        PaymentStatus = "UNKNOWN"
)

// InvoiceKey is the Amberflo composite key that uniquely identifies a
// customer-product invoice for a billing period. Stored on Milo Invoice
// resources under the amberflo.billing.miloapis.com/invoiceKey annotation.
type InvoiceKey struct {
	CustomerID    string `json:"customerId,omitempty"`
	ProductID     string `json:"productId,omitempty"`
	ProductPlanID string `json:"productPlanId,omitempty"`
	Year          int64  `json:"year,omitempty"`
	Month         int64  `json:"month,omitempty"`
	Day           int64  `json:"day,omitempty"`
}

// Complete reports whether key has the fields required by GetInvoice.
func (k InvoiceKey) Complete() bool {
	return k.CustomerID != "" && k.ProductPlanID != "" && k.Year > 0 && k.Month > 0 && k.Day > 0
}

// ProductPlanBill is the Amberflo invoice total breakdown. Only TotalPrice
// is required for Milo Invoice projection today.
type ProductPlanBill struct {
	TotalPrice float64 `json:"totalPrice"`
}

// CustomerProductInvoice is the Amberflo customer-product invoice payload
// fields the provider needs to project onto a Milo Invoice.
type CustomerProductInvoice struct {
	InvoiceURI                string          `json:"invoiceUri"`
	InvoiceKey                InvoiceKey      `json:"invoiceKey"`
	InvoiceStartTimeInSeconds int64           `json:"invoiceStartTimeInSeconds"`
	InvoiceEndTimeInSeconds   int64           `json:"invoiceEndTimeInSeconds"`
	GracePeriodInHours        int64           `json:"gracePeriodInHours"`
	TotalBill                 ProductPlanBill `json:"totalBill"`
	InvoicePriceStatus        string          `json:"invoicePriceStatus"`
	PaymentStatus             PaymentStatus   `json:"paymentStatus"`
	PaymentCreatedInSeconds   int64           `json:"paymentCreatedInSeconds"`
	Raw                       json.RawMessage `json:"-"`
}

// ListInvoices fetches every customer-product invoice for customerID.
// productId defaults to "1"; fromCache and withPaymentStatus are always
// requested so paymentStatus is populated when Amberflo has it.
func (c *client) ListInvoices(ctx context.Context, customerID string) ([]CustomerProductInvoice, error) {
	if customerID == "" {
		return nil, &PermanentError{Err: errors.New("customerID is required")}
	}

	path := invoiceListPath(customerID, false)
	var wire []CustomerProductInvoice
	_, body, err := c.doJSON(ctx, http.MethodGet, path, nil, &wire)
	if err != nil {
		return nil, err
	}
	// Amberflo may return a bare null / empty body for customers with no
	// invoices yet — treat that as an empty list, not an error.
	if len(body) == 0 || string(body) == "null" {
		return nil, nil
	}
	return attachInvoiceRaw(wire, body), nil
}

// GetLatestInvoice fetches the most recent customer-product invoice for
// customerID. Returns ErrInvoiceNotFound when Amberflo has none.
func (c *client) GetLatestInvoice(ctx context.Context, customerID string) (CustomerProductInvoice, error) {
	if customerID == "" {
		return CustomerProductInvoice{}, &PermanentError{Err: errors.New("customerID is required")}
	}

	path := invoiceListPath(customerID, true)
	var wire CustomerProductInvoice
	status, body, err := c.doJSON(ctx, http.MethodGet, path, nil, &wire)
	if err != nil {
		var perm *PermanentError
		if errors.As(err, &perm) && perm.StatusCode == http.StatusNotFound {
			return CustomerProductInvoice{}, fmt.Errorf("%w: %s", ErrInvoiceNotFound, customerID)
		}
		return CustomerProductInvoice{}, err
	}
	if status == http.StatusOK && (len(body) == 0 || string(body) == "null" || string(body) == "{}") {
		return CustomerProductInvoice{}, fmt.Errorf("%w: %s", ErrInvoiceNotFound, customerID)
	}
	if len(body) > 0 {
		wire.Raw = append(json.RawMessage(nil), body...)
	}
	return wire, nil
}

// GetInvoice fetches a specific customer-product invoice by Amberflo
// invoice key. Used by the ready-product-invoices webhook so we refresh
// a single invoice rather than listing the customer's full history.
func (c *client) GetInvoice(ctx context.Context, key InvoiceKey) (CustomerProductInvoice, error) {
	if key.CustomerID == "" {
		return CustomerProductInvoice{}, &PermanentError{Err: errors.New("InvoiceKey.CustomerID is required")}
	}
	if key.ProductPlanID == "" || key.Year <= 0 || key.Month <= 0 || key.Day <= 0 {
		return CustomerProductInvoice{}, &PermanentError{Err: errors.New("InvoiceKey productPlanId, year, month, and day are required")}
	}

	path := invoiceGetPath(key)
	var wire CustomerProductInvoice
	status, body, err := c.doJSON(ctx, http.MethodGet, path, nil, &wire)
	if err != nil {
		var perm *PermanentError
		if errors.As(err, &perm) && perm.StatusCode == http.StatusNotFound {
			return CustomerProductInvoice{}, fmt.Errorf("%w: %s", ErrInvoiceNotFound, FormatInvoiceKey(key))
		}
		return CustomerProductInvoice{}, err
	}
	if status == http.StatusOK && (len(body) == 0 || string(body) == "null" || string(body) == "{}") {
		return CustomerProductInvoice{}, fmt.Errorf("%w: %s", ErrInvoiceNotFound, FormatInvoiceKey(key))
	}
	if len(body) > 0 {
		wire.Raw = append(json.RawMessage(nil), body...)
	}
	if wire.InvoiceKey.CustomerID == "" {
		wire.InvoiceKey = key
	}
	return wire, nil
}

// invoiceListPath builds the Amberflo customer-product-invoice list URL.
// When latest is true the same /all endpoint is queried with latest=true
// per the Amberflo Stripe invoicing design.
func invoiceListPath(customerID string, latest bool) string {
	q := url.Values{}
	q.Set("customerId", customerID)
	q.Set("productId", defaultInvoiceProductID)
	q.Set("fromCache", "true")
	q.Set("withPaymentStatus", "true")
	if latest {
		q.Set("latest", "true")
	}
	return "/payments/billing/customer-product-invoice/all?" + q.Encode()
}

// invoiceGetPath builds the Amberflo get-by-key customer-product-invoice
// URL (without /all), matching amberflo/metering-go InvoiceClient.GetInvoice.
func invoiceGetPath(key InvoiceKey) string {
	productID := key.ProductID
	if productID == "" {
		productID = defaultInvoiceProductID
	}
	q := url.Values{}
	q.Set("customerId", key.CustomerID)
	q.Set("productId", productID)
	q.Set("productPlanId", key.ProductPlanID)
	q.Set("year", strconv.FormatInt(key.Year, 10))
	q.Set("month", strconv.FormatInt(key.Month, 10))
	q.Set("day", strconv.FormatInt(key.Day, 10))
	q.Set("fromCache", "true")
	q.Set("withPaymentStatus", "true")
	return "/payments/billing/customer-product-invoice?" + q.Encode()
}

// attachInvoiceRaw best-effort attaches the list response body onto each
// item. Individual item raw bodies are not available from a list decode;
// callers that need per-invoice raw should call GetLatestInvoice / Get by
// key. Leaving Raw nil is fine for Syncer projection.
func attachInvoiceRaw(items []CustomerProductInvoice, _ []byte) []CustomerProductInvoice {
	return items
}

// FormatInvoiceKey returns a stable JSON encoding of key for the
// amberflo.billing.miloapis.com/invoiceKey annotation.
func FormatInvoiceKey(key InvoiceKey) string {
	b, err := json.Marshal(key)
	if err != nil {
		// json.Marshal on this struct cannot fail; defensive fallback.
		return fmt.Sprintf(
			`{"customerId":%q,"productId":%q,"productPlanId":%q,"year":%s,"month":%s,"day":%s}`,
			key.CustomerID, key.ProductID, key.ProductPlanID,
			strconv.FormatInt(key.Year, 10),
			strconv.FormatInt(key.Month, 10),
			strconv.FormatInt(key.Day, 10),
		)
	}
	return string(b)
}
