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
	"sort"
	"strconv"
	"time"
)

// DesiredCustomer is the controller-facing representation of a customer
// the reconciler wants to exist in Amberflo. The concrete wire encoding
// (traits, field mapping) is the client package's concern — callers only
// assemble this struct.
type DesiredCustomer struct {
	// ID is the stable identifier used as Amberflo customerId. For the
	// amberflo-provider this is always the Milo BillingAccount.metadata.name.
	ID string
	// Name is the customer's display name. If empty, ID is used as the
	// fallback so Amberflo's required customerName field is always set.
	Name string
	// Email is the primary contact email. May be empty.
	Email string
	// CurrencyCode is an ISO 4217 code. Stored as traits["currencyCode"]
	// on every upsert; callers enforce validation separately.
	CurrencyCode string
	// PaymentTerms, if non-nil, is encoded into payment.* traits.
	PaymentTerms *PaymentTerms
	// Projects is the canonical list of Milo projects bound to this
	// billing account. The client sorts and deduplicates this list
	// before encoding it, so the trait is stable regardless of input
	// ordering.
	Projects []string
	// ExtraTraits is a forward-compatible extension point. Keys here
	// are merged into the customer's trait map; they must not collide
	// with reserved keys (projects, currencyCode, payment.*, enabled,
	// archived_at) or the Reserved key will take precedence.
	ExtraTraits map[string]string
}

// PaymentTerms is the controller-facing representation of Milo's
// BillingAccount payment terms. Fields map 1:1 onto Amberflo traits.
type PaymentTerms struct {
	NetDays           int
	InvoiceFrequency  string
	InvoiceDayOfMonth int
}

// Customer is the provider-facing view of an Amberflo customer record.
// Raw is the server response verbatim so the controller can reference
// the last-known Amberflo payload (logged on debug, surfaced to events)
// for drift investigation.
type Customer struct {
	ID        string
	Name      string
	Email     string
	Enabled   bool
	Traits    map[string]string
	UpdatedAt time.Time
	Raw       json.RawMessage
}

// wireCustomer mirrors Amberflo's JSON payload shape for a customer. The
// SDK type github.com/amberflo/metering-go/v2.Customer uses the same
// field names; we declare our own copy so the client is not coupled to
// the SDK's struct layout for traits-only operations.
type wireCustomer struct {
	CustomerId     string            `json:"customerId"`
	CustomerName   string            `json:"customerName"`
	CustomerEmail  string            `json:"customerEmail,omitempty"`
	Traits         map[string]string `json:"traits,omitempty"`
	LifecycleStage string            `json:"lifecycleStage,omitempty"`
	Enabled        bool              `json:"enabled"`
	UpdateTime     int64             `json:"updateTime,omitempty"`
	CreateTime     int64             `json:"createTime,omitempty"`
}

// reserved trait keys the client always owns. ExtraTraits entries that
// collide with these are overwritten by the client-managed values.
const (
	traitProjects            = "projects"
	traitCurrencyCode        = "currencyCode"
	traitEnabled             = "enabled"
	traitArchivedAt          = "archived_at"
	traitPaymentNetDays      = "payment.netDays"
	traitPaymentInvoiceFreq  = "payment.invoiceFrequency"
	traitPaymentInvoiceDayOM = "payment.invoiceDayOfMonth"
)

// GetCustomer fetches a customer by id. Returns ErrCustomerNotFound on 404.
func (c *client) GetCustomer(ctx context.Context, customerID string) (Customer, error) {
	if customerID == "" {
		return Customer{}, &PermanentError{Err: errors.New("customerID is required")}
	}

	path := fmt.Sprintf("/customers/%s", customerID)
	var wc wireCustomer
	status, body, err := c.doJSON(ctx, http.MethodGet, path, nil, &wc)
	if err != nil {
		// Classify 404 as ErrCustomerNotFound for callers.
		var perm *PermanentError
		if errors.As(err, &perm) && perm.StatusCode == http.StatusNotFound {
			return Customer{}, fmt.Errorf("%w: %s", ErrCustomerNotFound, customerID)
		}
		return Customer{}, err
	}

	// Defensive: some Amberflo endpoints historically return 200 with
	// an empty body when the customer is missing. Treat that as 404.
	if status == http.StatusOK && (len(body) == 0 || string(body) == "{}" || wc.CustomerId == "") {
		return Customer{}, fmt.Errorf("%w: %s", ErrCustomerNotFound, customerID)
	}

	return fromWire(wc, body), nil
}

// EnsureCustomer creates or updates the customer so Amberflo matches the
// DesiredCustomer. The call is idempotent: if Amberflo already agrees,
// no write happens and only the GET is issued.
func (c *client) EnsureCustomer(ctx context.Context, desired DesiredCustomer) (Customer, error) {
	if desired.ID == "" {
		return Customer{}, &PermanentError{Err: errors.New("DesiredCustomer.ID is required")}
	}

	want := buildWireCustomer(desired)

	existing, err := c.GetCustomer(ctx, desired.ID)
	switch {
	case errors.Is(err, ErrCustomerNotFound):
		// Create path.
		return c.putCustomer(ctx, http.MethodPost, want)
	case err != nil:
		return Customer{}, err
	}

	if !customerNeedsUpdate(existing, want) {
		// Already in desired state.
		return existing, nil
	}

	// Update path. Amberflo's upsert endpoint is POST /customers; the
	// SDK uses POST for both create and update, differentiated only by
	// whether the customer already exists. We mirror that: POST on
	// create (no pre-existing), PUT on update. The fake server accepts
	// both.
	return c.putCustomer(ctx, http.MethodPut, want)
}

// DisableCustomer soft-disables the customer by setting the enabled
// trait to "false" and the archived_at trait to the current RFC3339
// timestamp. It always issues a write, even if the customer already
// shows enabled=false, so archived_at is refreshed to the most recent
// disable attempt.
func (c *client) DisableCustomer(ctx context.Context, customerID string) error {
	if customerID == "" {
		return &PermanentError{Err: errors.New("customerID is required")}
	}

	existing, err := c.GetCustomer(ctx, customerID)
	if err != nil {
		if errors.Is(err, ErrCustomerNotFound) {
			// Nothing to disable. Treat as success — the desired end
			// state is "no active customer" and it is already true.
			return nil
		}
		return err
	}

	// Preserve existing customerName/email and traits, overlay the
	// disable markers. We must retain Name because Amberflo requires it.
	wc := wireCustomer{
		CustomerId:    existing.ID,
		CustomerName:  existing.Name,
		CustomerEmail: existing.Email,
		Enabled:       false,
		Traits:        cloneTraits(existing.Traits),
	}
	if wc.CustomerName == "" {
		wc.CustomerName = existing.ID
	}
	if wc.Traits == nil {
		wc.Traits = map[string]string{}
	}
	wc.Traits[traitEnabled] = "false"
	wc.Traits[traitArchivedAt] = c.now().UTC().Format(time.RFC3339)

	_, err = c.putCustomer(ctx, http.MethodPut, wc)
	return err
}

// putCustomer issues a create/update against POST/PUT /customers,
// decoding the response into a Customer.
func (c *client) putCustomer(ctx context.Context, method string, wc wireCustomer) (Customer, error) {
	var path string
	switch method {
	case http.MethodPost:
		path = "/customers?autoCreateCustomerInStripe=false"
	case http.MethodPut:
		path = "/customers"
	default:
		return Customer{}, &PermanentError{Err: fmt.Errorf("unsupported method %q", method)}
	}
	var got wireCustomer
	_, body, err := c.doJSON(ctx, method, path, wc, &got)
	if err != nil {
		return Customer{}, err
	}
	// If the server echoed nothing useful, fall back to the request
	// payload so callers always get a populated Customer.
	if got.CustomerId == "" {
		got = wc
		got.Enabled = wc.Enabled
	}
	return fromWire(got, body), nil
}

// buildWireCustomer renders a DesiredCustomer into the Amberflo wire
// shape. Trait encoding is deterministic: projects are sorted + deduped,
// payment fields are zero-padded into stable strings.
func buildWireCustomer(d DesiredCustomer) wireCustomer {
	name := d.Name
	if name == "" {
		name = d.ID
	}
	traits := map[string]string{}
	for k, v := range d.ExtraTraits {
		traits[k] = v
	}
	// Reserved keys overwrite ExtraTraits.
	traits[traitCurrencyCode] = d.CurrencyCode
	if len(d.Projects) > 0 {
		traits[traitProjects] = encodeProjects(d.Projects)
	}
	if d.PaymentTerms != nil {
		traits[traitPaymentNetDays] = strconv.Itoa(d.PaymentTerms.NetDays)
		traits[traitPaymentInvoiceFreq] = d.PaymentTerms.InvoiceFrequency
		traits[traitPaymentInvoiceDayOM] = strconv.Itoa(d.PaymentTerms.InvoiceDayOfMonth)
	}
	return wireCustomer{
		CustomerId:    d.ID,
		CustomerName:  name,
		CustomerEmail: d.Email,
		Traits:        traits,
		Enabled:       true,
	}
}

// encodeProjects returns a JSON-encoded array of the sorted, deduped
// project list. Encoding uses encoding/json so the string is a valid
// JSON literal and round-trips cleanly.
func encodeProjects(in []string) string {
	set := make(map[string]struct{}, len(in))
	for _, p := range in {
		if p == "" {
			continue
		}
		set[p] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	b, err := json.Marshal(out)
	if err != nil {
		// json.Marshal on []string cannot fail; defensive fallback.
		return "[]"
	}
	return string(b)
}

// customerNeedsUpdate returns true when the existing Customer does not
// already match the desired wire representation. The comparison is
// intentionally narrow: name, email, and the trait map. Enabled state
// is compared so re-enabling a soft-disabled customer still triggers a
// write.
func customerNeedsUpdate(existing Customer, want wireCustomer) bool {
	if existing.Name != want.CustomerName {
		return true
	}
	if existing.Email != want.CustomerEmail {
		return true
	}
	if existing.Enabled != want.Enabled {
		return true
	}
	return !traitsEqual(existing.Traits, want.Traits)
}

// traitsEqual compares two trait maps by sorted keys + string equality.
func traitsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		bv, ok := b[k]
		if !ok || bv != v {
			return false
		}
	}
	return true
}

// cloneTraits returns a shallow copy of traits, or nil if traits is nil.
func cloneTraits(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// fromWire converts the wire representation plus raw body into the
// provider-facing Customer. UpdatedAt is derived from UpdateTime
// (Amberflo returns epoch millis).
func fromWire(wc wireCustomer, raw []byte) Customer {
	updated := time.Time{}
	if wc.UpdateTime > 0 {
		updated = time.UnixMilli(wc.UpdateTime).UTC()
	}
	// Copy the raw body so callers mutating their copy don't affect ours.
	var rawCopy json.RawMessage
	if len(raw) > 0 {
		rawCopy = make(json.RawMessage, len(raw))
		copy(rawCopy, raw)
	}
	return Customer{
		ID:        wc.CustomerId,
		Name:      wc.CustomerName,
		Email:     wc.CustomerEmail,
		Enabled:   wc.Enabled,
		Traits:    cloneTraits(wc.Traits),
		UpdatedAt: updated,
		Raw:       rawCopy,
	}
}
