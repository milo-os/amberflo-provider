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
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ErrCustomerNotFound is the sentinel returned by GetCustomer when Amberflo
// responds with a 404 for the requested customerId. Callers compare with
// errors.Is to decide whether to create or update the customer.
var ErrCustomerNotFound = errors.New("amberflo: customer not found")

// ErrMeterNotFound is the sentinel returned by GetMeter when Amberflo
// responds with a 404 for the requested meterApiName. Callers compare with
// errors.Is to decide whether to create or update the meter, and
// DeleteMeter treats it as success.
var ErrMeterNotFound = errors.New("amberflo: meter not found")

// ErrInvoiceNotFound is the sentinel returned by GetLatestInvoice when
// Amberflo has no invoice for the requested customer.
var ErrInvoiceNotFound = errors.New("amberflo: invoice not found")

// ErrProductPlanNotFound is the sentinel returned by GetProductPlan when
// Amberflo responds with a 404 for the requested product plan id.
var ErrProductPlanNotFound = errors.New("amberflo: product plan not found")

// ErrCustomerPlanNotFound is the sentinel returned when no customer-
// pricing assignment matches the requested customer/plan pair.
var ErrCustomerPlanNotFound = errors.New("amberflo: customer plan not found")

// TransientError wraps a failure that the caller should retry with
// backoff. Network errors, 429s, and 5xx responses all surface as
// TransientError.
type TransientError struct {
	// Err is the underlying error (network error, or a synthetic error
	// constructed from an HTTP response body).
	Err error
	// StatusCode is the HTTP status code when the failure came from a
	// response; zero for pre-response failures (network, TLS, DNS).
	StatusCode int
	// RetryAfter is parsed from the Retry-After header when present on a
	// 429 or 503. Zero means the header was not present or not parseable;
	// callers should fall back to their own backoff schedule.
	RetryAfter time.Duration
}

// Error implements error.
func (e *TransientError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.StatusCode != 0 {
		return fmt.Sprintf("amberflo: transient error (status=%d): %v", e.StatusCode, e.Err)
	}
	return fmt.Sprintf("amberflo: transient error: %v", e.Err)
}

// Unwrap exposes the underlying error so errors.Is / errors.As traverse it.
func (e *TransientError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// PermanentError wraps a failure the caller MUST NOT retry. 4xx responses
// (other than 429) and malformed server responses surface as
// PermanentError. The response body is captured verbatim for surfacing to
// the user via status conditions.
type PermanentError struct {
	Err          error
	StatusCode   int
	ResponseBody string
}

// Error implements error.
func (e *PermanentError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.StatusCode != 0 {
		return fmt.Sprintf("amberflo: permanent error (status=%d): %v", e.StatusCode, e.Err)
	}
	return fmt.Sprintf("amberflo: permanent error: %v", e.Err)
}

// Unwrap exposes the underlying error.
func (e *PermanentError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// IsTransient reports whether err (or any error wrapped in it) is a
// TransientError.
func IsTransient(err error) bool {
	var t *TransientError
	return errors.As(err, &t)
}

// IsPermanent reports whether err (or any error wrapped in it) is a
// PermanentError.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// classify inspects an HTTP response and body and returns the correct
// provider error. The resp may be nil, which is treated as a transient
// network-level failure; in that case networkErr must be non-nil and is
// used as the underlying error.
//
// Call sites pass the already-read body bytes so the response body is
// captured for surfacing in PermanentError.ResponseBody and so the body
// text is available as part of the TransientError message.
func classify(resp *http.Response, bodyBytes []byte, networkErr error) error {
	if resp == nil {
		// Network-level failure (DNS, connection refused, TLS, etc.).
		if networkErr == nil {
			networkErr = errors.New("nil response and nil network error")
		}
		return &TransientError{Err: networkErr}
	}

	status := resp.StatusCode
	bodySnippet := strings.TrimSpace(string(bodyBytes))
	// Cap body snippet to avoid pathological sizes leaking into logs.
	const maxBody = 4096
	if len(bodySnippet) > maxBody {
		bodySnippet = bodySnippet[:maxBody] + "...(truncated)"
	}

	switch {
	case status >= 200 && status < 300:
		// Not an error.
		return nil

	case status == http.StatusTooManyRequests:
		return &TransientError{
			Err:        fmt.Errorf("rate limited by amberflo: %s", bodySnippet),
			StatusCode: status,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}

	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return &PermanentError{
			Err:          fmt.Errorf("amberflo rejected authentication (status=%d); verify X-API-KEY is set and valid: %s", status, bodySnippet),
			StatusCode:   status,
			ResponseBody: bodySnippet,
		}

	case status >= 500:
		return &TransientError{
			Err:        fmt.Errorf("amberflo server error: %s", bodySnippet),
			StatusCode: status,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}

	case status >= 400:
		return &PermanentError{
			Err:          fmt.Errorf("amberflo rejected request: %s", bodySnippet),
			StatusCode:   status,
			ResponseBody: bodySnippet,
		}

	default:
		// 1xx / 3xx unexpected at this layer; Go's http client follows
		// redirects so we should not see 3xx here. Treat as permanent so
		// the reconciler surfaces the surprise rather than looping.
		return &PermanentError{
			Err:          fmt.Errorf("unexpected status from amberflo (status=%d): %s", status, bodySnippet),
			StatusCode:   status,
			ResponseBody: bodySnippet,
		}
	}
}

// parseRetryAfter parses a Retry-After header value. Amberflo (per HTTP
// spec) may set either a delta-seconds integer or an HTTP-date. We support
// delta-seconds; HTTP-date parsing is intentionally skipped because
// Amberflo only documents numeric values and full date parsing adds no
// real value for our backoff strategy.
func parseRetryAfter(h string) time.Duration {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		d := time.Until(t)
		if d < 0 {
			return 0
		}
		return d
	}
	return 0
}
