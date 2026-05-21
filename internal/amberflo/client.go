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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	// defaultBaseURL is the public Amberflo control plane.
	defaultBaseURL = "https://app.amberflo.io"
	// defaultHTTPTimeout is applied to http.DefaultClient when the caller
	// does not supply an HTTPClient. It is a per-request timeout across
	// the full request/response lifecycle, not just connect.
	defaultHTTPTimeout = 30 * time.Second
	// defaultUserAgent is used when ClientOptions.UserAgent is empty.
	defaultUserAgent = "amberflo-provider/dev"
	// defaultRetryAttempts is how many HTTP calls the client makes in
	// total (including the first) before giving up with a transient error.
	defaultRetryAttempts = 4
	// retryBaseDelay / retryMaxDelay cap the exponential backoff between
	// retry attempts.
	retryBaseDelay = 100 * time.Millisecond
	retryMaxDelay  = 2 * time.Second
)

// Client is the public interface the controller depends on. It exposes
// exactly the Amberflo operations the reconciler needs, hiding the HTTP
// transport, retry, and rate-limit concerns.
type Client interface {
	// EnsureCustomer reconciles the desired customer state against
	// Amberflo, creating or updating as needed. Repeated calls with the
	// same DesiredCustomer are no-ops after the first.
	EnsureCustomer(ctx context.Context, desired DesiredCustomer) (Customer, error)

	// DisableCustomer soft-disables the customer by setting the
	// enabled=false and archived_at traits. It does NOT hard-delete.
	DisableCustomer(ctx context.Context, customerID string) error

	// GetCustomer fetches the current customer record. Returns
	// ErrCustomerNotFound on 404.
	GetCustomer(ctx context.Context, customerID string) (Customer, error)

	// EnsureMeter reconciles the desired meter state against Amberflo,
	// creating the meter via POST /meters when absent and updating via
	// PUT /meters when the existing record drifts from desired. Repeated
	// calls with the same DesiredMeter are no-ops after the first.
	EnsureMeter(ctx context.Context, desired DesiredMeter) (Meter, error)

	// DeleteMeter removes the meter identified by meterAPIName. 404s are
	// tolerated as success so the reconciler can cleanly finalize a
	// MeterDefinition whose Amberflo counterpart is already gone.
	DeleteMeter(ctx context.Context, meterAPIName string) error

	// GetMeter fetches the current meter record. Returns ErrMeterNotFound
	// on 404.
	GetMeter(ctx context.Context, meterAPIName string) (Meter, error)

	// SubmitUsage posts usage records to the Amberflo ingest API.
	// Returns nil on 2xx. Returns *TransientError on 5xx/429/network.
	// Returns *PermanentError on 4xx (non-429).
	SubmitUsage(ctx context.Context, records []UsageRecord) error
}

// ClientOptions configures a Client. BaseURL and APIKey are the only
// required fields; everything else has sensible defaults.
type ClientOptions struct {
	// BaseURL of the Amberflo control plane. Defaults to
	// https://app.amberflo.io when empty.
	BaseURL string
	// APIKey is sent as X-API-KEY on every request. Required.
	APIKey string
	// HTTPClient, when nil, uses a fresh *http.Client with a 30s timeout.
	HTTPClient *http.Client
	// RateLimitPerSec caps the combined request rate across every method
	// on the returned Client. Defaults to 10.
	RateLimitPerSec int
	// UserAgent header value. Defaults to "amberflo-provider/dev".
	UserAgent string
	// RetryAttempts is the total number of HTTP attempts (including the
	// first) per request. Defaults to 4. A value <= 1 disables retries.
	RetryAttempts int
	// now overrides time.Now for deterministic tests. Optional.
	now func() time.Time
	// sleep overrides the retry sleep for deterministic tests. It
	// receives the desired delay and the context; tests can replace it
	// with a no-op that advances the fake clock. Optional.
	sleep func(ctx context.Context, d time.Duration) error
	// jitter overrides the retry jitter. Defaults to a +/- 25% band.
	jitter func(d time.Duration) time.Duration
}

// client is the concrete implementation of the Client interface.
type client struct {
	baseURL    *url.URL
	apiKey     string
	httpClient *http.Client
	userAgent  string
	rl         *rateLimiter
	retries    int

	now    func() time.Time
	sleep  func(ctx context.Context, d time.Duration) error
	jitter func(d time.Duration) time.Duration

	// rngMu guards rng; the client can be called from multiple
	// goroutines and math/rand.Source is not safe for concurrent use.
	rngMu sync.Mutex
	rng   *rand.Rand
}

// NewClient constructs a Client. It validates the required options and
// returns an error if APIKey is empty or BaseURL is unparseable.
func NewClient(opts ClientOptions) (Client, error) {
	if strings.TrimSpace(opts.APIKey) == "" {
		return nil, errors.New("amberflo: APIKey is required")
	}

	rawURL := opts.BaseURL
	if rawURL == "" {
		rawURL = defaultBaseURL
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("amberflo: invalid BaseURL %q: %w", opts.BaseURL, err)
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}

	ua := opts.UserAgent
	if ua == "" {
		ua = defaultUserAgent
	}

	retries := opts.RetryAttempts
	if retries <= 0 {
		retries = defaultRetryAttempts
	}

	now := opts.now
	if now == nil {
		now = time.Now
	}

	sleep := opts.sleep
	if sleep == nil {
		sleep = defaultSleep
	}

	jitter := opts.jitter
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	c := &client{
		baseURL:    parsed,
		apiKey:     opts.APIKey,
		httpClient: httpClient,
		userAgent:  ua,
		rl:         newRateLimiter(opts.RateLimitPerSec),
		retries:    retries,
		now:        now,
		sleep:      sleep,
		rng:        rng,
	}
	if jitter == nil {
		c.jitter = c.defaultJitter
	} else {
		c.jitter = jitter
	}
	return c, nil
}

// defaultSleep sleeps for d or returns ctx.Err() on cancellation.
func defaultSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// defaultJitter returns d +/- up to 25% to spread retry storms across
// concurrent clients. Minimum returned value is 0.
func (c *client) defaultJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	c.rngMu.Lock()
	frac := c.rng.Float64() // [0, 1)
	c.rngMu.Unlock()
	// Map [0, 1) to [-0.25, 0.25).
	delta := (frac - 0.5) * 0.5
	out := time.Duration(float64(d) * (1.0 + delta))
	if out < 0 {
		return 0
	}
	return out
}

// resolve builds a full URL from a path (optionally including a query
// string) relative to the client's base. The path is parsed to separate
// Path and RawQuery so net/http server-side routing sees a clean Path.
func (c *client) resolve(relPath string) string {
	u := *c.baseURL
	if !strings.HasPrefix(relPath, "/") {
		relPath = "/" + relPath
	}
	if idx := strings.Index(relPath, "?"); idx >= 0 {
		u.Path = relPath[:idx]
		u.RawQuery = relPath[idx+1:]
	} else {
		u.Path = relPath
		u.RawQuery = ""
	}
	return u.String()
}

// doJSON issues a single HTTP request with a JSON body, subject to
// rate-limiting and retry-on-transient. body may be nil for GETs.
// decodeInto, when non-nil, receives json.Unmarshal on the response body
// for 2xx responses.
//
// Returns (status code, raw body, error). For non-2xx responses, the
// error is a classified TransientError or PermanentError.
func (c *client) doJSON(ctx context.Context, method, path string, body any, decodeInto any) (int, []byte, error) {
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return 0, nil, &PermanentError{
				Err: fmt.Errorf("encode request body: %w", err),
			}
		}
	}

	fullURL := c.resolve(path)

	var (
		lastErr    error
		lastStatus int
		lastBody   []byte
	)
	for attempt := 1; attempt <= c.retries; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		if err := c.rl.Wait(ctx); err != nil {
			return 0, nil, err
		}

		var reqBody io.Reader
		if bodyBytes != nil {
			reqBody = bytes.NewReader(bodyBytes)
		}
		req, err := http.NewRequestWithContext(ctx, method, fullURL, reqBody)
		if err != nil {
			// Unparseable method/url: permanent.
			return 0, nil, &PermanentError{Err: fmt.Errorf("build request: %w", err)}
		}
		req.Header.Set("X-API-KEY", c.apiKey)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", c.userAgent)
		if bodyBytes != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, httpErr := c.httpClient.Do(req)
		var (
			respBody   []byte
			classified error
			status     int
		)
		if resp != nil {
			status = resp.StatusCode
			respBody, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		classified = classify(resp, respBody, httpErr)

		if classified == nil {
			if decodeInto != nil && len(respBody) > 0 {
				if err := json.Unmarshal(respBody, decodeInto); err != nil {
					return status, respBody, &PermanentError{
						Err:          fmt.Errorf("decode response body: %w", err),
						StatusCode:   status,
						ResponseBody: string(respBody),
					}
				}
			}
			return status, respBody, nil
		}

		lastErr = classified
		lastStatus = status
		lastBody = respBody

		// Permanent errors terminate the retry loop.
		if IsPermanent(classified) {
			return status, respBody, classified
		}

		// Transient: decide whether to sleep and retry.
		if attempt == c.retries {
			break
		}

		delay := c.backoff(attempt, classified)
		if err := c.sleep(ctx, delay); err != nil {
			return status, respBody, err
		}
	}

	return lastStatus, lastBody, lastErr
}

// backoff computes the delay before the next attempt. Retry-After from a
// TransientError overrides the exponential schedule when it is larger.
func (c *client) backoff(attempt int, err error) time.Duration {
	// attempt is 1-based and we are computing the delay AFTER attempt.
	// So attempt=1 -> base, attempt=2 -> base*2, capped at retryMaxDelay.
	delay := retryBaseDelay << (attempt - 1)
	if delay > retryMaxDelay || delay < 0 {
		delay = retryMaxDelay
	}
	delay = c.jitter(delay)

	var te *TransientError
	if errors.As(err, &te) && te.RetryAfter > 0 {
		// Honour server guidance only when it exceeds our backoff: we
		// never want to retry faster than the server asked us to, but
		// we also won't wait longer than necessary.
		if te.RetryAfter > delay {
			return te.RetryAfter
		}
	}
	return delay
}
