package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
	"unicode/utf8"
)

const (
	// REST calls block their caller, so retries are short, as for prompts.
	restRetryLimit           = 2
	restRetryInitialInterval = 500 * time.Millisecond

	// restResponseLimit leaves room for the server echoing a 4.5 MB dataset
	// item.
	restResponseLimit     = 8 << 20
	restPageResponseLimit = 16 << 20
)

// RESTError is a public REST API failure with static text.
type RESTError struct {
	Message  string
	NotFound bool
	// OutcomeUnknown marks a write that may have been applied.
	OutcomeUnknown bool
	// Cause is the network or context error behind the failure, if any.
	Cause error
}

func (e *RESTError) Error() string { return "langfuse transport: " + e.Message }

// RESTClient calls the Langfuse public REST API and is safe for concurrent
// use.
type RESTClient struct {
	base       string
	publicKey  string
	secretKey  string
	sdkVersion string
	client     *http.Client
}

// NewRESTClient builds a REST client from a validated configuration.
func NewRESTClient(cfg Config) (*RESTClient, error) {
	base, err := NormalizeAPIBase(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	return &RESTClient{
		base:       base,
		publicKey:  cfg.PublicKey,
		secretKey:  cfg.SecretKey,
		sdkVersion: cfg.SDKVersion,
		client: &http.Client{
			Timeout: timeout,
			// Never follow redirects: the target would receive the
			// credentials and the request content.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

type restCall struct {
	op     string // static error text
	method string
	url    string
	body   []byte
	write  bool
	limit  int
}

// do performs call and returns the 2xx body. Reads retry network errors,
// 408, 429, and 5xx; a write is sent once.
func (d *RESTClient) do(ctx context.Context, call restCall) ([]byte, error) {
	interval := restRetryInitialInterval
	for attempt := 0; ; attempt++ {
		body, retryable, retryAfter, err := d.attempt(ctx, call)
		if err == nil {
			return body, nil
		}
		if !retryable || attempt >= restRetryLimit || ctx.Err() != nil {
			return nil, err
		}
		delay := max(interval/2+rand.N(interval), retryAfter)
		if deadline, ok := ctx.Deadline(); ok && time.Now().Add(delay).After(deadline) {
			return nil, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, canceledError(call, ctx.Err(), false)
		}
		interval *= 2
	}
}

func canceledError(call restCall, cause error, unknown bool) error {
	return &RESTError{Message: "the " + call.op + " request did not finish", OutcomeUnknown: unknown, Cause: cause}
}

func (d *RESTClient) attempt(ctx context.Context, call restCall) (
	body []byte, retryable bool, retryAfter time.Duration, err error,
) {
	if err := ctx.Err(); err != nil {
		return nil, false, 0, canceledError(call, err, false)
	}
	var requestBody io.Reader
	if call.body != nil {
		requestBody = bytes.NewReader(call.body)
	}
	request, err := http.NewRequestWithContext(ctx, call.method, call.url, requestBody)
	if err != nil {
		return nil, false, 0, &RESTError{Message: "the " + call.op + " request could not be built"}
	}
	request.SetBasicAuth(d.publicKey, d.secretKey)
	request.Header.Set("X-Langfuse-Sdk-Name", sdkName)
	request.Header.Set("X-Langfuse-Sdk-Version", d.sdkVersion)
	request.Header.Set("X-Langfuse-Public-Key", d.publicKey)
	request.Header.Set("Accept", "application/json")
	if call.body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := d.client.Do(request)
	if err != nil {
		if cause := ctx.Err(); cause != nil {
			return nil, false, 0, canceledError(call, cause, call.write)
		}
		var dial *net.OpError
		sent := !errors.As(err, &dial) || dial.Op != "dial"
		return nil, !call.write, 0, &RESTError{
			Message:        "the " + call.op + " request failed",
			OutcomeUnknown: call.write && sent,
			Cause:          safeCause(err),
		}
	}
	defer func() { _ = response.Body.Close() }()
	if status := response.StatusCode; status < 200 || status >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxScoreDrainBytes))
		failure := &RESTError{
			Message:        "the " + call.op + " endpoint returned status " + strconv.Itoa(status),
			NotFound:       status == http.StatusNotFound,
			OutcomeUnknown: call.write && !writeRejectedBeforeCommit(status),
		}
		if call.write || !retryableStatus(status) {
			return nil, false, 0, failure
		}
		return nil, true, parseRetryAfter(response.Header.Get("Retry-After")), failure
	}
	body, err = io.ReadAll(io.LimitReader(response.Body, int64(call.limit)+1))
	switch {
	case err != nil && ctx.Err() != nil:
		return nil, false, 0, canceledError(call, ctx.Err(), call.write)
	case err != nil:
		return nil, false, 0, &RESTError{
			Message:        "the " + call.op + " response could not be read",
			OutcomeUnknown: call.write,
		}
	case len(body) > call.limit:
		return nil, false, 0, &RESTError{
			Message:        "the " + call.op + " response exceeds the local size limit",
			OutcomeUnknown: call.write,
		}
	}
	return body, false, 0, nil
}

// safeCause keeps the part of a request error whose text cannot carry the
// URL or response bytes: a network operation error, or a timeout. Parse
// errors for a malformed response quote the offending bytes, so they and
// anything else are dropped.
func safeCause(err error) error {
	var operation *net.OpError
	if errors.As(err, &operation) {
		return operation
	}
	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		return os.ErrDeadlineExceeded
	}
	return nil
}

// writeRejectedBeforeCommit reports statuses Langfuse returns for a write
// before applying it. Any other status, even 408, 429, or 5xx, can come from
// a gateway after the server committed the write.
func writeRejectedBeforeCommit(status int) bool {
	switch status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden,
		http.StatusNotFound, http.StatusConflict, http.StatusRequestEntityTooLarge:
		return true
	default:
		return false
	}
}

func nonNull(raw json.RawMessage) json.RawMessage {
	if string(raw) == "null" {
		return nil
	}
	return raw
}

// Page is one decoded page of a listing with pagination metadata M.
type Page[T, M any] struct {
	Data []T `json:"data"`
	Meta *M  `json:"meta"`
}

// PageMeta is the pagination metadata of a page-numbered listing.
//
//nolint:tagliatelle // Langfuse wire keys are camelCase.
type PageMeta struct {
	TotalPages *int `json:"totalPages"`
}

// Pages returns the reported page count, or 0 without metadata.
func (meta *PageMeta) Pages() int {
	if meta == nil || meta.TotalPages == nil {
		return 0
	}
	return *meta.TotalPages
}

// send performs call and decodes a response that valid accepts. The UTF-8
// check runs first because encoding/json silently replaces invalid bytes.
func send[T any](ctx context.Context, d *RESTClient, call restCall, valid func(*T) bool) (T, error) {
	var value T
	body, err := d.do(ctx, call)
	if err != nil {
		return value, err
	}
	if !utf8.Valid(body) || json.Unmarshal(body, &value) != nil || !valid(&value) {
		var zero T
		return zero, &RESTError{
			Message:        "the " + call.op + " response was invalid or did not match the request",
			OutcomeUnknown: call.write,
		}
	}
	return value, nil
}

// CursorMeta is the pagination metadata of a cursor listing.
type CursorMeta struct {
	Cursor string `json:"cursor"`
}

// Next returns the cursor of the next page, or "" after the last page.
func (meta *CursorMeta) Next() string {
	if meta == nil {
		return ""
	}
	return meta.Cursor
}

// listPage reads one page of a listing, which must carry its data and
// metadata and only valid entries.
func listPage[T, M any](
	ctx context.Context, d *RESTClient, op, path string, query url.Values, valid func(*T) bool,
) (Page[T, M], error) {
	call := restCall{
		op: op, method: http.MethodGet, url: d.base + path + "?" + query.Encode(), limit: restPageResponseLimit,
	}
	return send(ctx, d, call, func(page *Page[T, M]) bool {
		return page.Data != nil && page.Meta != nil && validMeta(page.Meta) && allValid(page.Data, valid)
	})
}

// validMeta requires the page count of a page-numbered listing, without
// which the last page cannot be told apart.
func validMeta(meta any) bool {
	if numbered, ok := meta.(*PageMeta); ok {
		return numbered.TotalPages != nil && *numbered.TotalPages >= 0
	}
	return true
}

func allValid[T any](values []T, valid func(*T) bool) bool {
	for index := range values {
		if !valid(&values[index]) {
			return false
		}
	}
	return true
}
