package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	// datasetRetryLimit and datasetRetryInitialInterval shape the short
	// blocking retry loop, matching GetPrompt: dataset calls block their
	// caller, so the budget is seconds, not the score pipeline's minute.
	datasetRetryLimit           = 2
	datasetRetryInitialInterval = 500 * time.Millisecond

	// DefaultDatasetResponseLimit bounds one dataset or dataset item
	// response. The server accepts 4.5 MB item requests and echoes the stored
	// item, so the bound leaves room for escaping differences and media
	// references.
	DefaultDatasetResponseLimit = 8 << 20
	// DefaultDatasetPageResponseLimit bounds one page of dataset items. A
	// page of one item always fits.
	DefaultDatasetPageResponseLimit = 16 << 20
)

// DatasetError is a payload-free dataset REST failure. Message is static
// text naming the operation and, at most, an HTTP status code.
type DatasetError struct {
	Message string
	// Status is the HTTP status code, or zero when no response arrived.
	Status int
	// NotFound marks a 404 response. The root package maps it to an
	// operation-specific sentinel.
	NotFound bool
	// OutcomeUnknown marks a write that may have been applied: it failed
	// after the request could have reached the server, or its success
	// response could not be read or validated.
	OutcomeUnknown bool
	// Cause is the context error when the request context ended.
	Cause error
}

func (e *DatasetError) Error() string { return "langfuse transport: " + e.Message }

func (e *DatasetError) Unwrap() error { return e.Cause }

// DatasetsClient calls the Langfuse dataset REST API. It is stateless and
// safe for concurrent use; admission, budgets, and lifecycle live in the
// root package.
type DatasetsClient struct {
	base       string
	publicKey  string
	secretKey  string
	sdkVersion string
	client     *http.Client

	// These are test seams; production values come from the constants above.
	retryLimit        int
	retryInterval     time.Duration
	responseLimit     int
	pageResponseLimit int
}

// NewDatasetsClient builds a dataset client from an already validated
// transport configuration. It performs no network I/O.
func NewDatasetsClient(cfg Config) (*DatasetsClient, error) {
	base, err := NormalizeAPIBase(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	return &DatasetsClient{
		base:       base,
		publicKey:  cfg.PublicKey,
		secretKey:  cfg.SecretKey,
		sdkVersion: cfg.SDKVersion,
		client: &http.Client{
			Timeout: timeout,
			// Never follow redirects: a Location target is server-controlled
			// and following it would re-send the credentials and the dataset
			// content to another URL. A 3xx response fails as terminal.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		retryLimit:        datasetRetryLimit,
		retryInterval:     datasetRetryInitialInterval,
		responseLimit:     DefaultDatasetResponseLimit,
		pageResponseLimit: DefaultDatasetPageResponseLimit,
	}, nil
}

// datasetCall describes one REST operation. op is static text used in errors.
type datasetCall struct {
	op     string
	method string
	url    string
	body   []byte
	// write selects write semantics: a failure that may have followed a
	// server commit is never retried and is reported as OutcomeUnknown.
	write bool
	limit int
}

// do performs call with bounded retries and returns the 2xx body. Reads
// retry network errors, 408, 429, and 5xx. A write is retried only when the
// transport failed before any request header was written: a server cannot
// act on a request whose headers it never received. Once a write was
// dispatched it is never repeated, and every failure except a status that
// Langfuse only returns before applying a write is reported as
// OutcomeUnknown. Every attempt sends the same body bytes. A retry whose
// delay would pass the context deadline is declined.
func (d *DatasetsClient) do(ctx context.Context, call datasetCall) ([]byte, error) {
	interval := d.retryInterval
	for attempt := 0; ; attempt++ {
		body, retryable, retryAfter, err := d.attempt(ctx, call)
		if err == nil {
			return body, nil
		}
		if !retryable || attempt >= d.retryLimit || ctx.Err() != nil {
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
			// The last attempt's outcome is already known; waiting was
			// canceled before another request was sent.
			return nil, canceledError(call, ctx.Err(), false)
		}
		interval *= 2
	}
}

func canceledError(call datasetCall, cause error, sent bool) error {
	return &DatasetError{
		Message:        "the " + call.op + " request was canceled",
		OutcomeUnknown: call.write && sent,
		Cause:          cause,
	}
}

func (d *DatasetsClient) attempt(ctx context.Context, call datasetCall) (
	body []byte, retryable bool, retryAfter time.Duration, err error,
) {
	if err := ctx.Err(); err != nil {
		return nil, false, 0, canceledError(call, err, false)
	}
	var requestBody io.Reader
	if call.body != nil {
		requestBody = bytes.NewReader(call.body)
	}
	// sent turns true once any request header reached the connection. Before
	// that, no server can have seen the request, so a write is safe to retry.
	var sent atomic.Bool
	traced := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteHeaders: func() { sent.Store(true) },
	})
	request, err := http.NewRequestWithContext(traced, call.method, call.url, requestBody)
	if err != nil {
		return nil, false, 0, &DatasetError{Message: "the " + call.op + " request could not be built"}
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
			return nil, false, 0, canceledError(call, cause, sent.Load())
		}
		if call.write && sent.Load() {
			return nil, false, 0, &DatasetError{
				Message:        "the " + call.op + " request failed after it was sent",
				OutcomeUnknown: true,
			}
		}
		return nil, true, 0, &DatasetError{Message: "the " + call.op + " request failed before a response"}
	}
	defer func() { _ = response.Body.Close() }()
	status := response.StatusCode
	if status < 200 || status >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxScoreDrainBytes))
		failure := &DatasetError{
			Message:  "the " + call.op + " endpoint returned status " + strconv.Itoa(status),
			Status:   status,
			NotFound: status == http.StatusNotFound,
		}
		if call.write {
			// A status alone does not prove where processing stopped: a
			// gateway can time out or rate-limit after forwarding, and a
			// committed write whose response cannot be serialized returns 422.
			// Only rejections Langfuse issues before applying a write are
			// known outcomes.
			failure.OutcomeUnknown = !writeRejectedBeforeCommit(status)
			return nil, false, 0, failure
		}
		retryable = status == http.StatusRequestTimeout || status == http.StatusTooManyRequests ||
			status >= 500 && status <= 599
		if retryable {
			retryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
		}
		return nil, retryable, retryAfter, failure
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, int64(call.limit)+1))
	if readErr != nil {
		if cause := ctx.Err(); cause != nil {
			return nil, false, 0, canceledError(call, cause, true)
		}
		// A broken success body is terminal: the request was processed.
		return nil, false, 0, &DatasetError{
			Message:        "the " + call.op + " response could not be read",
			Status:         status,
			OutcomeUnknown: call.write,
		}
	}
	if len(body) > call.limit {
		return nil, false, 0, &DatasetError{
			Message:        "the " + call.op + " response exceeds the local size limit",
			Status:         status,
			OutcomeUnknown: call.write,
		}
	}
	return body, false, 0, nil
}

// writeRejectedBeforeCommit reports statuses that the Langfuse public API
// returns for a write before applying it: malformed or invalid requests
// (400), authentication and authorization (401, 403), a missing dataset or
// item (404), an item ID owned by another dataset or a concurrent version
// conflict (409), and a body over the route limit (413).
func writeRejectedBeforeCommit(status int) bool {
	switch status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden,
		http.StatusNotFound, http.StatusConflict, http.StatusRequestEntityTooLarge:
		return true
	default:
		return false
	}
}

// Dataset is one decoded dataset.
type Dataset struct {
	ID                   string
	Name                 string
	Description          string
	Metadata             json.RawMessage
	InputSchema          json.RawMessage
	ExpectedOutputSchema json.RawMessage
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// DatasetItem is one decoded dataset item.
type DatasetItem struct {
	ID                  string
	DatasetID           string
	DatasetName         string
	Status              string
	Input               json.RawMessage
	ExpectedOutput      json.RawMessage
	Metadata            json.RawMessage
	SourceTraceID       string
	SourceObservationID string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// DatasetItemPage is one decoded page of dataset items with its metadata.
type DatasetItemPage struct {
	Items      []DatasetItem
	Page       int
	Limit      int
	TotalItems int
	TotalPages int
}

// DatasetItemListQuery selects one page of dataset items. Version must be
// the already formatted as-of instant.
type DatasetItemListQuery struct {
	DatasetName         string
	SourceTraceID       string
	SourceObservationID string
	Version             string
	Page                int
	Limit               int
}

// UpsertDataset posts one serialized dataset body. The response must name
// the requested dataset.
func (d *DatasetsClient) UpsertDataset(ctx context.Context, body []byte, name string) (Dataset, error) {
	call := datasetCall{
		op: "dataset upsert", method: http.MethodPost, url: d.base + "/v2/datasets",
		body: body, write: true, limit: d.responseLimit,
	}
	response, err := d.do(ctx, call)
	if err != nil {
		return Dataset{}, err
	}
	dataset, err := decodeDataset(response)
	if err != nil || dataset.Name != name {
		return Dataset{}, invalidResponse(call)
	}
	return dataset, nil
}

// GetDataset reads one dataset by name.
func (d *DatasetsClient) GetDataset(ctx context.Context, name string) (Dataset, error) {
	call := datasetCall{
		op: "dataset read", method: http.MethodGet,
		url:   d.base + "/v2/datasets/" + url.PathEscape(name),
		limit: d.responseLimit,
	}
	response, err := d.do(ctx, call)
	if err != nil {
		return Dataset{}, err
	}
	dataset, err := decodeDataset(response)
	if err != nil || dataset.Name != name {
		return Dataset{}, invalidResponse(call)
	}
	return dataset, nil
}

// UpsertItem posts one serialized dataset item body. The response must
// belong to datasetName and, when id is non-empty, carry that ID.
func (d *DatasetsClient) UpsertItem(ctx context.Context, body []byte, datasetName, id string) (DatasetItem, error) {
	call := datasetCall{
		op: "dataset item upsert", method: http.MethodPost, url: d.base + "/dataset-items",
		body: body, write: true, limit: d.responseLimit,
	}
	response, err := d.do(ctx, call)
	if err != nil {
		return DatasetItem{}, err
	}
	item, err := decodeDatasetItem(response)
	if err != nil || item.DatasetName != datasetName || id != "" && item.ID != id {
		return DatasetItem{}, invalidResponse(call)
	}
	return item, nil
}

// GetItem reads one dataset item by ID.
func (d *DatasetsClient) GetItem(ctx context.Context, id string) (DatasetItem, error) {
	call := datasetCall{
		op: "dataset item read", method: http.MethodGet,
		url:   d.base + "/dataset-items/" + url.PathEscape(id),
		limit: d.responseLimit,
	}
	response, err := d.do(ctx, call)
	if err != nil {
		return DatasetItem{}, err
	}
	item, err := decodeDatasetItem(response)
	if err != nil || item.ID != id {
		return DatasetItem{}, invalidResponse(call)
	}
	return item, nil
}

// DeleteItem deletes one dataset item by ID. The response body is drained
// within the response limit and otherwise ignored.
func (d *DatasetsClient) DeleteItem(ctx context.Context, id string) error {
	_, err := d.do(ctx, datasetCall{
		op: "dataset item delete", method: http.MethodDelete,
		url:   d.base + "/dataset-items/" + url.PathEscape(id),
		write: true, limit: d.responseLimit,
	})
	return err
}

// ListItems reads one page of dataset items. Only the shape of the page is
// validated here; cross-page consistency belongs to the caller.
func (d *DatasetsClient) ListItems(ctx context.Context, query DatasetItemListQuery) (DatasetItemPage, error) {
	values := url.Values{}
	values.Set("datasetName", query.DatasetName)
	if query.SourceTraceID != "" {
		values.Set("sourceTraceId", query.SourceTraceID)
	}
	if query.SourceObservationID != "" {
		values.Set("sourceObservationId", query.SourceObservationID)
	}
	if query.Version != "" {
		values.Set("version", query.Version)
	}
	values.Set("page", strconv.Itoa(query.Page))
	values.Set("limit", strconv.Itoa(query.Limit))
	call := datasetCall{
		op: "dataset item list", method: http.MethodGet,
		url:   d.base + "/dataset-items?" + values.Encode(),
		limit: d.pageResponseLimit,
	}
	response, err := d.do(ctx, call)
	if err != nil {
		return DatasetItemPage{}, err
	}
	page, err := decodeDatasetItemPage(response)
	if err != nil {
		return DatasetItemPage{}, invalidResponse(call)
	}
	return page, nil
}

func invalidResponse(call datasetCall) error {
	return &DatasetError{
		Message:        "the " + call.op + " response was invalid or did not match the request",
		OutcomeUnknown: call.write,
	}
}

var errDatasetDecode = errors.New("dataset response could not be decoded")

// decodeObject decodes a complete JSON object after checking UTF-8, before
// encoding/json can silently replace malformed bytes. json.Unmarshal rejects
// trailing values.
func decodeObject(body []byte) (map[string]json.RawMessage, error) {
	if !utf8.Valid(body) {
		return nil, errDatasetDecode
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return nil, errDatasetDecode
	}
	return fields, nil
}

func requiredString(fields map[string]json.RawMessage, key string) (string, error) {
	var value string
	raw, ok := fields[key]
	if !ok || json.Unmarshal(raw, &value) != nil || value == "" {
		return "", errDatasetDecode
	}
	return value, nil
}

// nullableString accepts an absent key, null, or a string.
func nullableString(fields map[string]json.RawMessage, key string) (string, error) {
	raw, ok := fields[key]
	if !ok || string(raw) == "null" {
		return "", nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", errDatasetDecode
	}
	return value, nil
}

// rawValue returns a copy of a present, non-null JSON value, or nil.
func rawValue(fields map[string]json.RawMessage, key string) json.RawMessage {
	raw, ok := fields[key]
	if !ok || string(raw) == "null" {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func requiredTime(fields map[string]json.RawMessage, key string) (time.Time, error) {
	value, err := requiredString(fields, key)
	if err != nil {
		return time.Time{}, err
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, errDatasetDecode
	}
	return parsed, nil
}

func decodeDataset(body []byte) (Dataset, error) {
	fields, err := decodeObject(body)
	if err != nil {
		return Dataset{}, err
	}
	var dataset Dataset
	if dataset.ID, err = requiredString(fields, "id"); err != nil {
		return Dataset{}, err
	}
	if dataset.Name, err = requiredString(fields, "name"); err != nil {
		return Dataset{}, err
	}
	if dataset.Description, err = nullableString(fields, "description"); err != nil {
		return Dataset{}, err
	}
	if dataset.CreatedAt, err = requiredTime(fields, "createdAt"); err != nil {
		return Dataset{}, err
	}
	if dataset.UpdatedAt, err = requiredTime(fields, "updatedAt"); err != nil {
		return Dataset{}, err
	}
	dataset.Metadata = rawValue(fields, "metadata")
	dataset.InputSchema = rawValue(fields, "inputSchema")
	dataset.ExpectedOutputSchema = rawValue(fields, "expectedOutputSchema")
	return dataset, nil
}

func decodeDatasetItem(body []byte) (DatasetItem, error) {
	fields, err := decodeObject(body)
	if err != nil {
		return DatasetItem{}, err
	}
	return decodeDatasetItemFields(fields)
}

func decodeDatasetItemFields(fields map[string]json.RawMessage) (DatasetItem, error) {
	var item DatasetItem
	var err error
	if item.ID, err = requiredString(fields, "id"); err != nil {
		return DatasetItem{}, err
	}
	if item.DatasetID, err = requiredString(fields, "datasetId"); err != nil {
		return DatasetItem{}, err
	}
	if item.DatasetName, err = requiredString(fields, "datasetName"); err != nil {
		return DatasetItem{}, err
	}
	if item.Status, err = requiredString(fields, "status"); err != nil {
		return DatasetItem{}, err
	}
	if item.Status != "ACTIVE" && item.Status != "ARCHIVED" {
		return DatasetItem{}, errDatasetDecode
	}
	if item.SourceTraceID, err = nullableString(fields, "sourceTraceId"); err != nil {
		return DatasetItem{}, err
	}
	if item.SourceObservationID, err = nullableString(fields, "sourceObservationId"); err != nil {
		return DatasetItem{}, err
	}
	if item.CreatedAt, err = requiredTime(fields, "createdAt"); err != nil {
		return DatasetItem{}, err
	}
	if item.UpdatedAt, err = requiredTime(fields, "updatedAt"); err != nil {
		return DatasetItem{}, err
	}
	item.Input = rawValue(fields, "input")
	item.ExpectedOutput = rawValue(fields, "expectedOutput")
	item.Metadata = rawValue(fields, "metadata")
	return item, nil
}

func decodeDatasetItemPage(body []byte) (DatasetItemPage, error) {
	fields, err := decodeObject(body)
	if err != nil {
		return DatasetItemPage{}, err
	}
	var data []json.RawMessage
	if raw, ok := fields["data"]; !ok || json.Unmarshal(raw, &data) != nil || data == nil {
		return DatasetItemPage{}, errDatasetDecode
	}
	// Wire keys are camelCase; extract them explicitly, as decodePrompt does.
	var meta map[string]json.RawMessage
	if raw, ok := fields["meta"]; !ok || json.Unmarshal(raw, &meta) != nil || meta == nil {
		return DatasetItemPage{}, errDatasetDecode
	}
	page := DatasetItemPage{Items: make([]DatasetItem, 0, len(data))}
	for key, target := range map[string]*int{
		"page": &page.Page, "limit": &page.Limit,
		"totalItems": &page.TotalItems, "totalPages": &page.TotalPages,
	} {
		raw, ok := meta[key]
		if !ok || json.Unmarshal(raw, target) != nil {
			return DatasetItemPage{}, errDatasetDecode
		}
	}
	for _, raw := range data {
		var itemFields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &itemFields); err != nil || itemFields == nil {
			return DatasetItemPage{}, errDatasetDecode
		}
		item, err := decodeDatasetItemFields(itemFields)
		if err != nil {
			return DatasetItemPage{}, err
		}
		page.Items = append(page.Items, item)
	}
	return page, nil
}
