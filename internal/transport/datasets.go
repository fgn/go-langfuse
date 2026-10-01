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
	"strconv"
	"time"
	"unicode/utf8"
)

const (
	// Dataset calls block their caller, so retries are short, as for prompts.
	datasetRetryLimit           = 2
	datasetRetryInitialInterval = 500 * time.Millisecond

	// datasetResponseLimit leaves room for the server echoing a 4.5 MB item.
	datasetResponseLimit     = 8 << 20
	datasetPageResponseLimit = 16 << 20
)

// DatasetError is a dataset REST failure with static text.
type DatasetError struct {
	Message  string
	NotFound bool
	// OutcomeUnknown marks a write that may have been applied.
	OutcomeUnknown bool
	// Cause is the context error when the request context ended.
	Cause error
}

func (e *DatasetError) Error() string { return "langfuse transport: " + e.Message }

// DatasetsClient calls the Langfuse dataset REST API and is safe for
// concurrent use.
type DatasetsClient struct {
	base       string
	publicKey  string
	secretKey  string
	sdkVersion string
	client     *http.Client
}

// NewDatasetsClient builds a dataset client from a validated configuration.
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
			// Never follow redirects: the target would receive the
			// credentials and the dataset content.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

type datasetCall struct {
	op     string // static error text
	method string
	url    string
	body   []byte
	write  bool
	limit  int
}

// do performs call and returns the 2xx body. Reads retry network errors,
// 408, 429, and 5xx; a write is sent once.
func (d *DatasetsClient) do(ctx context.Context, call datasetCall) ([]byte, error) {
	interval := datasetRetryInitialInterval
	for attempt := 0; ; attempt++ {
		body, retryable, retryAfter, err := d.attempt(ctx, call)
		if err == nil {
			return body, nil
		}
		if !retryable || attempt >= datasetRetryLimit || ctx.Err() != nil {
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

func canceledError(call datasetCall, cause error, unknown bool) error {
	return &DatasetError{Message: "the " + call.op + " request was canceled", OutcomeUnknown: unknown, Cause: cause}
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
	request, err := http.NewRequestWithContext(ctx, call.method, call.url, requestBody)
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
			return nil, false, 0, canceledError(call, cause, call.write)
		}
		var dial *net.OpError
		sent := !errors.As(err, &dial) || dial.Op != "dial"
		return nil, !call.write, 0, &DatasetError{
			Message:        "the " + call.op + " request failed",
			OutcomeUnknown: call.write && sent,
		}
	}
	defer func() { _ = response.Body.Close() }()
	if status := response.StatusCode; status < 200 || status >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxScoreDrainBytes))
		failure := &DatasetError{
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
		return nil, false, 0, &DatasetError{
			Message:        "the " + call.op + " response could not be read",
			OutcomeUnknown: call.write,
		}
	case len(body) > call.limit:
		return nil, false, 0, &DatasetError{
			Message:        "the " + call.op + " response exceeds the local size limit",
			OutcomeUnknown: call.write,
		}
	}
	return body, false, 0, nil
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

// Dataset is one decoded dataset.
//
//nolint:tagliatelle // Langfuse wire keys are camelCase.
type Dataset struct {
	ID                   string          `json:"id"`
	Name                 string          `json:"name"`
	Description          string          `json:"description"`
	Metadata             json.RawMessage `json:"metadata"`
	InputSchema          json.RawMessage `json:"inputSchema"`
	ExpectedOutputSchema json.RawMessage `json:"expectedOutputSchema"`
	CreatedAt            time.Time       `json:"createdAt"`
	UpdatedAt            time.Time       `json:"updatedAt"`
}

// valid normalizes JSON null to nil and reports whether the required fields
// are present.
func (dataset *Dataset) valid() bool {
	dataset.Metadata = nonNull(dataset.Metadata)
	dataset.InputSchema = nonNull(dataset.InputSchema)
	dataset.ExpectedOutputSchema = nonNull(dataset.ExpectedOutputSchema)
	return dataset.ID != "" && dataset.Name != "" && !dataset.CreatedAt.IsZero() && !dataset.UpdatedAt.IsZero()
}

// DatasetItem is one decoded dataset item.
//
//nolint:tagliatelle // Langfuse wire keys are camelCase.
type DatasetItem struct {
	ID                  string          `json:"id"`
	DatasetID           string          `json:"datasetId"`
	DatasetName         string          `json:"datasetName"`
	Status              string          `json:"status"`
	Input               json.RawMessage `json:"input"`
	ExpectedOutput      json.RawMessage `json:"expectedOutput"`
	Metadata            json.RawMessage `json:"metadata"`
	SourceTraceID       string          `json:"sourceTraceId"`
	SourceObservationID string          `json:"sourceObservationId"`
	CreatedAt           time.Time       `json:"createdAt"`
	UpdatedAt           time.Time       `json:"updatedAt"`
}

// valid normalizes JSON null to nil and reports whether the required fields
// are present.
func (item *DatasetItem) valid() bool {
	item.Input = nonNull(item.Input)
	item.ExpectedOutput = nonNull(item.ExpectedOutput)
	item.Metadata = nonNull(item.Metadata)
	return item.ID != "" && item.DatasetID != "" && item.DatasetName != "" &&
		(item.Status == "ACTIVE" || item.Status == "ARCHIVED") &&
		!item.CreatedAt.IsZero() && !item.UpdatedAt.IsZero()
}

func nonNull(raw json.RawMessage) json.RawMessage {
	if string(raw) == "null" {
		return nil
	}
	return raw
}

// DatasetItemPage is one decoded page of dataset items.
//
//nolint:tagliatelle // Langfuse wire keys are camelCase.
type DatasetItemPage struct {
	Items []DatasetItem `json:"data"`
	Meta  *struct {
		TotalPages int `json:"totalPages"`
	} `json:"meta"`
}

// DatasetItemListQuery selects one page of dataset items. Version is a
// formatted as-of instant.
type DatasetItemListQuery struct {
	DatasetName         string
	SourceTraceID       string
	SourceObservationID string
	Version             string
	Page                int
	Limit               int
}

// send performs call and decodes a response that valid accepts. The UTF-8
// check runs first because encoding/json silently replaces invalid bytes.
func send[T any](ctx context.Context, d *DatasetsClient, call datasetCall, valid func(*T) bool) (T, error) {
	var value T
	body, err := d.do(ctx, call)
	if err != nil {
		return value, err
	}
	if !utf8.Valid(body) || json.Unmarshal(body, &value) != nil || !valid(&value) {
		var zero T
		return zero, &DatasetError{
			Message:        "the " + call.op + " response was invalid or did not match the request",
			OutcomeUnknown: call.write,
		}
	}
	return value, nil
}

// UpsertDataset posts one serialized dataset body.
func (d *DatasetsClient) UpsertDataset(ctx context.Context, body []byte, name string) (Dataset, error) {
	call := datasetCall{
		op: "dataset upsert", method: http.MethodPost, url: d.base + "/v2/datasets",
		body: body, write: true, limit: datasetResponseLimit,
	}
	return send(ctx, d, call, func(dataset *Dataset) bool { return dataset.valid() && dataset.Name == name })
}

// GetDataset reads one dataset by name.
func (d *DatasetsClient) GetDataset(ctx context.Context, name string) (Dataset, error) {
	call := datasetCall{
		op: "dataset read", method: http.MethodGet,
		url: d.base + "/v2/datasets/" + url.PathEscape(name), limit: datasetResponseLimit,
	}
	return send(ctx, d, call, func(dataset *Dataset) bool { return dataset.valid() && dataset.Name == name })
}

// UpsertItem posts one serialized dataset item body.
func (d *DatasetsClient) UpsertItem(ctx context.Context, body []byte, datasetName, id string) (DatasetItem, error) {
	call := datasetCall{
		op: "dataset item upsert", method: http.MethodPost, url: d.base + "/dataset-items",
		body: body, write: true, limit: datasetResponseLimit,
	}
	return send(ctx, d, call, func(item *DatasetItem) bool {
		return item.valid() && item.DatasetName == datasetName && (id == "" || item.ID == id)
	})
}

// GetItem reads one dataset item by ID.
func (d *DatasetsClient) GetItem(ctx context.Context, id string) (DatasetItem, error) {
	call := datasetCall{
		op: "dataset item read", method: http.MethodGet,
		url: d.base + "/dataset-items/" + url.PathEscape(id), limit: datasetResponseLimit,
	}
	return send(ctx, d, call, func(item *DatasetItem) bool { return item.valid() && item.ID == id })
}

// DeleteItem deletes one dataset item by ID.
func (d *DatasetsClient) DeleteItem(ctx context.Context, id string) error {
	_, err := d.do(ctx, datasetCall{
		op: "dataset item delete", method: http.MethodDelete,
		url:   d.base + "/dataset-items/" + url.PathEscape(id),
		write: true, limit: datasetResponseLimit,
	})
	return err
}

// ListItems reads one page of dataset items.
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
		url: d.base + "/dataset-items?" + values.Encode(), limit: datasetPageResponseLimit,
	}
	return send(ctx, d, call, func(page *DatasetItemPage) bool {
		if page.Items == nil || page.Meta == nil {
			return false
		}
		for index := range page.Items {
			if !page.Items[index].valid() {
				return false
			}
		}
		return true
	})
}
