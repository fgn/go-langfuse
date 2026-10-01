package langfuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
	"github.com/fgn/go-langfuse/internal/transport"
)

// ErrDatasetNotFound reports that the dataset named by a dataset operation
// does not exist. Test with errors.Is.
var ErrDatasetNotFound = errors.New("langfuse: dataset not found")

// ErrDatasetItemNotFound reports that the requested dataset item, or its
// dataset, does not exist. Test with errors.Is.
var ErrDatasetItemNotFound = errors.New("langfuse: dataset item not found")

// ErrWriteOutcomeUnknown reports a dataset write that may have been applied:
// it failed after it was sent, other than by a status Langfuse returns before
// applying a write, or its success response was invalid. The SDK never
// repeats a write. Test with errors.Is.
var ErrWriteOutcomeUnknown = errors.New("langfuse: write outcome unknown")

const (
	// datasetOperationBudget bounds one dataset request with its retries,
	// even when the caller's context has no deadline.
	datasetOperationBudget = 30 * time.Second
	maxDatasetFieldBytes   = 1 << 20
	// The server accepts 4.5 MB item bodies and 1 MB dataset bodies.
	maxDatasetItemBodyBytes = 4 << 20
	maxDatasetBodyBytes     = 1 << 20
	defaultDatasetPageSize  = 20
)

// DatasetSpec creates or updates a dataset. Langfuse upserts datasets by
// name: non-nil fields replace the stored values and nil fields keep them.
type DatasetSpec struct {
	// Name identifies the dataset and may contain "/" folder separators.
	// Required.
	Name string
	// Description replaces the stored description when non-nil; a pointer
	// to "" clears it. It is not masked.
	Description *string
	// Metadata replaces the stored metadata when non-nil, even when empty;
	// Langfuse cannot clear it. It is masked as MaskDatasetMetadata.
	Metadata map[string]any
	// InputSchema and ExpectedOutputSchema are JSON Schemas that Langfuse
	// validates item content against; JSON null removes one. They are not
	// masked.
	InputSchema          json.RawMessage
	ExpectedOutputSchema json.RawMessage
}

// Dataset is one Langfuse dataset as stored by the server.
type Dataset struct {
	ID          string
	Name        string
	Description string
	// Metadata and the schemas are the server's JSON, verbatim; nil when
	// absent or null.
	Metadata             json.RawMessage
	InputSchema          json.RawMessage
	ExpectedOutputSchema json.RawMessage
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// DatasetItemStatus is the lifecycle state of a dataset item.
type DatasetItemStatus string

const (
	DatasetItemActive   DatasetItemStatus = "ACTIVE"
	DatasetItemArchived DatasetItemStatus = "ARCHIVED"
)

// DatasetItemSpec creates or updates one dataset item.
//
// Nil content fields, including typed nils, are omitted: a new item stores
// null and an update keeps the stored value. Content cannot be cleared, so a
// Mask result of nil is an error. json.RawMessage values must be valid JSON
// and are sent verbatim.
type DatasetItemSpec struct {
	// DatasetName selects the dataset. Required.
	DatasetName string
	// ID selects the item to create or update. When empty, the server
	// creates a new item with a generated ID on every request.
	ID string
	// Input, ExpectedOutput, and Metadata are masked as MaskDatasetItemInput,
	// MaskDatasetItemExpectedOutput, and MaskDatasetItemMetadata. Each is
	// limited to 1 MiB serialized.
	Input          any
	ExpectedOutput any
	Metadata       map[string]any
	// SourceTraceID and SourceObservationID link the item to the trace it
	// was curated from.
	SourceTraceID       string
	SourceObservationID string
	// Status is empty (ACTIVE for a new item), DatasetItemActive, or
	// DatasetItemArchived.
	Status DatasetItemStatus
}

// DatasetItem is one dataset item as stored by the server. The content
// fields hold the server's JSON verbatim, nil when absent or null; a string
// value is a quoted JSON string. Media references are not decoded.
type DatasetItem struct {
	ID                  string
	DatasetID           string
	DatasetName         string
	Status              DatasetItemStatus
	Input               json.RawMessage
	ExpectedOutput      json.RawMessage
	Metadata            json.RawMessage
	SourceTraceID       string
	SourceObservationID string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// DatasetItemQuery selects the items of one dataset for
// [Client.DatasetItems].
type DatasetItemQuery struct {
	// DatasetName is required.
	DatasetName string
	// AsOf, when non-zero, reads the item versions valid at that instant,
	// sent as UTC with millisecond precision; pass the same value as
	// ExperimentItem.Version. A server not using Langfuse's default versioned
	// dataset implementation ignores it and returns current items.
	AsOf time.Time
	// SourceTraceID and SourceObservationID restrict the listing to items
	// curated from that trace or observation.
	SourceTraceID       string
	SourceObservationID string
	// PageSize is the number of items per request: 0 selects 20, and
	// Langfuse accepts at most 100. Lower it when items are large; a page
	// response is limited to 16 MiB.
	PageSize int
}

// datasetGate admits dataset I/O. Shutdown refuses new operations and
// cancels admitted ones before the OpenTelemetry teardown, then drains them.
type datasetGate struct {
	mu      sync.Mutex
	closing bool
	wg      sync.WaitGroup
	// newOperationContext bounds an operation by the caller's context, the
	// operation budget, and the client lifecycle.
	newOperationContext func(parent context.Context) (context.Context, context.CancelFunc)
	cancelLifecycle     context.CancelFunc
	stop                <-chan struct{}
}

func newDatasetGate() *datasetGate {
	lifecycle, cancel := context.WithCancel(context.Background())
	return &datasetGate{
		newOperationContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			operation, cancelOperation := context.WithTimeout(parent, datasetOperationBudget)
			detach := context.AfterFunc(lifecycle, cancelOperation)
			return operation, func() { detach(); cancelOperation() }
		},
		cancelLifecycle: cancel,
		stop:            lifecycle.Done(),
	}
}

var errDatasetShutdown = errors.New("langfuse: dataset request after client shutdown")

// enter admits one operation. Callers enter only after Mask and
// serialization, so a callback that calls Shutdown cannot block the drain.
// release must be called once the operation's I/O has finished.
func (g *datasetGate) enter(parent context.Context) (ctx context.Context, release func(), err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing {
		return nil, nil, errDatasetShutdown
	}
	g.wg.Add(1)
	ctx, cancel := g.newOperationContext(parent)
	return ctx, func() { cancel(); g.wg.Done() }, nil
}

func (g *datasetGate) lifecycleEnded() bool {
	select {
	case <-g.stop:
		return true
	default:
		return false
	}
}

// beginShutdown stops admission and cancels all admitted dataset I/O.
func (g *datasetGate) beginShutdown() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.closing = true
	g.mu.Unlock()
	g.cancelLifecycle()
}

// shutdown drains admitted operations, bounded by ctx.
func (g *datasetGate) shutdown(ctx context.Context) error {
	if g == nil {
		return nil
	}
	g.beginShutdown()
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("langfuse: dataset shutdown: %w", ctx.Err())
	}
}

// datasetError is a payload-free failure that can match several sentinels
// with errors.Is.
type datasetError struct {
	message string
	wrapped []error
}

func (e *datasetError) Error() string { return e.message }

func (e *datasetError) Unwrap() []error { return e.wrapped }

// datasetReady checks a call's context and arguments, then the client.
func (c *Client) datasetReady(ctx context.Context, invalid error) error {
	if ctx == nil {
		return errors.New("langfuse: dataset context is nil")
	}
	if invalid != nil {
		return invalid
	}
	return c.datasetUnavailable()
}

func (c *Client) datasetUnavailable() error {
	if c == nil || c.isDisabled() || c.datasets == nil || c.datasetTransport == nil {
		return errors.New("langfuse: dataset request on a disabled client")
	}
	if c.stopped.Load() {
		return errDatasetShutdown
	}
	return nil
}

func requireDataset(field, value string) error {
	if value == "" {
		return errors.New("langfuse: " + field + " is required")
	}
	return nil
}

// runDatasetOperation admits and performs one dataset request. notFound is
// the operation's sentinel for a 404, or nil.
func runDatasetOperation[T any](
	ctx context.Context, c *Client, notFound error, call func(context.Context) (T, error),
) (T, error) {
	var zero T
	operationCtx, release, err := c.datasets.enter(ctx)
	if err != nil {
		return zero, err
	}
	result, err := call(operationCtx)
	release()
	if err != nil {
		return zero, c.datasetFailure(ctx, notFound, err)
	}
	return result, nil
}

func (c *Client) datasetFailure(ctx context.Context, notFound, err error) error {
	var failure *transport.DatasetError
	if !errors.As(err, &failure) {
		return errors.New("langfuse: dataset request failed")
	}
	if failure.NotFound && notFound != nil {
		return notFound
	}
	var unknown, cause error
	if failure.OutcomeUnknown {
		unknown = ErrWriteOutcomeUnknown
	}
	switch {
	case ctx.Err() != nil:
		cause = ctx.Err()
	case c.datasets.lifecycleEnded():
		cause = errDatasetShutdown
	default:
		cause = failure.Cause // the operation budget expired, or nil
	}
	return errors.Join(errors.New("langfuse: "+failure.Message), unknown, cause)
}

// UpsertDataset creates the named dataset, or updates it when it exists. It
// blocks for at most 30 seconds, bounded further by ctx. The write is sent
// once; a failure that may follow its application wraps
// [ErrWriteOutcomeUnknown]. Metadata is masked once, and a nil result, a
// panic, or a result that is not a map[string]any fails before any request.
func (c *Client) UpsertDataset(ctx context.Context, spec DatasetSpec) (Dataset, error) {
	if err := c.datasetReady(ctx, requireDataset("dataset name", spec.Name)); err != nil {
		return Dataset{}, err
	}
	body := map[string]any{"name": spec.Name}
	if spec.Description != nil {
		body["description"] = *spec.Description
	}
	if spec.Metadata != nil {
		metadata, err := c.maskedDatasetContent(MaskDatasetMetadata, spec.Metadata, true)
		if err != nil {
			return Dataset{}, err
		}
		body["metadata"] = metadata
	}
	if len(spec.InputSchema) != 0 {
		body["inputSchema"] = spec.InputSchema
	}
	if len(spec.ExpectedOutputSchema) != 0 {
		body["expectedOutputSchema"] = spec.ExpectedOutputSchema
	}
	payload, err := marshalDatasetBody(body, maxDatasetBodyBytes, "dataset")
	if err != nil {
		return Dataset{}, err
	}
	result, err := runDatasetOperation(ctx, c, nil, func(ctx context.Context) (transport.Dataset, error) {
		return c.datasetTransport.UpsertDataset(ctx, payload, spec.Name)
	})
	return Dataset(result), err
}

// GetDataset reads one dataset by name. A missing dataset wraps
// [ErrDatasetNotFound]. Transient failures are retried within a 30-second
// budget bounded by ctx.
func (c *Client) GetDataset(ctx context.Context, name string) (Dataset, error) {
	if err := c.datasetReady(ctx, requireDataset("dataset name", name)); err != nil {
		return Dataset{}, err
	}
	result, err := runDatasetOperation(ctx, c, ErrDatasetNotFound, func(ctx context.Context) (transport.Dataset, error) {
		return c.datasetTransport.GetDataset(ctx, name)
	})
	return Dataset(result), err
}

// UpsertDatasetItem creates or updates one dataset item and returns the
// stored item. A missing dataset wraps [ErrDatasetNotFound].
//
// Every accepted write creates a new item version, and the last write wins.
// The write is sent once; a failure that may follow its application wraps
// [ErrWriteOutcomeUnknown]. Repeating such a write with an ID may add a
// version or overwrite a concurrent change; without an ID it can create a
// duplicate item.
//
// Each supplied content field is masked once. A nil result, a panic, a
// metadata result that is not a map[string]any, or an oversized or
// unserializable value fails before any request. Upserting does not redact
// earlier versions.
func (c *Client) UpsertDatasetItem(ctx context.Context, spec DatasetItemSpec) (DatasetItem, error) {
	if err := c.datasetReady(ctx, requireDataset("dataset name", spec.DatasetName)); err != nil {
		return DatasetItem{}, err
	}
	body := map[string]any{"datasetName": spec.DatasetName}
	if spec.ID != "" {
		body["id"] = spec.ID
	}
	for _, field := range []struct {
		name  string
		mask  MaskField
		value any
		isMap bool
	}{
		{"input", MaskDatasetItemInput, spec.Input, false},
		{"expectedOutput", MaskDatasetItemExpectedOutput, spec.ExpectedOutput, false},
		{"metadata", MaskDatasetItemMetadata, spec.Metadata, true},
	} {
		if isNilValue(field.value) {
			continue
		}
		encoded, err := c.maskedDatasetContent(field.mask, field.value, field.isMap)
		if err != nil {
			return DatasetItem{}, err
		}
		body[field.name] = encoded
	}
	if spec.SourceTraceID != "" {
		body["sourceTraceId"] = spec.SourceTraceID
	}
	if spec.SourceObservationID != "" {
		body["sourceObservationId"] = spec.SourceObservationID
	}
	if spec.Status != "" {
		body["status"] = string(spec.Status)
	}
	payload, err := marshalDatasetBody(body, maxDatasetItemBodyBytes, "dataset item")
	if err != nil {
		return DatasetItem{}, err
	}
	result, err := runDatasetOperation(ctx, c, ErrDatasetNotFound, func(ctx context.Context) (transport.DatasetItem, error) {
		return c.datasetTransport.UpsertItem(ctx, payload, spec.DatasetName, spec.ID)
	})
	return datasetItemFromWire(result), err
}

// GetDatasetItem reads one dataset item by ID. A missing item, or an item
// whose dataset was deleted, wraps [ErrDatasetItemNotFound].
func (c *Client) GetDatasetItem(ctx context.Context, id string) (DatasetItem, error) {
	if err := c.datasetReady(ctx, requireDataset("dataset item ID", id)); err != nil {
		return DatasetItem{}, err
	}
	result, err := runDatasetOperation(ctx, c, ErrDatasetItemNotFound, func(ctx context.Context) (transport.DatasetItem, error) {
		return c.datasetTransport.GetItem(ctx, id)
	})
	return datasetItemFromWire(result), err
}

// DeleteDatasetItem asks Langfuse to delete one dataset item. On a versioned
// server this writes a deletion marker, so earlier versions and the
// experiments that referenced them remain. A missing item wraps
// [ErrDatasetItemNotFound]. The write follows the [ErrWriteOutcomeUnknown]
// rules of [Client.UpsertDatasetItem].
func (c *Client) DeleteDatasetItem(ctx context.Context, id string) error {
	if err := c.datasetReady(ctx, requireDataset("dataset item ID", id)); err != nil {
		return err
	}
	_, err := runDatasetOperation(ctx, c, ErrDatasetItemNotFound, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, c.datasetTransport.DeleteItem(ctx, id)
	})
	return err
}

// DatasetItems iterates over the ACTIVE items of one dataset. Iteration is
// lazy and synchronous: each page is one request within a 30-second budget,
// sent when the loop reaches it, and breaking out of the loop sends no
// further requests. Pages are separate requests, so without AsOf items
// written during iteration can be skipped or repeated. A missing dataset
// (wrapping [ErrDatasetNotFound]) or a failed request yields one final error
// with a zero DatasetItem, after which the loop ends.
func (c *Client) DatasetItems(ctx context.Context, query DatasetItemQuery) iter.Seq2[DatasetItem, error] {
	return func(yield func(DatasetItem, error) bool) {
		if ctx == nil {
			yield(DatasetItem{}, errors.New("langfuse: dataset context is nil"))
			return
		}
		if err := requireDataset("dataset name", query.DatasetName); err != nil {
			yield(DatasetItem{}, err)
			return
		}
		list := transport.DatasetItemListQuery{
			DatasetName:         query.DatasetName,
			SourceTraceID:       query.SourceTraceID,
			SourceObservationID: query.SourceObservationID,
			Limit:               query.PageSize,
		}
		if list.Limit == 0 {
			list.Limit = defaultDatasetPageSize
		}
		if !query.AsOf.IsZero() {
			list.Version = formatDatasetInstant(query.AsOf)
		}
		for list.Page = 1; ; list.Page++ {
			if err := c.datasetUnavailable(); err != nil {
				yield(DatasetItem{}, err)
				return
			}
			page, err := runDatasetOperation(ctx, c, ErrDatasetNotFound, func(ctx context.Context) (transport.DatasetItemPage, error) {
				return c.datasetTransport.ListItems(ctx, list)
			})
			if err != nil {
				yield(DatasetItem{}, err)
				return
			}
			for _, item := range page.Items {
				if !yield(datasetItemFromWire(item), nil) {
					return
				}
			}
			// An empty page ends a listing whose page count overstates it.
			if len(page.Items) == 0 || list.Page >= page.Meta.TotalPages {
				return
			}
		}
	}
}

// maskedDatasetContent applies Mask once to a supplied content value and
// serializes the result. Unlike observation content, every failure is an
// error: an omitted field would silently keep the stored value.
func (c *Client) maskedDatasetContent(field MaskField, value any, isMap bool) (json.RawMessage, error) {
	name := string(field)
	masked := value
	if c.mask != nil {
		var panicked bool
		func() {
			defer func() {
				if recover() != nil {
					panicked = true
				}
			}()
			masked = c.mask(name, value)
		}()
		if panicked {
			return nil, errors.New("langfuse: masker panicked on " + name)
		}
		if isNilValue(masked) {
			return nil, errors.New("langfuse: masker removed " + name + "; omitting it would keep the stored value")
		}
	}
	if isMap {
		if _, ok := masked.(map[string]any); !ok {
			return nil, errors.New("langfuse: masker changed " + name + " to an unsupported type")
		}
	}
	if raw, ok := masked.(json.RawMessage); ok {
		if !json.Valid(raw) || !utf8.Valid(raw) {
			return nil, errors.New("langfuse: " + name + " is not valid UTF-8 JSON")
		}
	}
	data, err, panicked := lfattr.MarshalJSON(masked, maxDatasetFieldBytes)
	if panicked || err != nil {
		return nil, errors.New("langfuse: " + name + " could not be serialized or exceeds the 1 MiB limit")
	}
	if len(data) > maxDatasetFieldBytes {
		return nil, errors.New("langfuse: " + name + " exceeds the 1 MiB limit")
	}
	if string(data) == "null" {
		return nil, errors.New("langfuse: " + name + " is JSON null; omitting it would keep the stored value")
	}
	return data, nil
}

// marshalDatasetBody serializes a request body of strings and validated
// JSON, so no caller code runs here. Strings are checked first because
// encoding/json silently replaces invalid UTF-8.
func marshalDatasetBody(body map[string]any, limit int, kind string) ([]byte, error) {
	for _, value := range body {
		if text, ok := value.(string); ok && !utf8.ValidString(text) {
			return nil, errors.New("langfuse: " + kind + " request contains invalid UTF-8")
		}
	}
	data, err := json.Marshal(body)
	if err != nil || !utf8.Valid(data) {
		return nil, errors.New("langfuse: " + kind + " request could not be serialized")
	}
	if len(data) > limit {
		return nil, fmt.Errorf("langfuse: %s request exceeds the %d MiB limit", kind, limit>>20)
	}
	return data, nil
}

func isNilValue(value any) bool {
	if raw, ok := value.(json.RawMessage); ok {
		return len(raw) == 0
	}
	return lfattr.IsNil(value)
}

func containsControl(value string) bool {
	return strings.ContainsFunc(value, unicode.IsControl)
}

// validDatasetInstant reports whether t fits a four-digit RFC 3339 year.
func validDatasetInstant(t time.Time) bool {
	year := t.UTC().Year()
	return year >= 0 && year <= 9999
}

// formatDatasetInstant renders the millisecond-precision UTC form that the
// server parses and stores.
func formatDatasetInstant(t time.Time) string {
	return t.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z")
}

func datasetItemFromWire(wire transport.DatasetItem) DatasetItem {
	return DatasetItem{
		ID:                  wire.ID,
		DatasetID:           wire.DatasetID,
		DatasetName:         wire.DatasetName,
		Status:              DatasetItemStatus(wire.Status),
		Input:               wire.Input,
		ExpectedOutput:      wire.ExpectedOutput,
		Metadata:            wire.Metadata,
		SourceTraceID:       wire.SourceTraceID,
		SourceObservationID: wire.SourceObservationID,
		CreatedAt:           wire.CreatedAt,
		UpdatedAt:           wire.UpdatedAt,
	}
}
