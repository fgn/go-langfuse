package langfuse

import (
	"bytes"
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
// does not exist in the Langfuse project. Test with errors.Is.
var ErrDatasetNotFound = errors.New("langfuse: dataset not found")

// ErrDatasetItemNotFound reports that the requested dataset item, or the
// dataset it belonged to, does not exist. Test with errors.Is.
var ErrDatasetItemNotFound = errors.New("langfuse: dataset item not found")

// ErrWriteOutcomeUnknown reports a dataset write that may have been applied:
// it failed after the request could have reached the server (a network error
// after sending, a 5xx response, or cancellation in flight), or the server's
// success response could not be read or did not match the request. The SDK
// never retries such a write. Test with errors.Is and decide whether to
// repeat it; see [Client.UpsertDatasetItem] for what a repeat means.
var ErrWriteOutcomeUnknown = errors.New("langfuse: write outcome unknown")

const (
	// datasetOperationBudget bounds one dataset HTTP operation end to end:
	// every attempt, backoff, and Retry-After wait, even when the caller's
	// context has no deadline. Each page of DatasetItems is one operation.
	datasetOperationBudget = 30 * time.Second
	// maxDatasetIdentifierBytes is a local wire-safety bound for dataset
	// names and item, trace, and observation IDs. Langfuse allows item IDs of
	// 255 characters; a byte bound is never looser.
	maxDatasetIdentifierBytes = 255
	maxDatasetDescription     = lfattr.MaxDirectStringBytes
	// maxDatasetFieldBytes bounds each serialized content field after Mask.
	maxDatasetFieldBytes = 1 << 20
	// maxDatasetItemBodyBytes bounds a complete item write; the server
	// accepts 4.5 MB.
	maxDatasetItemBodyBytes = 4 << 20
	// maxDatasetBodyBytes bounds a complete dataset write; the server route
	// uses the framework's default 1 MB body limit.
	maxDatasetBodyBytes    = 1 << 20
	defaultDatasetPageSize = 20
	maxDatasetPageSize     = 100
	// maxDatasetPages bounds one iteration; it is checked against the first
	// page's totalPages before any item is yielded.
	maxDatasetPages = 10000
	// maxDatasetAsOfSkew tolerates clock skew for an as-of instant taken
	// from the local clock just before a read.
	maxDatasetAsOfSkew = time.Minute
	// maxDatasetOperations bounds concurrently admitted dataset requests, so
	// response buffers stay bounded: at most 16 × 16 MiB pages.
	maxDatasetOperations = 16
)

// DatasetSpec creates or updates a dataset. Langfuse upserts datasets by
// name, so an existing dataset's description, metadata, and schemas are
// replaced by the fields supplied here, while nil fields keep the stored
// values.
type DatasetSpec struct {
	// Name identifies the dataset and may contain "/" folder separators.
	// Required: 1 to 255 bytes of valid UTF-8 without control characters or
	// surrounding whitespace.
	Name string
	// Description replaces the stored description when non-nil; a pointer
	// to "" clears it. It is not processed by Config.Mask. At most 16 KiB.
	Description *string
	// Metadata replaces the stored metadata when non-nil, even when empty.
	// It is passed to Config.Mask as MaskDatasetMetadata. Langfuse cannot
	// clear dataset metadata; an empty map stores {}.
	Metadata map[string]any
	// InputSchema and ExpectedOutputSchema are JSON Schema objects that
	// Langfuse validates item content against: nil keeps the stored schema,
	// an object replaces it, and JSON null removes it. They are not
	// processed by Config.Mask.
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
// Content fields that are nil (including typed nils) are not supplied: a new
// item stores null and an update keeps the stored value. The server treats
// JSON null the same way, so a field cannot be cleared, and a field whose
// Mask result is nil is an error rather than an omission. json.RawMessage
// values are sent verbatim after a validity check.
type DatasetItemSpec struct {
	// DatasetName selects the dataset. Required; same rules as
	// DatasetSpec.Name.
	DatasetName string
	// ID selects the item to create or update. When empty, the server
	// creates a new item with a generated ID on every request. Same rules as
	// DatasetSpec.Name.
	ID string
	// Input, ExpectedOutput, and Metadata are passed to Config.Mask as
	// MaskDatasetItemInput, MaskDatasetItemExpectedOutput, and
	// MaskDatasetItemMetadata. Each is limited to 1 MiB serialized.
	Input          any
	ExpectedOutput any
	Metadata       map[string]any
	// SourceTraceID and SourceObservationID link the item to the trace it
	// was curated from. SourceObservationID requires SourceTraceID.
	SourceTraceID       string
	SourceObservationID string
	// Status is empty (the server default, ACTIVE, for new items),
	// DatasetItemActive, or DatasetItemArchived.
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
	// AsOf reads the dataset as it was at this instant: every page requests
	// the item versions valid at AsOf, sent as UTC with millisecond
	// precision. Use it for experiment cohorts and pass the same value as
	// ExperimentItem.Version. It must not be more than a minute in the
	// future. This holds only on a Langfuse server using its versioned
	// dataset implementation, the default; a server configured otherwise
	// ignores AsOf and returns current rows. Zero reads current rows.
	// Either way the SDK validates the pages but cannot prove a transactional
	// snapshot: a write still committing at AsOf may appear on a later page.
	AsOf time.Time
	// SourceTraceID and SourceObservationID restrict the listing to items
	// curated from that trace or observation.
	SourceTraceID       string
	SourceObservationID string
	// PageSize is the number of items requested per page: 0 selects 20, and
	// other values must be within [1, 100]. Lower it when items are large; a
	// page response is limited to 32 MiB.
	PageSize int
}

// datasetGate is the dataset admission gate. It mirrors the prompt cache:
// every HTTP operation enters it, Shutdown refuses new entries and cancels
// admitted I/O before the OpenTelemetry teardown, then drains. No lock is
// held while an operation runs. The closure owns the lifecycle context, so
// no context lives in the struct.
type datasetGate struct {
	mu      sync.Mutex
	closing bool
	wg      sync.WaitGroup
	// slots bounds concurrently admitted operations; a caller waits for a
	// slot within its own context and the client lifecycle.
	slots chan struct{}
	// newOperationContext derives one operation's context: bounded by the
	// caller's context, the operation budget, and the client lifecycle.
	newOperationContext func(parent context.Context) (context.Context, context.CancelFunc)
	cancelLifecycle     context.CancelFunc
	stop                <-chan struct{}
}

func newDatasetGate() *datasetGate {
	lifecycle, cancel := context.WithCancel(context.Background())
	return &datasetGate{
		slots: make(chan struct{}, maxDatasetOperations),
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

// enter admits one operation after all of its preparation (validation,
// Mask, serialization) is done, so callbacks never run while the operation
// counts toward the Shutdown drain. The returned release must be called
// exactly once when the operation's I/O has finished and before any result
// reaches caller code.
func (g *datasetGate) enter(parent context.Context) (context.Context, func(), error) {
	select {
	case g.slots <- struct{}{}:
	case <-parent.Done():
		return nil, nil, parent.Err()
	case <-g.stop:
		return nil, nil, errDatasetShutdown
	}
	g.mu.Lock()
	if g.closing {
		g.mu.Unlock()
		<-g.slots
		return nil, nil, errDatasetShutdown
	}
	g.wg.Add(1)
	g.mu.Unlock()
	ctx, cancel := g.newOperationContext(parent)
	return ctx, func() { cancel(); g.wg.Done(); <-g.slots }, nil
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

// shutdown drains admitted operations, bounded by ctx. They end promptly
// because their contexts are already canceled.
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

// datasetError is a payload-free dataset failure that can match several
// sentinels and a context error with errors.Is.
type datasetError struct {
	message string
	wrapped []error
}

func (e *datasetError) Error() string { return e.message }

func (e *datasetError) Unwrap() []error { return e.wrapped }

// datasetOperation is a static operation name used in error text.
type datasetOperation string

const (
	opUpsertDataset     datasetOperation = "dataset upsert"
	opGetDataset        datasetOperation = "dataset read"
	opUpsertDatasetItem datasetOperation = "dataset item upsert"
	opGetDatasetItem    datasetOperation = "dataset item read"
	opDeleteDatasetItem datasetOperation = "dataset item delete"
	opListDatasetItems  datasetOperation = "dataset item list"
)

// run admits and performs one dataset HTTP operation and maps its failure.
// notFound is the operation's sentinel for a 404, or nil for none.
func (c *Client) runDatasetOperation(
	ctx context.Context,
	op datasetOperation,
	notFound error,
	call func(context.Context) error,
) error {
	if err := ctx.Err(); err != nil {
		return &datasetError{
			message: "langfuse: " + string(op) + " canceled",
			wrapped: []error{err},
		}
	}
	operationCtx, release, err := c.datasets.enter(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return &datasetError{message: "langfuse: " + string(op) + " canceled", wrapped: []error{ctxErr}}
		}
		return err
	}
	err = call(operationCtx)
	release()
	if err == nil {
		return nil
	}
	return c.mapDatasetError(ctx, op, notFound, err)
}

func (c *Client) mapDatasetError(ctx context.Context, op datasetOperation, notFound, err error) error {
	var failure *transport.DatasetError
	if !errors.As(err, &failure) {
		return &datasetError{message: "langfuse: " + string(op) + " failed"}
	}
	if failure.NotFound && notFound != nil {
		return &datasetError{message: notFound.Error(), wrapped: []error{notFound}}
	}
	message := "langfuse: " + strings.TrimPrefix(failure.Error(), "langfuse transport: ")
	var wrapped []error
	if failure.OutcomeUnknown {
		message += "; write outcome unknown"
		wrapped = append(wrapped, ErrWriteOutcomeUnknown)
	}
	switch {
	case ctx.Err() != nil:
		// The caller's own context ended: report its error.
		wrapped = append(wrapped, ctx.Err())
	case c.datasets.lifecycleEnded():
		message += "; client shut down"
		wrapped = append(wrapped, errDatasetShutdown)
	case failure.Cause != nil:
		// The operation budget expired.
		wrapped = append(wrapped, failure.Cause)
	}
	return &datasetError{message: message, wrapped: wrapped}
}

// datasetUnavailable reports why dataset calls cannot run on c, or nil.
func (c *Client) datasetUnavailable() error {
	if c == nil || c.isDisabled() || c.datasets == nil || c.datasetTransport == nil {
		return errors.New("langfuse: dataset request on a disabled client")
	}
	if c.stopped.Load() {
		return errDatasetShutdown
	}
	return nil
}

// UpsertDataset creates the named dataset, or updates it when it exists.
// It blocks for at most 30 seconds, bounded further by ctx, and retries only
// failures that prove the write was not applied (a request never sent, 408,
// or 429). Every other failure after sending wraps [ErrWriteOutcomeUnknown].
// Metadata passes Config.Mask once; a nil result, a panic, or a result that
// is not a map[string]any fails before any request. A nil or disabled client
// returns an error.
func (c *Client) UpsertDataset(ctx context.Context, spec DatasetSpec) (Dataset, error) {
	if ctx == nil {
		return Dataset{}, errors.New("langfuse: dataset context is nil")
	}
	if err := validateDatasetSpec(spec); err != nil {
		return Dataset{}, err
	}
	if err := c.datasetUnavailable(); err != nil {
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
	var result transport.Dataset
	err = c.runDatasetOperation(ctx, opUpsertDataset, nil, func(ctx context.Context) error {
		var err error
		result, err = c.datasetTransport.UpsertDataset(ctx, payload, spec.Name)
		return err
	})
	if err != nil {
		return Dataset{}, err
	}
	return datasetFromWire(result), nil
}

// GetDataset reads one dataset by name. A missing dataset wraps
// [ErrDatasetNotFound]. Transient failures are retried within a 30-second
// budget bounded by ctx.
func (c *Client) GetDataset(ctx context.Context, name string) (Dataset, error) {
	if ctx == nil {
		return Dataset{}, errors.New("langfuse: dataset context is nil")
	}
	if err := validateDatasetIdentifier("dataset name", name, false); err != nil {
		return Dataset{}, err
	}
	if err := c.datasetUnavailable(); err != nil {
		return Dataset{}, err
	}
	var result transport.Dataset
	err := c.runDatasetOperation(ctx, opGetDataset, ErrDatasetNotFound, func(ctx context.Context) error {
		var err error
		result, err = c.datasetTransport.GetDataset(ctx, name)
		return err
	})
	if err != nil {
		return Dataset{}, err
	}
	return datasetFromWire(result), nil
}

// UpsertDatasetItem creates or updates one dataset item and returns the
// stored item.
//
// Langfuse versions dataset items: every accepted write creates a new
// version, even with an identical payload, and the last write wins. The SDK
// sends each write at most once as far as it can tell, retrying only
// failures that prove the request was not applied (never sent, 408, or 429).
// Any other failure after sending wraps [ErrWriteOutcomeUnknown]. Repeating
// such a write with an ID is at-least-once: it may add a version, and can
// overwrite a concurrent writer's change. Repeating a write without an ID can
// create a duplicate item.
//
// Content passes Config.Mask exactly once per supplied field before the
// request body is serialized; a nil result, a panic, a metadata result that
// is not a map[string]any, or an oversized or unserializable value fails
// before any request, because omitting the field would keep the stored
// value. Upserting does not redact earlier versions. A missing dataset wraps
// [ErrDatasetNotFound]. A nil or disabled client returns an error.
func (c *Client) UpsertDatasetItem(ctx context.Context, spec DatasetItemSpec) (DatasetItem, error) {
	if ctx == nil {
		return DatasetItem{}, errors.New("langfuse: dataset context is nil")
	}
	if err := validateDatasetItemSpec(spec); err != nil {
		return DatasetItem{}, err
	}
	if err := c.datasetUnavailable(); err != nil {
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
		{"metadata", MaskDatasetItemMetadata, metadataValue(spec.Metadata), true},
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
	var result transport.DatasetItem
	err = c.runDatasetOperation(ctx, opUpsertDatasetItem, ErrDatasetNotFound, func(ctx context.Context) error {
		var err error
		result, err = c.datasetTransport.UpsertItem(ctx, payload, spec.DatasetName, spec.ID)
		return err
	})
	if err != nil {
		return DatasetItem{}, err
	}
	return datasetItemFromWire(result), nil
}

// GetDatasetItem reads one dataset item by ID. A missing item, or an item
// whose dataset was deleted, wraps [ErrDatasetItemNotFound].
func (c *Client) GetDatasetItem(ctx context.Context, id string) (DatasetItem, error) {
	if ctx == nil {
		return DatasetItem{}, errors.New("langfuse: dataset context is nil")
	}
	if err := validateDatasetIdentifier("dataset item ID", id, false); err != nil {
		return DatasetItem{}, err
	}
	if err := c.datasetUnavailable(); err != nil {
		return DatasetItem{}, err
	}
	var result transport.DatasetItem
	err := c.runDatasetOperation(ctx, opGetDatasetItem, ErrDatasetItemNotFound, func(ctx context.Context) error {
		var err error
		result, err = c.datasetTransport.GetItem(ctx, id)
		return err
	})
	if err != nil {
		return DatasetItem{}, err
	}
	return datasetItemFromWire(result), nil
}

// DeleteDatasetItem asks Langfuse to delete one dataset item. On a versioned
// server this writes a deletion marker: earlier versions, their media, and
// experiment observations that referenced the item remain, so it is not a
// purge. A missing item wraps [ErrDatasetItemNotFound]. The write follows the
// retry and [ErrWriteOutcomeUnknown] rules of [Client.UpsertDatasetItem].
func (c *Client) DeleteDatasetItem(ctx context.Context, id string) error {
	if ctx == nil {
		return errors.New("langfuse: dataset context is nil")
	}
	if err := validateDatasetIdentifier("dataset item ID", id, false); err != nil {
		return err
	}
	if err := c.datasetUnavailable(); err != nil {
		return err
	}
	return c.runDatasetOperation(ctx, opDeleteDatasetItem, ErrDatasetItemNotFound, func(ctx context.Context) error {
		return c.datasetTransport.DeleteItem(ctx, id)
	})
}

// DatasetItems iterates over the ACTIVE items of one dataset; archived items
// are never listed. Iteration is lazy and synchronous: no request is sent
// until the loop starts, each page is one request within a 30-second budget,
// and breaking out of the loop sends no further requests. Caller
// cancellation and client shutdown are checked before every page.
//
// Every page is validated before any of its items is yielded: page metadata
// must match the request and stay consistent with the first page, only the
// last page may be short and it must hold exactly the remaining items, and no
// item ID may repeat. A violation, a missing dataset (wrapping
// [ErrDatasetNotFound]), or a failed request yields one final error with a
// zero DatasetItem, after which the loop ends; the items already yielded are
// then not a complete cohort. See DatasetItemQuery.AsOf for what a pinned
// read guarantees.
func (c *Client) DatasetItems(ctx context.Context, query DatasetItemQuery) iter.Seq2[DatasetItem, error] {
	return func(yield func(DatasetItem, error) bool) {
		if ctx == nil {
			yield(DatasetItem{}, errors.New("langfuse: dataset context is nil"))
			return
		}
		if err := validateDatasetItemQuery(query); err != nil {
			yield(DatasetItem{}, err)
			return
		}
		pageSize := query.PageSize
		if pageSize == 0 {
			pageSize = defaultDatasetPageSize
		}
		version := ""
		if !query.AsOf.IsZero() {
			version = formatDatasetInstant(query.AsOf)
		}
		pager := datasetPager{query: query, pageSize: pageSize, seen: make(map[string]struct{})}
		for page := 1; ; page++ {
			if err := c.datasetUnavailable(); err != nil {
				yield(DatasetItem{}, err)
				return
			}
			var result transport.DatasetItemPage
			err := c.runDatasetOperation(ctx, opListDatasetItems, ErrDatasetNotFound, func(ctx context.Context) error {
				var err error
				result, err = c.datasetTransport.ListItems(ctx, transport.DatasetItemListQuery{
					DatasetName:         query.DatasetName,
					SourceTraceID:       query.SourceTraceID,
					SourceObservationID: query.SourceObservationID,
					Version:             version,
					Page:                page,
					Limit:               pageSize,
				})
				return err
			})
			if err == nil {
				err = pager.accept(page, result)
			}
			if err != nil {
				yield(DatasetItem{}, err)
				return
			}
			for _, item := range result.Items {
				if !yield(datasetItemFromWire(item), nil) {
					return
				}
			}
			if page >= pager.totalPages {
				return
			}
		}
	}
}

// datasetPager validates pages of one iteration against each other.
type datasetPager struct {
	query      DatasetItemQuery
	pageSize   int
	started    bool
	totalItems int
	totalPages int
	seen       map[string]struct{}
}

var errDatasetPageInconsistent = errors.New("langfuse: dataset item pages were inconsistent; the dataset may have changed during iteration")

func (p *datasetPager) accept(page int, result transport.DatasetItemPage) error {
	if result.Page != page || result.Limit != p.pageSize || result.TotalItems < 0 ||
		result.TotalPages != (result.TotalItems+p.pageSize-1)/p.pageSize {
		return errDatasetPageInconsistent
	}
	if !p.started {
		if result.TotalPages > maxDatasetPages {
			return errors.New("langfuse: dataset item listing exceeds the local page limit")
		}
		p.started = true
		p.totalItems = result.TotalItems
		p.totalPages = result.TotalPages
	} else if result.TotalItems != p.totalItems {
		return errDatasetPageInconsistent
	}
	want := 0
	switch {
	case page < p.totalPages:
		want = p.pageSize
	case page == p.totalPages:
		want = p.totalItems - (page-1)*p.pageSize
	case p.totalPages != 0:
		return errDatasetPageInconsistent
	}
	if len(result.Items) != want {
		return errDatasetPageInconsistent
	}
	for _, item := range result.Items {
		if item.DatasetName != p.query.DatasetName || item.Status != string(DatasetItemActive) ||
			p.query.SourceTraceID != "" && item.SourceTraceID != p.query.SourceTraceID ||
			p.query.SourceObservationID != "" && item.SourceObservationID != p.query.SourceObservationID {
			return errors.New("langfuse: dataset item listing returned an item outside the query")
		}
		if _, duplicate := p.seen[item.ID]; duplicate {
			return errDatasetPageInconsistent
		}
		p.seen[item.ID] = struct{}{}
	}
	return nil
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

func marshalDatasetBody(body map[string]any, limit int, kind string) ([]byte, error) {
	// Every value is a string or already-validated JSON, so no caller code
	// runs here and the bytes are deterministic.
	data, err := json.Marshal(body)
	if err != nil {
		return nil, errors.New("langfuse: " + kind + " request could not be serialized")
	}
	if len(data) > limit {
		return nil, fmt.Errorf("langfuse: %s request exceeds the %d MiB limit", kind, limit>>20)
	}
	return data, nil
}

func metadataValue(metadata map[string]any) any {
	if metadata == nil {
		return nil
	}
	return metadata
}

func isNilValue(value any) bool {
	if raw, ok := value.(json.RawMessage); ok {
		return len(raw) == 0
	}
	return lfattr.IsNil(value)
}

func validateDatasetSpec(spec DatasetSpec) error {
	if err := validateDatasetIdentifier("dataset name", spec.Name, false); err != nil {
		return err
	}
	if spec.Description != nil &&
		(!utf8.ValidString(*spec.Description) || len(*spec.Description) > maxDatasetDescription) {
		return errors.New("langfuse: dataset description is invalid UTF-8 or exceeds 16 KiB")
	}
	for name, schema := range map[string]json.RawMessage{
		"input schema":           spec.InputSchema,
		"expected output schema": spec.ExpectedOutputSchema,
	} {
		if len(schema) == 0 || string(bytes.TrimSpace(schema)) == "null" {
			continue
		}
		var object map[string]json.RawMessage
		if !utf8.Valid(schema) || json.Unmarshal(schema, &object) != nil || object == nil {
			return errors.New("langfuse: dataset " + name + " must be a JSON object or JSON null")
		}
	}
	if len(spec.InputSchema)+len(spec.ExpectedOutputSchema) > maxDatasetBodyBytes {
		return errors.New("langfuse: dataset schemas exceed the 1 MiB limit")
	}
	return nil
}

func validateDatasetItemSpec(spec DatasetItemSpec) error {
	if err := validateDatasetIdentifier("dataset name", spec.DatasetName, false); err != nil {
		return err
	}
	if err := validateDatasetIdentifier("dataset item ID", spec.ID, true); err != nil {
		return err
	}
	if err := validateDatasetIdentifier("source trace ID", spec.SourceTraceID, true); err != nil {
		return err
	}
	if err := validateDatasetIdentifier("source observation ID", spec.SourceObservationID, true); err != nil {
		return err
	}
	if spec.SourceObservationID != "" && spec.SourceTraceID == "" {
		return errors.New("langfuse: dataset item source observation ID requires a source trace ID")
	}
	switch spec.Status {
	case "", DatasetItemActive, DatasetItemArchived:
	default:
		return errors.New("langfuse: unsupported dataset item status")
	}
	return nil
}

func validateDatasetItemQuery(query DatasetItemQuery) error {
	if err := validateDatasetIdentifier("dataset name", query.DatasetName, false); err != nil {
		return err
	}
	if err := validateDatasetIdentifier("source trace ID", query.SourceTraceID, true); err != nil {
		return err
	}
	if err := validateDatasetIdentifier("source observation ID", query.SourceObservationID, true); err != nil {
		return err
	}
	if query.PageSize < 0 || query.PageSize > maxDatasetPageSize {
		return errors.New("langfuse: dataset page size must be within [0, 100]")
	}
	if !query.AsOf.IsZero() && !validDatasetInstant(query.AsOf) {
		return errors.New("langfuse: dataset as-of time is outside the RFC 3339 year range")
	}
	if query.AsOf.After(time.Now().Add(maxDatasetAsOfSkew)) {
		// A future instant would admit writes made during iteration.
		return errors.New("langfuse: dataset as-of time is in the future")
	}
	return nil
}

// validateDatasetIdentifier applies the local identifier rule: 1 to 255
// bytes of valid UTF-8, no control characters, no surrounding whitespace.
// Error text names the field, never the value.
func validateDatasetIdentifier(field, value string, emptyAllowed bool) error {
	if value == "" {
		if emptyAllowed {
			return nil
		}
		return errors.New("langfuse: " + field + " is required")
	}
	if len(value) > maxDatasetIdentifierBytes || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value || containsControl(value) {
		return errors.New("langfuse: " + field + " must be 1 to 255 bytes of valid UTF-8 without control characters or surrounding whitespace")
	}
	return nil
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

func datasetFromWire(wire transport.Dataset) Dataset {
	return Dataset{
		ID:                   wire.ID,
		Name:                 wire.Name,
		Description:          wire.Description,
		Metadata:             wire.Metadata,
		InputSchema:          wire.InputSchema,
		ExpectedOutputSchema: wire.ExpectedOutputSchema,
		CreatedAt:            wire.CreatedAt,
		UpdatedAt:            wire.UpdatedAt,
	}
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
