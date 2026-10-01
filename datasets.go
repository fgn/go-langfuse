package langfuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"sync"
	"time"
	"unicode/utf8"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
	"github.com/fgn/go-langfuse/internal/transport"
)

// ErrDatasetNotFound reports that the dataset does not exist.
var ErrDatasetNotFound = errors.New("langfuse: dataset not found")

// ErrDatasetItemNotFound reports that the dataset item, or its dataset, does
// not exist.
var ErrDatasetItemNotFound = errors.New("langfuse: dataset item not found")

// ErrWriteOutcomeUnknown reports a dataset write that failed after it was
// sent and may have been applied. The SDK never repeats a write.
var ErrWriteOutcomeUnknown = errors.New("langfuse: write outcome unknown")

const (
	datasetOperationBudget = 30 * time.Second
	maxDatasetFieldBytes   = 1 << 20
	// The server accepts 4.5 MB item bodies and 1 MB dataset bodies.
	maxDatasetItemBodyBytes = 4 << 20
	maxDatasetBodyBytes     = 1 << 20
	defaultDatasetPageSize  = 20
)

// DatasetSpec creates or updates a dataset by name; nil fields keep the
// stored values.
type DatasetSpec struct {
	// Name may contain "/" folder separators. Required.
	Name string
	// Description is not masked; a pointer to "" clears it.
	Description *string
	// Metadata is masked as MaskDatasetMetadata. Langfuse cannot clear it,
	// so an empty map stores {}.
	Metadata map[string]any
	// InputSchema and ExpectedOutputSchema are JSON Schemas for item
	// content; JSON null removes one. They are not masked.
	InputSchema          json.RawMessage
	ExpectedOutputSchema json.RawMessage
}

// Dataset is a dataset as stored by Langfuse.
type Dataset struct {
	ID          string
	Name        string
	Description string
	// Metadata and the schemas are the server's JSON; nil when absent or null.
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

// DatasetItemSpec creates or updates one dataset item. Nil content fields,
// including typed nils, keep the stored value (null for a new item), so a
// Mask result of nil is an error.
type DatasetItemSpec struct {
	// DatasetName is required.
	DatasetName string
	// ID selects the item to update; when empty, every request creates a new
	// item.
	ID string
	// Input, ExpectedOutput, and Metadata are masked as MaskDatasetItemInput,
	// MaskDatasetItemExpectedOutput, and MaskDatasetItemMetadata, and are
	// limited to 1 MiB each. json.RawMessage values are sent verbatim.
	Input               any
	ExpectedOutput      any
	Metadata            map[string]any
	SourceTraceID       string
	SourceObservationID string
	// Status is empty (ACTIVE for a new item) or a DatasetItemStatus.
	Status DatasetItemStatus
}

// DatasetItem is a dataset item as stored by Langfuse. Content fields hold
// the server's JSON, nil when absent or null; media references are not
// decoded.
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
	// Version is the DatasetItemQuery.AsOf the item was read at; zero when
	// read without one.
	Version time.Time
}

// ExperimentItem returns the item for [Client.StartExperimentItem], pinned
// to its Version. It fails when the stored metadata is not a JSON object.
func (i DatasetItem) ExperimentItem() (ExperimentItem, error) {
	item := ExperimentItem{ID: i.ID, Version: i.Version}
	if len(i.ExpectedOutput) != 0 {
		item.ExpectedOutput = i.ExpectedOutput
	}
	if len(i.Metadata) != 0 {
		decoder := json.NewDecoder(bytes.NewReader(i.Metadata))
		decoder.UseNumber()
		if err := decoder.Decode(&item.Metadata); err != nil {
			return ExperimentItem{}, fmt.Errorf("%w: dataset item metadata is not a JSON object", ErrInvalidExperiment)
		}
	}
	return item, nil
}

// DatasetItemQuery selects the items of one dataset for [Client.DatasetItems].
type DatasetItemQuery struct {
	// DatasetName is required.
	DatasetName string
	// AsOf, when non-zero, reads the item versions valid at that instant, at
	// millisecond precision; pass the same value as ExperimentItem.Version.
	// Servers without Langfuse's default versioned datasets ignore it.
	AsOf                time.Time
	SourceTraceID       string
	SourceObservationID string
	// PageSize is the number of items per request; 0 selects 20 and Langfuse
	// accepts at most 100. A page response is limited to 16 MiB.
	PageSize int
}

type datasetGate struct {
	mu                  sync.Mutex
	closing             bool
	wg                  sync.WaitGroup
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

// enter must follow Mask and serialization, so a callback that calls
// Shutdown cannot block the drain. Call release once the I/O has finished.
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

func (g *datasetGate) beginShutdown() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.closing = true
	g.mu.Unlock()
	g.cancelLifecycle()
}

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

// runDatasetOperation maps a 404 to notFound unless it is nil.
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
	result := errors.New("langfuse: " + failure.Message)
	if failure.OutcomeUnknown {
		result = fmt.Errorf("%w: %w", result, ErrWriteOutcomeUnknown)
	}
	cause := failure.Cause // the operation budget expired, or nil
	switch {
	case ctx.Err() != nil:
		cause = ctx.Err()
	case c.datasets.lifecycleEnded():
		cause = errDatasetShutdown
	}
	if cause != nil {
		result = fmt.Errorf("%w: %w", result, cause)
	}
	return result
}

// UpsertDataset creates or updates the named dataset. A nil, panicking, or
// non-map Mask result for Metadata fails before any request. The write is
// sent once; a failure after sending wraps [ErrWriteOutcomeUnknown].
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
// [ErrDatasetNotFound].
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
// stored item; every accepted write adds a version and earlier versions
// remain. A nil, panicking, non-map metadata, oversized, or unserializable
// Mask result fails before any request. The write is sent once; a failure
// after sending wraps [ErrWriteOutcomeUnknown], and repeating it can add a
// version or, without an ID, a duplicate item. A missing dataset wraps
// [ErrDatasetNotFound].
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

// GetDatasetItem reads one dataset item by ID. A missing item or dataset
// wraps [ErrDatasetItemNotFound].
func (c *Client) GetDatasetItem(ctx context.Context, id string) (DatasetItem, error) {
	if err := c.datasetReady(ctx, requireDataset("dataset item ID", id)); err != nil {
		return DatasetItem{}, err
	}
	result, err := runDatasetOperation(ctx, c, ErrDatasetItemNotFound, func(ctx context.Context) (transport.DatasetItem, error) {
		return c.datasetTransport.GetItem(ctx, id)
	})
	return datasetItemFromWire(result), err
}

// DeleteDatasetItem deletes one dataset item; on a versioned server earlier
// versions remain. A missing item wraps [ErrDatasetItemNotFound], and a
// failure after sending wraps [ErrWriteOutcomeUnknown].
func (c *Client) DeleteDatasetItem(ctx context.Context, id string) error {
	if err := c.datasetReady(ctx, requireDataset("dataset item ID", id)); err != nil {
		return err
	}
	_, err := runDatasetOperation(ctx, c, ErrDatasetItemNotFound, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, c.datasetTransport.DeleteItem(ctx, id)
	})
	return err
}

// DatasetItems lazily iterates over the ACTIVE items of one dataset, one
// request per page, each within a 30-second budget. Without AsOf, items
// written during iteration can be skipped or repeated. A failure, wrapping
// [ErrDatasetNotFound] for a missing dataset, is yielded once with a zero
// DatasetItem and ends the loop.
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
			for _, wire := range page.Items {
				item := datasetItemFromWire(wire)
				item.Version = query.AsOf
				if !yield(item, nil) {
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

// maskedDatasetContent fails where observation masking would omit the
// value, because an omitted field keeps the stored value.
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

// marshalDatasetBody checks strings first because encoding/json silently
// replaces invalid UTF-8.
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

// formatDatasetInstant renders the millisecond UTC form the server stores.
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
