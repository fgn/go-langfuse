package langfuse

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
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

const (
	maxDatasetFieldBytes = 1 << 20
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
	// Metadata is any JSON value, masked as MaskDatasetMetadata. Langfuse
	// cannot clear it, so an empty map stores {}.
	Metadata any
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
	// item. IDs are unique per project, so an ID already used in another
	// dataset fails with status 409, as does a concurrent edit of the item.
	ID string
	// Input, ExpectedOutput, and Metadata are any JSON values, masked as
	// MaskDatasetItemInput, MaskDatasetItemExpectedOutput, and
	// MaskDatasetItemMetadata, and are limited to 1 MiB each. json.RawMessage
	// values are sent verbatim.
	Input               any
	ExpectedOutput      any
	Metadata            any
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

// ExperimentItem returns the item for [Client.RunExperiment] and
// [Client.StartExperimentItem], linked to its dataset and pinned to its
// Version; an item read without AsOf runs unpinned. Input, ExpectedOutput,
// and Metadata are the stored json.RawMessage values, or nil.
func (i DatasetItem) ExperimentItem() ExperimentItem {
	item := ExperimentItem{ID: i.ID, DatasetID: i.DatasetID, Version: i.Version}
	for _, field := range []struct {
		target *any
		value  json.RawMessage
	}{{&item.Input, i.Input}, {&item.ExpectedOutput, i.ExpectedOutput}, {&item.Metadata, i.Metadata}} {
		if len(field.value) != 0 {
			*field.target = field.value
		}
	}
	return item
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

// UpsertDataset creates or updates the named dataset. A nil or panicking Mask
// result for Metadata fails before any request. The write is sent once; a
// failure after sending wraps [ErrWriteOutcomeUnknown].
func (c *Client) UpsertDataset(ctx context.Context, spec DatasetSpec) (Dataset, error) {
	if err := c.restReady(ctx, requireField("dataset name", spec.Name)); err != nil {
		return Dataset{}, err
	}
	body := map[string]any{"name": spec.Name}
	if spec.Description != nil {
		body["description"] = *spec.Description
	}
	if !isNilValue(spec.Metadata) {
		metadata, err := c.maskedDatasetContent(MaskDatasetMetadata, spec.Metadata)
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
	payload, err := marshalBody(body, maxDatasetBodyBytes, "dataset")
	if err != nil {
		return Dataset{}, err
	}
	result, err := runREST(ctx, c, nil, func(ctx context.Context) (transport.Dataset, error) {
		return c.restTransport.UpsertDataset(ctx, payload, spec.Name)
	})
	return Dataset(result), err
}

// GetDataset reads one dataset by name. A missing dataset wraps
// [ErrDatasetNotFound].
func (c *Client) GetDataset(ctx context.Context, name string) (Dataset, error) {
	if err := c.restReady(ctx, requireField("dataset name", name)); err != nil {
		return Dataset{}, err
	}
	result, err := runREST(ctx, c, ErrDatasetNotFound, func(ctx context.Context) (transport.Dataset, error) {
		return c.restTransport.GetDataset(ctx, name)
	})
	return Dataset(result), err
}

// UpsertDatasetItem creates or updates one dataset item and returns the stored
// item; every accepted write adds a version and earlier versions remain. A
// nil, panicking, oversized, or unserializable Mask result fails before any
// request. The write is sent once; a failure after sending wraps
// [ErrWriteOutcomeUnknown], and repeating it can add a version or, without an
// ID, a duplicate item. A missing dataset wraps [ErrDatasetNotFound].
func (c *Client) UpsertDatasetItem(ctx context.Context, spec DatasetItemSpec) (DatasetItem, error) {
	if err := c.restReady(ctx, requireField("dataset name", spec.DatasetName)); err != nil {
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
	}{
		{"input", MaskDatasetItemInput, spec.Input},
		{"expectedOutput", MaskDatasetItemExpectedOutput, spec.ExpectedOutput},
		{"metadata", MaskDatasetItemMetadata, spec.Metadata},
	} {
		if isNilValue(field.value) {
			continue
		}
		encoded, err := c.maskedDatasetContent(field.mask, field.value)
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
	payload, err := marshalBody(body, maxDatasetItemBodyBytes, "dataset item")
	if err != nil {
		return DatasetItem{}, err
	}
	result, err := runREST(ctx, c, ErrDatasetNotFound, func(ctx context.Context) (transport.DatasetItem, error) {
		return c.restTransport.UpsertItem(ctx, payload, spec.DatasetName, spec.ID)
	})
	return datasetItemFromWire(result), err
}

// GetDatasetItem reads one dataset item by ID. A missing item or dataset
// wraps [ErrDatasetItemNotFound].
func (c *Client) GetDatasetItem(ctx context.Context, id string) (DatasetItem, error) {
	if err := c.restReady(ctx, requireField("dataset item ID", id)); err != nil {
		return DatasetItem{}, err
	}
	result, err := runREST(ctx, c, ErrDatasetItemNotFound, func(ctx context.Context) (transport.DatasetItem, error) {
		return c.restTransport.GetItem(ctx, id)
	})
	return datasetItemFromWire(result), err
}

// DeleteDatasetItem deletes one dataset item; on a versioned server earlier
// versions remain. A missing item wraps [ErrDatasetItemNotFound], and a
// failure after sending wraps [ErrWriteOutcomeUnknown].
func (c *Client) DeleteDatasetItem(ctx context.Context, id string) error {
	if err := c.restReady(ctx, requireField("dataset item ID", id)); err != nil {
		return err
	}
	_, err := runREST(ctx, c, ErrDatasetItemNotFound, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, c.restTransport.DeleteItem(ctx, id)
	})
	return err
}

// DatasetItems lazily iterates over the ACTIVE items of one dataset, one
// request per page, each within a 30-second budget. Without AsOf, items
// written during iteration can be skipped or repeated. A failure, wrapping
// [ErrDatasetNotFound] for a missing dataset, is yielded once with a zero
// DatasetItem and ends the loop.
func (c *Client) DatasetItems(ctx context.Context, query DatasetItemQuery) iter.Seq2[DatasetItem, error] {
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
	version := query.AsOf.UTC().Truncate(time.Millisecond)
	return numberedPages(ctx, c, requireField("dataset name", query.DatasetName), ErrDatasetNotFound,
		func(ctx context.Context, page int) ([]DatasetItem, int, error) {
			request := list // each traversal may run concurrently
			request.Page = page
			wire, err := c.restTransport.ListItems(ctx, request)
			items := make([]DatasetItem, len(wire.Data))
			for index, item := range wire.Data {
				items[index] = datasetItemFromWire(item)
				items[index].Version = version
			}
			return items, wire.Meta.Pages(), err
		})
}

// DatasetQuery configures [Client.Datasets].
type DatasetQuery struct {
	// PageSize is the number of datasets per request; 0 selects 20 and
	// Langfuse accepts at most 100.
	PageSize int
}

// Datasets lazily iterates over the datasets of the project, newest first,
// with the paging and failure rules of [Client.DatasetItems]; datasets
// created during the loop can make a page repeat earlier ones.
func (c *Client) Datasets(ctx context.Context, query DatasetQuery) iter.Seq2[Dataset, error] {
	pageSize := query.PageSize
	if pageSize == 0 {
		pageSize = defaultDatasetPageSize
	}
	return numberedPages(ctx, c, nil, nil, func(ctx context.Context, page int) ([]Dataset, int, error) {
		wire, err := c.restTransport.ListDatasets(ctx, page, pageSize)
		datasets := make([]Dataset, len(wire.Data))
		for index, dataset := range wire.Data {
			datasets[index] = Dataset(dataset)
		}
		return datasets, wire.Meta.Pages(), err
	})
}

// maskedDatasetContent fails where observation masking would omit the
// value, because an omitted field keeps the stored value.
func (c *Client) maskedDatasetContent(field MaskField, value any) (json.RawMessage, error) {
	name := string(field)
	masked, ok := c.maskStrict(field, value)
	if !ok {
		return nil, errors.New("langfuse: masker panicked on " + name)
	}
	if isNilValue(masked) {
		return nil, errors.New("langfuse: masker removed " + name + "; omitting it would keep the stored value")
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
