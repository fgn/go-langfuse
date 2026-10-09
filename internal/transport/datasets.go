package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

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

// UpsertDataset posts one serialized dataset body.
func (d *RESTClient) UpsertDataset(ctx context.Context, body []byte, name string) (Dataset, error) {
	call := restCall{
		op: "dataset upsert", method: http.MethodPost, url: d.base + "/v2/datasets",
		body: body, write: true, limit: restResponseLimit,
	}
	return send(ctx, d, call, func(dataset *Dataset) bool { return dataset.valid() && dataset.Name == name })
}

// GetDataset reads one dataset by name.
func (d *RESTClient) GetDataset(ctx context.Context, name string) (Dataset, error) {
	call := restCall{
		op: "dataset read", method: http.MethodGet,
		url: d.base + "/v2/datasets/" + url.PathEscape(name), limit: restResponseLimit,
	}
	return send(ctx, d, call, func(dataset *Dataset) bool { return dataset.valid() && dataset.Name == name })
}

// UpsertItem posts one serialized dataset item body.
func (d *RESTClient) UpsertItem(ctx context.Context, body []byte, datasetName, id string) (DatasetItem, error) {
	call := restCall{
		op: "dataset item upsert", method: http.MethodPost, url: d.base + "/dataset-items",
		body: body, write: true, limit: restResponseLimit,
	}
	return send(ctx, d, call, func(item *DatasetItem) bool {
		return item.valid() && item.DatasetName == datasetName && (id == "" || item.ID == id)
	})
}

// GetItem reads one dataset item by ID.
func (d *RESTClient) GetItem(ctx context.Context, id string) (DatasetItem, error) {
	call := restCall{
		op: "dataset item read", method: http.MethodGet,
		url: d.base + "/dataset-items/" + url.PathEscape(id), limit: restResponseLimit,
	}
	return send(ctx, d, call, func(item *DatasetItem) bool { return item.valid() && item.ID == id })
}

// DeleteItem deletes one dataset item by ID.
func (d *RESTClient) DeleteItem(ctx context.Context, id string) error {
	_, err := d.do(ctx, restCall{
		op: "dataset item delete", method: http.MethodDelete,
		url:   d.base + "/dataset-items/" + url.PathEscape(id),
		write: true, limit: restResponseLimit,
	})
	return err
}

// ListItems reads one page of dataset items.
func (d *RESTClient) ListItems(ctx context.Context, query DatasetItemListQuery) (Page[DatasetItem, PageMeta], error) {
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
	return listPage[DatasetItem, PageMeta](ctx, d, "dataset item list", "/dataset-items", values, (*DatasetItem).valid)
}

// ListDatasets reads one page of datasets.
func (d *RESTClient) ListDatasets(ctx context.Context, page, limit int) (Page[Dataset, PageMeta], error) {
	values := url.Values{}
	values.Set("page", strconv.Itoa(page))
	values.Set("limit", strconv.Itoa(limit))
	return listPage[Dataset, PageMeta](ctx, d, "dataset list", "/v2/datasets", values, (*Dataset).valid)
}

// DatasetRunItem is one decoded dataset run item link.
//
//nolint:tagliatelle // Langfuse wire keys are camelCase.
type DatasetRunItem struct {
	DatasetRunID   string `json:"datasetRunId"`
	DatasetRunName string `json:"datasetRunName"`
	DatasetItemID  string `json:"datasetItemId"`
	TraceID        string `json:"traceId"`
	ObservationID  string `json:"observationId"`
}

// CreateRunItem posts one serialized dataset run item body and returns the
// dataset run the item was linked to. The response must echo the request's
// run, item, trace, and observation and carry a usable run ID; anything else
// is an invalid response to a write that may have been applied.
func (d *RESTClient) CreateRunItem(ctx context.Context, body []byte, want DatasetRunItem) (DatasetRunItem, error) {
	call := restCall{
		op: "dataset run item create", method: http.MethodPost, url: d.base + "/dataset-run-items",
		body: body, write: true, limit: restResponseLimit,
	}
	return send(ctx, d, call, func(item *DatasetRunItem) bool {
		return validIdentifier(item.DatasetRunID) && item.DatasetRunName == want.DatasetRunName &&
			item.DatasetItemID == want.DatasetItemID && item.TraceID == want.TraceID &&
			item.ObservationID == want.ObservationID
	})
}

// validIdentifier is the SDK's rule for experiment identifiers: 1 to 255
// bytes of valid UTF-8 without control characters.
func validIdentifier(value string) bool {
	return value != "" && len(value) <= 255 && utf8.ValidString(value) &&
		!strings.ContainsFunc(value, unicode.IsControl)
}

// ExperimentRecord is one decoded experiment.
//
//nolint:tagliatelle // Langfuse wire keys are camelCase.
type ExperimentRecord struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	StartTime   time.Time       `json:"startTime"`
	EndTime     time.Time       `json:"endTime"`
	ItemCount   int             `json:"itemCount"`
	DatasetID   string          `json:"datasetId"`
	Metadata    json.RawMessage `json:"metadata"`
	Scores      []ScoreRecord   `json:"scores"`
}

func (experiment *ExperimentRecord) valid() bool {
	experiment.Metadata = nonNull(experiment.Metadata)
	return experiment.ID != "" && experiment.Name != "" && !experiment.StartTime.IsZero() && experiment.ItemCount >= 0 &&
		allValid(experiment.Scores, (*ScoreRecord).valid)
}

// ExperimentItemRecord is one decoded experiment item.
//
//nolint:tagliatelle // Langfuse wire keys are camelCase.
type ExperimentItemRecord struct {
	ID                    string          `json:"id"`
	TraceID               string          `json:"traceId"`
	StartTime             time.Time       `json:"startTime"`
	EndTime               *time.Time      `json:"endTime"`
	Level                 string          `json:"level"`
	Environment           string          `json:"environment"`
	ExperimentID          string          `json:"experimentId"`
	ExperimentName        string          `json:"experimentName"`
	ExperimentItemID      string          `json:"experimentItemId"`
	ExperimentDatasetID   string          `json:"experimentDatasetId"`
	ExperimentItemVersion *time.Time      `json:"experimentItemVersion"`
	Input                 json.RawMessage `json:"input"`
	Output                json.RawMessage `json:"output"`
	ExpectedOutput        json.RawMessage `json:"expectedOutput"`
	Metadata              json.RawMessage `json:"metadata"`
	ItemMetadata          json.RawMessage `json:"experimentItemMetadata"`
	ExperimentMetadata    json.RawMessage `json:"experimentMetadata"`
	ExperimentDescription string          `json:"experimentDescription"`
	Scores                []ScoreRecord   `json:"scores"`
}

func (item *ExperimentItemRecord) valid() bool {
	for _, field := range []*json.RawMessage{
		&item.Input, &item.Output, &item.ExpectedOutput, &item.Metadata, &item.ItemMetadata, &item.ExperimentMetadata,
	} {
		*field = nonNull(*field)
	}
	return item.ID != "" && item.TraceID != "" && item.ExperimentID != "" && item.ExperimentItemID != "" &&
		!item.StartTime.IsZero() && allValid(item.Scores, (*ScoreRecord).valid)
}

// ListExperiments reads one page of experiments.
func (d *RESTClient) ListExperiments(
	ctx context.Context, query url.Values,
) (Page[ExperimentRecord, CursorMeta], error) {
	return listPage[ExperimentRecord, CursorMeta](ctx, d, "experiment list", "/experiments", query,
		(*ExperimentRecord).valid)
}

// ListExperimentItems reads one page of experiment items.
func (d *RESTClient) ListExperimentItems(
	ctx context.Context, query url.Values,
) (Page[ExperimentItemRecord, CursorMeta], error) {
	return listPage[ExperimentItemRecord, CursorMeta](ctx, d, "experiment item list", "/experiment-items", query,
		(*ExperimentItemRecord).valid)
}
