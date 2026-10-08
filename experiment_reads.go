package langfuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"iter"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/fgn/go-langfuse/internal/transport"
)

const (
	defaultExperimentPageSize = 50
	maxExperimentPageSize     = 100
)

// ExperimentQuery selects stored experiments for [Client.Experiments].
type ExperimentQuery struct {
	// From is required: only experiments started at or after it are read.
	From time.Time
	// To, when non-zero, excludes experiments started at or after it.
	To time.Time
	// IDs, Names, and DatasetIDs, when set, each restrict the result to
	// experiments matching one of their values; values must not contain
	// commas.
	IDs, Names, DatasetIDs []string
	// PageSize is the number of experiments per request; 0 selects 50 and
	// Langfuse accepts at most 100.
	PageSize int
}

// StoredExperiment is an experiment as Langfuse reports it. Its ID is the
// experiment ID of its items, which is also the dataset run ID for runs over
// dataset items.
type StoredExperiment struct {
	ID          string
	Name        string
	Description string
	DatasetID   string
	StartTime   time.Time
	EndTime     time.Time
	ItemCount   int
	// Metadata is the server's JSON object; nil when absent.
	Metadata json.RawMessage
	// Scores holds up to 50 scores on the experiment itself, such as run
	// evaluations.
	Scores []Score
}

// ExperimentItemQuery selects stored experiment items for
// [Client.ExperimentItems].
type ExperimentItemQuery struct {
	// From is required: only items started at or after it are read.
	From time.Time
	// To, when non-zero, excludes items started at or after it.
	To time.Time
	// ExperimentIDs, ExperimentNames, ItemIDs, and DatasetIDs, when set, each
	// restrict the result to items matching one of their values; values must
	// not contain commas.
	ExperimentIDs, ExperimentNames, ItemIDs, DatasetIDs []string
	// PageSize is the number of items per request; 0 selects 50 and Langfuse
	// accepts at most 100.
	PageSize int
}

// StoredExperimentItem is one experiment item as Langfuse reports it: the
// item's root observation and its experiment identity.
type StoredExperimentItem struct {
	// ObservationID and TraceID identify the item root.
	ObservationID string
	TraceID       string
	StartTime     time.Time
	// EndTime is zero while the root is open.
	EndTime     time.Time
	Level       Level
	Environment string

	ExperimentID          string
	ExperimentName        string
	ExperimentDescription string
	ItemID                string
	DatasetID             string
	// ItemVersion is zero for an unpinned item.
	ItemVersion time.Time

	// Content fields hold the server's JSON, nil when absent.
	Input              json.RawMessage
	Output             json.RawMessage
	ExpectedOutput     json.RawMessage
	Metadata           json.RawMessage
	ItemMetadata       json.RawMessage
	ExperimentMetadata json.RawMessage

	// Scores holds up to 50 scores on the item's trace and observations.
	Scores []Score
}

// Experiments lazily iterates over stored experiments, most recently active
// first, one request per page with the budget and failure rules of
// [Client.DatasetItems]. Langfuse builds them from exported item traces, so a
// run appears only after its traces are ingested.
func (c *Client) Experiments(ctx context.Context, query ExperimentQuery) iter.Seq2[StoredExperiment, error] {
	values, err := experimentReadQuery(query.From, query.To, query.PageSize, "core,metadata,scores", map[string][]string{
		"id": query.IDs, "name": query.Names, "datasetId": query.DatasetIDs,
	})
	return cursorPages(ctx, c, err, values, func(ctx context.Context, values url.Values) ([]StoredExperiment, string, error) {
		page, err := c.datasetTransport.ListExperiments(ctx, values)
		experiments := make([]StoredExperiment, len(page.Data))
		for index, wire := range page.Data {
			experiments[index] = StoredExperiment{
				ID: wire.ID, Name: wire.Name, Description: wire.Description, DatasetID: wire.DatasetID,
				StartTime: wire.StartTime, EndTime: wire.EndTime, ItemCount: wire.ItemCount,
				Metadata: wire.Metadata, Scores: scoresFromWire(wire.Scores),
			}
		}
		return experiments, page.Meta.Next(), err
	})
}

// ExperimentItems lazily iterates over stored experiment items, newest
// first, with the rules of [Client.Experiments].
func (c *Client) ExperimentItems(
	ctx context.Context, query ExperimentItemQuery,
) iter.Seq2[StoredExperimentItem, error] {
	values, err := experimentReadQuery(query.From, query.To, query.PageSize,
		"core,dataset,io,metadata,itemMetadata,experimentMetadata,scores", map[string][]string{
			"experimentId": query.ExperimentIDs, "experimentName": query.ExperimentNames,
			"experimentItemId": query.ItemIDs, "datasetId": query.DatasetIDs,
		})
	return cursorPages(ctx, c, err, values, func(ctx context.Context, values url.Values) ([]StoredExperimentItem, string, error) {
		page, err := c.datasetTransport.ListExperimentItems(ctx, values)
		items := make([]StoredExperimentItem, len(page.Data))
		for index, wire := range page.Data {
			item := StoredExperimentItem{
				ObservationID: wire.ID, TraceID: wire.TraceID, StartTime: wire.StartTime,
				Level: Level(wire.Level), Environment: wire.Environment,
				ExperimentID: wire.ExperimentID, ExperimentName: wire.ExperimentName,
				ExperimentDescription: wire.ExperimentDescription, ItemID: wire.ExperimentItemID,
				DatasetID: wire.ExperimentDatasetID, Input: wire.Input, Output: wire.Output,
				ExpectedOutput: wire.ExpectedOutput, Metadata: wire.Metadata, ItemMetadata: wire.ItemMetadata,
				ExperimentMetadata: wire.ExperimentMetadata, Scores: scoresFromWire(wire.Scores),
			}
			if wire.EndTime != nil {
				item.EndTime = *wire.EndTime
			}
			if wire.ExperimentItemVersion != nil {
				item.ItemVersion = *wire.ExperimentItemVersion
			}
			items[index] = item
		}
		return items, page.Meta.Next(), err
	})
}

func experimentReadQuery(from, to time.Time, pageSize int, fields string, filters map[string][]string) (url.Values, error) {
	if from.IsZero() || !validDatasetInstant(from) || (!to.IsZero() && !validDatasetInstant(to)) {
		return nil, errors.New("langfuse: experiment query needs a From time in the RFC 3339 year range")
	}
	if pageSize < 0 || pageSize > maxExperimentPageSize {
		return nil, errors.New("langfuse: experiment query page size must be 0 to 100")
	}
	if pageSize == 0 {
		pageSize = defaultExperimentPageSize
	}
	values := url.Values{}
	values.Set("fields", fields)
	values.Set("limit", strconv.Itoa(pageSize))
	values.Set("fromStartTime", formatDatasetInstant(from))
	if !to.IsZero() {
		values.Set("toStartTime", formatDatasetInstant(to))
	}
	for key, filter := range filters {
		for _, value := range filter {
			if value == "" || strings.Contains(value, ",") {
				return nil, errors.New("langfuse: experiment query filter values must be non-empty and contain no commas")
			}
		}
		if len(filter) != 0 {
			values.Set(key, strings.Join(filter, ","))
		}
	}
	return values, nil
}

// cursorPages yields the values of fetch page by page until a page has no
// next cursor. A repeated cursor ends the iteration with an error rather than
// looping.
func cursorPages[T any](
	ctx context.Context, c *Client, invalid error, query url.Values,
	fetch func(ctx context.Context, query url.Values) ([]T, string, error),
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		if ctx == nil {
			yield(zero, errors.New("langfuse: dataset context is nil"))
			return
		}
		if invalid != nil {
			yield(zero, invalid)
			return
		}
		request := cloneValues(query) // each traversal starts at the first page
		seen := map[string]bool{}
		for {
			if err := c.datasetUnavailable(); err != nil {
				yield(zero, err)
				return
			}
			type pageResult struct {
				values []T
				cursor string
			}
			result, err := runDatasetOperation(ctx, c, nil, func(ctx context.Context) (pageResult, error) {
				values, cursor, err := fetch(ctx, request)
				return pageResult{values, cursor}, err
			})
			if err != nil {
				yield(zero, err)
				return
			}
			for _, value := range result.values {
				if !yield(value, nil) {
					return
				}
			}
			if result.cursor == "" {
				return
			}
			if seen[result.cursor] {
				yield(zero, errors.New("langfuse: experiment listing repeated a page cursor"))
				return
			}
			seen[result.cursor] = true
			request.Set("cursor", result.cursor)
		}
	}
}

func cloneValues(values url.Values) url.Values {
	clone := make(url.Values, len(values))
	for key, value := range values {
		clone[key] = append([]string(nil), value...)
	}
	return clone
}

// scoresFromWire converts v3 scores; a BOOLEAN value becomes NumericValue 0
// or 1, as [Score] writes it.
func scoresFromWire(wire []transport.ScoreRecord) []Score {
	if len(wire) == 0 {
		return nil
	}
	scores := make([]Score, 0, len(wire))
	for _, record := range wire {
		score := Score{
			ID: record.ID, Name: record.Name, DataType: ScoreDataType(record.DataType),
			ConfigID: record.ConfigID, Comment: record.Comment, Timestamp: record.Timestamp,
		}
		switch value := bytes.TrimSpace(record.Value); {
		case string(value) == "true" || string(value) == "false":
			number := 0.0
			if string(value) == "true" {
				number = 1
			}
			score.NumericValue = &number
		case len(value) != 0 && value[0] == '"':
			var text string
			if json.Unmarshal(value, &text) == nil {
				score.StringValue = &text
			}
		default:
			if number, err := strconv.ParseFloat(string(value), 64); err == nil {
				score.NumericValue = &number
			}
		}
		if record.Subject != nil {
			switch record.Subject.Kind {
			case "trace":
				score.TraceID = record.Subject.ID
			case "observation":
				score.TraceID, score.ObservationID = record.Subject.TraceID, record.Subject.ID
			case "session":
				score.SessionID = record.Subject.ID
			case "experiment":
				score.DatasetRunID = record.Subject.ID
			}
		}
		if len(record.Metadata) != 0 {
			decoder := json.NewDecoder(bytes.NewReader(record.Metadata))
			decoder.UseNumber()
			var metadata map[string]any
			if decoder.Decode(&metadata) == nil {
				score.Metadata = metadata
			}
		}
		scores = append(scores, score)
	}
	return scores
}
