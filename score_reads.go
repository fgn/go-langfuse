package langfuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"iter"
	"math"
	"net/url"
	"strconv"
	"time"

	"github.com/fgn/go-langfuse/internal/transport"
)

// StoredScore is a score as stored by Langfuse. Score holds the fields a
// write sets; a BOOLEAN value is NumericValue 0 or 1, as [Score] writes it,
// and a CATEGORICAL value is its label in StringValue. [Client.Scores] fills
// every field; the scores of experiment reads lack Comment, ConfigID,
// Metadata, QueueID, and AuthorUserID, which Langfuse omits there.
type StoredScore struct {
	Score
	Environment string
	// AuthorUserID is the Langfuse user who annotated the score, if any.
	AuthorUserID string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ScoreQuery selects scores for [Client.Scores]. Each non-empty filter
// restricts the result to scores matching one of its values; values must not
// contain commas or surrounding whitespace.
type ScoreQuery struct {
	IDs           []string
	Names         []string
	Sources       []ScoreSource
	DataTypes     []ScoreDataType
	Environments  []string
	ConfigIDs     []string
	QueueIDs      []string
	AuthorUserIDs []string
	// TraceIDs, SessionIDs, and DatasetRunIDs select the scored object and
	// exclude each other; TraceIDs also match the scores of the traces'
	// observations. ObservationIDs requires TraceIDs.
	TraceIDs       []string
	ObservationIDs []string
	SessionIDs     []string
	DatasetRunIDs  []string
	// Values matches exact values and requires exactly one of the NUMERIC
	// (finite numbers in JavaScript syntax), BOOLEAN ("true" or "false"), or
	// CATEGORICAL (labels) data types.
	Values []string
	// MinValue and MaxValue bound the value inclusively and require
	// DataTypes to be exactly NUMERIC.
	MinValue *float64
	MaxValue *float64
	// From and To, when non-zero, bound the score timestamp to [From, To).
	// Updating a score keeps its timestamp, so they cannot find recent
	// changes to older scores.
	From time.Time
	To   time.Time
	// PageSize is the number of scores per request; 0 selects 50 and Langfuse
	// accepts at most 100.
	PageSize int
}

// Scores lazily iterates over the project's scores, one request per page,
// with the budget and failure rules of [Client.DatasetItems]. It reads the
// Langfuse v3 score API, the score listing available on servers in
// events-only mode, and returns every field group.
func (c *Client) Scores(ctx context.Context, query ScoreQuery) iter.Seq2[StoredScore, error] {
	values, err := scoreReadQuery(query)
	return cursorPages(ctx, c, err, values, func(ctx context.Context, values url.Values) ([]StoredScore, string, error) {
		page, err := c.restTransport.ListScores(ctx, values)
		return scoresFromWire(page.Data), page.Meta.Next(), err
	})
}

func scoreReadQuery(query ScoreQuery) (url.Values, error) {
	pageSize, err := restPageSize(query.PageSize)
	if err != nil {
		return nil, err
	}
	targets := 0
	for _, target := range [][]string{query.TraceIDs, query.SessionIDs, query.DatasetRunIDs} {
		if len(target) != 0 {
			targets++
		}
	}
	if targets > 1 {
		return nil, errors.New("langfuse: score query trace, session, and dataset run IDs exclude each other")
	}
	if len(query.ObservationIDs) != 0 && len(query.TraceIDs) == 0 {
		return nil, errors.New("langfuse: score query observation IDs require trace IDs")
	}
	for _, source := range query.Sources {
		switch source {
		case ScoreSourceAPI, ScoreSourceAnnotation, ScoreSourceEval:
		default:
			return nil, errors.New("langfuse: score query source must be API, ANNOTATION, or EVAL")
		}
	}
	for _, dataType := range query.DataTypes {
		switch dataType {
		case ScoreTypeNumeric, ScoreTypeBoolean, ScoreTypeCategorical, ScoreTypeText, ScoreTypeCorrection:
		default:
			return nil, errors.New("langfuse: unsupported score query data type")
		}
	}
	if len(query.Values) != 0 && (len(query.DataTypes) != 1 || query.DataTypes[0] == ScoreTypeText ||
		query.DataTypes[0] == ScoreTypeCorrection) {
		return nil, errors.New("langfuse: score query values require one NUMERIC, BOOLEAN, or CATEGORICAL data type")
	}
	for _, value := range query.Values {
		switch query.DataTypes[0] {
		case ScoreTypeNumeric:
			if !finiteJSNumber(value) {
				return nil, errors.New("langfuse: NUMERIC score query values must be finite numbers")
			}
		case ScoreTypeBoolean:
			if value != "true" && value != "false" {
				return nil, errors.New(`langfuse: BOOLEAN score query values must be "true" or "false"`)
			}
		case ScoreTypeCategorical, ScoreTypeText, ScoreTypeCorrection: // labels; TEXT and CORRECTION failed above
		}
	}
	values := url.Values{}
	for _, bound := range []struct {
		key   string
		value *float64
	}{{"valueMin", query.MinValue}, {"valueMax", query.MaxValue}} {
		if bound.value == nil {
			continue
		}
		if len(query.DataTypes) != 1 || query.DataTypes[0] != ScoreTypeNumeric {
			return nil, errors.New("langfuse: score query value bounds require the NUMERIC data type")
		}
		if math.IsNaN(*bound.value) || math.IsInf(*bound.value, 0) {
			return nil, errors.New("langfuse: score query value bounds must be finite")
		}
		values.Set(bound.key, strconv.FormatFloat(*bound.value, 'g', -1, 64))
	}
	for _, bound := range []struct {
		key   string
		value time.Time
	}{{"fromTimestamp", query.From}, {"toTimestamp", query.To}} {
		if bound.value.IsZero() {
			continue
		}
		if !validDatasetInstant(bound.value) {
			return nil, errors.New("langfuse: score query times must be in the RFC 3339 year range")
		}
		values.Set(bound.key, formatDatasetInstant(bound.value))
	}
	values.Set("fields", "details,subject,annotation")
	values.Set("limit", strconv.Itoa(pageSize))
	return values, setListFilters(values, map[string][]string{
		"id": query.IDs, "name": query.Names, "source": stringValues(query.Sources),
		"dataType": stringValues(query.DataTypes), "environment": query.Environments,
		"configId": query.ConfigIDs, "queueId": query.QueueIDs, "authorUserId": query.AuthorUserIDs,
		"traceId": query.TraceIDs, "observationId": query.ObservationIDs, "sessionId": query.SessionIDs,
		"experimentId": query.DatasetRunIDs, "value": query.Values,
	})
}

func stringValues[T ~string](values []T) []string {
	strs := make([]string, len(values))
	for index, value := range values {
		strs[index] = string(value)
	}
	return strs
}

// scoresFromWire converts v3 scores.
func scoresFromWire(wire []transport.ScoreRecord) []StoredScore {
	if len(wire) == 0 {
		return nil
	}
	scores := make([]StoredScore, 0, len(wire))
	for _, record := range wire {
		// The transport validated these members as strings or null.
		comment, _ := transport.NullableString(record.Comment)
		configID, _ := transport.NullableString(record.ConfigID)
		queueID, _ := transport.NullableString(record.QueueID)
		author, _ := transport.NullableString(record.AuthorUserID)
		score := Score{
			ID: record.ID, Name: record.Name, DataType: ScoreDataType(record.DataType),
			ConfigID: configID, Comment: comment, Timestamp: record.Timestamp,
			Source: ScoreSource(record.Source), QueueID: queueID,
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
		if len(record.Metadata) != 0 && string(record.Metadata) != "null" {
			decoder := json.NewDecoder(bytes.NewReader(record.Metadata))
			decoder.UseNumber()
			var metadata map[string]any
			if decoder.Decode(&metadata) == nil && len(metadata) != 0 {
				score.Metadata = metadata
			}
		}
		scores = append(scores, StoredScore{
			Score: score, Environment: record.Environment, AuthorUserID: author,
			CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
		})
	}
	return scores
}
