package langfuse

import (
	"context"
	"encoding/json"
	"errors"
	"unicode/utf8"
)

// ScoreDataType identifies how Langfuse stores and aggregates a score value.
type ScoreDataType string

const (
	ScoreTypeBoolean     ScoreDataType = "BOOLEAN"
	ScoreTypeCategorical ScoreDataType = "CATEGORICAL"
	ScoreTypeCorrection  ScoreDataType = "CORRECTION"
	ScoreTypeNumeric     ScoreDataType = "NUMERIC"
	ScoreTypeText        ScoreDataType = "TEXT"
)

const (
	maxScoreNameCharacters = 200
	maxScorePayloadBytes   = 128 << 10
)

// Score is one evaluation or feedback value attached to a trace, a session,
// or an observation. Scores are submitted synchronously through the Langfuse
// REST API rather than the OpenTelemetry trace pipeline.
type Score struct {
	// ID makes submissions idempotent: Langfuse upserts scores by ID.
	// Optional; Langfuse generates an ID when empty.
	ID string
	// Name identifies the score series, for example "user-feedback".
	// Required; at most 200 characters.
	Name string
	// TraceID, SessionID, and ObservationID select the score target. At
	// least one of TraceID or SessionID is required, and ObservationID
	// additionally requires TraceID.
	TraceID       string
	SessionID     string
	ObservationID string
	// Exactly one of NumericValue or StringValue must be set. Boolean
	// scores use NumericValue 0 or 1 with ScoreTypeBoolean.
	NumericValue *float64
	StringValue  *string
	// DataType is optional; when empty, Langfuse infers NUMERIC or
	// CATEGORICAL from the value type.
	DataType ScoreDataType
	// Comment is explicit content supplied by the caller. It is not
	// processed by Config.Mask; sanitize it before calling the SDK.
	Comment string
	// Metadata is serialized as one JSON value.
	Metadata map[string]any
}

// RecordScore submits one score through the Langfuse REST scores endpoint
// using the client's credentials and environment. Unlike observations it is
// synchronous and returns transport errors to the caller; there is no
// buffering or retry. A disabled client returns nil without sending, and a
// shut-down client returns an error. The complete serialized score is limited
// to 128 KiB.
func (c *Client) RecordScore(ctx context.Context, score Score) error {
	if c == nil || c.isDisabled() || c.scores == nil {
		return nil
	}
	if c.stopped.Load() {
		return errors.New("langfuse: score rejected after client shutdown")
	}
	if ctx == nil {
		return errors.New("langfuse: score context is nil")
	}
	payload, err := c.scorePayload(score)
	if err != nil {
		return err
	}
	return c.scores.Send(ctx, payload)
}

func (c *Client) scorePayload(score Score) ([]byte, error) {
	if err := validScoreString("score name", score.Name, false); err != nil {
		return nil, err
	}
	for field, value := range map[string]string{
		"score ID":             score.ID,
		"score trace ID":       score.TraceID,
		"score session ID":     score.SessionID,
		"score observation ID": score.ObservationID,
	} {
		if err := validScoreString(field, value, true); err != nil {
			return nil, err
		}
	}
	if score.TraceID == "" && score.SessionID == "" {
		return nil, errors.New("langfuse: score requires a trace ID or session ID target")
	}
	if score.ObservationID != "" && score.TraceID == "" {
		return nil, errors.New("langfuse: score observation ID requires a trace ID")
	}
	if (score.NumericValue == nil) == (score.StringValue == nil) {
		return nil, errors.New("langfuse: score requires exactly one of numeric value or string value")
	}
	switch score.DataType {
	case "", ScoreTypeBoolean, ScoreTypeCategorical, ScoreTypeCorrection, ScoreTypeNumeric, ScoreTypeText:
	default:
		return nil, errors.New("langfuse: unsupported score data type")
	}
	if score.Comment != "" && !utf8.ValidString(score.Comment) {
		return nil, errors.New("langfuse: score comment is not valid UTF-8")
	}

	payload := map[string]any{
		"name":        score.Name,
		"environment": c.environment,
	}
	if score.NumericValue != nil {
		payload["value"] = *score.NumericValue
	} else {
		if !utf8.ValidString(*score.StringValue) {
			return nil, errors.New("langfuse: score string value is not valid UTF-8")
		}
		payload["value"] = *score.StringValue
	}
	if score.ID != "" {
		payload["id"] = score.ID
	}
	if score.TraceID != "" {
		payload["traceId"] = score.TraceID
	}
	if score.SessionID != "" {
		payload["sessionId"] = score.SessionID
	}
	if score.ObservationID != "" {
		payload["observationId"] = score.ObservationID
	}
	if score.DataType != "" {
		payload["dataType"] = string(score.DataType)
	}
	if score.Comment != "" {
		payload["comment"] = score.Comment
	}
	if len(score.Metadata) != 0 {
		payload["metadata"] = score.Metadata
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.New("langfuse: score could not be serialized")
	}
	if len(encoded) > maxScorePayloadBytes {
		return nil, errors.New("langfuse: score exceeds the 128 KiB payload limit")
	}
	return encoded, nil
}

func validScoreString(field, value string, emptyAllowed bool) error {
	if value == "" {
		if emptyAllowed {
			return nil
		}
		return errors.New("langfuse: " + field + " is required")
	}
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > maxScoreNameCharacters {
		return errors.New("langfuse: " + field + " is invalid or exceeds 200 characters")
	}
	return nil
}
