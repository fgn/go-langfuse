package langfuse

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
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
// or an observation. Scores are submitted through the Langfuse REST API
// rather than the OpenTelemetry trace pipeline.
type Score struct {
	// ID makes submissions idempotent: Langfuse upserts scores by ID.
	// Optional; the SDK generates a random ID when empty so retried
	// deliveries cannot create duplicates.
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
// using the client's credentials and environment. The score is validated
// synchronously — every returned error marks a score that was not accepted —
// and then queued for asynchronous delivery with bounded retry (network
// errors and HTTP 408, 429, and 5xx responses, using the same backoff
// defaults as observation export), so transport failures never reach the
// caller: after the retry budget they are reported as payload-free
// OpenTelemetry diagnostics and the score is dropped. [Client.Flush] and [Client.Shutdown] drain accepted
// scores. When the queue is full a score is dropped with a diagnostic unless
// Config.BlockOnQueueFull waits for space, bounded by ctx. A disabled client
// returns nil without sending, and a shut-down client returns an error. The
// complete serialized score is limited to 128 KiB.
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
	return c.scores.Enqueue(ctx, payload)
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
	// Always submit an ID: retried deliveries must upsert, not duplicate.
	scoreID := score.ID
	if scoreID == "" {
		generated, err := newScoreID()
		if err != nil {
			return nil, err
		}
		scoreID = generated
	}
	payload["id"] = scoreID
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

// newScoreID returns a random UUID version 4 string. Generating the upsert
// key client-side keeps asynchronous retries idempotent even when a delivery
// succeeded but its response was lost.
func newScoreID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", errors.New("langfuse: generate score ID")
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
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
