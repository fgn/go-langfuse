package langfuse

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/fgn/go-langfuse/internal/diagnostic"
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
// or an observation. Scores are submitted through the Langfuse JSON ingestion
// API rather than the OpenTelemetry trace pipeline.
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
	// ConfigID references a Langfuse score config by its identifier.
	// Optional; at most 200 characters. Langfuse validates the score against
	// the config server-side, so a violating score is rejected during
	// asynchronous delivery and dropped with a diagnostic rather than
	// returned as a RecordScore error.
	ConfigID string
	// Comment is explicit content supplied by the caller. It is not
	// processed by Config.Mask; sanitize it before calling the SDK.
	Comment string
	// Metadata is serialized as one JSON value.
	Metadata map[string]any
	// Timestamp records when the scored interaction happened, for example
	// when feedback is computed by a batch job hours after the trace. The
	// zero value stamps the score with the time RecordScore accepted it. The
	// UTC year must stay within the four-digit RFC 3339 range.
	Timestamp time.Time
}

// RecordScore submits one score through the Langfuse JSON ingestion endpoint
// using the client's credentials and environment. The score is validated
// synchronously — every returned error marks a score that was not accepted —
// and then queued for asynchronous delivery with bounded retry (network
// errors, HTTP 408, 429, and 5xx responses, and per-item ingestion errors
// with those statuses, using the same backoff defaults as observation
// export), so transport failures never reach the caller: after the retry
// budget they are reported as payload-free OpenTelemetry diagnostics and the
// score is dropped. [Client.Flush] and [Client.Shutdown] drain accepted
// scores. When the queue is full a score is dropped with a diagnostic unless
// Config.BlockOnQueueFull waits for space, bounded by ctx. A disabled client
// returns nil without sending, and a shut-down client returns an error. The
// complete serialized score event is limited to 128 KiB.
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
	if err := validateScore(score); err != nil {
		return err
	}
	if c.suppressScore(ctx, score) {
		return nil
	}
	payload, eventID, err := c.buildScorePayload(score)
	if err != nil {
		return err
	}
	return c.scores.Enqueue(ctx, payload, eventID)
}

// suppressScore applies the sampling decision of the caller's context path to
// a validated score. Suppression is a policy, not a proof: a score recorded
// directly on a sampled-out, SDK-originated context path inherits that path's
// drop decision — deliberate, documented loss, narrower than the official
// SDKs, which suppress on the local sampler decision alone. The conditions
// keep suppression where it is least likely to discard an attachable score;
// they cannot rule out a sibling branch having handed the context to a
// foreign exporter. Everything not matched — session-only scores, other
// traces, out-of-context scores, foreign-origin or downgraded paths, borrowed
// mode — is delivered.
func (c *Client) suppressScore(ctx context.Context, score Score) bool {
	if !c.owned || score.TraceID == "" {
		return false
	}
	decision, ok := ctx.Value(traceDecisionContextKey{client: c}).(traceDecision)
	if !ok || !decision.authoritative || decision.sampled {
		return false
	}
	if score.TraceID != decision.traceID.String() {
		return false
	}
	ambient := oteltrace.SpanFromContext(ctx).SpanContext()
	if !ambient.IsValid() || ambient.TraceID() != decision.traceID || ambient.SpanID() != decision.lastSDKSpanID {
		return false
	}
	if c.scoreSuppressionWarning.CompareAndSwap(false, true) {
		diagnostic.Report("score suppressed for a sampled-out trace; further suppressions are silent")
	}
	return true
}

// validateScore performs the complete synchronous validation of a score. It
// is pure: no ID generation, no clock reads, no serialization, so an
// intentionally suppressed score does none of that work.
func validateScore(score Score) error {
	if err := validScoreString("score name", score.Name, false); err != nil {
		return err
	}
	for field, value := range map[string]string{
		"score ID":             score.ID,
		"score trace ID":       score.TraceID,
		"score session ID":     score.SessionID,
		"score observation ID": score.ObservationID,
		"score config ID":      score.ConfigID,
	} {
		if err := validScoreString(field, value, true); err != nil {
			return err
		}
	}
	if score.TraceID == "" && score.SessionID == "" {
		return errors.New("langfuse: score requires a trace ID or session ID target")
	}
	if score.ObservationID != "" && score.TraceID == "" {
		return errors.New("langfuse: score observation ID requires a trace ID")
	}
	if (score.NumericValue == nil) == (score.StringValue == nil) {
		return errors.New("langfuse: score requires exactly one of numeric value or string value")
	}
	if score.StringValue != nil && !utf8.ValidString(*score.StringValue) {
		return errors.New("langfuse: score string value is not valid UTF-8")
	}
	switch score.DataType {
	case "", ScoreTypeBoolean, ScoreTypeCategorical, ScoreTypeCorrection, ScoreTypeNumeric, ScoreTypeText:
	default:
		return errors.New("langfuse: unsupported score data type")
	}
	if score.Comment != "" && !utf8.ValidString(score.Comment) {
		return errors.New("langfuse: score comment is not valid UTF-8")
	}
	if !score.Timestamp.IsZero() {
		// RFC 3339 timestamps carry a four-digit year; anything else would
		// serialize to an invalid wire value.
		if year := score.Timestamp.UTC().Year(); year < 0 || year > 9999 {
			return errors.New("langfuse: score timestamp year is outside the RFC 3339 range")
		}
	}
	return nil
}

// buildScorePayload serializes a validated score as a complete single-event
// ingestion request, returning the envelope event ID the ingestion result
// must account for.
func (c *Client) buildScorePayload(score Score) ([]byte, string, error) {
	payload := map[string]any{
		"name":        score.Name,
		"environment": c.environment,
	}
	if score.NumericValue != nil {
		payload["value"] = *score.NumericValue
	} else {
		payload["value"] = *score.StringValue
	}
	// Always submit an ID: retried deliveries must upsert, not duplicate.
	scoreID := score.ID
	if scoreID == "" {
		generated, err := newScoreID()
		if err != nil {
			return nil, "", err
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
	if score.ConfigID != "" {
		payload["configId"] = score.ConfigID
	}
	if score.Comment != "" {
		payload["comment"] = score.Comment
	}
	if len(score.Metadata) != 0 {
		payload["metadata"] = score.Metadata
	}

	// The ingestion event envelope carries the score's timestamp: Langfuse
	// stores a score-create event's envelope timestamp as the score time, which
	// is how the official SDKs backdate scores. The envelope is serialized once
	// here, so a retried delivery resends the identical event and stays
	// idempotent through the event ID and the score ID upsert.
	timestamp := score.Timestamp.UTC()
	if score.Timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	eventID, err := newScoreID()
	if err != nil {
		return nil, "", err
	}
	event := map[string]any{
		"batch": []any{map[string]any{
			"id":        eventID,
			"type":      "score-create",
			"timestamp": timestamp.Format(time.RFC3339Nano),
			"body":      payload,
		}},
	}

	encoded, err := json.Marshal(event)
	if err != nil {
		return nil, "", errors.New("langfuse: score could not be serialized")
	}
	if len(encoded) > maxScorePayloadBytes {
		return nil, "", errors.New("langfuse: score exceeds the 128 KiB payload limit")
	}
	return encoded, eventID, nil
}

// newScoreID returns a random UUID version 4 string, used both as the score
// upsert key and as the ingestion event ID. Generating them client-side keeps
// asynchronous retries idempotent even when a delivery succeeded but its
// response was lost.
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
