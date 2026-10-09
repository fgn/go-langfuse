package langfuse

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
	"github.com/fgn/go-langfuse/internal/diagnostic"
	"github.com/fgn/go-langfuse/internal/transport"
)

// ErrScoreQueueFull reports that a non-blocking client did not accept a score
// because its bounded delivery queue was full.
var ErrScoreQueueFull = errors.New("langfuse: score queue is full")

// ScoreDataType identifies how Langfuse stores and aggregates a score value.
type ScoreDataType string

const (
	ScoreTypeBoolean     ScoreDataType = "BOOLEAN"
	ScoreTypeCategorical ScoreDataType = "CATEGORICAL"
	ScoreTypeCorrection  ScoreDataType = "CORRECTION"
	ScoreTypeNumeric     ScoreDataType = "NUMERIC"
	ScoreTypeText        ScoreDataType = "TEXT"
)

// ScoreSource records how a score was produced.
type ScoreSource string

const (
	// ScoreSourceAPI is the default for scores written through the SDK.
	ScoreSourceAPI ScoreSource = "API"
	// ScoreSourceAnnotation marks a human annotation, or a prefilled value
	// for a reviewer to confirm in an annotation queue.
	ScoreSourceAnnotation ScoreSource = "ANNOTATION"
	// ScoreSourceEval marks a score from a Langfuse-managed evaluator; the
	// SDK reads it but cannot write it.
	ScoreSourceEval ScoreSource = "EVAL"
)

const (
	// maxScoreMetadataBytes bounds frozen evaluation metadata; the whole score
	// event must also fit maxScorePayloadBytes.
	maxScoreMetadataBytes  = maxScorePayloadBytes
	maxScoreNameCharacters = 200
	maxTextScoreCharacters = 500
	maxScorePayloadBytes   = 128 << 10
)

// Score is one evaluation or feedback value attached to a trace, a session, an
// observation, or a dataset run. Scores are submitted through the Langfuse
// JSON ingestion API rather than the OpenTelemetry trace pipeline.
type Score struct {
	// ID makes submissions idempotent: Langfuse upserts scores by ID.
	// Optional; the SDK generates a random ID when empty so retried
	// deliveries cannot create duplicates.
	ID string
	// Name identifies the score series, for example "user-feedback".
	// Required; at most 200 characters.
	Name string
	// TraceID, SessionID, DatasetRunID, and ObservationID select the score
	// target. Exactly one of TraceID, SessionID, or DatasetRunID is required,
	// and ObservationID additionally requires TraceID. DatasetRunID is an
	// experiment ID returned by [Client.RunExperiment] for dataset items.
	// Correction scores require a trace target.
	TraceID       string
	SessionID     string
	DatasetRunID  string
	ObservationID string
	// Exactly one of NumericValue or StringValue must be set. NUMERIC and
	// BOOLEAN use NumericValue, while CATEGORICAL, CORRECTION, and TEXT use
	// StringValue. Boolean scores use NumericValue 0 or 1.
	NumericValue *float64
	StringValue  *string
	// DataType is optional; when empty, Langfuse infers NUMERIC or
	// CATEGORICAL from the value type. TEXT values must contain 1 to 500
	// UTF-16 code units.
	DataType ScoreDataType
	// ConfigID references a Langfuse score config by its identifier.
	// Optional; at most 200 characters. Langfuse checks the score against the
	// config only after accepting it and drops a violating score without
	// reporting it to the client, for RecordScore and CreateScore alike.
	// Correction scores cannot use ConfigID.
	ConfigID string
	// Comment is explicit content supplied by the caller. It is not
	// processed by Config.Mask; sanitize it before calling the SDK.
	Comment string
	// Metadata is passed to Config.Mask as one complete value and must remain a
	// map[string]any to be retained. The resulting map is serialized as one JSON
	// value.
	Metadata map[string]any
	// Timestamp records when the scored interaction happened, for example
	// when feedback is computed by a batch job hours after the trace. The
	// zero value stamps the score with the time RecordScore accepted it. The
	// UTC year must stay within the four-digit RFC 3339 range.
	// [Client.CreateScore] cannot set it.
	Timestamp time.Time
	// Source is empty or ScoreSourceAPI for RecordScore. CreateScore also
	// accepts ScoreSourceAnnotation, which requires ConfigID unless the
	// score is CORRECTION, so the value can prefill an annotation queue.
	Source ScoreSource
	// QueueID names the annotation queue the score belongs to. Optional.
	QueueID string
}

// RecordScore submits one score through the Langfuse JSON ingestion endpoint
// using the client's credentials and environment; a score for the experiment
// item trace that ctx belongs to uses the item's "sdk-experiment" environment
// instead. The score is validated synchronously, so every returned error marks
// a score that was not accepted, and then queued for asynchronous delivery
// with bounded retry (network errors, HTTP 408, 429, and 5xx responses, and
// per-item ingestion errors with those statuses, using the same backoff
// defaults as observation export), so transport failures never reach the
// caller: after the retry budget they are reported as payload-free
// OpenTelemetry diagnostics and the score is dropped. [Client.Flush] and
// [Client.Shutdown] drain accepted scores. When the queue is full, the call
// returns [ErrScoreQueueFull] unless Config.BlockOnQueueFull waits for space,
// bounded by ctx. A disabled client returns nil without sending, and a
// shut-down client returns an error. The complete serialized score event is
// limited to 128 KiB.
func (c *Client) RecordScore(ctx context.Context, score Score) error {
	return c.recordScore(ctx, score, false)
}

// CreateScore sends one score to the Langfuse score REST endpoint and waits
// for the server to accept it, so a rejected request returns an error;
// [Client.RecordScore] queues scores for background delivery instead. It is
// the call for a score whose Source is ScoreSourceAnnotation, such as a value
// prefilled for an annotation queue. Langfuse checks the request shape
// before accepting the score but processes it asynchronously: a score whose
// value does not fit its ConfigID, or whose config is archived, is dropped
// later without an error, and the score appears in [Client.Scores] after a
// few seconds. With ConfigID, the score takes the config's name.
//
// The score is validated as RecordScore validates it and uses the same
// environment and Metadata masking, but is never suppressed by sampling.
// Timestamp must be zero because the server stamps the time.
//
// CreateScore returns the score ID, Score.ID or a generated one, with
// success and with every error once the request is built, including
// [ErrWriteOutcomeUnknown]; invalid scores and unavailable clients return
// "". The write is sent once, and repeating it with the returned ID updates
// the same score. Writing an existing ID merges into it: an omitted Comment,
// ConfigID, ObservationID, SessionID, or DatasetRunID keeps its stored
// value, an omitted QueueID or Source does not, metadata keys accumulate,
// and the trace ID, environment, and timestamp of the first write remain.
func (c *Client) CreateScore(ctx context.Context, score Score) (string, error) {
	invalid := validateScore(score)
	if invalid == nil {
		invalid = validateScoreSource(score)
	}
	if err := c.restReady(ctx, invalid); err != nil {
		return "", err
	}
	body, err := c.scoreBody(score, c.scoreEnvironment(ctx, score), false)
	if err != nil {
		return "", err
	}
	id, _ := body["id"].(string)
	if score.Source != "" {
		body["source"] = string(score.Source)
	}
	payload, err := marshalScore(body)
	if err != nil {
		return id, err
	}
	_, err = runREST(ctx, c, nil, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, c.restTransport.CreateScore(ctx, payload, id)
	})
	return id, err
}

func validateScoreSource(score Score) error {
	if !score.Timestamp.IsZero() {
		return errors.New("langfuse: CreateScore cannot set a score timestamp")
	}
	switch score.Source {
	case "", ScoreSourceAPI:
	case ScoreSourceAnnotation:
		if score.ConfigID == "" && score.DataType != ScoreTypeCorrection {
			return errors.New("langfuse: an ANNOTATION score requires a config ID unless it is CORRECTION")
		}
	default:
		return errors.New("langfuse: CreateScore source must be API or ANNOTATION")
	}
	return nil
}

// recordScore records score; metadataMasked marks Metadata that Mask has
// already processed.
func (c *Client) recordScore(ctx context.Context, score Score, metadataMasked bool) error {
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
	if score.Source != "" && score.Source != ScoreSourceAPI {
		return errors.New("langfuse: RecordScore sends only API scores; use CreateScore for another source")
	}
	if c.suppressScore(ctx, score) {
		return nil
	}
	payload, eventID, err := c.buildScorePayload(score, c.scoreEnvironment(ctx, score), metadataMasked)
	if err != nil {
		return err
	}
	err = c.scores.Enqueue(ctx, payload, eventID)
	if errors.Is(err, transport.ErrScoreQueueFull) {
		return ErrScoreQueueFull
	}
	return err
}

func (c *Client) scoreEnvironment(ctx context.Context, score Score) string {
	if score.TraceID != "" {
		if traceID, err := oteltrace.TraceIDFromHex(score.TraceID); err == nil &&
			len(c.experimentAttributes(ctx, traceID)) != 0 {
			return lfattr.ExperimentEnvironment
		}
	}
	return c.environment
}

// suppressScore applies the sampling decision of the caller's context path to
// a validated score. Suppression is a policy, not a proof: a score recorded
// directly on a sampled-out, SDK-originated context path inherits that path's
// drop decision. This is deliberate, documented loss, narrower than the
// official SDKs, which suppress on the local sampler decision alone. The
// conditions keep suppression where it is least likely to discard an
// attachable score; they cannot rule out a sibling branch having handed the
// context to a foreign exporter. Everything not matched is delivered:
// session-only scores, other traces, out-of-context scores, foreign-origin
// or downgraded paths, and borrowed mode.
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
		"score dataset run ID": score.DatasetRunID,
		"score observation ID": score.ObservationID,
		"score config ID":      score.ConfigID,
		"score queue ID":       score.QueueID,
	} {
		if err := validScoreString(field, value, true); err != nil {
			return err
		}
	}
	targets := 0
	for _, target := range []string{score.TraceID, score.SessionID, score.DatasetRunID} {
		if target != "" {
			targets++
		}
	}
	if targets != 1 {
		return errors.New("langfuse: score requires exactly one trace ID, session ID, or dataset run ID target")
	}
	if score.ObservationID != "" && score.TraceID == "" {
		return errors.New("langfuse: score observation ID requires a trace ID")
	}
	if (score.NumericValue == nil) == (score.StringValue == nil) {
		return errors.New("langfuse: score requires exactly one of numeric value or string value")
	}
	if score.NumericValue != nil && (math.IsNaN(*score.NumericValue) || math.IsInf(*score.NumericValue, 0)) {
		return errors.New("langfuse: score numeric value must be finite")
	}
	if score.StringValue != nil && !utf8.ValidString(*score.StringValue) {
		return errors.New("langfuse: score string value is not valid UTF-8")
	}
	switch score.DataType {
	case "":
	case ScoreTypeNumeric:
		if score.NumericValue == nil {
			return errors.New("langfuse: NUMERIC score requires a numeric value")
		}
	case ScoreTypeCategorical:
		if score.StringValue == nil {
			return errors.New("langfuse: CATEGORICAL score requires a string value")
		}
	case ScoreTypeBoolean:
		if score.NumericValue == nil || (*score.NumericValue != 0 && *score.NumericValue != 1) {
			return errors.New("langfuse: BOOLEAN score requires a numeric value equal to 0 or 1")
		}
	case ScoreTypeCorrection:
		if score.StringValue == nil {
			return errors.New("langfuse: CORRECTION score requires a string value")
		}
		if score.TraceID == "" {
			return errors.New("langfuse: CORRECTION score requires a trace or observation target")
		}
		if score.ConfigID != "" {
			return errors.New("langfuse: CORRECTION score cannot use a config ID")
		}
	case ScoreTypeText:
		if score.StringValue == nil {
			return errors.New("langfuse: TEXT score requires a string value")
		}
		length := lengthJS(*score.StringValue)
		if length == 0 || length > maxTextScoreCharacters {
			return errors.New("langfuse: TEXT score must contain 1 to 500 UTF-16 code units")
		}
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
func (c *Client) buildScorePayload(score Score, environment string, metadataMasked bool) ([]byte, string, error) {
	payload, err := c.scoreBody(score, environment, metadataMasked)
	if err != nil {
		return nil, "", err
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
	encoded, err := marshalScore(event)
	return encoded, eventID, err
}

// scoreBody builds the score fields shared by ingestion and the score REST
// endpoint, generating an ID when the score has none.
func (c *Client) scoreBody(score Score, environment string, metadataMasked bool) (map[string]any, error) {
	payload := map[string]any{
		"name":        score.Name,
		"environment": environment,
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
	if score.DatasetRunID != "" {
		payload["datasetRunId"] = score.DatasetRunID
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
	if score.QueueID != "" {
		payload["queueId"] = score.QueueID
	}
	metadata := score.Metadata
	if !metadataMasked {
		metadata = lfattr.ScoreMetadata(score.Metadata, c.mask)
	}
	if len(metadata) != 0 {
		payload["metadata"] = metadata
	}
	return payload, nil
}

func marshalScore(value any) ([]byte, error) {
	encoded, err, panicked := lfattr.MarshalJSON(value, maxScorePayloadBytes)
	if panicked || err != nil {
		return nil, errors.New("langfuse: score could not be serialized")
	}
	if len(encoded) > maxScorePayloadBytes {
		return nil, errors.New("langfuse: score exceeds the 128 KiB payload limit")
	}
	return encoded, nil
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
