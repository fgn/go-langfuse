package langfuse_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

type scoreWireRequest struct {
	path        string
	contentType string
	username    string
	password    string
	authOK      bool
	body        map[string]any
}

type scoreWireReceiver struct {
	mu       sync.Mutex
	status   int
	requests []scoreWireRequest
}

type panickingScoreMetadata struct{}

func (panickingScoreMetadata) MarshalJSON() ([]byte, error) {
	panic("score metadata marshal panic")
}

func (r *scoreWireReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	record := scoreWireRequest{
		path:        req.URL.Path,
		contentType: req.Header.Get("Content-Type"),
	}
	record.username, record.password, record.authOK = req.BasicAuth()
	_ = json.Unmarshal(body, &record.body)
	r.mu.Lock()
	r.requests = append(r.requests, record)
	status := r.status
	r.mu.Unlock()
	if strings.HasSuffix(req.URL.Path, "/v1/traces") {
		// Borrowed-mode sampling tests export OTLP spans to this server too;
		// a plain success keeps the exporter quiet without a protobuf body.
		w.WriteHeader(http.StatusOK)
		return
	}
	if status == 0 {
		// Answer like the real ingestion endpoint: 207 with per-item results
		// accounting for the submitted envelope event ID.
		eventID := ""
		if batch, ok := record.body["batch"].([]any); ok && len(batch) == 1 {
			if event, ok := batch[0].(map[string]any); ok {
				eventID, _ = event["id"].(string)
			}
		}
		w.WriteHeader(http.StatusMultiStatus)
		_, _ = fmt.Fprintf(w, `{"successes":[{"id":%q,"status":201}],"errors":[]}`, eventID)
		return
	}
	w.WriteHeader(status)
}

func (r *scoreWireReceiver) all() []scoreWireRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]scoreWireRequest(nil), r.requests...)
}

func newScoreWireClient(t *testing.T, change func(*langfuse.Config)) (*langfuse.Client, *scoreWireReceiver) {
	t.Helper()
	receiver := &scoreWireReceiver{}
	server := httptest.NewServer(receiver)
	t.Cleanup(server.Close)
	config := langfuse.Config{
		BaseURL:     server.URL,
		PublicKey:   "pk-lf-score-wire",
		SecretKey:   "sk-lf-score-wire",
		Environment: "score_wire",
	}
	if change != nil {
		change(&config)
	}
	client, err := langfuse.New(context.Background(), config)
	if err != nil {
		t.Fatalf("langfuse.New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Shutdown(ctx); err != nil {
			t.Errorf("Client.Shutdown() error = %v", err)
		}
	})
	return client, receiver
}

func flushClient(t *testing.T, client *langfuse.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Client.Flush() error = %v", err)
	}
}

// scoreWireEvent unwraps the single score-create event from an ingestion
// request and returns the event envelope and the score body.
func scoreWireEvent(t *testing.T, request scoreWireRequest) (envelope, body map[string]any) {
	t.Helper()
	batch, ok := request.body["batch"].([]any)
	if !ok || len(batch) != 1 {
		t.Fatalf("ingestion request batch = %v, want exactly one event", request.body["batch"])
	}
	envelope, ok = batch[0].(map[string]any)
	if !ok {
		t.Fatalf("ingestion event = %v, want an object", batch[0])
	}
	if got, _ := envelope["type"].(string); got != "score-create" {
		t.Fatalf("ingestion event type = %v, want score-create", envelope["type"])
	}
	if id, _ := envelope["id"].(string); len(id) != 36 {
		t.Fatalf("ingestion event id = %v, want a generated 36-character UUID", envelope["id"])
	}
	timestamp, _ := envelope["timestamp"].(string)
	if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		t.Fatalf("ingestion event timestamp = %v, want RFC 3339: %v", envelope["timestamp"], err)
	}
	body, ok = envelope["body"].(map[string]any)
	if !ok {
		t.Fatalf("ingestion event body = %v, want an object", envelope["body"])
	}
	return envelope, body
}

func TestScoreWireSubmitsAuthenticatedJSON(t *testing.T) {
	t.Parallel()
	client, receiver := newScoreWireClient(t, func(config *langfuse.Config) {
		// The scores endpoint must derive from any accepted base URL form.
		config.BaseURL += "/api/public/otel"
	})

	rating := 4.0
	err := client.RecordScore(context.Background(), langfuse.Score{
		ID:           "feedback-42",
		Name:         "user-feedback",
		SessionID:    "conversation:609",
		NumericValue: &rating,
		DataType:     langfuse.ScoreTypeNumeric,
		Comment:      "clear report",
		Metadata:     map[string]any{"report_id": "7"},
	})
	if err != nil {
		t.Fatalf("RecordScore() error = %v", err)
	}
	flushClient(t, client)

	requests := receiver.all()
	if len(requests) != 1 {
		t.Fatalf("score request count = %d, want 1", len(requests))
	}
	request := requests[0]
	if request.path != "/api/public/ingestion" {
		t.Fatalf("score path = %q, want /api/public/ingestion", request.path)
	}
	if request.contentType != "application/json" {
		t.Fatalf("score content type = %q, want application/json", request.contentType)
	}
	if !request.authOK || request.username != "pk-lf-score-wire" || request.password != "sk-lf-score-wire" {
		t.Fatalf("score basic auth = (%q, ok %v), want the client credentials", request.username, request.authOK)
	}
	envelope, body := scoreWireEvent(t, request)
	if len(envelope) != 4 {
		t.Fatalf("ingestion event has %d fields, want id, type, timestamp, body: %v", len(envelope), envelope)
	}
	want := map[string]any{
		"id":          "feedback-42",
		"name":        "user-feedback",
		"sessionId":   "conversation:609",
		"value":       4.0,
		"dataType":    "NUMERIC",
		"comment":     "clear report",
		"metadata":    map[string]any{"report_id": "7"},
		"environment": "score_wire",
	}
	for key, wantValue := range want {
		got, exists := body[key]
		if !exists {
			t.Fatalf("score payload is missing %q; payload: %v", key, body)
		}
		gotJSON, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("marshal score payload %q: %v", key, err)
		}
		wantJSON, err := json.Marshal(wantValue)
		if err != nil {
			t.Fatalf("marshal expected score payload %q: %v", key, err)
		}
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("score payload %q = %s, want %s", key, gotJSON, wantJSON)
		}
	}
	if len(body) != len(want) {
		t.Fatalf("score payload has %d fields, want %d: %v", len(body), len(want), body)
	}
}

func TestRecordScoreContainsMetadataMarshalPanic(t *testing.T) {
	client, receiver := newScoreWireClient(t, nil)
	rating := 1.0
	err := client.RecordScore(context.Background(), langfuse.Score{
		Name: "panic-safe", SessionID: "s", NumericValue: &rating,
		Metadata: map[string]any{"value": panickingScoreMetadata{}},
	})
	if err == nil {
		t.Fatal("RecordScore() error = nil, want payload-free serialization error")
	}
	if strings.Contains(err.Error(), "score metadata marshal panic") {
		t.Fatalf("RecordScore() error exposed panic payload: %v", err)
	}
	if got := len(receiver.all()); got != 0 {
		t.Fatalf("serialization failure sent %d requests, want 0", got)
	}
}

func TestScoreWireMasksCompleteMetadataOnce(t *testing.T) {
	maskCalls := 0
	client, receiver := newScoreWireClient(t, func(config *langfuse.Config) {
		config.Mask = func(value any) any {
			maskCalls++
			metadata, ok := value.(map[string]any)
			if !ok || metadata["secret"] != "remove-me" || metadata["keep"] != "original" {
				t.Fatalf("Mask value = %#v, want the complete score metadata map", value)
			}
			return map[string]any{"keep": "masked"}
		}
	})
	rating := 1.0
	if err := client.RecordScore(context.Background(), langfuse.Score{
		Name: "masked", SessionID: "s", NumericValue: &rating,
		Metadata: map[string]any{"secret": "remove-me", "keep": "original"},
	}); err != nil {
		t.Fatalf("RecordScore() error = %v", err)
	}
	flushClient(t, client)
	if maskCalls != 1 {
		t.Fatalf("Mask calls = %d, want 1", maskCalls)
	}
	_, body := scoreWireEvent(t, receiver.all()[0])
	want := map[string]any{"keep": "masked"}
	if got := body["metadata"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("score metadata = %#v, want %#v", got, want)
	}
}

func TestScoreWireOmitsMetadataWhenMaskDoesNotReturnMap(t *testing.T) {
	tests := []struct {
		name string
		mask func(any) any
	}{
		{name: "nil", mask: func(any) any { return nil }},
		{name: "wrong type", mask: func(any) any { return "not metadata" }},
		{name: "panic", mask: func(any) any { panic("mask secret") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, receiver := newScoreWireClient(t, func(config *langfuse.Config) {
				config.Mask = test.mask
			})
			rating := 1.0
			if err := client.RecordScore(context.Background(), langfuse.Score{
				Name: "masked", SessionID: "s", NumericValue: &rating,
				Metadata: map[string]any{"secret": "remove-me"},
			}); err != nil {
				t.Fatalf("RecordScore() error = %v", err)
			}
			flushClient(t, client)
			_, body := scoreWireEvent(t, receiver.all()[0])
			if metadata, exists := body["metadata"]; exists {
				t.Fatalf("score metadata = %#v, want omitted", metadata)
			}
		})
	}
}

func TestRecordScoreReportsFullQueue(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
	arrived := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case arrived <- struct{}{}:
		default:
		}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(releaseServer)
	client, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL: server.URL, PublicKey: "pk-full-score-queue", SecretKey: "sk-full-score-queue",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(ctx)
	})
	rating := 1.0
	record := func() error {
		return client.RecordScore(context.Background(), langfuse.Score{
			Name: "queue", SessionID: "s", NumericValue: &rating,
		})
	}
	if err := record(); err != nil {
		t.Fatalf("first RecordScore() error = %v", err)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("first score never reached the blocking server")
	}
	for index := range 256 {
		if err := record(); err != nil {
			t.Fatalf("RecordScore() filling slot %d error = %v", index, err)
		}
	}
	if err := record(); !errors.Is(err, langfuse.ErrScoreQueueFull) {
		t.Fatalf("RecordScore() on full queue error = %v, want ErrScoreQueueFull", err)
	}
	releaseServer()
}

func TestScoreWireTimestampAndConfigID(t *testing.T) {
	t.Parallel()
	client, receiver := newScoreWireClient(t, nil)

	// A batch job scoring yesterday's trace must be able to backdate the
	// score; the ingestion event envelope carries that timestamp.
	backdated := time.Date(2026, 7, 19, 8, 30, 0, 250000000, time.UTC)
	rating := 3.0
	before := time.Now()
	err := client.RecordScore(context.Background(), langfuse.Score{
		Name:         "report-quality",
		SessionID:    "conversation:610",
		NumericValue: &rating,
		ConfigID:     "config-123",
		Timestamp:    backdated,
	})
	if err != nil {
		t.Fatalf("RecordScore() error = %v", err)
	}
	err = client.RecordScore(context.Background(), langfuse.Score{
		Name:         "report-quality",
		SessionID:    "conversation:611",
		NumericValue: &rating,
	})
	if err != nil {
		t.Fatalf("RecordScore() error = %v", err)
	}
	flushClient(t, client)

	requests := receiver.all()
	if len(requests) != 2 {
		t.Fatalf("score request count = %d, want 2", len(requests))
	}
	envelope, body := scoreWireEvent(t, requests[0])
	if got := envelope["timestamp"]; got != "2026-07-19T08:30:00.25Z" {
		t.Fatalf("backdated event timestamp = %v, want 2026-07-19T08:30:00.25Z", got)
	}
	if got := body["configId"]; got != "config-123" {
		t.Fatalf("score payload configId = %v, want config-123", got)
	}

	defaulted, defaultedBody := scoreWireEvent(t, requests[1])
	if _, exists := defaultedBody["configId"]; exists {
		t.Fatalf("score payload configId = %v, want it omitted", defaultedBody["configId"])
	}
	stamp, err := time.Parse(time.RFC3339Nano, defaulted["timestamp"].(string))
	if err != nil {
		t.Fatalf("parse defaulted event timestamp: %v", err)
	}
	if stamp.Before(before.Add(-time.Second)) || stamp.After(time.Now().Add(time.Second)) {
		t.Fatalf("defaulted event timestamp = %v, want approximately the RecordScore call time", stamp)
	}
}

func TestScoreWireStringValueAndObservationTarget(t *testing.T) {
	t.Parallel()
	client, receiver := newScoreWireClient(t, nil)

	tag := "too_short"
	err := client.RecordScore(context.Background(), langfuse.Score{
		Name:          "rating-tag",
		TraceID:       strings.Repeat("ab", 16),
		ObservationID: strings.Repeat("cd", 8),
		StringValue:   &tag,
	})
	if err != nil {
		t.Fatalf("RecordScore() error = %v", err)
	}
	flushClient(t, client)
	requests := receiver.all()
	if len(requests) != 1 {
		t.Fatalf("score request count = %d, want 1", len(requests))
	}
	_, body := scoreWireEvent(t, requests[0])
	if body["value"] != "too_short" || body["traceId"] != strings.Repeat("ab", 16) ||
		body["observationId"] != strings.Repeat("cd", 8) {
		t.Fatalf("score payload = %v, want string value with trace and observation target", body)
	}
	if _, exists := body["dataType"]; exists {
		t.Fatalf("score payload sets dataType %v, want it omitted for inference", body["dataType"])
	}
	if id, _ := body["id"].(string); len(id) != 36 {
		t.Fatalf("score payload id = %v, want a generated 36-character UUID", body["id"])
	}
}

func TestScoreWireGeneratesDistinctIdempotencyIDs(t *testing.T) {
	t.Parallel()
	client, receiver := newScoreWireClient(t, nil)

	rating := 2.0
	for range 2 {
		err := client.RecordScore(context.Background(), langfuse.Score{
			Name: "user-feedback", SessionID: "s", NumericValue: &rating,
		})
		if err != nil {
			t.Fatalf("RecordScore() error = %v", err)
		}
	}
	flushClient(t, client)
	requests := receiver.all()
	if len(requests) != 2 {
		t.Fatalf("score request count = %d, want 2", len(requests))
	}
	firstEnvelope, firstBody := scoreWireEvent(t, requests[0])
	secondEnvelope, secondBody := scoreWireEvent(t, requests[1])
	first, _ := firstBody["id"].(string)
	second, _ := secondBody["id"].(string)
	if len(first) != 36 || len(second) != 36 || first == second {
		t.Fatalf("generated score IDs = (%q, %q), want two distinct UUIDs", first, second)
	}
	if firstEnvelope["id"] == secondEnvelope["id"] {
		t.Fatalf("generated event IDs are both %v, want two distinct UUIDs", firstEnvelope["id"])
	}
}

func TestScoreWireShutdownDrainsPendingScores(t *testing.T) {
	t.Parallel()
	client, receiver := newScoreWireClient(t, nil)

	rating := 5.0
	err := client.RecordScore(context.Background(), langfuse.Score{
		Name: "user-feedback", SessionID: "s", NumericValue: &rating,
	})
	if err != nil {
		t.Fatalf("RecordScore() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("Client.Shutdown() error = %v", err)
	}
	if got := len(receiver.all()); got != 1 {
		t.Fatalf("score request count after shutdown = %d, want 1", got)
	}
}

func TestScoreWireValidationAndLifecycle(t *testing.T) {
	t.Parallel()
	client, receiver := newScoreWireClient(t, nil)
	rating := 1.0
	outsideBooleanRange := 2.0
	stringValue := "value"
	emptyText := ""
	oversizedText := strings.Repeat("t", 501)
	valid := langfuse.Score{Name: "user-feedback", SessionID: "s", NumericValue: &rating}

	invalid := map[string]langfuse.Score{
		"missing name":           {SessionID: "s", NumericValue: &rating},
		"missing target":         {Name: "n", NumericValue: &rating},
		"two targets":            {Name: "n", TraceID: "t", SessionID: "s", NumericValue: &rating},
		"observation sans trace": {Name: "n", SessionID: "s", ObservationID: "o", NumericValue: &rating},
		"no value":               {Name: "n", SessionID: "s"},
		"two values":             {Name: "n", SessionID: "s", NumericValue: &rating, StringValue: new(string)},
		"bad data type":          {Name: "n", SessionID: "s", NumericValue: &rating, DataType: "MOOD"},
		"numeric categorical":    {Name: "n", SessionID: "s", NumericValue: &rating, DataType: langfuse.ScoreTypeCategorical},
		"numeric correction":     {Name: "n", TraceID: "t", NumericValue: &rating, DataType: langfuse.ScoreTypeCorrection},
		"numeric text":           {Name: "n", SessionID: "s", NumericValue: &rating, DataType: langfuse.ScoreTypeText},
		"string numeric":         {Name: "n", SessionID: "s", StringValue: &stringValue, DataType: langfuse.ScoreTypeNumeric},
		"string boolean":         {Name: "n", SessionID: "s", StringValue: &stringValue, DataType: langfuse.ScoreTypeBoolean},
		"boolean outside range":  {Name: "n", SessionID: "s", NumericValue: &outsideBooleanRange, DataType: langfuse.ScoreTypeBoolean},
		"empty text":             {Name: "n", SessionID: "s", StringValue: &emptyText, DataType: langfuse.ScoreTypeText},
		"oversized text":         {Name: "n", SessionID: "s", StringValue: &oversizedText, DataType: langfuse.ScoreTypeText},
		"correction on session":  {Name: "n", SessionID: "s", StringValue: &stringValue, DataType: langfuse.ScoreTypeCorrection},
		"correction with config": {Name: "n", TraceID: "t", StringValue: &stringValue, DataType: langfuse.ScoreTypeCorrection, ConfigID: "c"},
		"oversized name":         {Name: strings.Repeat("n", 201), SessionID: "s", NumericValue: &rating},
		"oversized config ID":    {Name: "n", SessionID: "s", NumericValue: &rating, ConfigID: strings.Repeat("c", 201)},
		"five-digit year":        {Name: "n", SessionID: "s", NumericValue: &rating, Timestamp: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
		"negative year":          {Name: "n", SessionID: "s", NumericValue: &rating, Timestamp: time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	for label, score := range invalid {
		if err := client.RecordScore(context.Background(), score); err == nil {
			t.Fatalf("RecordScore(%s) error = nil, want validation error", label)
		}
	}
	if got := len(receiver.all()); got != 0 {
		t.Fatalf("invalid scores sent %d requests, want 0", got)
	}

	// Transport failures no longer surface through RecordScore: the score is
	// accepted, sent once (a 400 is not retryable), and dropped with a
	// payload-free diagnostic.
	receiver.mu.Lock()
	receiver.status = http.StatusBadRequest
	receiver.mu.Unlock()
	if err := client.RecordScore(context.Background(), valid); err != nil {
		t.Fatalf("RecordScore() with a failing server error = %v, want nil async accept", err)
	}
	flushClient(t, client)
	if got := len(receiver.all()); got != 1 {
		t.Fatalf("failing-server request count = %d, want exactly 1 (no retry on 400)", got)
	}

	disabled, err := langfuse.New(context.Background(), langfuse.Config{Disabled: true})
	if err != nil {
		t.Fatalf("langfuse.New(disabled) error = %v", err)
	}
	if err := disabled.RecordScore(context.Background(), valid); err != nil {
		t.Fatalf("disabled RecordScore() error = %v, want nil no-op", err)
	}
	var nilClient *langfuse.Client
	if err := nilClient.RecordScore(context.Background(), valid); err != nil {
		t.Fatalf("nil client RecordScore() error = %v, want nil no-op", err)
	}

	stopped, stoppedReceiver := newScoreWireClient(t, nil)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stopped.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Client.Shutdown() error = %v", err)
	}
	if err := stopped.RecordScore(context.Background(), valid); err == nil {
		t.Fatal("RecordScore() after shutdown error = nil, want an error")
	}
	if got := len(stoppedReceiver.all()); got != 0 {
		t.Fatalf("stopped client sent %d score requests, want 0", got)
	}
}

func TestScoreWireAcceptsExplicitDataTypeShapes(t *testing.T) {
	t.Parallel()
	client, receiver := newScoreWireClient(t, nil)
	booleanValue := 1.0
	categoricalValue := "helpful"
	textValue := strings.Repeat("t", 500)
	correctionValue := "corrected output"

	valid := map[string]langfuse.Score{
		"boolean": {
			Name: "grounded", SessionID: "s", NumericValue: &booleanValue,
			DataType: langfuse.ScoreTypeBoolean,
		},
		"categorical": {
			Name: "tone", SessionID: "s", StringValue: &categoricalValue,
			DataType: langfuse.ScoreTypeCategorical,
		},
		"text": {
			Name: "notes", SessionID: "s", StringValue: &textValue,
			DataType: langfuse.ScoreTypeText,
		},
		"correction": {
			Name: "output", TraceID: "t", StringValue: &correctionValue,
			DataType: langfuse.ScoreTypeCorrection,
		},
	}
	for label, score := range valid {
		if err := client.RecordScore(context.Background(), score); err != nil {
			t.Fatalf("RecordScore(%s) error = %v", label, err)
		}
	}
	flushClient(t, client)
	if got := len(receiver.all()); got != len(valid) {
		t.Fatalf("valid score request count = %d, want %d", got, len(valid))
	}
}
