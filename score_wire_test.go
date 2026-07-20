package langfuse_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
	if status == 0 {
		status = http.StatusOK
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
		SessionID:    "consultation:609",
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
	if request.path != "/api/public/scores" {
		t.Fatalf("score path = %q, want /api/public/scores", request.path)
	}
	if request.contentType != "application/json" {
		t.Fatalf("score content type = %q, want application/json", request.contentType)
	}
	if !request.authOK || request.username != "pk-lf-score-wire" || request.password != "sk-lf-score-wire" {
		t.Fatalf("score basic auth = (%q, ok %v), want the client credentials", request.username, request.authOK)
	}
	want := map[string]any{
		"id":          "feedback-42",
		"name":        "user-feedback",
		"sessionId":   "consultation:609",
		"value":       4.0,
		"dataType":    "NUMERIC",
		"comment":     "clear report",
		"metadata":    map[string]any{"report_id": "7"},
		"environment": "score_wire",
	}
	for key, wantValue := range want {
		got, exists := request.body[key]
		if !exists {
			t.Fatalf("score payload is missing %q; payload: %v", key, request.body)
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
	if len(request.body) != len(want) {
		t.Fatalf("score payload has %d fields, want %d: %v", len(request.body), len(want), request.body)
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
	body := requests[0].body
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
	first, _ := requests[0].body["id"].(string)
	second, _ := requests[1].body["id"].(string)
	if len(first) != 36 || len(second) != 36 || first == second {
		t.Fatalf("generated score IDs = (%q, %q), want two distinct UUIDs", first, second)
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
	valid := langfuse.Score{Name: "user-feedback", SessionID: "s", NumericValue: &rating}

	invalid := map[string]langfuse.Score{
		"missing name":           {SessionID: "s", NumericValue: &rating},
		"missing target":         {Name: "n", NumericValue: &rating},
		"observation sans trace": {Name: "n", SessionID: "s", ObservationID: "o", NumericValue: &rating},
		"no value":               {Name: "n", SessionID: "s"},
		"two values":             {Name: "n", SessionID: "s", NumericValue: &rating, StringValue: new(string)},
		"bad data type":          {Name: "n", SessionID: "s", NumericValue: &rating, DataType: "MOOD"},
		"oversized name":         {Name: strings.Repeat("n", 201), SessionID: "s", NumericValue: &rating},
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
