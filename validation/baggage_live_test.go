//go:build validation

package validation

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	langfuse "github.com/fgn/go-langfuse"
	"go.opentelemetry.io/otel/propagation"
)

// TestBaggageCrossProcessLive drives a producer-to-consumer HTTP hop
// between two real clients of the same Langfuse project and reads the
// result back through the public API: one trace holds both spans with
// the propagated attributes, and the unique session marker matches
// exactly one trace, so an orphaned second trace cannot hide.
func TestBaggageCrossProcessLive(t *testing.T) {
	r := newRun(t)
	consumerClient, err := langfuse.New(context.Background(), langfuse.ConfigFromEnv())
	if err != nil {
		t.Fatalf("create consumer client: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = consumerClient.Shutdown(ctx)
	})

	marker := r.marker
	userID := "baggage-user-" + marker
	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{})

	consumeName := "baggage-consume-" + marker
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		ctx := propagator.Extract(request.Context(), propagation.HeaderCarrier(request.Header))
		ctx = consumerClient.WithTraceAttributesFromBaggage(ctx)
		_, observation := consumerClient.StartObservation(ctx, consumeName,
			langfuse.TypeSpan, langfuse.ObservationAttributes{})
		observation.End()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	produceName := "baggage-produce-" + marker
	ctx := r.lf.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
		Name:      produceName,
		UserID:    userID,
		SessionID: marker,
		Metadata:  map[string]any{"baggage_marker": marker},
	})
	ctx = r.lf.WithBaggagePropagation(ctx)
	rootCtx, root := r.lf.StartObservation(ctx, produceName,
		langfuse.TypeSpan, langfuse.ObservationAttributes{})
	request, err := http.NewRequestWithContext(rootCtx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	propagator.Inject(rootCtx, propagation.HeaderCarrier(request.Header))
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("cross-process request: %v", err)
	}
	_ = response.Body.Close()
	root.End()

	flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.lf.Flush(flushCtx); err != nil {
		t.Fatalf("flush producer: %v", err)
	}
	if err := consumerClient.Flush(flushCtx); err != nil {
		t.Fatalf("flush consumer: %v", err)
	}

	// Both spans land in ONE trace, correlated by the producer root's ID.
	r.observation(t, root.TraceID(), produceName)
	r.observation(t, root.TraceID(), consumeName)

	// Trace-level attributes reflect the propagated state.
	trace := fetchTraceDocument(t, r, root.TraceID())
	if trace.UserID != userID {
		t.Errorf("trace userId = %q, want %q", trace.UserID, userID)
	}
	if trace.SessionID != marker {
		t.Errorf("trace sessionId = %q, want %q", trace.SessionID, marker)
	}

	// Exactly one trace carries the unique marker: a second trace would
	// mean the claim or parentage was lost on the hop.
	traceIDs := tracesBySession(t, r, marker)
	if len(traceIDs) != 1 || traceIDs[0] != root.TraceID() {
		t.Errorf("traces for session %s = %v, want exactly [%s]", marker, traceIDs, root.TraceID())
	}
}

type traceDocument struct {
	ID        string `json:"id"`
	UserID    string `json:"userId"`
	SessionID string `json:"sessionId"`
}

func fetchTraceDocument(t *testing.T, r *run, traceID string) traceDocument {
	t.Helper()
	body := apiGET(t, r, "/api/public/traces/"+traceID)
	var document traceDocument
	if err := json.Unmarshal(body, &document); err != nil {
		t.Fatalf("decode trace document: %v", err)
	}
	return document
}

func tracesBySession(t *testing.T, r *run, sessionID string) []string {
	t.Helper()
	body := apiGET(t, r, "/api/public/traces?sessionId="+sessionID+"&limit=50")
	var response struct {
		Data []traceDocument `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode trace list: %v", err)
	}
	ids := make([]string, 0, len(response.Data))
	for _, item := range response.Data {
		ids = append(ids, item.ID)
	}
	return ids
}

func apiGET(t *testing.T, r *run, path string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.baseURL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.SetBasicAuth(r.publicKey, r.secretKey)
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d, err %v, body %s", path, response.StatusCode, err, body)
	}
	return body
}
