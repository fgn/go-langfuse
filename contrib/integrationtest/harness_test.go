package integrationtest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/fgn/go-langfuse"
)

// otlpReceiver captures spans exported by the core client.
type otlpReceiver struct {
	server *httptest.Server
	spans  chan *tracepb.Span
}

func newOTLPReceiver(t *testing.T) *otlpReceiver {
	t.Helper()
	receiver := &otlpReceiver{spans: make(chan *tracepb.Span, 64)}
	receiver.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return
		}
		var payload collectortracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &payload); err != nil {
			return
		}
		for _, resourceSpans := range payload.GetResourceSpans() {
			for _, scopeSpans := range resourceSpans.GetScopeSpans() {
				for _, span := range scopeSpans.GetSpans() {
					receiver.spans <- span
				}
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(receiver.server.Close)
	return receiver
}

func (r *otlpReceiver) nextSpan(t *testing.T) *tracepb.Span {
	t.Helper()
	select {
	case span := <-r.spans:
		return span
	case <-time.After(10 * time.Second):
		t.Fatal("no span exported within 10s")
		return nil
	}
}

func attrString(span *tracepb.Span, key string) string {
	for _, attribute := range span.GetAttributes() {
		if attribute.GetKey() == key {
			return attribute.GetValue().GetStringValue()
		}
	}
	return ""
}

func newTestClient(t *testing.T, receiver *otlpReceiver) *langfuse.Client {
	t.Helper()
	client, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL:   receiver.server.URL,
		PublicKey: "pk-lf-test",
		SecretKey: "sk-lf-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(ctx)
	})
	return client
}

func flush(t *testing.T, client *langfuse.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}
