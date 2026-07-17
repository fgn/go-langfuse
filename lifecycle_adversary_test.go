package langfuse

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
	"github.com/fgn/go-langfuse/internal/otlpreceiver"
)

func TestBorrowedProviderOnStartCanReenterClientShutdown(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	client := newInteropClient(t, receiver, Config{TracerProvider: provider})

	reentrant := &shutdownOnStartProcessor{client: client}
	provider.RegisterSpanProcessor(reentrant)
	startReturned := make(chan error, 1)
	go func() {
		_, observation := client.StartObservation(
			context.Background(),
			"reentrant-on-start",
			TypeSpan,
			ObservationAttributes{},
		)
		observation.End()
		startReturned <- reentrant.err
	}()

	select {
	case err := <-startReturned:
		if err != nil {
			t.Fatalf("reentrant Shutdown() error = %v", err)
		}
		shutdownProvider(t, provider)
	case <-time.After(250 * time.Millisecond):
		t.Error("StartObservation did not return when a borrowed-provider OnStart callback re-entered Client.Shutdown")
	}
}

func TestUpdateAfterEndDiagnosticCanReenterClientShutdown(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	_, observation := client.StartObservation(
		context.Background(),
		"ended-before-update",
		TypeSpan,
		ObservationAttributes{},
	)
	observation.End()

	handler := newShutdownDiagnosticHandler(client, "update ignored after observation end")
	restoreOTelErrorHandler(t, handler)
	updateReturned := make(chan struct{})
	go func() {
		observation.Update(ObservationAttributes{Output: "ignored"})
		close(updateReturned)
	}()

	select {
	case <-updateReturned:
	case <-time.After(250 * time.Millisecond):
		t.Error("Update did not return when its after-End diagnostic handler re-entered Client.Shutdown")
		return
	}
	if !handler.wasInvoked() {
		t.Fatal("after-End Update did not invoke the expected diagnostic handler")
	}
	if err := handler.shutdownError(); err != nil {
		t.Fatalf("reentrant Shutdown() error = %v", err)
	}
}

func TestDroppedAttributeOnEndDiagnosticCanReenterClientShutdown(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	limits := sdktrace.NewSpanLimits()
	limits.AttributeCountLimit = 1
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithRawSpanLimits(limits),
	)
	client := newInteropClient(t, receiver, Config{TracerProvider: provider})
	_, observation := client.StartObservation(
		context.Background(),
		"dropped-attribute-diagnostic",
		TypeSpan,
		ObservationAttributes{Input: "forces-an-additional-attribute"},
	)

	handler := newShutdownDiagnosticHandler(client, "exceeded the tracer provider's span attribute limits")
	restoreOTelErrorHandler(t, handler)
	endReturned := make(chan struct{})
	go func() {
		observation.End()
		close(endReturned)
	}()

	select {
	case <-endReturned:
		if !handler.wasInvoked() {
			t.Fatal("dropped-attribute OnEnd did not invoke the expected diagnostic handler")
		}
		if err := handler.shutdownError(); err != nil {
			t.Fatalf("reentrant Shutdown() error = %v", err)
		}
		shutdownProvider(t, provider)
	case <-time.After(250 * time.Millisecond):
		t.Error("Observation.End did not return when its dropped-attribute diagnostic handler re-entered Client.Shutdown")
	}
}

func TestShutdownDeadlineIsNotBlockedByBackgroundFlush(t *testing.T) {
	server := newBlockingOTLPServer()
	t.Cleanup(server.close)
	client, err := New(context.Background(), Config{
		BaseURL:   server.URL(),
		PublicKey: "pk-stalled-flush",
		SecretKey: "sk-stalled-flush",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, observation := client.StartObservation(
		context.Background(),
		"stalled-flush",
		TypeSpan,
		ObservationAttributes{},
	)
	observation.End()

	flushReturned := make(chan error, 1)
	go func() { flushReturned <- client.Flush(context.Background()) }()
	select {
	case <-server.requestArrived:
	case <-time.After(time.Second):
		t.Fatal("background Flush never reached the blocking OTLP server")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	shutdownReturned := make(chan error, 1)
	started := time.Now()
	go func() { shutdownReturned <- client.Shutdown(shutdownCtx) }()

	var shutdownErr error
	returnedPromptly := false
	select {
	case shutdownErr = <-shutdownReturned:
		returnedPromptly = true
	case <-time.After(250 * time.Millisecond):
	}
	server.unblock()

	select {
	case <-flushReturned:
	case <-time.After(time.Second):
		t.Fatal("background Flush did not return after the OTLP server was unblocked")
	}
	if !returnedPromptly {
		select {
		case shutdownErr = <-shutdownReturned:
		case <-time.After(time.Second):
			t.Fatal("Shutdown remained blocked after the OTLP server was unblocked")
		}
		t.Errorf("Shutdown with a 20ms deadline waited %v for an unrelated background Flush", time.Since(started))
	}
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Errorf("Shutdown() error = %v, want context deadline exceeded", shutdownErr)
	}
}

func TestInvalidUTF8TraceAttributesAreOmittedAndOTLPRemainsValid(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })
	captureRootDiagnostics(t)

	invalid := string([]byte{'b', 'a', 'd', 0xff})
	ctx := client.WithTraceAttributes(context.Background(), TraceAttributes{
		Name:      invalid,
		UserID:    "valid-user",
		SessionID: invalid,
		Tags:      []string{invalid, "valid-tag"},
		Metadata: map[string]any{
			invalid:     "invalid-key",
			"bad-value": invalid,
			"valid":     "metadata-value",
		},
		Version: invalid,
	})
	_, observation := client.StartObservation(ctx, "valid-observation", TypeSpan, ObservationAttributes{})
	observation.End()
	flushClient(t, client)

	span := interopSpanMap(t, receiver)["valid-observation"]
	if span == nil {
		t.Fatal("valid observation was not exported")
	}
	assertInteropMissingAttribute(t, span, lfattr.TraceNameKey)
	assertInteropStringAttribute(t, span, lfattr.TraceUserIDKey, "valid-user")
	assertInteropMissingAttribute(t, span, lfattr.TraceSessionIDKey)
	assertInteropStringSliceAttribute(t, span, lfattr.TraceTagsKey, []string{"valid-tag"})
	assertInteropMissingAttribute(t, span, lfattr.TraceMetadataKey+".bad-value")
	assertInteropStringAttribute(t, span, lfattr.TraceMetadataKey+".valid", "metadata-value")
	assertInteropMissingAttribute(t, span, lfattr.VersionKey)
	for _, item := range span.GetAttributes() {
		if !utf8.ValidString(item.GetKey()) {
			t.Fatalf("exported invalid UTF-8 attribute key %q", item.GetKey())
		}
		assertValidUTF8AnyValue(t, item.GetValue())
	}
}

func TestObservationEndProcessorCanReenterEnd(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	client := newInteropClient(t, receiver, Config{TracerProvider: provider})
	reentrant := &reenterObservationOnEndProcessor{}
	provider.RegisterSpanProcessor(reentrant)
	_, observation := client.StartObservation(context.Background(), "reentrant-observation-end", TypeSpan, ObservationAttributes{})
	reentrant.observation = observation

	returned := make(chan struct{})
	go func() {
		observation.End()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Observation.End held its mutex across a re-entrant processor callback")
	}
	shutdownClient(t, client)
	shutdownProvider(t, provider)
}

type shutdownOnStartProcessor struct {
	client *Client
	once   sync.Once
	err    error
}

func (p *shutdownOnStartProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {
	p.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		p.err = p.client.Shutdown(ctx)
	})
}

func (*shutdownOnStartProcessor) OnEnd(sdktrace.ReadOnlySpan) {}

func (*shutdownOnStartProcessor) ForceFlush(context.Context) error { return nil }

func (*shutdownOnStartProcessor) Shutdown(context.Context) error { return nil }

type reenterObservationOnEndProcessor struct{ observation *Observation }

func (*reenterObservationOnEndProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}

func (p *reenterObservationOnEndProcessor) OnEnd(sdktrace.ReadOnlySpan) {
	p.observation.End()
}

func (*reenterObservationOnEndProcessor) ForceFlush(context.Context) error { return nil }

func (*reenterObservationOnEndProcessor) Shutdown(context.Context) error { return nil }

type shutdownDiagnosticHandler struct {
	client *Client
	match  string

	once    sync.Once
	invoked chan struct{}
	result  chan error
}

func newShutdownDiagnosticHandler(client *Client, match string) *shutdownDiagnosticHandler {
	return &shutdownDiagnosticHandler{
		client:  client,
		match:   match,
		invoked: make(chan struct{}),
		result:  make(chan error, 1),
	}
}

func (h *shutdownDiagnosticHandler) Handle(err error) {
	if err == nil || !strings.Contains(err.Error(), h.match) {
		return
	}
	h.once.Do(func() {
		close(h.invoked)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		h.result <- h.client.Shutdown(ctx)
	})
}

func (h *shutdownDiagnosticHandler) wasInvoked() bool {
	select {
	case <-h.invoked:
		return true
	default:
		return false
	}
}

func (h *shutdownDiagnosticHandler) shutdownError() error {
	select {
	case err := <-h.result:
		return err
	default:
		return errors.New("diagnostic handler did not finish its reentrant Shutdown call")
	}
}

func restoreOTelErrorHandler(t *testing.T, handler otel.ErrorHandler) {
	t.Helper()
	previous := otel.GetErrorHandler()
	otel.SetErrorHandler(handler)
	t.Cleanup(func() { otel.SetErrorHandler(previous) })
}

type blockingOTLPServer struct {
	server         *httptest.Server
	requestArrived chan struct{}
	release        chan struct{}
	arrivedOnce    sync.Once
	releaseOnce    sync.Once
}

func newBlockingOTLPServer() *blockingOTLPServer {
	result := &blockingOTLPServer{
		requestArrived: make(chan struct{}),
		release:        make(chan struct{}),
	}
	result.server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
		result.arrivedOnce.Do(func() { close(result.requestArrived) })
		<-result.release
		response.Header().Set("Content-Type", "application/x-protobuf")
		response.WriteHeader(http.StatusOK)
	}))
	return result
}

func (s *blockingOTLPServer) URL() string { return s.server.URL }

func (s *blockingOTLPServer) unblock() { s.releaseOnce.Do(func() { close(s.release) }) }

func (s *blockingOTLPServer) close() {
	s.unblock()
	s.server.Close()
}

func assertValidUTF8AnyValue(t *testing.T, value *commonpb.AnyValue) {
	t.Helper()
	if value == nil {
		return
	}
	if text, ok := value.GetValue().(*commonpb.AnyValue_StringValue); ok && !utf8.ValidString(text.StringValue) {
		t.Fatalf("exported invalid UTF-8 attribute value %q", text.StringValue)
	}
	if array := value.GetArrayValue(); array != nil {
		for _, item := range array.GetValues() {
			assertValidUTF8AnyValue(t, item)
		}
	}
	if list := value.GetKvlistValue(); list != nil {
		for _, item := range list.GetValues() {
			if !utf8.ValidString(item.GetKey()) {
				t.Fatalf("exported invalid UTF-8 key/value-list key %q", item.GetKey())
			}
			assertValidUTF8AnyValue(t, item.GetValue())
		}
	}
}
