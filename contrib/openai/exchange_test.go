package langfuseopenai_test

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/fgn/go-langfuse"
	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// innerApplication starts an always-sampled application span from the
// context the base transport receives and reports that span's actual parent.
func innerApplication(t *testing.T, parent *oteltrace.SpanContext) http.RoundTripper {
	t.Helper()
	inner := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = inner.Shutdown(context.Background()) })
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		_, span := inner.Tracer("inner-http").Start(r.Context(), "inner-http")
		*parent = span.(sdktrace.ReadOnlySpan).Parent()
		span.End()
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(chatResponse)),
			Request:    r,
		}, nil
	})
}

func exchange(t *testing.T, lf *langfuse.Client, ctx context.Context, base http.RoundTripper) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.invalid/v1/chat/completions",
		strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"q"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := langfuseopenai.NewTransport(lf, base).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	drainAndClose(t, resp)
	flush(t, lf)
}

type dropAll struct{}

func (dropAll) ShouldSample(sdktrace.SamplingParameters) sdktrace.SamplingResult {
	return sdktrace.SamplingResult{Decision: sdktrace.Drop}
}
func (dropAll) Description() string { return "dropAll" }

// wrappedSpan reports its own provider while sharing the wrapped span's pipeline.
type wrappedSpan struct{ oteltrace.Span }

type wrapperProvider struct{ embedded.TracerProvider }

func (wrapperProvider) Tracer(string, ...oteltrace.TracerOption) oteltrace.Tracer { return nil }

func (wrappedSpan) TracerProvider() oteltrace.TracerProvider { return wrapperProvider{} }

func TestExchangeKeepsAnApplicationParentFromAnotherProvider(t *testing.T) {
	remote := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: oteltrace.TraceID{1}, SpanID: oteltrace.SpanID{2}, TraceFlags: oteltrace.FlagsSampled, Remote: true,
	})
	cases := map[string]func(t *testing.T) (context.Context, oteltrace.SpanContext){
		"kept": func(t *testing.T) (context.Context, oteltrace.SpanContext) {
			t.Helper()
			return applicationSpan(t, sdktrace.AlwaysSample(), false)
		},
		"dropped": func(t *testing.T) (context.Context, oteltrace.SpanContext) {
			t.Helper()
			return applicationSpan(t, dropAll{}, false)
		},
		"ended": func(t *testing.T) (context.Context, oteltrace.SpanContext) {
			t.Helper()
			return applicationSpan(t, sdktrace.AlwaysSample(), true)
		},
		"remote only": func(*testing.T) (context.Context, oteltrace.SpanContext) {
			return oteltrace.ContextWithRemoteSpanContext(context.Background(), remote), remote
		},
		"wrapper reporting its own provider": func(t *testing.T) (context.Context, oteltrace.SpanContext) {
			t.Helper()
			ctx, sc := applicationSpan(t, sdktrace.AlwaysSample(), false)
			return oteltrace.ContextWithSpan(ctx, wrappedSpan{oteltrace.SpanFromContext(ctx)}), sc
		},
	}
	for name, start := range cases {
		t.Run(name, func(t *testing.T) {
			receiver := newOTLPReceiver(t)
			lf := newTestClient(t, receiver, nil)
			ctx, want := start(t)
			var parent oteltrace.SpanContext
			exchange(t, lf, ctx, innerApplication(t, &parent))
			if parent.TraceID() != want.TraceID() || parent.SpanID() != want.SpanID() {
				t.Fatalf("inner application span parent = %v, want the application parent %v", parent, want)
			}
			if span := receiver.nextSpan(t); span.GetName() != "openai.chat.completions" {
				t.Fatalf("generation %q not recorded", span.GetName())
			}
		})
	}
}

func applicationSpan(t *testing.T, sampler sdktrace.Sampler, ended bool) (context.Context, oteltrace.SpanContext) {
	t.Helper()
	application := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	ctx, span := application.Tracer("application").Start(context.Background(), "request")
	if ended {
		span.End()
	} else {
		t.Cleanup(func() { span.End() })
	}
	return ctx, span.SpanContext()
}

func TestExchangeUsesTheGenerationForTheSameProviderOrNoParent(t *testing.T) {
	t.Run("borrowed", func(t *testing.T) {
		receiver := newOTLPReceiver(t)
		application := sdktrace.NewTracerProvider()
		t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
		lf := newTestClient(t, receiver, func(cfg *langfuse.Config) { cfg.TracerProvider = application })
		ctx, request := application.Tracer("application").Start(context.Background(), "request")
		defer request.End()

		var parent oteltrace.SpanContext
		exchange(t, lf, ctx, innerApplication(t, &parent))
		generation := receiver.nextSpan(t)
		if parent.SpanID().String() != spanIDHex(generation.GetSpanId()) {
			t.Fatalf("inner application span parent = %v, want the generation", parent)
		}
	})
	t.Run("spanless", func(t *testing.T) {
		receiver := newOTLPReceiver(t)
		lf := newTestClient(t, receiver, nil)
		var parent oteltrace.SpanContext
		exchange(t, lf, context.Background(), innerApplication(t, &parent))
		if generation := receiver.nextSpan(t); parent.SpanID().String() != spanIDHex(generation.GetSpanId()) {
			t.Fatalf("inner application span parent = %v, want the generation", parent)
		}
	})
}

func spanIDHex(id []byte) string {
	var spanID oteltrace.SpanID
	copy(spanID[:], id)
	return spanID.String()
}

// TestExchangeWithGlobalPlaceholderSpans runs in a child process because it
// installs the global tracer provider.
func TestExchangeWithGlobalPlaceholderSpans(t *testing.T) {
	if os.Getenv("LANGFUSEOPENAI_GLOBAL_PROBE") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestExchangeWithGlobalPlaceholderSpans$", "-test.count=1")
		cmd.Env = append(os.Environ(), "LANGFUSEOPENAI_GLOBAL_PROBE=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("global provider probe failed: %v\n%s", err, out)
		}
		return
	}
	tracer := otel.Tracer("application")
	remote := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: oteltrace.TraceID{3}, SpanID: oteltrace.SpanID{4}, TraceFlags: oteltrace.FlagsSampled, Remote: true,
	})
	beforeCtx, before := tracer.Start(oteltrace.ContextWithRemoteSpanContext(context.Background(), remote), "before-setup")
	defer before.End()

	application := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	otel.SetTracerProvider(application)
	afterCtx, after := tracer.Start(context.Background(), "after-setup")
	defer after.End()

	contexts := map[string]context.Context{
		"placeholder before setup": beforeCtx, "delegated after setup": afterCtx, "borrowed after setup": afterCtx,
	}
	for name, tc := range map[string]struct {
		borrowed bool
		want     oteltrace.SpanContext
	}{
		"placeholder before setup": {false, before.SpanContext()},
		"delegated after setup":    {false, after.SpanContext()},
		"borrowed after setup":     {true, oteltrace.SpanContext{}},
	} {
		receiver := newOTLPReceiver(t)
		lf := newTestClient(t, receiver, func(cfg *langfuse.Config) {
			if tc.borrowed {
				cfg.TracerProvider = application
			}
		})
		var parent oteltrace.SpanContext
		exchange(t, lf, contexts[name], innerApplication(t, &parent))
		generation := receiver.nextSpan(t)
		if tc.borrowed {
			if parent.SpanID().String() != spanIDHex(generation.GetSpanId()) {
				t.Fatalf("%s: inner parent = %v, want the generation", name, parent)
			}
			continue
		}
		if parent.SpanID() != tc.want.SpanID() {
			t.Fatalf("%s: inner parent = %v, want the application span %v", name, parent, tc.want)
		}
	}
}
