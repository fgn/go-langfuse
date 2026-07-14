# langfuse-go

`langfuse-go` adds Langfuse export to an OpenTelemetry setup you already own.
It does not create a tracer provider, replace global OpenTelemetry state, or
introduce a separate client lifecycle.

The public integration surface is deliberately small: `Config` and
`NewSpanProcessor`.

## Add it to an existing provider

The following is the complete, compiled example from
`examples/existingotel/telemetry.go`. Call it once during startup.

<!-- quickstart:start -->
```go
package existingotel

import (
	"context"
	"os"

	langfuse "github.com/fgn/langfuse-go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// AddLangfuse attaches Langfuse to the application's existing provider.
// Call it once during startup; provider.Shutdown flushes both existing
// processors and Langfuse.
func AddLangfuse(
	ctx context.Context,
	provider *sdktrace.TracerProvider,
) error {
	processor, err := langfuse.NewSpanProcessor(ctx, langfuse.Config{
		BaseURL:   os.Getenv("LANGFUSE_BASE_URL"),
		PublicKey: os.Getenv("LANGFUSE_PUBLIC_KEY"),
		SecretKey: os.Getenv("LANGFUSE_SECRET_KEY"),
	})
	if err != nil {
		return err
	}

	provider.RegisterSpanProcessor(processor)
	return nil
}
```
<!-- quickstart:end -->

At application shutdown, call `TracerProvider.Shutdown` as usual. It flushes
both your existing exporter and Langfuse. For an explicit checkpoint, call
`TracerProvider.ForceFlush`. Do not close the Langfuse processor separately.

Configuration is explicit: this package does not read environment variables.
The example chooses to read them in the application. If you are constructing a
new provider, pass the processor with `sdktrace.WithSpanProcessor` instead.

`BaseURL` accepts a Langfuse host root, `/api/public/otel`, or the full
`/api/public/otel/v1/traces` endpoint. An empty `BaseURL` uses the EU cloud at
`https://cloud.langfuse.com`.

## What is exported

The Langfuse processor exports the same focused categories as the official
Python and TypeScript SDKs:

- spans with `langfuse.observation.type`;
- spans with standard `gen_ai.*` attributes;
- spans emitted by known OpenTelemetry LLM instrumentations.

Your existing processors still see every sampled span. Sampling, resources,
service identity, global registration, and shutdown remain application-owned.

## Existing OpenAI and Gemini hooks

Keep existing OpenAI or Gemini OTel instrumentation unchanged. When its spans
use `gen_ai.*` semantic-convention attributes or a recognized LLM
instrumentation scope, the same provider sends them to Langfuse. This package
does not replace HTTP transports, wrap model clients, or create another tracer.

Spans are selected when they end, so attributes recorded while consuming a
stream are preserved. The instrumentor must end its span only after the stream
has been fully consumed or closed; this package does not alter span lifetime.

For a manual span, use OTel's GenAI semantic conventions. This is the complete,
compiled `examples/manualspan/chat.go` example:

<!-- manualspan:start -->
```go
package manualspan

import (
	"context"

	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
)

// StartChat starts a manual GenAI span using only OTel semantic conventions.
// End the returned span after the response stream has been fully consumed.
func StartChat(
	ctx context.Context,
	tracer trace.Tracer,
	model string,
) (context.Context, trace.Span) {
	return tracer.Start(ctx, "chat", trace.WithAttributes(
		semconv.GenAIOperationNameChat,
		semconv.GenAIProviderNameOpenAI,
		semconv.GenAIRequestModel(model),
	))
}
```
<!-- manualspan:end -->

The processor does not capture prompts or responses by itself. Content reaches
Langfuse only when your instrumentation explicitly records it.

Langfuse trace-level fields such as user, session, and tags must be recorded on
every exported span. Keep using your application's existing OTel context or
baggage-to-attribute processor for that propagation; this package does not add
a second propagation system.

## Configuration errors

Construction fails immediately for missing credentials or an invalid base URL.
Runtime export failures use the standard OpenTelemetry error handler, and
`TracerProvider.ForceFlush` provides a synchronous error checkpoint. The
official Go OTLP/HTTP exporter owns protobuf encoding, HTTP, TLS, proxy support,
timeouts, and retries.
