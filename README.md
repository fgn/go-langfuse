# go-langfuse

go-langfuse is an independent, observation-first community Langfuse client for
Go, built on the official OpenTelemetry Go SDK. It sends traces using OTLP over
HTTP/protobuf and always uses Langfuse ingestion version 4. go-langfuse is not
affiliated with or endorsed by Langfuse.

<!-- PRE_RELEASE_WARNING: remove in the release-preparation PR before v0.1.0. -->
This module is under active development. The public API below is the intended
v0.1 contract; do not use it in production until a tagged release is available.

## Install

```sh
go get github.com/fgn/go-langfuse
```

Set the project credentials from **Langfuse -> Settings -> API Keys**:

```sh
export LANGFUSE_PUBLIC_KEY=pk-lf-...
export LANGFUSE_SECRET_KEY=sk-lf-...
export LANGFUSE_BASE_URL=https://cloud.langfuse.com
```

`LANGFUSE_BASE_URL` may point at Langfuse Cloud or a self-hosted instance. The
client normalizes it to `/api/public/otel/v1/traces`, uses HTTP Basic
authentication, and sends `x-langfuse-ingestion-version: 4` on every request.
Path-prefixed reverse-proxy base URLs (for example
`https://gw.example.com/langfuse/api/public/otel`) are not supported in v0.1;
use a host root, `/api/public/otel`, or the full traces endpoint.

## Quickstart

<!-- README_QUICKSTART_BEGIN -->
```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fgn/go-langfuse"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		return fmt.Errorf("create Langfuse client: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = lf.Shutdown(shutdownCtx)
	}()

	ctx = lf.WithTraceAttributes(ctx, langfuse.TraceAttributes{
		Name: "chat-turn", UserID: "user-123", SessionID: "conversation-456",
		Tags: []string{"chat"},
	})

	question := "What is context in Go?"
	rootCtx, root := lf.StartObservation(ctx, "chat-turn", langfuse.TypeAgent,
		langfuse.ObservationAttributes{Input: question})
	defer root.End()

	messages := []string{question}
	generationCtx, generation := lf.StartObservation(rootCtx, "generate-answer",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{
			Model: "gemini-2.5-flash", Input: messages,
		})
	defer generation.End()

	answer, usage, err := callModel(generationCtx, messages)
	if err != nil {
		generation.RecordError(err)
		root.RecordError(err)
		return err
	}

	generation.Update(langfuse.ObservationAttributes{Output: answer, Usage: &usage})
	root.Update(langfuse.ObservationAttributes{Output: answer})
	return nil
}

// Replace this stub with a provider SDK call and pass ctx to that SDK.
func callModel(ctx context.Context, messages []string) (string, langfuse.Usage, error) {
	if err := ctx.Err(); err != nil {
		return "", langfuse.Usage{}, err
	}
	return "Context carries cancellation and request-scoped values.", langfuse.Usage{
		InputTokens: int64(len(messages) * 6), OutputTokens: 8,
	}, nil
}
```
<!-- README_QUICKSTART_END -->

The complete runnable version is in
[`examples/quickstart`](examples/quickstart/main.go).
Additional compiled examples cover [streaming](examples/streaming/main.go) and
[short-lived jobs, events, masking, disabled mode, and flushing](examples/shortlived/main.go).

Three rules prevent most tracing mistakes:

1. Pass the context returned by `StartObservation` to child work, and keep
   parent contexts in distinct variables as the example does with `rootCtx`
   and `generationCtx`. Go context is the parent-child relationship; there is
   no package-global current observation.
2. End an observation only after its work is complete. For streaming model
   calls, consume the stream before ending the generation.
3. End observations before flushing or shutting down, and use a fresh timeout
   context rather than a canceled request context for lifecycle calls.

## Observations

Every operation uses the same API and differs only by its observation type:

```go
childCtx, observation := lf.StartObservation(
	parentCtx,
	"retrieve-documents",
	langfuse.TypeRetriever,
	langfuse.ObservationAttributes{Input: query},
)
defer observation.End()

documents, err := retrieve(childCtx, query)
if err != nil {
	observation.RecordError(err)
	return err
}
observation.Update(langfuse.ObservationAttributes{Output: documents})
```

When the work fits one function, prefer `Observe`: the callback receives the
child context, the observation always ends (a panic is marked as a
payload-free failure before it propagates), and a returned error is recorded
on the observation and passed through unchanged:

```go
err := lf.Observe(parentCtx, "retrieve-documents", langfuse.TypeRetriever,
	langfuse.ObservationAttributes{Input: query},
	func(ctx context.Context, observation *langfuse.Observation) error {
		documents, err := retrieve(ctx, query)
		if err != nil {
			return err // recorded via RecordError automatically
		}
		observation.Update(langfuse.ObservationAttributes{Output: documents})
		return nil
	})
```

Use `StartObservation` directly when an observation's lifetime cannot be
scoped to one function, such as ending a generation only after a stream is
fully consumed.

Supported types are `span`, `generation`, `event`, `embedding`, `agent`,
`tool`, `chain`, `retriever`, `evaluator`, and `guardrail`. `Event` is a
shortcut that creates and immediately ends a point-in-time event.

For generations and embeddings, keep the model, input, output, usage, cost,
and completion-start time on the same observation. `Usage` accepts inclusive
provider totals: input includes cache tokens and output includes reasoning
tokens. The client normalizes them to Langfuse's exclusive `usage_details`
buckets.

`Update` merges non-zero fields and never clears a value. It is safe to call
`Update`, `RecordError`, and `End` concurrently; `End` is idempotent. Calls made
after an observation has ended are ignored. Each observation retains at most
eight `RecordError` exception events; further errors are omitted with one
payload-free diagnostic.

## Scores

`RecordScore` submits evaluations and user feedback through the Langfuse REST
scores endpoint using the client's credentials and environment. Unlike
observations, scores are synchronous with no buffering or retry: transport
errors return to the caller, a disabled client is a no-op, and a shut-down
client returns an error.

```go
rating := float64(feedback.Rating)
err := lf.RecordScore(ctx, langfuse.Score{
	ID:           "feedback-" + feedback.ID, // idempotent upsert key
	Name:         "user-feedback",
	SessionID:    "conversation-456",        // or TraceID / TraceID+ObservationID
	NumericValue: &rating,
	Comment:      feedback.Text,
})
```

A score targets a trace, a session, or an observation within a trace; exactly
one of `NumericValue` or `StringValue` must be set, and `DataType` is inferred
by Langfuse when omitted. The serialized score is limited to 128 KiB, and
`Comment` is explicit content that does not pass through `Config.Mask`.

## Trace attributes and context

Call `WithTraceAttributes` near the start of request handling. It stores the
fields in the returned context, applies them to the client's active
observation if its context is current, and stamps them on spans subsequently
started from that context: trace name, user ID, session ID, tags, metadata,
and version. It does not retroactively rewrite an already-started third-party
span.

The client preserves a valid parent `SpanContext`, including a parent created
by another provider, so ordinary W3C trace IDs continue across services and
backends. Langfuse trace attributes are different: v0.1 does **not** place
user, session, tags, metadata, or version in W3C baggage. Those values and the
internal application-root claim propagate only through the local Go context.
Consequently, a downstream service can correctly continue the trace ID while
still being shown as a separate Langfuse application root.

Limits, applied to SDK-authored observations (not third-party spans from a
borrowed provider):

- Scalar trace values and tags: 200 characters. Environment names: at most 40
  characters of lowercase letters, numbers, `_`, or `-`, not starting with
  `langfuse`.
- Trace and observation metadata: 32 top-level keys each, keys up to 200
  bytes. `Usage.Details`: 64 buckets with the same key limit. Tags: caller
  order, at most 64 unique values and 16 KiB per trace context.
- Each JSON-serialized input, output, metadata value, model-parameter map, or
  cost map: 1 MiB. Direct text (names, model names, versions, prompts, status
  messages): 16 KiB. `RecordError` replaces invalid UTF-8 or text over 64 KiB
  with `"error"`.
- Observation payload attributes: 2 MiB in aggregate; lower-priority fields
  over the budget are omitted with a payload-free diagnostic. One OTLP request
  is capped at 4 MiB, with automatic splitting described under
  [Buffering and backpressure](#buffering-and-backpressure).
- Serialization rejects nesting beyond 100 levels using a bounded structural
  preflight. Caller-provided `Mask`, `MarshalJSON`, and `MarshalText` are
  trusted callbacks: their output is still rejected above 1 MiB, but work
  inside the callback cannot be bounded by the SDK.

## Provider ownership

The default mode creates an isolated SDK tracer provider. It exports
observations created through this client and never changes the global OTel
provider:

```go
lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
```

If the application already owns an `*sdktrace.TracerProvider`, attach the
client as another processor:

```go
cfg := langfuse.ConfigFromEnv()
cfg.TracerProvider = existingProvider
lf, err := langfuse.New(ctx, cfg)
```

See the [existing OpenTelemetry guide](docs/existing-opentelemetry.md) and
[`examples/existingotel`](examples/existingotel/main.go) for the complete
lifecycle.

| Behavior | Isolated provider | Borrowed provider |
| --- | --- | --- |
| Provider owner | SDK client | Application |
| SDK observations | Exported | Exported |
| Selected third-party AI spans | Not observed | Exported by the SDK's Langfuse smart filter |
| Sampler and resource | Always-sampled; SDK-owned resource | Existing provider remains authoritative |
| Span limits | Fixed SDK-safe limits; ambient `OTEL_SPAN_*` ignored | Caller limits remain authoritative |
| `Client.Shutdown` | Stops owned provider resources | Stops and unregisters only the SDK's processor |
| Global OTel provider | Never replaced | Never replaced |

Notes for borrowed mode:

- Langfuse annotations (environment, release, propagated trace attributes) are
  written onto shared spans at start, so the application's other exporters see
  them too. The SDK never removes or suppresses spans for those exporters.
- SDK observation scopes carry the project public key so each processor can
  reject another project's SDK spans; other exporters see that identifier as
  well. The secret key is never attached to telemetry.
- The caller owns span limits. If they are unusually low, OpenTelemetry may
  drop SDK fields; the client emits a payload-free diagnostic when it detects
  that on an SDK observation.
- Batching matches isolated mode, including the 4 MiB request splitting. The
  SDK does not copy or sanitize third-party attributes and events; their size
  and marshaling cost remain the caller's trust boundary.
- Only one active client per borrowed provider: a duplicate `New` reports a
  warning through the OTel error handler and returns a true no-op client that
  exports nothing and does not release the first attachment. Use isolated
  providers for multiple projects.

## Content and sensitive data

The SDK never inspects function arguments, HTTP bodies, or model clients and
captures no provider content automatically. It does export fields explicitly
supplied by the caller. Input/output are the obvious content fields, but
metadata, model parameters, status messages, and errors can also contain
sensitive data.

Set `LANGFUSE_CONTENT_CAPTURE_ENABLED=false`, or configure
`DisableContentCapture`, to drop SDK-supplied `Input` and `Output` while still
recording every other field. The privacy boundary is deliberately narrow:

| Data source | Dropped by `DisableContentCapture` | Passed to `Mask` |
| --- | --- | --- |
| `ObservationAttributes.Input` and `Output` | Yes | Yes, unless content capture is disabled |
| `ObservationAttributes.Metadata` | No | Yes, once as the complete `map[string]any` |
| `TraceAttributes.Metadata` | No | Yes, once as the complete `map[string]any` |
| Observation name/type, trace name, user/session IDs, tags, version, level, `StatusMessage`, model/parameters, usage, costs, prompt, and completion time | No | No |
| `RecordError(err)` text and exception event | No | No |
| OpenTelemetry resource attributes (`resource.Default`/`OTEL_RESOURCE_ATTRIBUTES` in isolated mode; caller resource in borrowed mode) | No | No |
| Third-party OTel span attributes and events | No | No |

Disabling content capture does not make metadata, model parameters, status
messages, or errors safe. `RecordError` exports `err.Error()` as the OTel
status description, Langfuse status message, and exception-event message. Use
payload-free error values or sanitize an error before passing it to
`RecordError`; never put credentials, PHI, prompts, or completions in an error
or `StatusMessage`.

`Mask` can transform only the SDK values shown in the table. A metadata masker
must return a `map[string]any`; returning another type omits that metadata.
Copy and recursively redact maps and slices rather than mutating caller-owned
data:

```go
cfg := langfuse.ConfigFromEnv()
cfg.DisableContentCapture = true
cfg.Mask = redactSDKValue

func redactSDKValue(value any) any {
	switch value := value.(type) {
	case string:
		return strings.ReplaceAll(value, "secret", "[redacted]")
	case map[string]any:
		redacted := make(map[string]any, len(value))
		for key, item := range value {
			switch strings.ToLower(key) {
			case "email", "customer_id", "authorization":
				redacted[key] = "[redacted]"
			default:
				redacted[key] = redactSDKValue(item)
			}
		}
		return redacted
	case []any:
		redacted := make([]any, len(value))
		for index, item := range value {
			redacted[index] = redactSDKValue(item)
		}
		return redacted
	default:
		return value
	}
}
```

The example assumes JSON-like `map[string]any` and `[]any` shapes. A
production masker must cover every concrete value type the application
supplies, must be concurrency-safe, and should have tests proving its
redaction policy.

These controls apply **only to data supplied through this client**; they never
rewrite third-party OTel instrumentation, so configure or sanitize those
instrumentors independently in borrowed mode (the client warns when content
capture is disabled there). Resource attributes are also untouched: isolated
mode preserves `resource.Default` (including `OTEL_SERVICE_NAME` and
`OTEL_RESOURCE_ATTRIBUTES`) and borrowed mode preserves the caller's resource,
so audit them before export.

## Flush and shutdown

Long-running services should end in-flight observations and then call
`Shutdown` during graceful termination. `Shutdown` flushes ended observations.
Short-lived jobs and serverless handlers can call `Flush` before returning if
the client must remain usable.

```go
shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := lf.Shutdown(shutdownCtx); err != nil {
	log.Printf("flush Langfuse telemetry: %v", err)
}
```

In borrowed mode, shut down the Langfuse client before the application's
tracer provider; the client never shuts down unrelated processors or
exporters. Create a fresh timeout context for each `Flush` or `Shutdown` call,
since a reused deadline keeps running. Repeated and concurrent lifecycle calls
are safe: the first `Shutdown` owns teardown and later calls return without
starting another.

## Configuration

`ConfigFromEnv` reads:

| Variable | Purpose |
| --- | --- |
| `LANGFUSE_PUBLIC_KEY` | Project public key |
| `LANGFUSE_SECRET_KEY` | Project secret key |
| `LANGFUSE_BASE_URL` | Cloud or self-hosted base URL |
| `LANGFUSE_TRACING_ENVIRONMENT` | Environment stamped on observations |
| `LANGFUSE_RELEASE` | Application release stamped on observations |
| `LANGFUSE_TRACING_ENABLED` | Set to `false` for a complete no-op client |
| `LANGFUSE_CONTENT_CAPTURE_ENABLED` | Set to `false` to drop SDK input/output |

Export buffering is tuned only through `Config`; it has no environment
variables:

| Config field | Purpose |
| --- | --- |
| `MaxQueueSize` | Bounds ended spans buffered for export. `0` selects the default 2048; negative values fail validation in `New` |
| `BlockOnQueueFull` | Opts into blocking backpressure when the queue is full instead of the default drop-on-full |

For an isolated provider, an empty `Config.ServiceName` preserves the standard
OpenTelemetry resource (including `OTEL_SERVICE_NAME`). Set `ServiceName` only
when an explicit SDK-local override is desired. A borrowed provider always
keeps the application's resource.

`New` validates configuration without a network request; a disabled client
needs no credentials and every operation is a safe no-op. Runtime exporter
failures go to the standard OTel error handler, so observation calls never
turn telemetry failures into application failures; `Flush` and `Shutdown`
return lifecycle errors to the caller.

Generic `OTEL_EXPORTER_OTLP_*`, `OTEL_BSP_*`, and `OTEL_SPAN_*` variables are
intentionally ignored by the isolated transport and provider; they often
configure an application's separate telemetry backend. HTTPS uses Go's system
trust configuration and the client follows standard
`HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` behavior. Use HTTPS whenever credentials
leave a trusted host; plain HTTP remains available for local development, but
Basic authentication does not encrypt credentials by itself.

### Buffering and backpressure

Ended observations wait in a bounded in-memory queue (2048 spans by default)
and are exported in batches of up to 512 spans, at the latest every 5 seconds.
When the queue is full, for example during a sustained Langfuse outage, newly
ended observations are dropped rather than blocking application work, matching
OpenTelemetry defaults. `Config.MaxQueueSize` resizes the queue and
`Config.BlockOnQueueFull` opts into backpressure; choose blocking only when
delivery matters more than latency, because an export outage can then stall
goroutines that end observations. One OTLP/HTTP request is capped at 4 MiB
before compression; larger batches are split across requests, and only a span
that alone exceeds the cap is dropped with a diagnostic.

## Sampling and current limitations

- Isolated mode always samples its own SDK observations, including children
  of an unsampled foreign parent, while retaining the foreign trace and parent
  IDs. In borrowed mode, spans rejected by the application's sampler cannot be
  recovered by the SDK. Smart filtering is an export selection step, not a
  sampler.
- The default smart filter exports SDK observations, spans with `gen_ai.*`
  attributes, known LLM instrumentation scopes, and required application roots.
  Unrelated HTTP, database, and logging spans are excluded by default.
- Filtering cannot reconstruct an unexported parent. Application-root detection
  follows the current official SDK's start-time, direct-parent expectation;
  late-added AI attributes and filtered middle spans can therefore create an
  additional application root.
- Trace attributes and the internal application-root claim are local-context
  only in v0.1. They are not baggage and do not cross process boundaries.
- Input, output, metadata, and model values must be JSON-serializable and are
  subject to the per-field and aggregate limits documented above plus the
  caller's OTel span limits. Invalid, cyclic, unsupported, or oversized fields
  are omitted and diagnosed without including their payload.
- Batch export improves application latency but cannot survive an abrupt
  process exit. Graceful shutdown is required.
- Custom filters, export-all mode, multiple projects on one provider, prompt
  fetching, datasets, and administrative APIs are outside v0.1.

## Development

The module language version is Go 1.25 and the repository suggests the current
patched Go 1.25.12 toolchain for reproducible security checks.

```sh
go test ./...
go test -race ./...
go vet ./...
```

Supported protocol and server combinations are tracked in the
[compatibility matrix](docs/compatibility.md).
The opt-in live gate and release checklist are documented in
[RELEASING.md](RELEASING.md); ordinary tests never require credentials.
