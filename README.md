# go-langfuse

go-langfuse is an independent, observation-first community Langfuse client for
Go, built on the official OpenTelemetry Go SDK: tracing over OTLP/HTTP
protobuf (Langfuse ingestion version 4), all ten observation types,
request-scoped trace identity, scores for evaluations and user feedback, and
strict content-privacy controls. Prompt retrieval, caching, compilation, and
trace linking are included; datasets and administrative APIs remain out of
scope. Use the Langfuse REST API for those.
go-langfuse is not affiliated with or endorsed by Langfuse.

Observation types are first-class: alongside `span`, `generation`, and
`event`, go-langfuse supports the typed observations Langfuse introduced in
2025 — `agent`, `tool`, `chain`, `retriever`, `evaluator`, `embedding`, and
`guardrail` — with the same semantics as `as_type` in the Python SDK (v3.3+)
and `asType` in the JS/TS SDK (v4+). These types give traces clearer,
filterable structure in the Langfuse UI.

go-langfuse follows semantic versioning. Until v1.0, minor releases may
contain documented breaking changes; patch releases are always backward
compatible.

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

`LANGFUSE_BASE_URL` may point at Langfuse Cloud or a self-hosted instance as a
host root, `/api/public/otel`, or the full traces endpoint. Path-prefixed
reverse-proxy base URLs are not supported in v0.1.

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

Runnable examples: [quickstart](examples/quickstart/main.go),
[streaming](examples/streaming/main.go),
[existing OpenTelemetry provider](examples/existingotel/main.go), and
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
`span`, `generation`, `event`, `embedding`, `agent`, `tool`, `chain`,
`retriever`, `evaluator`, or `guardrail` — the full set defined by the
current Langfuse platform ([observation
types](https://langfuse.com/docs/observability/features/observation-types)).
When the work fits one function,
prefer `Observe`: the callback receives the child context, the observation
always ends (a panic is marked as a payload-free failure before it
propagates), and a returned error is recorded and passed through unchanged:

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
scoped to one function, as the quickstart does. `Event` records a
point-in-time event, `Update` merges non-zero fields, and `RecordError` marks
an observation failed without ending it. Instrumentation that records
already-finished work can reproduce the observed timeline with
`ObservationAttributes.StartTime`, `CompletionStartTime`, and `EndAt`. For
work that outlives its request — a goroutine that keeps running after the
handler returned — clear the parent span context with the standard
OpenTelemetry helper (`oteltrace.ContextWithSpanContext(ctx,
oteltrace.SpanContext{})`) so the work becomes a new trace root with the
propagated user and session intact, instead of a child of an already-ended
request span; the [reference](docs/reference.md) shows the full pattern along
with semantics and limits.

## Scores

`RecordScore` submits evaluations and user feedback through the Langfuse JSON
ingestion endpoint. A score is validated synchronously — every returned error
means the score was not accepted — and then delivered asynchronously with
bounded retry using the same backoff defaults as observation export, so a
Langfuse blip neither blocks the request path nor loses feedback to one
failed attempt. `Flush` and `Shutdown`
drain accepted scores; a delivery that outlives the retry budget is dropped
with a payload-free diagnostic through the OpenTelemetry error handler. When
`ID` is empty the SDK generates one, keeping retried deliveries idempotent. A
disabled client is a no-op. `Timestamp` backdates a score — a nightly
evaluation job can stamp feedback with the time of the scored interaction —
and `ConfigID` binds it to a Langfuse score config for server-side
validation.

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

## Prompts

`GetPrompt` loads a prompt version from Langfuse prompt management with
client-side caching: a fresh cache hit is a local read, an expired entry is
served immediately while one background refresh runs (stale-while-revalidate),
and concurrent cache misses share a single fetch. `PromptQuery.Type` rejects a
server or cached prompt with the wrong shape, resolving to `Fallback` when one
is supplied or `ErrPromptTypeMismatch` otherwise. `Prompt.Source` distinguishes
server fetches, fresh cache hits, stale cache hits, and local fallbacks through
`PromptSourceServer`, `PromptSourceCache`, `PromptSourceStale`, and
`PromptSourceFallback`.

`Fallback` pins a local prompt for fetch and type failures, so prompt loading
never becomes a hard runtime dependency. `GetPrompt` is nil-safe: an optional,
disabled, or shut-down client returns the fallback without requiring a nil
guard. `Compile` remains lenient, while `CompileStrict` reports unresolved
variables, values that cannot be stringified, and unfilled chat placeholders.
`DecodeConfig` applies prompt config to a caller-defaulted target, and `Ref()`
links only server-backed prompt versions to generations:

```go
prompt, err := lf.GetPrompt(ctx, "response-template", langfuse.PromptQuery{
	Type:     langfuse.PromptTypeText,
	Fallback: &langfuse.PromptFallback{Text: "Process {{input}} concisely."},
})
if err != nil {
	return err
}
compiled, err := prompt.CompileStrict(map[string]any{"input": input})
if err != nil {
	return err
}
_ = lf.Observe(ctx, "generate-response", langfuse.TypeGeneration,
	langfuse.ObservationAttributes{Input: compiled.Text, Prompt: prompt.Ref()},
	generate)
```

Selection defaults to the `production` label; `Version` or `Label` pin others.
Caching, bounds, and failure semantics are detailed in the
[reference](docs/reference.md).

## Sampling

In isolated mode the client samples whole traces deterministically by trace
ID. `Config.SampleRate` (or `LANGFUSE_SAMPLE_RATE`) sets the default
fraction, and `WithSampleRate` overrides it per context path, so one process
can keep every trace from a critical path while exporting only a fraction of
high-volume routine work. Set the rate once per request, before the first
observation; the decision is then inherited by every observation in that
trace:

```go
ctx = lf.WithSampleRate(ctx, 0.02) // keep 2% of a high-volume path
ctx, root := lf.StartObservation(ctx, "generate-answer", langfuse.TypeGeneration,
	langfuse.ObservationAttributes{Input: prompt})
```

`TraceSampledAt` exposes the same deterministic predicate for correlated
application-level sampling. Because smaller fractions select subsets of
larger ones, gating an expensive LLM-judge evaluation at 2% guarantees every
evaluated trace was also kept for export when the trace fraction is at least
2% (kept, not delivered: export still requires ending observations and a
graceful shutdown):

```go
keep, err := langfuse.TraceSampledAt(root.TraceID(), 0.02)
if err == nil && keep && root.Sampled() {
	verdict := judge(ctx, output)
	_ = lf.RecordScore(ctx, langfuse.Score{
		Name: "judge", TraceID: root.TraceID(), NumericValue: &verdict,
	})
}
```

Sampled-out observations keep their IDs, become cheap no-ops, and suppress
scores recorded directly on their own context path so sampled-out traces do
not accumulate orphaned scores. In borrowed mode the application's sampler
remains authoritative and these controls are ignored with a diagnostic. The
[reference](docs/reference.md) details the decision scope and score
semantics.

## Provider modes

The default mode creates an isolated SDK tracer provider that exports only
observations created through this client:

```go
lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
```

If the application already owns an `*sdktrace.TracerProvider`, attach the
client as another processor; a smart filter then also exports third-party AI
spans (`gen_ai.*` attributes and known LLM instrumentation scopes):

```go
cfg := langfuse.ConfigFromEnv()
cfg.TracerProvider = existingProvider
lf, err := langfuse.New(ctx, cfg)
```

| Behavior | Isolated provider | Borrowed provider |
| --- | --- | --- |
| Provider owner | SDK client | Application |
| SDK observations | Exported | Exported |
| Selected third-party AI spans | Not observed | Exported by the SDK's smart filter |
| Sampler and resource | Deterministic trace sampling (default: keep everything); SDK-owned resource | Existing provider remains authoritative |
| Span limits | Fixed SDK-safe limits; ambient `OTEL_SPAN_*` ignored | Caller limits remain authoritative |
| `Client.Shutdown` | Stops owned provider resources | Stops and unregisters only the SDK's processor |
| Global OTel provider | Never replaced | Never replaced |

Neither mode ever changes the global OpenTelemetry provider. Borrowed-mode
lifecycle, annotation visibility, and the one-client-per-provider rule are
covered in the [existing OpenTelemetry guide](docs/existing-opentelemetry.md).

## Content and sensitive data

The SDK never inspects function arguments, HTTP bodies, or model clients; it
exports only fields explicitly supplied by the caller.
`LANGFUSE_CONTENT_CAPTURE_ENABLED=false` drops SDK-supplied input and output,
and `Config.Mask` transforms input, output, and metadata before export.
Identifiers, model data, status messages, error text, and third-party spans
sit outside both controls; the exact boundary and a masker example are in the
[privacy guide](docs/privacy.md).

## Documentation

- [Configuration and behavior reference](docs/reference.md): environment
  variables, buffering and backpressure, flush/shutdown, limits, sampling,
  and current limitations
- [Privacy guide](docs/privacy.md): the content-capture and masking boundary
- [Existing OpenTelemetry guide](docs/existing-opentelemetry.md): borrowed
  provider lifecycle
- [Compatibility matrix](docs/compatibility.md): protocol and server support
- [Langfuse documentation](https://langfuse.com/docs)

## Development

```sh
task ci
```

This checks formatting and module tidiness, runs static analysis, compiles the
examples and README quickstart, and runs the test, fuzz-smoke, and vulnerability
suites. Run `task format` to apply source and module formatting.

The module language version is Go 1.25; `go.mod` records the suggested
patched toolchain. Tests never require Langfuse credentials. Release steps
are documented in [RELEASING.md](RELEASING.md).
