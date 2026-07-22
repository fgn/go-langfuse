# go-langfuse

[![Go Reference](https://pkg.go.dev/badge/github.com/fgn/go-langfuse.svg)](https://pkg.go.dev/github.com/fgn/go-langfuse)
[![CI](https://github.com/fgn/go-langfuse/actions/workflows/ci.yml/badge.svg)](https://github.com/fgn/go-langfuse/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

An independent, observation-first community Langfuse client for Go, built on
the official OpenTelemetry Go SDK. Not affiliated with or endorsed by
Langfuse.

- **One small API for everything you trace.** Every operation is an
  observation. Only its type differs: `span`, `generation`, `event`,
  `agent`, `tool`, `chain`, `retriever`, `evaluator`, `embedding`, or
  `guardrail`, the full set the Langfuse platform defines.
- **OpenTelemetry-native.** Exports OTLP/HTTP protobuf (Langfuse ingestion
  version 4). Owns an isolated tracer provider, or attaches to yours and
  also exports third-party `gen_ai.*` spans. Never touches global OTel
  state.
- **Scores and prompts included.** Evaluations and user feedback with
  asynchronous retried delivery; prompt management reads with caching,
  compilation, and guaranteed-availability fallbacks.
- **Deterministic trace sampling.** Per-request rates in one process, and a
  pure predicate for correlated app-level sampling such as gating an
  expensive LLM-judge evaluation to a subset of the traces kept for export.
- **Strict content privacy.** Exports only what you explicitly supply; a
  capture kill-switch and a masking hook cover input, output, and metadata.
- **Safe by default.** Nil and disabled clients are true no-ops, zero
  values are safe, lifecycle calls are idempotent, and telemetry failures
  never become application failures.

Datasets and administrative APIs are out of scope; use the Langfuse REST
API for those. go-langfuse follows semantic versioning: until v1.0, minor
releases may contain documented breaking changes; patch releases are always
backward compatible.

## Install

```sh
go get github.com/fgn/go-langfuse
```

Set the project credentials from **Langfuse -> Settings -> API Keys**:

```sh
export LANGFUSE_PUBLIC_KEY=pk-lf-...
export LANGFUSE_SECRET_KEY=sk-lf-...
export LANGFUSE_BASE_URL=https://cloud.langfuse.com  # or self-hosted
```

`LANGFUSE_BASE_URL` accepts a host root, `/api/public/otel`, or the full
traces endpoint. Path-prefixed reverse-proxy base URLs are not supported.

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

Three rules prevent most tracing mistakes:

1. **Pass the returned context to child work.** Go context is the
   parent-child relationship; keep parent contexts in distinct variables as
   the example does with `rootCtx` and `generationCtx`.
2. **End an observation only after its work is complete.** For streaming
   model calls, consume the stream before ending the generation.
3. **End observations before flushing or shutting down**, and give
   lifecycle calls a fresh timeout context, not a canceled request context.

More runnable examples: [quickstart](examples/quickstart/main.go),
[streaming](examples/streaming/main.go),
[prompt management](examples/prompts/main.go),
[scores](examples/scores/main.go),
[sampling](examples/sampling/main.go),
[existing OpenTelemetry provider](examples/existingotel/main.go), and
[short-lived jobs, events, masking, disabled mode, and flushing](examples/shortlived/main.go).
The main entry points have runnable examples on
[pkg.go.dev](https://pkg.go.dev/github.com/fgn/go-langfuse).

## Observations

Everything you trace uses the same two calls; only the
[observation type](https://langfuse.com/docs/observability/features/observation-types)
differs. Prefer `Observe` when the work fits one function. The callback
receives the child context, and the observation always ends, even on a
panic, which is marked as a payload-free failure before it propagates. A
returned error is recorded and passed through unchanged:

```go
err := lf.Observe(ctx, "retrieve-documents", langfuse.TypeRetriever,
	langfuse.ObservationAttributes{Input: query},
	func(ctx context.Context, o *langfuse.Observation) error {
		documents, err := retrieve(ctx, query)
		if err != nil {
			return err // recorded automatically
		}
		o.Update(langfuse.ObservationAttributes{Output: documents})
		return nil
	})
```

Use `StartObservation` when a lifetime spans functions, as the quickstart
does; `Event` records a point in time. `Update` merges non-zero fields,
`RecordError` marks a failure without ending, and
`StartTime`/`CompletionStartTime`/`EndAt` reproduce an already-observed
timeline when instrumenting after the fact. For background work that
outlives its request, clear the span context with the standard
OpenTelemetry helper so the work becomes a new trace that keeps the
propagated user and session; the [reference](docs/reference.md) shows the
full pattern.

## Scores

`RecordScore` submits evaluations and user feedback. Validation is
synchronous, so every returned error means the score was not accepted.
Delivery is asynchronous with bounded retry, and `Flush`/`Shutdown` drain
accepted scores:

```go
rating := float64(feedback.Rating)
err := lf.RecordScore(ctx, langfuse.Score{
	ID:           "feedback-" + feedback.ID, // idempotent upsert key
	Name:         "user-feedback",
	SessionID:    sessionID, // or TraceID / TraceID+ObservationID
	NumericValue: &rating,
	Comment:      feedback.Text,
})
```

The SDK generates the upsert ID when `ID` is empty, so retried deliveries
cannot create duplicates. `Timestamp` backdates a score from a later
evaluation job, and `ConfigID` binds it to a Langfuse score config. The
[scores example](examples/scores/main.go) records session, observation, and
trace scores in one runnable program.

## Prompts

`GetPrompt` loads prompt-management prompts with client-side caching. Fresh
hits are local reads. Expired entries are served stale while one background
refresh runs, and concurrent misses share a single fetch. A `Fallback`
makes prompt loading safe to depend on. It also covers nil, disabled, and
shut-down clients, so optional observability needs no guards:

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

Selection defaults to the `production` label; `Version` or `Label` pin
others. `Compile` is lenient, `CompileStrict` reports unresolved variables,
`DecodeConfig` applies prompt config to a caller-defaulted struct, and
`Ref()` links only server-backed versions to generations.
`Prompt.Source` distinguishes server, cache, stale, and fallback results.
The [prompts example](examples/prompts/main.go) runs this flow end to end.

## Sampling

In isolated mode the client samples whole traces deterministically by trace
ID. `Config.SampleRate` (or `LANGFUSE_SAMPLE_RATE`) sets the default
fraction; `WithSampleRate` overrides it per request, so one process can
keep every trace from a critical path while exporting a fraction of
high-volume routine work:

```go
ctx = lf.WithSampleRate(ctx, 0.02) // keep 2% of a high-volume path
ctx, root := lf.StartObservation(ctx, "generate-answer", langfuse.TypeGeneration,
	langfuse.ObservationAttributes{Input: prompt})
```

Set the rate once per request, before the first observation; the whole
trace is then kept or dropped together. Because smaller fractions select
subsets of larger ones, `TraceSampledAt` can gate an expensive LLM-judge
evaluation at 2% with the guarantee that every evaluated trace was also
kept for export:

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
scores recorded on their own context path so dropped traces do not
accumulate orphaned scores. In borrowed mode the application's sampler
remains authoritative. Decision scope and score semantics are in the
[reference](docs/reference.md); the
[sampling example](examples/sampling/main.go) shows both gates on a
simulated high-volume route.

## Provider modes

The default mode creates an isolated SDK tracer provider that exports only
observations created through this client:

```go
lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
```

If the application already owns an `*sdktrace.TracerProvider`, attach the
client as another processor; a smart filter then also exports third-party
AI spans (`gen_ai.*` attributes and known LLM instrumentation scopes):

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
lifecycle and the one-client-per-provider rule are covered in the
[existing OpenTelemetry guide](docs/existing-opentelemetry.md).

## Content and sensitive data

The SDK never inspects function arguments, HTTP bodies, or model clients;
it exports only fields explicitly supplied by the caller.
`LANGFUSE_CONTENT_CAPTURE_ENABLED=false` drops SDK-supplied input and
output, and `Config.Mask` transforms input, output, and metadata before
export. Identifiers, model data, status messages, error text, and
third-party spans sit outside both controls; the exact boundary and a
masker example are in the [privacy guide](docs/privacy.md).

## Documentation

- [API reference and examples on pkg.go.dev](https://pkg.go.dev/github.com/fgn/go-langfuse)
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

This checks formatting and module tidiness, runs static analysis, compiles
the examples and README quickstart, and runs the test, fuzz-smoke, and
vulnerability suites. Run `task format` to apply source and module
formatting. The module language version is Go 1.25; `go.mod` records the
suggested patched toolchain. Tests never require Langfuse credentials.
Release steps are documented in [RELEASING.md](RELEASING.md).
