# go-langfuse

go-langfuse is an independent, observation-first community Langfuse client for
Go, built on the official OpenTelemetry Go SDK: tracing over OTLP/HTTP
protobuf (Langfuse ingestion version 4), request-scoped trace identity,
scores for evaluations and user feedback, and strict content-privacy
controls. Prompt management, datasets, and administrative APIs are out of
scope; use the Langfuse REST API for those. go-langfuse is not affiliated
with or endorsed by Langfuse.

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
`retriever`, `evaluator`, or `guardrail`. When the work fits one function,
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
an observation failed without ending it. Semantics and limits are detailed in
the [reference](docs/reference.md).

## Scores

`RecordScore` submits evaluations and user feedback through the Langfuse REST
scores endpoint. Unlike observations, scores are synchronous with no buffering
or retry: transport errors return to the caller and a disabled client is a
no-op.

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
| Sampler and resource | Always-sampled; SDK-owned resource | Existing provider remains authoritative |
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
go test ./...
go test -race ./...
go vet ./...
```

The module language version is Go 1.25; `go.mod` records the suggested
patched toolchain. Tests never require Langfuse credentials. Release steps
are documented in [RELEASING.md](RELEASING.md).
