# langfuseopenai

Opt-in Langfuse instrumentation for OpenAI-wire API calls from Go. The
core `github.com/fgn/go-langfuse` module has **no OpenAI dependency**;
install this adapter only when you want it:

```sh
go get github.com/fgn/go-langfuse/contrib/openai
```

This module adds no provider SDK to your dependency graph: beyond the
core module and its OpenTelemetry dependencies it uses only the
standard library. There is no OpenAI SDK dependency either: it observes
the documented wire format at the HTTP transport, so one adapter covers
`sashabaranov/go-openai`, the official `openai-go`, Azure OpenAI
deployments, and OpenAI-compatible endpoints, and your native client
code stays exactly as it is.

## Wiring

```go
httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}

// official openai-go
client := openai.NewClient(
	option.WithAPIKey(key),
	option.WithHTTPClient(httpClient),
	option.WithMaxRetries(0), // optional: one observation per logical call
)

// sashabaranov/go-openai
cfg := gopenai.DefaultConfig(token)
cfg.HTTPClient = httpClient
```

Every recognized call now records a generation or embedding
observation, parented by whatever observation is in the request
context. Without the adapter, each of these fields is code you write
and maintain by hand for every provider call site:

| Field | Source |
| --- | --- |
| Observation name and type | route (`openai.chat.completions` -> generation) |
| Model | response `model` (validated); request model as metadata |
| Token usage | `usage`, including cached and reasoning token detail, and the usage chunk OpenAI sends after the finish chunk |
| Input / output | request messages and response content, media replaced by placeholders, tool calls as distinct structured calls |
| Time-to-first-token | first semantic output delta of a stream |
| Status | wire-provable only: `http <code>`, `incomplete`, `canceled`, `closed_early`, `telemetry_partial` |
| Metadata | provider, route, API version, finish reason, HTTP status, `azure.deployment` |

Runnable end-to-end examples (working without OpenAI credentials via
built-in synthetic servers):
[official `openai-go` streaming chat](../integrationtest/examples/openaichat/main.go)
and [`sashabaranov/go-openai` streaming chat](../integrationtest/examples/sashabaranovchat/main.go).

## Scope (v0.1)

| Route | Observation | Streaming |
| --- | --- | --- |
| `/chat/completions` | generation | yes (SSE) |
| `/completions` | generation | yes (SSE) |
| `/embeddings` | embedding | no |

The Responses API and multipart routes (audio, files) pass through
unobserved; Responses support is planned. Azure deployment names are
recorded as `azure.deployment` metadata, never as the model; the model
prefers the response's `model` field.

## Attempts, retries, and metrics

The adapter records **one observation per HTTP attempt** (one adapter
invocation): SDK-level retries and redirect hops are separate attempts,
while transparent `net/http` connection-loss replays fold into one.
Group attempts under a logical operation by wrapping the call in a
span-typed observation, and keep model and usage off that wrapper so
generation metrics stay attempt-level and are never double-counted:

```go
err := lf.Observe(ctx, "answer-question", langfuse.TypeSpan,
	langfuse.ObservationAttributes{},
	func(ctx context.Context, _ *langfuse.Observation) error {
		response, err := client.CreateChatCompletion(ctx, request)
		...
	})
```

Clients that retry internally (official `openai-go`) can be pinned to
one attempt per call with `option.WithMaxRetries(0)`. With retries
enabled, note that openai-go abandons a failed attempt's response body
without closing it; the adapter's safety net still finalizes and
exports that failed attempt (with its `http <code>` status) at the
next garbage collection, and closes the leaked body.

Per-call attributes, including prompt links, come from the context:

```go
ctx = langfuseopenai.ContextWithCall(ctx, langfuseopenai.CallAttributes{
	Prompt: prompt.Ref(),
})
```

## Privacy

Importing this adapter is the opt-in to wire inspection; the core
module continues to inspect nothing. Everything recorded flows through
the core controls: `Config.Mask`, `LANGFUSE_CONTENT_CAPTURE_ENABLED`,
sampling, and payload limits apply unchanged. The adapter never reads
request headers, never exports any header, replaces multimodal media
parts with placeholders during parsing, restricts `ModelParameters` to
a numeric/boolean allowlist, and reports errors as fixed categories
(`http 429`, `transport error`) rather than raw error text. One
documented exception to the Mask boundary exists: the response's model
string, validated against a strict shape (128 chars,
`[A-Za-z0-9._:/-]`), is promoted to the observation's model field
because Langfuse model-based pricing requires it; request-body models
are never promoted and travel as Mask-governed metadata. Content
larger than the 512 KiB capture cap is omitted entirely, never
truncated; individual streaming events are additionally bounded at
256 KiB, and an oversized event is discarded whole (including any
usage fields inside it) with a `telemetry_partial` warning while
framing and terminal detection continue. `WithoutContentExport()` keeps usage and model but drops
Input/Output; `WithoutBodyInspection()` prevents body reading
completely. A disabled core client (`LANGFUSE_TRACING_ENABLED=false`)
disables inspection, not only export.

## Statuses

Wire-provable outcomes only: `http <code>` (non-2xx), `incomplete`
(stream ended before its `[DONE]` terminal), `canceled` (causally
observed context cancellation), `closed_early` (body closed before the
protocol finished), and `telemetry_partial` (the adapter could not
parse a successful response; never an application error).
