# langfuseopenai

Opt-in Langfuse instrumentation for OpenAI-wire API calls from Go. The
core `github.com/fgn/go-langfuse` module has **no OpenAI dependency**;
install this adapter only when you want it:

```sh
go get github.com/fgn/go-langfuse/contrib/openai
```

This module depends on the core module and the standard library only.
It has no OpenAI SDK dependency either: it observes the documented wire
format at the HTTP transport, so one adapter covers
`sashabaranov/go-openai`, the official `openai-go`, Azure OpenAI
deployments, and OpenAI-compatible endpoints, and your native client
code stays exactly as it is.

## Wiring

```go
httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}

// sashabaranov/go-openai
cfg := openai.DefaultConfig(token)
cfg.HTTPClient = httpClient

// official openai-go
client := openaisdk.NewClient(option.WithHTTPClient(httpClient))
```

Every recognized call now records a generation or embedding
observation, parented by whatever observation is in the request
context, with model, content, token usage, time-to-first-token for
streams, and status.

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
one attempt per call with `option.WithMaxRetries(0)`.

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
(`http 429`, `transport error`) rather than raw error text. Content
larger than the 512 KiB capture cap is omitted entirely, never
truncated. `WithoutContentExport()` keeps usage and model but drops
Input/Output; `WithoutBodyInspection()` prevents body reading
completely. A disabled core client (`LANGFUSE_TRACING_ENABLED=false`)
disables inspection, not only export.

## Statuses

Wire-provable outcomes only: `http <code>` (non-2xx), `incomplete`
(stream ended before its `[DONE]` terminal), `canceled` (causally
observed context cancellation), `closed_early` (body closed before the
protocol finished), and `telemetry_partial` (the adapter could not
parse a successful response; never an application error).
