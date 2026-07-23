# langfusegenai

Opt-in Langfuse instrumentation for Gemini API calls from Go (Google AI
Developer API and Vertex AI). The core `github.com/fgn/go-langfuse`
module has **no Google dependency**; install this adapter only when you
want it:

```sh
go get github.com/fgn/go-langfuse/contrib/googlegenai
```

This module adds no provider SDK to your dependency graph: beyond the
core module and its OpenTelemetry dependencies it uses only the
standard library. There is no Google SDK dependency either: it observes
the documented wire format at the HTTP transport, so
`google.golang.org/genai` client code stays exactly as it is.

## What gets recorded

Every recognized call records a generation or embedding observation
with the URL-derived model (overridden by the response
`modelVersion`), prompt/candidate/thought token usage (inclusive
semantics, `toolUsePromptTokenCount` included), sanitized output parts
(text, function calls, executable code; media as placeholders; thought
parts retained marked), finish reasons, and wire-provable status. A
runnable end-to-end example, including the complete Vertex credentials
composition, lives at
[`contrib/integrationtest/examples/vertexgenai`](../integrationtest/examples/vertexgenai/main.go).

## Wiring: Developer API

```go
client, err := genai.NewClient(ctx, &genai.ClientConfig{
	APIKey: apiKey,
	HTTPClient: &http.Client{
		Transport: langfusegenai.NewTransport(lf, nil),
	},
})
```

## Wiring: Vertex AI

genai uses a caller-supplied `HTTPClient` as-is and does not wire your
credentials into it. Compose the authenticated transport explicitly
with the public `cloud.google.com/go/auth/httptransport` API and layer
the Langfuse transport outside it. Starting from your existing client
preserves its `Timeout`, `CheckRedirect`, cookie jar, and transport
policy:

```go
base := app.HTTPClient // your policy client; may be plain &http.Client{}
baseRT := base.Transport
if baseRT == nil {
	baseRT = http.DefaultTransport
}
authed, err := httptransport.NewClient(&httptransport.Options{
	Credentials:      creds, // cloud.google.com/go/auth
	BaseRoundTripper: baseRT,
})
if err != nil { ... }
client := *base
client.Transport = langfusegenai.NewTransport(lf, authed.Transport)

gemini, err := genai.NewClient(ctx, &genai.ClientConfig{
	Backend:     genai.BackendVertexAI,
	Project:     projectID,
	Location:    location,
	Credentials: creds,
	HTTPClient:  &client,
})
```

The Authorization header is added by the inner auth transport after the
Langfuse layer runs, so the adapter never sees credentials; the
adapter reads no request headers in any composition. Token refresh time
counts toward the attempt's duration; the refresh request itself is not
observed.

## Scope (v0.1)

| Route | Observation | Streaming |
| --- | --- | --- |
| `generateContent` | generation | no |
| `streamGenerateContent` | generation | yes (SSE) |
| `embedContent`, `batchEmbedContents` | embedding | no |
| `predict` (Vertex embedding models) | embedding | no |

`countTokens` and file/cache management routes pass through
unobserved. The model is extracted from the URL (bare models, tuned
models, and any Vertex publisher); a response `modelVersion` overrides
it. Gemini streams have no terminal sentinel: clean EOF completes the
observation, and `finishReason` values are recorded as metadata, never
used to end the stream early (Gemini can send usage and further
content after a `STOP`). Thought parts are reasoning, not output: they
are marked in exported content but do not stamp time-to-first-token,
which measures the first user-visible output delta.

## Attempts, retries, metrics, and privacy

Identical to the OpenAI adapter: one observation per HTTP attempt;
group logical operations under a span-typed observation without
duplicating model or usage; `ContextWithCall` attaches names, prompt
links, and metadata; the core Mask/capture controls govern all content;
media parts (`inlineData`, `fileData`) are replaced with placeholders
during parsing; `generationConfig` exports through a numeric/boolean
allowlist only; retained structured parts are byte-accounted against
the same capture cap as text, and oversized streaming events are
discarded whole with `telemetry_partial`. See the [OpenAI adapter README](../openai/README.md)
for the shared semantics and status vocabulary.
