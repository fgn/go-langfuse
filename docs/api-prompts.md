# Prompt authoring and runtime deployment

This unreleased API/prompt milestone is part of draft PR #40. See
[the delivery ledger](gap-closure-status.md) for the required work not yet
implemented, and [API coverage](api-coverage.md) for the exact five operations.
It has mock-server validation, not a verified live Langfuse server version.

## Two independent clients

`github.com/fgn/go-langfuse/api` provides synchronous project HTTP operations.
`api.NewClient` validates configuration without starting workers or constructing
an OpenTelemetry provider. There is no API-client shutdown or flush operation.
The root `langfuse.Client` continues to own runtime prompt caching, observations,
and queued scores; its existing provider-ownership and shutdown rules remain.

```go
management, err := api.NewClient(api.Config{
    BaseURL: os.Getenv("LANGFUSE_BASE_URL"),
    PublicKey: os.Getenv("LANGFUSE_PUBLIC_KEY"),
    SecretKey: os.Getenv("LANGFUSE_SECRET_KEY"),
})
if err != nil {
    return err
}
created, err := management.Prompts.Create(ctx, api.CreatePromptRequest{
    Name: "synthetic/explain",
    Prompt: api.TextContent("Explain {{topic}}."),
    Labels: api.Set([]string{}), // explicitly no deployment labels
    Config: json.RawMessage(`{"temperature":0}`),
    CommitMessage: api.Set("Add explanation template"),
})
if err != nil {
    return err
}
```

The snippets assume a caller context and error-returning function. The complete
compiling workflow is in [examples/promptmanagement](../examples/promptmanagement).
Set an explicit HTTPS endpoint for credential-bearing remote requests; an empty
base URL selects EU Cloud. HTTP is allowed for explicitly configured local or
self-hosted servers, so protecting the connection is the caller's responsibility.
A borrowed HTTP client is shallow-copied, its cookie jar is disabled, redirects
are refused, and its transport must be concurrency-safe and must not replay
writes or inject unrelated credentials.

## Content and field presence

Creating the same name creates an immutable new version; it does not edit the
existing content. A text prompt is a string. A chat prompt is an array of
`api.MessageEntry(role, content)` and `api.PlaceholderEntry(name)` values.
Message-list placeholders remain a distinct variant, not ordinary messages.
`ChatEntry.Extra` preserves additional JSON object fields without overwriting
the variant's discriminator or required fields. `Config` and resolution-graph
JSON retain numeric precision through `json.RawMessage`.

A nil optional field omits its key; `api.Set([]string{})` writes `[]`,
`api.Set("")` writes an empty string, and `api.Null[T]()` writes `null`.
These are not interchangeable. Server semantics determine whether an explicit
null is meaningful. `api.Field[T]` preserves response absence, null, and value.
Authoring calls deliberately send the caller's requested content. Runtime
capture settings and mask callbacks do not sanitize management writes: apply
an explicit content policy before sending prompt content, config, or tags.

`Prompts.Get` accepts a positive version or a label, never both. With neither,
the server selects `production`. `Resolve` set to a pointer to `false` returns raw dependency
tags for one-off export/debugging; it does not alter the runtime cache's
retrieval mode. A file-backed import/export example is still unfinished.
`Prompts.List` fetches one numbered page (default 50, maximum 100), not the whole
project. `api.WalkPages` requires explicit traversal bounds. Generic cursor and
score-value helpers do not imply implemented observation or score-read APIs.

## Deploy, invalidate, verify, and roll back

```go
_, err = management.Prompts.UpdateLabels(ctx, created.Name, created.Version,
    []string{"staging"})
if err != nil {
    return err
}
lf.InvalidatePromptCache(created.Name)
prompt, err := lf.GetPrompt(ctx, created.Name,
    langfuse.PromptQuery{Label: "staging"})
if err != nil {
    return err
}
if prompt.Version != created.Version {
    return errors.New("deployment did not resolve the expected version")
}
// Use prompt.CompileStrict(...) and prompt.Ref() when recording a generation.
```

Label updates add or move the supplied labels; they do not replace the complete
label set. An empty `newLabels` array removes nothing. `latest` is server-managed
and cannot be explicitly assigned by this API. To roll back, move the deployment
label to the previous immutable version, invalidate, and verify that version.

Invalidation removes every cached selector for the exact name on that runtime
client. Old in-flight responses may still reach their original callers, but
cannot repopulate the cache. Subsequent reads wait for an abandoned miss flight
to drain rather than joining it. Normal cancellation, admission limits, and
explicit fallback behavior remain intact. Invalidation performs no network I/O
and is safe on nil, disabled, and shut-down clients.

This is not distributed cache coherence: invalidate each affected runtime
client or allow its configured TTL to expire. Dependency composition does not
build a reverse-invalidation graph. Invalidate each affected parent prompt by
name as well. Management writes do not automatically know which runtime clients
need invalidation.

Deletion requires exactly one explicit selector: `Version`, `Label`, or
`AllVersions: true`. A label selector **deletes matching versions**, not just the
label; it is not a deployment rollback operation. Do not use all-version deletion
for routine workflow cleanup, and invalidate affected clients after deletion.

## Transport and ambiguous writes

The default total deadline is 10 seconds, including retries and response reads.
Success bodies default to an 8 MiB limit, error bodies to 8 KiB, and requests to
8 MiB. Reads make at most three attempts by default, with bounded Retry-After
handling and jittered backoff. Writes are attempted once, with redirects disabled
and request-body replay disabled. A caller-supplied retrying transport is not
supported.

Use `errors.As` to inspect `*api.RequestError` or `*api.ResponseError`.
`OutcomeUnknown` marks an attempted write whose completion is not trustworthy,
such as a canceled request, malformed success response, or server error. Reconcile
the relevant resource before retrying; a timeout does not prove a write failed.
A pre-canceled request is not sent and is not marked ambiguous. `Retryable` is
true only for transient read responses, never for write failures. Error status,
bounded request ID, and truncation metadata are inspectable. Default `fmt`
representations omit response bodies, URLs, headers, and credentials. Explicitly
unwrapping an underlying transport error is outside that formatting guarantee.

## Runnable adoption check

```sh
# No credentials are needed and no writes occur without the flag.
go run ./examples/promptmanagement

# Credential-free local mock: text + chat, staging deployment, generation, rollback.
go test -race -count=1 ./examples/promptmanagement

# Explicitly opt in only with credentials for a disposable Langfuse project.
go run ./examples/promptmanagement -write
```

The opt-in workflow uses a uniquely named synthetic text/chat pair, deploys to
`staging`, records deterministic generations linked to the selected version,
and rolls staging back. It never assigns `production` or deletes prompts. The
synthetic data remains in the disposable project for manual inspection. No model
provider account is needed. Live execution is not part of the reported tests.
