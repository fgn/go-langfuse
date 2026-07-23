# Real-provider validation

Slow, deliberate, credentialed verification that the adapters record
real provider behavior correctly, judged by reading traces back
through the Langfuse public API with each provider SDK's own response
as ground truth. No provider mocks in the smoke and parity tests. This
module is excluded from the released Go modules, absent from go.work,
and none of its packages are loaded or executed by `task ci`; every
file carries the `validation` build tag (parity additionally
`parity`), so nothing here executes by accident. (Repository-wide
source-format checks still inspect these files; that is the precise
boundary.)

## Tasks

| Task | Needs | Cost |
| --- | --- | --- |
| `task validate` | Langfuse + any of the provider credential sets below | 6 inference calls (temperature 0, max 16 output tokens) + 3 token-free error probes; unset providers skip, listing the missing variables |
| `task parity` | Langfuse + Azure credentials, committed golden | 1 inference call |
| `task parity:regen` | above + `uv` | 1 Python + 1 Go inference call; `ACCEPT=accept` replaces the golden |
| `task matrix` | nothing (credential-free) | 0 provider calls; runs the synthetic suite per SDK version |

## Environment

Langfuse (always required; the harness self-check runs with only
these): `LANGFUSE_BASE_URL`, `LANGFUSE_PUBLIC_KEY`,
`LANGFUSE_SECRET_KEY`. Tracing/content-capture must not be disabled
and the sample rate must be 1; violations fail by setting name.

- Azure OpenAI: `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_KEY`,
  `AZURE_OPENAI_DEPLOYMENT`, `AZURE_OPENAI_API_VERSION` (all required;
  nothing is defaulted).
- Vertex AI: `VERTEX_PROJECT`, `VERTEX_LOCATION`, `VERTEX_MODEL`, and
  credentials via `VERTEX_CREDENTIALS_JSON` (inline JSON, or a path
  OUTSIDE the checkout) or ambient application default credentials.
- OpenRouter: `OPENROUTER_API_KEY`, `OPENROUTER_MODEL` (a paid model;
  cost attribution is a hard assertion when the provider reports a
  positive cost).

Credentials never live in the checkout: the module's .gitignore blocks
credential-shaped files, and configured credential paths that resolve
inside the repository are rejected.

## What a failure means

Assertions compare the Langfuse readback against the same call's SDK
response: model identity (vendor prefixes included), every usage
bucket after the documented inclusive-to-exclusive mapping, exact
aggregated stream output, time ordering (start <= completionStart <=
end), provider/deployment/api-version metadata, wire-provable error
statuses, and OpenRouter cost attribution. A failure is a real
discrepancy between what the provider said and what Langfuse shows.

The parity golden (`testdata/parity/azure.golden.json`) is the
normalized snapshot of the pinned Python `langfuse.openai` oracle
(see `parity/pyproject.toml` for exact versions); the standalone
parity test asserts Go-versus-pinned-snapshot conformance. The
compatibility matrix (`docs/support-matrix.md`) is regenerated
evidence; see its header for exactly what a checkmark claims.
