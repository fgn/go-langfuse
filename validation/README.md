# Real-provider validation

Slow, deliberate, credentialed verification that the adapters record
real provider behavior correctly, judged by reading traces back
through the Langfuse public API with each provider SDK's own response
as ground truth. No provider mocks in the smoke and parity tests. This
module is excluded from the released Go modules, absent from go.work,
and never pulled by `go get`. Every file carries a build tag
(`validation`, plus `parity` for parity files; `interop` for the
credential-free baggage corpus), so nothing here executes by accident.
The credentialed suites never run in `task ci`; the credential-free
interop corpus is the one deliberate exception and runs there as a
cross-SDK gate. (Repository-wide
source-format checks still inspect these files; that is the precise
boundary.)

## Tasks

| Task | Needs | Cost |
| --- | --- | --- |
| `task validate` | Langfuse + any of the provider credential sets below | 6 inference calls (temperature 0; max 16 output tokens on Azure/Vertex, 256 on OpenRouter to clear reasoning-model hidden tokens) + 3 token-free error probes; unset providers skip, listing the missing variables |
| `task parity` | Langfuse + Azure credentials, committed golden | 1 inference call |
| `task parity:regen` | above + `uv` | 1 Python + 1 Go inference call; `ACCEPT=accept` replaces the golden |
| `task matrix` | nothing (credential-free) | 0 provider calls; runs the synthetic suite per SDK version |
| `task interop:datasets` | Langfuse credentials for a dedicated project + `LANGFUSE_INTEROP_PROJECT`, `uv`, `node`/`npm` | 0 provider calls; writes synthetic datasets, items, and experiments through Go, the pinned Python SDK, and the pinned TypeScript SDK and reads each back with another |
| `task interop` | `uv` only (credential-free) | 0 provider calls; baggage corpus + cross-language smokes against the uv-locked Python SDK, sealed in `testdata/interop/`; `ACCEPT=accept` reseals. Unlike everything else here, this one DOES run in `task ci` and gates core releases |

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

## Dataset and experiment interop

`task interop:datasets` (build tag `datasetinterop`) runs the local Go SDK,
the official Python SDK pinned in `datasetinterop/python` (uv-locked), and
the official TypeScript SDK pinned in `datasetinterop/ts` (npm-locked)
against one live project. Each SDK agent reads one JSON request on stdin and
answers on stdout; credentials reach it only through the inherited
`LANGFUSE_*` environment, never through arguments. Before writing anything
the harness requires the keys to belong to exactly one project named by
`LANGFUSE_INTEROP_PROJECT`; every missing prerequisite fails the run.
`INTEROP_NODE` selects the node binary and `INTEROP_REPORT_DIR` where the
JSON and Markdown reports go (default: a new temporary directory).

| Case | What crosses SDKs |
| --- | --- |
| E1 | Dataset (ID, description, metadata, input and expected-output schemas) and items written by one SDK, listed in pages of 3, filtered by source trace and source observation, and read by ID by another, which also enumerates the project's datasets in pages of 2 and must find three datasets the writer created, at least one past the first page: objects, arrays, Unicode, escapes, numbers, booleans, nested null, omitted fields, non-object metadata, archived status |
| E2 | One SDK mutates another's items in a dataset with array metadata: content and metadata with an omitted input kept, an explicit JSON null kept (Go rejects it locally), archive, unarchive, source trace and observation change, delete; the creator reads back |
| E3 | As-of reads at a pinned instant before later edits, an archive, a delete, and an addition, in pages of 2; latest reads in pages of 3 |
| E4 | Dataset experiments run by one SDK and read back through another's experiment API in pages of 2: run and item identity against the created dataset and fixture IDs, version linkage, input, output, expected output, item and run metadata, every item score with its ID, type, value, and root target, the exact run score on the experiment, latency without evaluators, and, for Go runs read by Python, one evaluator observation per evaluator under each root |
| E5 | The same for local data with a failing item, which every SDK stores as an ERROR item without scores; the official SDKs' input-hash item IDs are checked |
| E6 | Legacy dataset-run reads on a v4 events_only server (expected 404) |
| E7 | A Go experiment over items with array, string, empty, and object metadata, read back by Python |

Every client inherits one tracing environment; the harness sets
`LANGFUSE_TRACING_ENVIRONMENT=interop` when it is unset, so the score
environment assertions run with a non-default value. Every experiment read
polls until the exact expected counts are present,
then reads again after a settling delay; dataset REST reads are synchronous. The report records a SHA-256 manifest of the Go
SDK and harness sources before and after the run, and the run fails if they
differ. A comparator self-test (`TestDatasetInteropComparator`) checks that
the helpers reject deliberate corruptions.

Comparisons are exact except for documented server behavior, which applies
to every SDK alike: the server stores `""` as null and a top-level string
that parses as JSON as that value, returns `12345678901234567890` as
`12345678901234570000` (an observed case, not a general rounding rule),
returns experiment item input and output as the exported text, and flattens
experiment metadata into dotted keys with string values (null leaves become
`""`; server 4.55 also keeps the first of colliding dotted paths, which the
fixtures avoid). SDK behavior that differs is asserted as such: only Go
exports the item version attribute, Python and Go run a composite evaluator,
TypeScript records a failed item without its input, and Go scores items in
the `sdk-experiment` environment while the official runners score them in the
client environment.
