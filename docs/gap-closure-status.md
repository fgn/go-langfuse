# SDK gap-closure status

## Recovered milestone, not completion of the assignment

PR #40 now delivers a scoped prompt/API foundation from the uploaded
17 September 2026 delegation brief. **The full required P0/P1 assignment remains
unfinished.** This PR remains a draft; unfinished application work is not
reclassified as optional organization administration.

The previous editing environment stopped before its broader local implementation
was pushed. Recovery restored the committed source and a public, credential-free
Go dependency/toolchain archive. It did not recover the unpushed dataset,
experiment, provider, or other code. Earlier local test claims do not validate
this reconstruction. A temporary read-only source-archive workflow was used for
recovery and then removed; no branch-writing development workflow remains.

## Baseline and contract

- Baseline `main`: `8a1c76e2262aff2f241ee75e6d1d8ee3955bca2c`.
- Retained implementation head: `956226ad768a7d38d22b4d33e3277b24ad2d54ae`.
- Exact recovery snapshot: `f660c307959896cc321ca009806bf9db00baf309`.
- Selected official OpenAPI snapshot: 17 September 2026, committed under
  `api/testdata/langfuse-openapi-2026-09-17.yaml`.
- SHA-256: `b235737d48b117621f9d607b2beb48b135851fac7bff2aa72b50c6cc8796e9df`.
- The earlier GitHub recovery fetched the schema again and matched that hash.
  Current CI verifies the committed hash and reproducible operation index.
- [API coverage](api-coverage.md): five implemented prompt operations out of 117
  indexed operations. Empty resource-service placeholders were removed.

## Required workstreams

| Priority | Requirement | Source / validation | Status |
| --- | --- | --- | --- |
| P0 | Standalone synchronous API transport | `api/client.go`, `api/client_test.go`, `api/write_outcome_test.go`: finite deadlines, bounded bodies, GET-only retries, redirect refusal, redacted formatting, ambiguous writes, caller ownership | Implemented; local tests and lint pass |
| P0 | Prompt create/get/list/labels/explicit deletion | `api/prompts.go`, JSON/pagination helpers; prompt tests cover exact routes, unions, additional fields, omission/null/empty, lossless numbers and selectors | Implemented; mock-tested |
| P0 | Runtime cache invalidation and deployment/rollback | `prompt_invalidation.go`, race tests, `examples/promptmanagement`: text/chat, compile, label moves and prompt-linked generations | Implemented; mock/race-tested |
| P0 | File-backed prompt import/export workflow | Raw `Resolve=false` retrieval exists; file adoption example does not | Required, unfinished |
| P0 | Current observations, scores, metrics, experiment/item reads | No service methods delivered | Required, unfinished |
| P0 | Dataset definitions, versioned items, bounded import | No implementation delivered | Required, unfinished |
| P0 | Bounded OTel experiment execution and evaluations | No implementation delivered | Required, unfinished |
| P0 | Native Anthropic Messages/SSE instrumentation | No module delivered | Required, unfinished |
| P1 | Project-resource APIs and explicit safe media uploads | No implementation delivered | Required, unfinished |
| P1 | xAI compatibility, framework cookbook and migrations | Existing provider adapters preserved, not expanded to this scope | Required, unfinished |
| P1 | Complete application guides, consumer checks and release preparation | Prompt guide, example, coverage and unreleased notes delivered; broader guides/release work absent | Partially delivered; required remainder unfinished |
| Later | Organization provisioning, keys and memberships | No implementation promised | Separate backlog, outside this assignment's required application scope |

## Local verification and limits

The recovered Go 1.25.13 toolchain was used with cached dependencies. The following
checks passed against the repaired source before publication:

```sh
GOWORK=off go test -race -count=1 -mod=readonly -timeout=120s ./...
GOWORK=off go vet ./...
go test -race -count=20 -timeout=90s -run TestInvalidatePromptCache .
golangci-lint run --timeout 2m
python3 scripts/index-api-contract.py --check
```

The existing OpenAI, Google Gen AI, and provider integration-test modules also
passed `go vet` and race-enabled tests in the workspace. These are local results,
not a claim that the full `task ci` pipeline passed. Go 1.25.0 minimum-toolchain
coverage, formatting/module checks, fuzzing, readme/consumer checks, interop and
vulnerability scans remain governed by the existing GitHub CI. Consult the PR's
checks at its exact head revision rather than carrying forward earlier results.

No actual Langfuse server tag/image was tested. No live Langfuse writes, provider
inference, credentialed parity, releases, tags, merges, or deployments were
performed. The live example requires an explicit `-write` opt-in and a disposable
project. Runtime cache invalidation is local per client and does not propagate
across processes or automatically invalidate composed parent prompts.
