# Releasing

This module uses stable `vMAJOR.MINOR.PATCH` tags. While the major version is
zero, minor releases may contain explicitly documented API changes; patch
releases remain backward compatible.

Tags are an output of the release gate, not its trigger. Do not create or push
a release tag by hand. A tag push does not start the release workflow.

## Prepare the release on `main`

Merge a release-preparation pull request that completes every item below:

1. Complete the staged production dogfood. Resolve duplicate-observation and
   content-disclosure findings, then record a non-secret issue, checklist, or
   run URL as evidence.
2. Pre-seed the dedicated synthetic Langfuse project with prompt
   `langfuse-go-live-prompt` version 1 and the checklist evaluator. Run
   `go test -count=1 -tags=live -run TestLiveCompatibility -v .` with that
   project's credentials. Use the logged unique run marker to verify the
   UI/filter/evaluator checklist in
   [docs/compatibility.md](docs/compatibility.md), then record non-secret
   evidence. Never paste credentials or telemetry payloads into that evidence.
   The test fails closed when credentials are absent, tracing/content capture
   is disabled, or recording IDs are empty; a zero exit therefore cannot mean
   that it merely skipped export.
3. With the toolchain recorded in `go.mod`, run formatting, the standalone
   README compile check, all examples, the live-suite compile-only check,
   normal tests, race tests, fuzz smoke tests, vet, and `govulncheck`. CI and
   the release workflow repeat these gates.
4. Review the exported API surface, decoded OTLP goldens, dependency changes,
   security policy, and compatibility matrix.
5. Move `CHANGELOG.md` entries from `Unreleased` into a heading exactly like
   `## [0.1.0] - YYYY-MM-DD`. Set `sdkVersion` in `version.go` to the same
   version without the `v` prefix.
6. Remove both the `PRE_RELEASE_WARNING` marker and its “do not use in
   production until tagged” text from `README.md`. Replace them with accurate
   post-release stability/support wording. The release workflow intentionally
   fails while either pre-release warning remains.
7. Confirm the tree contains no live credentials, captured production
   telemetry, generated binaries, or the local `ref/` research corpus.

The release commit must be merged to and current with `main`; the workflow
rejects dispatches from another branch or an older commit.

## Dispatch the gated release

From GitHub Actions, manually run the **Release** workflow on `main` and supply:

- the exact stable version, such as `v0.1.0`;
- an explicit live-gate attestation and its non-secret evidence URL or ID; and
- an explicit production dogfood attestation and its non-secret evidence URL or
  ID.

The `release` GitHub environment should require maintainer approval. The
workflow validates the attestations and repeats all local static gates before
it inspects, creates, or verifies the requested tag. Only after those checks
pass does it create an annotated tag on the tested commit and create generated
GitHub release notes.

If a run pushes the tag but fails while creating release notes, rerun the same
workflow on the same `main` commit. It accepts an existing tag only when that
tag resolves to the tested commit. A tag on any other commit fails closed.

The workflow-created annotated tag is not a developer GPG signature. Release
authority instead comes from the protected `main` branch, the auditable manual
inputs, the protected `release` environment, and the workflow's scoped
`contents: write` token. Configure those repository protections before the
first release.
