# Changelog

All notable changes to this module will be documented here. The module
follows Semantic Versioning independently of the core module.

## [Unreleased]

- Observe the OpenAI Responses API (`/responses`, unary and streaming)
  as generations with a closed sanitization schema, terminal-
  authoritative streaming output with bounded incremental fallback,
  presence-preserving usage mapping, and fixed provider-error
  categories. Retrieval, input-item, and background-polling routes
  still pass through unobserved.
- Extend the shared wiretap (mirrored in both contrib modules) with a
  hard protocol-incomplete terminal, a unary incomplete verdict that
  refines only otherwise-complete finalizations, and a chunked salvage
  surface that streams over-cap SSE events and unary bodies through a
  bounded JSON scanner so control-plane facts survive payloads the
  buffers cannot hold.

## [0.1.0] - 2026-07-23

- Initial release: transport-level Langfuse instrumentation with no
  provider SDK dependencies, recording one generation or embedding
  observation per HTTP attempt with model, content, token usage,
  time-to-first-token, and wire-provable status, governed by the core
  client's masking, capture, sampling, and limit controls. Includes
  fixes from real-provider validation: unary responses finalize when a
  complete JSON document is decoded and closed without EOF (the
  keep-alive pattern of real SDK decoders), and consecutive duplicate
  finish reasons collapse to one value.
