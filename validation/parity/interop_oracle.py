"""Batch oracle for the baggage interop corpus.

Reads one JSON document on stdin ({"cases": [...]}) and writes one JSON
document on stdout ({"results": {id: ...}}). Each case exercises the
PUBLIC pinned Python SDK path:

- op "inject": a real recording Langfuse root span (unless with_root is
  false) wrapping propagate_attributes(..., as_baggage=True), then the
  standard W3C propagators inject into a carrier. Returns the raw
  baggage header, traceparent, and the root's trace/span IDs.
- op "extract": the standard W3C propagators extract the given header
  values, a real Langfuse span starts under that context (the pinned
  LangfuseSpanProcessor applies propagated attributes on start), and
  the span's langfuse.* attributes are read back from an in-memory
  exporter. Returns the attributes and the span's trace/parent IDs.

Credential-free: the client never reaches a network (unroutable host,
one-hour flush interval, no auth check).
"""

import json
import logging
import os
import sys

from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from langfuse import Langfuse, propagate_attributes
from langfuse._utils.environment import common_release_envs

logging.disable(logging.CRITICAL)

# The standard W3C pair, instantiated explicitly so an ambient
# OTEL_PROPAGATORS setting can never alter the oracle's behavior.
PROPAGATOR = CompositePropagator(
    [TraceContextTextMapPropagator(), W3CBaggagePropagator()]
)

# Ambient settings the pinned client would otherwise fold into span
# attributes or span emission, scrubbed so the corpus is identical on
# any machine. The release scrub reuses the SDK's own CI commit-var
# list (GITHUB_SHA and friends), which would otherwise stamp
# langfuse.release on every span produced under CI.
for name in (
    *common_release_envs,
    "LANGFUSE_RELEASE",
    "LANGFUSE_TRACING_ENVIRONMENT",
    "LANGFUSE_SAMPLE_RATE",
    "OTEL_SDK_DISABLED",
):
    os.environ.pop(name, None)

INJECT_KEYS = (
    "user_id",
    "session_id",
    "trace_name",
    "version",
    "environment",
    "metadata",
    "tags",
)


class ListGetter:
    """Carrier getter over dict[str, list[str]] so multi-header cases
    reach the extractor exactly as the HTTP layer would present them."""

    def get(self, carrier, key):
        return carrier.get(key)

    def keys(self, carrier):
        return list(carrier.keys())


def run_inject(client, exporter, case):
    kwargs = {
        key: case["attributes"][key]
        for key in INJECT_KEYS
        if case["attributes"].get(key) is not None
    }
    carrier = {}
    result = {}
    if case.get("with_root", True):
        exporter.clear()
        with client.start_as_current_observation(name="oracle-root"):
            span_context = trace.get_current_span().get_span_context()
            result["trace_id"] = f"{span_context.trace_id:032x}"
            result["span_id"] = f"{span_context.span_id:016x}"
            with propagate_attributes(as_baggage=True, **kwargs):
                PROPAGATOR.inject(carrier)
        # The producer root's exported attributes let the corpus assert
        # exactly one app-root marker across both processes.
        roots = [
            span
            for span in exporter.get_finished_spans()
            if span.name == "oracle-root"
        ]
        if len(roots) == 1:
            result["attributes"] = {
                key: value
                for key, value in sorted((roots[0].attributes or {}).items())
                if key.startswith("langfuse.") or key in ("user.id", "session.id")
            }
    else:
        with propagate_attributes(as_baggage=True, **kwargs):
            PROPAGATOR.inject(carrier)
    result["baggage"] = carrier.get("baggage", "")
    result["traceparent"] = carrier.get("traceparent", "")
    return result


def run_extract(client, exporter, case):
    carrier = {"baggage": list(case["headers"])}
    if case.get("traceparent"):
        carrier["traceparent"] = [case["traceparent"]]
    ctx = PROPAGATOR.extract(carrier, getter=ListGetter())

    exporter.clear()
    token = context_api.attach(ctx)
    try:
        with client.start_as_current_observation(name="oracle-receiver"):
            pass
    finally:
        context_api.detach(token)

    spans = [
        span
        for span in exporter.get_finished_spans()
        if span.name == "oracle-receiver"
    ]
    if len(spans) != 1:
        raise RuntimeError(f"expected one receiver span, got {len(spans)}")
    span = spans[0]
    # The pinned SDK writes user and session under the OTel semconv keys
    # user.id / session.id; everything else it owns is langfuse.*-prefixed.
    attributes = {
        key: value
        for key, value in sorted((span.attributes or {}).items())
        if key.startswith("langfuse.") or key in ("user.id", "session.id")
    }
    parent = span.parent
    return {
        "attributes": attributes,
        "trace_id": f"{span.context.trace_id:032x}",
        "parent_span_id": f"{parent.span_id:016x}" if parent else "",
    }


def main():
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    client = Langfuse(
        public_key="pk-lf-interop-oracle",
        secret_key="sk-lf-interop-oracle",
        host="http://127.0.0.1:9",
        tracer_provider=provider,
        flush_interval=3600,
        tracing_enabled=True,
    )

    request = json.load(sys.stdin)
    results = {}
    for case in request["cases"]:
        if case["op"] == "inject":
            results[case["id"]] = run_inject(client, exporter, case)
        elif case["op"] == "extract":
            results[case["id"]] = run_extract(client, exporter, case)
        else:
            raise RuntimeError(f"unknown op {case['op']!r}")
    json.dump({"results": results}, sys.stdout, sort_keys=True)


if __name__ == "__main__":
    main()
