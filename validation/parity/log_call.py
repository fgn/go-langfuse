"""Makes the identical Azure chat call the Go parity test makes, via
the first-party langfuse.openai wrapper, and prints the trace ID.

Requires AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY,
AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_API_VERSION, LANGFUSE_BASE_URL
(exported to LANGFUSE_HOST for the Python SDK), LANGFUSE_PUBLIC_KEY,
LANGFUSE_SECRET_KEY, and the run marker as argv[1].
"""

import os
import sys

os.environ.setdefault("LANGFUSE_HOST", os.environ["LANGFUSE_BASE_URL"])

from langfuse import Langfuse, get_client  # noqa: E402
from langfuse.openai import AzureOpenAI  # noqa: E402

marker = sys.argv[1]
langfuse = get_client()

# A deterministic trace ID seeded by the marker makes the trace
# addressable without depending on wrapper internals.
trace_id = Langfuse.create_trace_id(seed=marker)

client = AzureOpenAI(
    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
    api_key=os.environ["AZURE_OPENAI_API_KEY"],
    api_version=os.environ["AZURE_OPENAI_API_VERSION"],
)
kwargs = dict(
    model=os.environ["AZURE_OPENAI_DEPLOYMENT"],
    temperature=0,
    max_tokens=24,
    messages=[{"role": "user", "content": f"Reply with one short word. Marker: {marker}"}],
)
try:
    completion = client.chat.completions.create(langfuse_trace_id=trace_id, **kwargs)
except TypeError:
    # Wrapper signature drift: fall back to the wrapper's own trace and
    # report it instead.
    completion = client.chat.completions.create(**kwargs)
    trace_id = langfuse.get_current_trace_id() or ""

assert completion.choices[0].message.content
langfuse.flush()
print(trace_id)
