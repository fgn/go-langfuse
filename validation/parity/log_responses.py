"""Makes the identical Azure GA v1 Responses call the Go parity test
makes, via the first-party langfuse.openai wrapper, and prints the
trace ID.

Requires AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY,
AZURE_OPENAI_RESPONSES_DEPLOYMENT, LANGFUSE_BASE_URL (exported to
LANGFUSE_HOST for the Python SDK), LANGFUSE_PUBLIC_KEY,
LANGFUSE_SECRET_KEY, and the run marker as argv[1]. The wrapper's
default generation name collides with the chat parity alias, so this
call passes an explicit distinct name.
"""

import os
import sys

os.environ.setdefault("LANGFUSE_HOST", os.environ["LANGFUSE_BASE_URL"])

from langfuse import Langfuse, get_client  # noqa: E402
from langfuse.openai import OpenAI  # noqa: E402

marker = sys.argv[1]
langfuse = get_client()

trace_id = Langfuse.create_trace_id(seed=marker)

# The GA v1 route needs the generic OpenAI client at the /openai/v1/
# base with the Azure api-key header; AzureOpenAI would force the
# classic deployments route.
client = OpenAI(
    base_url=os.environ["AZURE_OPENAI_ENDPOINT"].rstrip("/") + "/openai/v1/",
    api_key="unused",
    default_headers={"Api-Key": os.environ["AZURE_OPENAI_API_KEY"]},
)

response = client.responses.create(
    trace_id=trace_id,
    name="OpenAI-responses-parity",
    model=os.environ["AZURE_OPENAI_RESPONSES_DEPLOYMENT"],
    temperature=0,
    max_output_tokens=64,
    instructions="Reply with one short word.",
    input=[
        {
            "role": "user",
            "content": [
                {"type": "input_text", "text": f"Say ok. Marker: {marker}"}
            ],
        }
    ],
)

assert response.output_text
langfuse.flush()
assert len(trace_id) == 32 and all(c in "0123456789abcdef" for c in trace_id), trace_id
print(trace_id)
