//go:build validation

package validation

import (
	"context"
	"net/http"
	"strings"
	"testing"

	openaigo "github.com/openai/openai-go"
	"github.com/openai/openai-go/azure"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/responses"
	"github.com/openai/openai-go/shared"

	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

// azureResponsesClient builds the ONLY correct GA v1 construction: the
// generic base URL option pointed at {endpoint}/openai/v1/ plus the
// Azure api-key header. azure.WithEndpoint is deliberately absent — it
// builds the classic deployments route with a required api-version and
// no Responses rewrite.
func azureResponsesClient(r *run, endpoint, apiKey string) openaigo.Client {
	return openaigo.NewClient(
		azure.WithAPIKey(apiKey),
		option.WithBaseURL(strings.TrimRight(endpoint, "/")+"/openai/v1/"),
		option.WithHTTPClient(&http.Client{
			Transport: langfuseopenai.NewTransport(r.lf, nil),
		}),
		option.WithMaxRetries(0),
	)
}

// TestResponsesRequestConstruction is credential-free: it proves the
// documented client construction targets the GA v1 route with the
// Azure auth header and no legacy api-version before any live smoke
// depends on it.
func TestResponsesRequestConstruction(t *testing.T) {
	captured := make(chan *http.Request, 1)
	client := openaigo.NewClient(
		azure.WithAPIKey("test-key"),
		option.WithBaseURL("https://example.openai.azure.com/openai/v1/"),
		option.WithHTTPClient(&http.Client{Transport: requestCapture{captured}}),
		option.WithMaxRetries(0),
	)
	_, _ = client.Responses.New(context.Background(), responses.ResponseNewParams{
		Model: shared.ResponsesModel("gpt-test"),
		Input: responses.ResponseNewParamsInputUnion{OfString: openaigo.String("ping")},
	})
	request := <-captured
	if got := request.URL.Path; got != "/openai/v1/responses" {
		t.Errorf("path = %q, want /openai/v1/responses", got)
	}
	if request.URL.Query().Get("api-version") != "" {
		t.Errorf("GA v1 must not carry an api-version query: %q", request.URL.RawQuery)
	}
	if request.Header.Get("Api-Key") != "test-key" {
		t.Error("Azure Api-Key header missing")
	}
}

type requestCapture struct{ captured chan *http.Request }

func (c requestCapture) RoundTrip(request *http.Request) (*http.Response, error) {
	c.captured <- request.Clone(request.Context())
	return &http.Response{
		StatusCode: http.StatusNotFound,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       http.NoBody,
		Request:    request,
	}, nil
}

// responsesUsageBuckets maps the SDK's inclusive wire usage onto the
// exclusive readback representation, mirroring the core normalization.
func responsesUsageBuckets(usage responses.ResponseUsage) map[string]int64 {
	cached := usage.InputTokensDetails.CachedTokens
	reasoning := usage.OutputTokensDetails.ReasoningTokens
	buckets := map[string]int64{
		"input":  usage.InputTokens - cached,
		"output": usage.OutputTokens - reasoning,
		"total":  usage.InputTokens + usage.OutputTokens,
	}
	if cached > 0 {
		buckets["input_cached_tokens"] = cached
	}
	if reasoning > 0 {
		buckets["output_reasoning_tokens"] = reasoning
	}
	return buckets
}

// expectedResponsesOutput builds the sanitized projection the adapter
// exports, from the SDK's own output items: message text and refusals,
// reasoning summaries with the thought convention, and fixed
// placeholders for everything else (reasoning models prepend a
// reasoning item before the message).
func expectedResponsesOutput(t *testing.T, response *responses.Response) any {
	t.Helper()
	if response.OutputText() == "" {
		t.Fatal("provider returned no output text; the smoke cannot assert output")
	}
	items := make([]any, 0, len(response.Output))
	for _, item := range response.Output {
		switch item.Type {
		case "message":
			message := item.AsMessage()
			content := make([]any, 0, len(message.Content))
			for _, part := range message.Content {
				switch part.Type {
				case "output_text":
					content = append(content, map[string]any{"type": "output_text", "text": part.Text})
				case "refusal":
					content = append(content, map[string]any{"type": "refusal", "refusal": part.Refusal})
				}
			}
			items = append(items, map[string]any{
				"type": "message", "role": string(message.Role), "content": content,
			})
		case "reasoning":
			reasoning := item.AsReasoning()
			expected := map[string]any{"type": "reasoning", "thought": true}
			summary := make([]any, 0, len(reasoning.Summary))
			for _, part := range reasoning.Summary {
				if part.Text != "" {
					summary = append(summary, part.Text)
				}
			}
			if len(summary) > 0 {
				expected["summary"] = summary
			}
			items = append(items, expected)
		default:
			kind := item.Type
			items = append(items, map[string]any{"type": kind, "omitted": true})
		}
	}
	if len(items) == 1 {
		return items[0]
	}
	return items
}

func runResponsesUnary(t *testing.T, r *run, client openaigo.Client, model, caseName string) {
	t.Helper()
	var response *responses.Response
	traceID, err := r.call(t, caseName, func(ctx context.Context) error {
		var callErr error
		response, callErr = client.Responses.New(ctx, responses.ResponseNewParams{
			Model:           shared.ResponsesModel(model),
			Instructions:    openaigo.String("Reply with one short word."),
			Input:           responses.ResponseNewParamsInputUnion{OfString: openaigo.String("Say ok. Marker: " + r.marker)},
			Temperature:     openaigo.Float(0),
			MaxOutputTokens: openaigo.Int(256),
		})
		return callErr
	})
	if err != nil {
		t.Fatalf("responses call failed (a configured provider must serve the route; 404 is a regression): %v", err)
	}
	if response.Status != "completed" {
		t.Fatalf("response status %q; the smoke needs a completed response", response.Status)
	}

	got := r.observation(t, traceID, "openai.responses", ingested)
	checkObservation(t, got, expectedObservation{
		Name:        "openai.responses",
		Type:        "GENERATION",
		Model:       string(response.Model),
		TraceID:     traceID,
		Usage:       responsesUsageBuckets(response.Usage),
		Output:      expectedResponsesOutput(t, response),
		InputMarker: r.marker,
	})
}

func runResponsesStreaming(t *testing.T, r *run, client openaigo.Client, model, caseName string) {
	t.Helper()
	var final *responses.Response
	terminals := 0
	traceID, err := r.call(t, caseName, func(ctx context.Context) error {
		stream := client.Responses.NewStreaming(ctx, responses.ResponseNewParams{
			Model:           shared.ResponsesModel(model),
			Instructions:    openaigo.String("Reply with one short word."),
			Input:           responses.ResponseNewParamsInputUnion{OfString: openaigo.String("Say ok. Marker: " + r.marker)},
			Temperature:     openaigo.Float(0),
			MaxOutputTokens: openaigo.Int(256),
		})
		for stream.Next() {
			event := stream.Current()
			switch event.Type {
			case "response.completed":
				terminals++
				completed := event.AsResponseCompleted().Response
				final = &completed
			case "response.failed", "response.incomplete", "error":
				terminals++
			}
		}
		return stream.Err()
	})
	if err != nil {
		t.Fatalf("streaming responses call failed: %v", err)
	}
	if terminals != 1 || final == nil {
		t.Fatalf("protocol terminals = %d (completed=%v), want exactly one completed", terminals, final != nil)
	}

	got := r.observation(t, traceID, "openai.responses", ingested)
	checkObservation(t, got, expectedObservation{
		Name:        "openai.responses",
		Type:        "GENERATION",
		Model:       string(final.Model),
		TraceID:     traceID,
		Usage:       responsesUsageBuckets(final.Usage),
		Output:      expectedResponsesOutput(t, final),
		InputMarker: r.marker,
		Stream:      true,
	})
}

func TestAzureResponsesUnary(t *testing.T) {
	r := newRun(t)
	env := requireEnv(t, "AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY", "AZURE_OPENAI_RESPONSES_DEPLOYMENT")
	client := azureResponsesClient(r, env["AZURE_OPENAI_ENDPOINT"], env["AZURE_OPENAI_API_KEY"])
	runResponsesUnary(t, r, client, env["AZURE_OPENAI_RESPONSES_DEPLOYMENT"], "azure-responses-unary")
}

func TestAzureResponsesStreaming(t *testing.T) {
	r := newRun(t)
	env := requireEnv(t, "AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY", "AZURE_OPENAI_RESPONSES_DEPLOYMENT")
	client := azureResponsesClient(r, env["AZURE_OPENAI_ENDPOINT"], env["AZURE_OPENAI_API_KEY"])
	runResponsesStreaming(t, r, client, env["AZURE_OPENAI_RESPONSES_DEPLOYMENT"], "azure-responses-streaming")
}

func TestOpenRouterResponsesUnary(t *testing.T) {
	r := newRun(t)
	env := requireEnv(t, "OPENROUTER_API_KEY", "OPENROUTER_RESPONSES_MODEL")
	client := openRouterClient(r, env["OPENROUTER_API_KEY"])
	runResponsesUnary(t, r, client, env["OPENROUTER_RESPONSES_MODEL"], "openrouter-responses-unary")
}

func TestOpenRouterResponsesStreaming(t *testing.T) {
	r := newRun(t)
	env := requireEnv(t, "OPENROUTER_API_KEY", "OPENROUTER_RESPONSES_MODEL")
	client := openRouterClient(r, env["OPENROUTER_API_KEY"])
	runResponsesStreaming(t, r, client, env["OPENROUTER_RESPONSES_MODEL"], "openrouter-responses-streaming")
}

// TestAzureResponsesHTTPError is an HTTP-error smoke, nothing more: it
// exercises a failed exchange before any response exists, not the
// protocol terminal table (deterministic parser fixtures cover that).
func TestAzureResponsesHTTPError(t *testing.T) {
	r := newRun(t)
	env := requireEnv(t, "AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY", "AZURE_OPENAI_RESPONSES_DEPLOYMENT")
	client := azureResponsesClient(r, env["AZURE_OPENAI_ENDPOINT"], env["AZURE_OPENAI_API_KEY"])
	traceID, err := r.call(t, "azure-responses-error", func(ctx context.Context) error {
		_, callErr := client.Responses.New(ctx, responses.ResponseNewParams{
			Model: shared.ResponsesModel("no-such-deployment-" + r.marker),
			Input: responses.ResponseNewParamsInputUnion{OfString: openaigo.String("ping")},
		})
		return callErr
	})
	if err == nil {
		t.Fatal("an unknown model must fail")
	}
	got := r.observation(t, traceID, "openai.responses")
	if !strings.HasPrefix(got.StatusMessage, "http ") {
		t.Errorf("status = %q, want an http error category", got.StatusMessage)
	}
}
