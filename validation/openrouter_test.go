//go:build validation

package validation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"testing"

	openaigo "github.com/openai/openai-go"
	"github.com/openai/openai-go/option"

	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

const openRouterBaseURL = "https://openrouter.ai/api/v1"

func openRouterClient(r *run, apiKey string) openaigo.Client {
	return openaigo.NewClient(
		option.WithAPIKey(apiKey),
		option.WithBaseURL(openRouterBaseURL),
		option.WithHTTPClient(&http.Client{
			Transport: langfuseopenai.NewTransport(r.lf, nil,
				langfuseopenai.WithProvider("openrouter")),
		}),
		option.WithMaxRetries(0),
	)
}

// openRouterCost extracts OpenRouter's usage.cost from the raw usage
// JSON of the same response: the typed struct and the stream
// accumulator both drop it, so raw JSON is the only ground truth. A
// malformed payload or a missing field is reported distinctly from a
// legitimate zero so the cost assertion can never disarm itself.
func openRouterCost(rawUsage string) (cost float64, present bool, err error) {
	var usage map[string]json.RawMessage
	if err := json.Unmarshal([]byte(rawUsage), &usage); err != nil {
		return 0, false, fmt.Errorf("malformed raw usage: %w", err)
	}
	raw, ok := usage["cost"]
	if !ok {
		return 0, false, nil
	}
	if err := json.Unmarshal(raw, &cost); err != nil {
		return 0, false, fmt.Errorf("usage.cost is not a number: %w", err)
	}
	if math.IsNaN(cost) || math.IsInf(cost, 0) || cost < 0 {
		return 0, false, fmt.Errorf("usage.cost %v is not a finite non-negative number", cost)
	}
	return cost, true, nil
}

func openRouterUsage(usage openaigo.CompletionUsage) map[string]int64 {
	cached := usage.PromptTokensDetails.CachedTokens
	reasoning := usage.CompletionTokensDetails.ReasoningTokens
	buckets := map[string]int64{
		"input":  usage.PromptTokens - cached,
		"output": usage.CompletionTokens - reasoning,
		"total":  usage.PromptTokens + usage.CompletionTokens,
	}
	if cached > 0 {
		buckets["input_cached_tokens"] = cached
	}
	if reasoning > 0 {
		buckets["output_reasoning_tokens"] = reasoning
	}
	return buckets
}

// wantCostAttribution is the hard cost-attribution assertion: the paid
// model must report a present, finite, positive usage.cost, and the
// readback must then record a positive cost in a specific documented
// field. Its absence, a malformed payload, or a missing model price
// mapping are all failures, never notes.
func wantCostAttribution(t *testing.T, got observation, rawUsage string) {
	t.Helper()
	providerCost, present, err := openRouterCost(rawUsage)
	if err != nil {
		t.Fatalf("provider cost extraction: %v", err)
	}
	if !present || providerCost <= 0 {
		t.Fatalf("paid OPENROUTER_MODEL reported no positive usage.cost (present=%v cost=%v); pick a paid model or recalibrate the extraction", present, providerCost)
	}
	for name, value := range map[string]float64{
		"totalCost": got.TotalCost, "calculatedTotalCost": got.CalculatedTotalCost,
		"costDetails.total": got.CostDetails["total"],
	} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			t.Errorf("readback %s = %v is not finite and non-negative", name, value)
		}
	}
	if got.TotalCost <= 0 && got.CalculatedTotalCost <= 0 && got.CostDetails["total"] <= 0 {
		t.Errorf("provider cost %v but Langfuse recorded no positive cost: the recorded model has no usable price mapping", providerCost)
	}
	t.Logf("cost: provider %v, langfuse totalCost=%v calculated=%v details=%v",
		providerCost, got.TotalCost, got.CalculatedTotalCost, got.CostDetails)
}

func TestOpenRouterUnary(t *testing.T) {
	r := newRun(t)
	env := requireEnv(t, "OPENROUTER_API_KEY", "OPENROUTER_MODEL")
	client := openRouterClient(r, env["OPENROUTER_API_KEY"])

	var response *openaigo.ChatCompletion
	traceID, err := r.call(t, "openrouter-unary", func(ctx context.Context) error {
		var callErr error
		response, callErr = client.Chat.Completions.New(ctx, openaigo.ChatCompletionNewParams{
			Model:               openaigo.ChatModel(env["OPENROUTER_MODEL"]),
			Temperature:         openaigo.Float(0),
			MaxCompletionTokens: openaigo.Int(16),
			Messages: []openaigo.ChatCompletionMessageParamUnion{
				// The unique marker also defeats response caching, which
				// would zero the provider cost.
				openaigo.UserMessage("Reply with one short word. Marker: " + r.marker),
			},
		})
		return callErr
	})
	if err != nil {
		t.Fatalf("openrouter unary call: %v", err)
	}
	if len(response.Choices) == 0 || response.Choices[0].Message.Content == "" {
		t.Fatal("inconclusive: the SDK response carries no comparable output")
	}

	got := r.observation(t, traceID, "openai.chat.completions", ingested)
	checkObservation(t, got, expectedObservation{
		Name: "openai.chat.completions",
		Type: "GENERATION",
		// Hard: readback model equals the SDK response model exactly;
		// vendor-prefix drift is precisely the discrepancy under test.
		Model:        response.Model,
		RequestModel: env["OPENROUTER_MODEL"],
		TraceID:      traceID,
		Usage:        openRouterUsage(response.Usage),
		Output: map[string]any{
			"role":    "assistant",
			"content": response.Choices[0].Message.Content,
		},
		InputMarker: r.marker,
		Metadata: map[string]string{
			"provider":      "openrouter",
			"finish_reason": string(response.Choices[0].FinishReason),
		},
	})
	wantCostAttribution(t, got, response.Usage.RawJSON())
}

func TestOpenRouterStreaming(t *testing.T) {
	r := newRun(t)
	env := requireEnv(t, "OPENROUTER_API_KEY", "OPENROUTER_MODEL")
	client := openRouterClient(r, env["OPENROUTER_API_KEY"])

	var aggregated strings.Builder
	var lastModel, lastRawUsage, finishReason string
	var lastUsage *openaigo.CompletionUsage
	traceID, err := r.call(t, "openrouter-stream", func(ctx context.Context) error {
		stream := client.Chat.Completions.NewStreaming(ctx, openaigo.ChatCompletionNewParams{
			Model:               openaigo.ChatModel(env["OPENROUTER_MODEL"]),
			Temperature:         openaigo.Float(0),
			MaxCompletionTokens: openaigo.Int(16),
			StreamOptions:       openaigo.ChatCompletionStreamOptionsParam{IncludeUsage: openaigo.Bool(true)},
			Messages: []openaigo.ChatCompletionMessageParamUnion{
				openaigo.UserMessage("Reply with one short word. Marker: " + r.marker),
			},
		})
		defer stream.Close()
		for stream.Next() {
			chunk := stream.Current()
			if chunk.Model != "" {
				lastModel = chunk.Model
			}
			// The accumulator sums only top-level counts; the final
			// usage-bearing chunk's raw JSON is the ground truth for
			// detailed buckets and cost.
			if chunk.Usage.RawJSON() != "" && chunk.Usage.TotalTokens > 0 {
				usage := chunk.Usage
				lastUsage = &usage
				lastRawUsage = chunk.Usage.RawJSON()
			}
			for _, choice := range chunk.Choices {
				aggregated.WriteString(choice.Delta.Content)
				if choice.FinishReason != "" {
					finishReason = choice.FinishReason
				}
			}
		}
		return stream.Err()
	})
	if err != nil {
		t.Fatalf("openrouter streaming call: %v", err)
	}
	if aggregated.Len() == 0 {
		t.Fatal("inconclusive: the stream carried no comparable output")
	}
	if lastUsage == nil {
		t.Fatal("inconclusive: no usage-bearing chunk despite include_usage")
	}

	got := r.observation(t, traceID, "openai.chat.completions", ingested)
	checkObservation(t, got, expectedObservation{
		Name:         "openai.chat.completions",
		Type:         "GENERATION",
		Model:        lastModel,
		RequestModel: env["OPENROUTER_MODEL"],
		TraceID:      traceID,
		Usage:        openRouterUsage(*lastUsage),
		Output:       aggregated.String(),
		InputMarker:  r.marker,
		Stream:       true,
		Metadata: map[string]string{
			"provider":      "openrouter",
			"finish_reason": finishReason,
		},
	})
	wantCostAttribution(t, got, lastRawUsage)
}

// TestOpenRouterError is a token-free authenticated request for a
// model that cannot exist.
func TestOpenRouterError(t *testing.T) {
	r := newRun(t)
	env := requireEnv(t, "OPENROUTER_API_KEY")
	client := openRouterClient(r, env["OPENROUTER_API_KEY"])
	invalid := "validation/invalid-" + r.marker

	traceID, err := r.call(t, "openrouter-error", func(ctx context.Context) error {
		_, callErr := client.Chat.Completions.New(ctx, openaigo.ChatCompletionNewParams{
			Model: openaigo.ChatModel(invalid),
			Messages: []openaigo.ChatCompletionMessageParamUnion{
				openaigo.UserMessage("marker " + r.marker),
			},
		})
		return callErr
	})
	if err == nil {
		t.Fatal("invalid model unexpectedly succeeded")
	}
	var apiErr *openaigo.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *openaigo.Error, got %T: %v", err, err)
	}

	got := r.observation(t, traceID, "openai.chat.completions")
	checkObservation(t, got, expectedObservation{
		Name:         "openai.chat.completions",
		Type:         "GENERATION",
		RequestModel: invalid,
		TraceID:      traceID,
		InputMarker:  r.marker,
		Status:       fmt.Sprintf("http %d", apiErr.StatusCode),
		Metadata:     map[string]string{"provider": "openrouter"},
	})
}
