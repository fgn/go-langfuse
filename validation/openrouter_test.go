//go:build validation

package validation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// accumulator both drop it, so raw JSON is the only ground truth.
func openRouterCost(rawUsage string) float64 {
	var usage struct {
		Cost float64 `json:"cost"`
	}
	_ = json.Unmarshal([]byte(rawUsage), &usage)
	return usage.Cost
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

// wantPositiveCost is the hard cost-attribution assertion: a positive
// provider cost with an absent Langfuse cost is the discrepancy this
// suite exists to catch, including a missing model definition.
func wantPositiveCost(t *testing.T, got observation, providerCost float64) {
	t.Helper()
	if providerCost <= 0 {
		t.Logf("provider reported no cost (%v); cost attribution not assertable this run", providerCost)
		return
	}
	langfuseCost := got.TotalCost + got.CalculatedTotalCost + got.CostDetails["total"]
	if langfuseCost <= 0 {
		t.Errorf("provider cost %v but Langfuse recorded no cost: model %q has no usable price mapping",
			providerCost, got.Model)
	}
	t.Logf("cost: provider %v, langfuse %v (model %q)", providerCost, langfuseCost, got.Model)
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
			MaxCompletionTokens: openaigo.Int(24),
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

	got := r.observation(t, traceID, "openai.chat.completions")
	checkObservation(t, got, expectedObservation{
		Name: "openai.chat.completions",
		Type: "GENERATION",
		// Hard: readback model equals the SDK response model exactly;
		// vendor-prefix drift is precisely the discrepancy under test.
		Model:        response.Model,
		RequestModel: env["OPENROUTER_MODEL"],
		Usage:        openRouterUsage(response.Usage),
		OutputFields: map[string]any{
			"role":    "assistant",
			"content": response.Choices[0].Message.Content,
		},
		InputMarker: r.marker,
		Metadata:    map[string]string{"provider": "openrouter"},
	})
	wantPositiveCost(t, got, openRouterCost(response.Usage.RawJSON()))
}

func TestOpenRouterStreaming(t *testing.T) {
	r := newRun(t)
	env := requireEnv(t, "OPENROUTER_API_KEY", "OPENROUTER_MODEL")
	client := openRouterClient(r, env["OPENROUTER_API_KEY"])

	var aggregated strings.Builder
	var lastModel, lastRawUsage string
	var lastUsage *openaigo.CompletionUsage
	traceID, err := r.call(t, "openrouter-stream", func(ctx context.Context) error {
		stream := client.Chat.Completions.NewStreaming(ctx, openaigo.ChatCompletionNewParams{
			Model:               openaigo.ChatModel(env["OPENROUTER_MODEL"]),
			Temperature:         openaigo.Float(0),
			MaxCompletionTokens: openaigo.Int(24),
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

	got := r.observation(t, traceID, "openai.chat.completions")
	checkObservation(t, got, expectedObservation{
		Name:         "openai.chat.completions",
		Type:         "GENERATION",
		Model:        lastModel,
		RequestModel: env["OPENROUTER_MODEL"],
		Usage:        openRouterUsage(*lastUsage),
		Output:       aggregated.String(),
		InputMarker:  r.marker,
		Stream:       true,
		Metadata:     map[string]string{"provider": "openrouter"},
	})
	wantPositiveCost(t, got, openRouterCost(lastRawUsage))
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
		Status:       fmt.Sprintf("http %d", apiErr.StatusCode),
		Metadata:     map[string]string{"provider": "openrouter"},
	})
}
