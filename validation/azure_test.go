//go:build validation

package validation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	gopenai "github.com/sashabaranov/go-openai"

	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

// azureEnv is the complete required Azure configuration: nothing is
// defaulted, because sashabaranov's DefaultAzureConfig would otherwise
// silently pin a stale API version and an implicit deployment mapping.
func azureEnv(t *testing.T) map[string]string {
	return requireEnv(t,
		"AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY",
		"AZURE_OPENAI_DEPLOYMENT", "AZURE_OPENAI_API_VERSION")
}

// azureClient builds a sashabaranov client whose model mapper returns
// exactly the given deployment, so azure.deployment asserts against a
// controlled value for success and error cases alike.
func azureClient(r *run, env map[string]string, deployment string) *gopenai.Client {
	cfg := gopenai.DefaultAzureConfig(env["AZURE_OPENAI_API_KEY"], env["AZURE_OPENAI_ENDPOINT"])
	cfg.APIVersion = env["AZURE_OPENAI_API_VERSION"]
	cfg.AzureModelMapperFunc = func(string) string { return deployment }
	cfg.HTTPClient = &http.Client{Transport: langfuseopenai.NewTransport(r.lf, nil)}
	return gopenai.NewClientWithConfig(cfg)
}

// azureUsage projects the SDK-reported usage into the exclusive
// readback buckets the core documents.
func azureUsage(usage gopenai.Usage) map[string]int64 {
	cached, reasoning := int64(0), int64(0)
	if usage.PromptTokensDetails != nil {
		cached = int64(usage.PromptTokensDetails.CachedTokens)
	}
	if usage.CompletionTokensDetails != nil {
		reasoning = int64(usage.CompletionTokensDetails.ReasoningTokens)
	}
	buckets := map[string]int64{
		"input":  int64(usage.PromptTokens) - cached,
		"output": int64(usage.CompletionTokens) - reasoning,
		"total":  int64(usage.PromptTokens) + int64(usage.CompletionTokens),
	}
	if cached > 0 {
		buckets["input_cached_tokens"] = cached
	}
	if reasoning > 0 {
		buckets["output_reasoning_tokens"] = reasoning
	}
	return buckets
}

func ingested(obs observation) bool {
	return len(obs.UsageDetails) > 0 && len(obs.Output) > 0
}

func TestAzureUnary(t *testing.T) {
	r := newRun(t)
	env := azureEnv(t)
	client := azureClient(r, env, env["AZURE_OPENAI_DEPLOYMENT"])

	var response gopenai.ChatCompletionResponse
	traceID, err := r.call(t, "azure-unary", func(ctx context.Context) error {
		var callErr error
		response, callErr = client.CreateChatCompletion(ctx, gopenai.ChatCompletionRequest{
			Model:       "azure-mapped", // replaced by the explicit mapper
			Temperature: 0,
			MaxTokens:   16,
			Messages: []gopenai.ChatCompletionMessage{
				{
					Role:    gopenai.ChatMessageRoleUser,
					Content: "Reply with one short word. Marker: " + r.marker,
				},
			},
		})
		return callErr
	})
	if err != nil {
		t.Fatalf("azure unary call: %v", err)
	}
	if len(response.Choices) == 0 || response.Choices[0].Message.Content == "" {
		t.Fatal("inconclusive: the SDK response carries no comparable output")
	}

	got := r.observation(t, traceID, "openai.chat.completions", ingested)
	checkObservation(t, got, expectedObservation{
		Name:         "openai.chat.completions",
		Type:         "GENERATION",
		Model:        response.Model,
		RequestModel: "azure-mapped",
		TraceID:      traceID,
		Usage:        azureUsage(response.Usage),
		Output: map[string]any{
			"role":    "assistant",
			"content": response.Choices[0].Message.Content,
		},
		InputMarker: r.marker,
		Metadata: map[string]string{
			"provider":         "azure-openai",
			"azure.deployment": env["AZURE_OPENAI_DEPLOYMENT"],
			"api_version":      env["AZURE_OPENAI_API_VERSION"],
			"finish_reason":    string(response.Choices[0].FinishReason),
		},
	})
}

func TestAzureStreaming(t *testing.T) {
	r := newRun(t)
	env := azureEnv(t)
	client := azureClient(r, env, env["AZURE_OPENAI_DEPLOYMENT"])

	var aggregated strings.Builder
	var lastUsage *gopenai.Usage
	var lastModel, finishReason string
	traceID, err := r.call(t, "azure-stream", func(ctx context.Context) error {
		stream, callErr := client.CreateChatCompletionStream(ctx, gopenai.ChatCompletionRequest{
			Model:         "azure-mapped",
			Temperature:   0,
			MaxTokens:     16,
			Stream:        true,
			StreamOptions: &gopenai.StreamOptions{IncludeUsage: true},
			Messages: []gopenai.ChatCompletionMessage{
				{
					Role:    gopenai.ChatMessageRoleUser,
					Content: "Reply with one short word. Marker: " + r.marker,
				},
			},
		})
		if callErr != nil {
			return callErr
		}
		defer stream.Close()
		for {
			chunk, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				return nil
			}
			if recvErr != nil {
				return recvErr
			}
			if chunk.Model != "" {
				lastModel = chunk.Model
			}
			if chunk.Usage != nil {
				lastUsage = chunk.Usage
			}
			for _, choice := range chunk.Choices {
				aggregated.WriteString(choice.Delta.Content)
				if choice.FinishReason != "" {
					finishReason = string(choice.FinishReason)
				}
			}
		}
	})
	if err != nil {
		t.Fatalf("azure streaming call: %v", err)
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
		RequestModel: "azure-mapped",
		TraceID:      traceID,
		Usage:        azureUsage(*lastUsage),
		Output:       aggregated.String(),
		InputMarker:  r.marker,
		Stream:       true,
		Metadata: map[string]string{
			"provider":         "azure-openai",
			"azure.deployment": env["AZURE_OPENAI_DEPLOYMENT"],
			"api_version":      env["AZURE_OPENAI_API_VERSION"],
			"finish_reason":    finishReason,
		},
	})
}

// TestAzureError is a token-free authenticated request against a
// deliberately invalid deployment; the expected status comes from the
// SDK-reported code, never a pinned number.
func TestAzureError(t *testing.T) {
	r := newRun(t)
	env := azureEnv(t)
	invalid := fmt.Sprintf("validation-invalid-%s", r.marker)
	client := azureClient(r, env, invalid)

	traceID, err := r.call(t, "azure-error", func(ctx context.Context) error {
		_, callErr := client.CreateChatCompletion(ctx, gopenai.ChatCompletionRequest{
			Model:     "azure-mapped",
			MaxTokens: 1,
			Messages: []gopenai.ChatCompletionMessage{
				{Role: gopenai.ChatMessageRoleUser, Content: "marker " + r.marker},
			},
		})
		return callErr
	})
	if err == nil {
		t.Fatal("invalid deployment unexpectedly succeeded")
	}
	var apiErr *gopenai.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *gopenai.APIError, got %T: %v", err, err)
	}

	got := r.observation(t, traceID, "openai.chat.completions")
	checkObservation(t, got, expectedObservation{
		Name:         "openai.chat.completions",
		Type:         "GENERATION",
		RequestModel: "azure-mapped",
		TraceID:      traceID,
		InputMarker:  r.marker,
		Status:       fmt.Sprintf("http %d", apiErr.HTTPStatusCode),
		Metadata: map[string]string{
			"provider":         "azure-openai",
			"azure.deployment": invalid,
			"api_version":      env["AZURE_OPENAI_API_VERSION"],
		},
	})
}
