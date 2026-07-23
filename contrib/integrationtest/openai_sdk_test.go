package integrationtest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	openai "github.com/sashabaranov/go-openai"

	"github.com/fgn/go-langfuse"
	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

// TestGoOpenAIUnary drives the real sashabaranov/go-openai client
// through the instrumented transport: the SDK's result must be
// untouched and the exported generation must carry model, content, and
// usage.
func TestGoOpenAIUnary(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"id":"chatcmpl-1","object":"chat.completion","model":"example-model-002",
			"choices":[{"index":0,"finish_reason":"stop",
				"message":{"role":"assistant","content":"synthetic answer"}}],
			"usage":{"prompt_tokens":7,"completion_tokens":3}
		}`)
	}))
	t.Cleanup(provider.Close)

	cfg := openai.DefaultConfig("sk-synthetic")
	cfg.BaseURL = provider.URL + "/v1"
	cfg.HTTPClient = &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	client := openai.NewClientWithConfig(cfg)

	response, err := client.CreateChatCompletion(context.Background(), openai.ChatCompletionRequest{
		Model:       "example-model",
		Temperature: 0.25,
		Messages: []openai.ChatCompletionMessage{
			{Role: openai.ChatMessageRoleUser, Content: "synthetic question"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Choices[0].Message.Content != "synthetic answer" {
		t.Fatalf("SDK result altered: %+v", response.Choices)
	}

	flush(t, lf)
	span := receiver.nextSpan(t)
	if span.GetName() != "openai.chat.completions" {
		t.Fatalf("span name %q", span.GetName())
	}
	if got := attrString(span, "langfuse.observation.model.name"); got != "example-model-002" {
		t.Fatalf("model %q", got)
	}
	if got := attrString(span, "langfuse.observation.usage_details"); got == "" {
		t.Fatal("usage missing")
	}
}

// TestGoOpenAIStreaming locks the terminal behavior against the real
// SDK stream reader: go-openai returns io.EOF at [DONE] and typically
// closes without draining; the observation must complete with
// accumulated output and the usage chunk that arrives after the finish
// chunk.
func TestGoOpenAIStreaming(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, chunk := range []string{
			`data: {"choices":[{"index":0,"delta":{"role":"assistant"}}],"model":"example-model-002"}`,
			`data: {"choices":[{"index":0,"delta":{"content":"streamed "}}]}`,
			`data: {"choices":[{"index":0,"delta":{"content":"answer"}}]}`,
			`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":2}}`,
			`data: [DONE]`,
		} {
			_, _ = io.WriteString(w, chunk+"\n\n")
			flusher.Flush()
		}
	}))
	t.Cleanup(provider.Close)

	cfg := openai.DefaultConfig("sk-synthetic")
	cfg.BaseURL = provider.URL + "/v1"
	cfg.HTTPClient = &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	client := openai.NewClientWithConfig(cfg)

	stream, err := client.CreateChatCompletionStream(context.Background(), openai.ChatCompletionRequest{
		Model:  "example-model",
		Stream: true,
		StreamOptions: &openai.StreamOptions{
			IncludeUsage: true,
		},
		Messages: []openai.ChatCompletionMessage{
			{Role: openai.ChatMessageRoleUser, Content: "synthetic question"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var assembled string
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(chunk.Choices) > 0 {
			assembled += chunk.Choices[0].Delta.Content
		}
	}
	_ = stream.Close()
	if assembled != "streamed answer" {
		t.Fatalf("SDK stream altered: %q", assembled)
	}

	flush(t, lf)
	span := receiver.nextSpan(t)
	if got := attrString(span, "langfuse.observation.output"); got != "streamed answer" {
		t.Fatalf("stream output %q", got)
	}
	if got := attrString(span, "langfuse.observation.status_message"); got != "" {
		t.Fatalf("clean SDK stream carries status %q", got)
	}
	if got := attrString(span, "langfuse.observation.usage_details"); got == "" {
		t.Fatal("usage-after-finish lost through real SDK consumption")
	}
	if got := attrString(span, "langfuse.observation.completion_start_time"); got == "" {
		t.Fatal("completion start time missing")
	}
}

// TestGoOpenAIAttemptUnderLogicalSpan locks the documented metric
// pattern with the real SDK: a span-typed logical wrapper with the
// generation attempt nested inside, usage only on the attempt.
func TestGoOpenAIAttemptUnderLogicalSpan(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"m","choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	}))
	t.Cleanup(provider.Close)

	cfg := openai.DefaultConfig("sk-synthetic")
	cfg.BaseURL = provider.URL + "/v1"
	cfg.HTTPClient = &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	client := openai.NewClientWithConfig(cfg)

	err := lf.Observe(context.Background(), "answer-question", langfuse.TypeSpan,
		langfuse.ObservationAttributes{},
		func(ctx context.Context, _ *langfuse.Observation) error {
			_, err := client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
				Model:    "m",
				Messages: []openai.ChatCompletionMessage{{Role: "user", Content: "q"}},
			})
			return err
		})
	if err != nil {
		t.Fatal(err)
	}

	flush(t, lf)
	spans := map[string]bool{}
	for range 2 {
		span := receiver.nextSpan(t)
		spans[span.GetName()] = attrString(span, "langfuse.observation.usage_details") != ""
	}
	if usageOnLogical, ok := spans["answer-question"]; !ok || usageOnLogical {
		t.Fatalf("logical span wrong or carries usage: %v", spans)
	}
	if usageOnAttempt, ok := spans["openai.chat.completions"]; !ok || !usageOnAttempt {
		t.Fatalf("attempt span wrong or missing usage: %v", spans)
	}
}
