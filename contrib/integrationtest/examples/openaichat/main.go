// This example shows what the langfuseopenai adapter buys: a completely
// ordinary sashabaranov/go-openai streaming chat call whose model,
// token usage, output, time-to-first-token, and status land in Langfuse
// automatically, nested under the application's logical span, with no
// call-site changes and no hand-rolled attribute assembly.
//
// It runs out of the box: without OPENAI_BASE_URL it starts a synthetic
// OpenAI-wire server, so only Langfuse credentials are needed:
//
//	export LANGFUSE_BASE_URL=http://localhost:3000
//	export LANGFUSE_PUBLIC_KEY=pk-lf-... LANGFUSE_SECRET_KEY=sk-lf-...
//	go run ./examples/openaichat
//
// Set OPENAI_BASE_URL and OPENAI_API_KEY to run against a real
// OpenAI-compatible endpoint instead.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	openai "github.com/sashabaranov/go-openai"

	"github.com/fgn/go-langfuse"
	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		return fmt.Errorf("create Langfuse client: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := lf.Shutdown(shutdownCtx); err != nil {
			log.Printf("shut down Langfuse client: %v", err)
		}
	}()

	baseURL, apiKey := os.Getenv("OPENAI_BASE_URL"), os.Getenv("OPENAI_API_KEY")
	if baseURL == "" {
		server := syntheticProvider()
		defer server.Close()
		baseURL, apiKey = server.URL+"/v1", "synthetic-key"
	}

	// The complete integration: one line at client construction. Every
	// call site below is plain go-openai.
	cfg := openai.DefaultConfig(apiKey)
	cfg.BaseURL = baseURL
	cfg.HTTPClient = &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
	client := openai.NewClientWithConfig(cfg)

	ctx = lf.WithTraceAttributes(ctx, langfuse.TraceAttributes{
		Name: "answer-question", UserID: "user-123", SessionID: "conversation-456",
	})

	// The logical operation is a span (never a generation: the adapter's
	// attempt carries the model and usage, so metrics are not double
	// counted). The adapter's generation nests under it automatically
	// because the context flows through the SDK into the HTTP request.
	var answer string
	err = lf.Observe(ctx, "answer-question", langfuse.TypeSpan,
		langfuse.ObservationAttributes{},
		func(ctx context.Context, root *langfuse.Observation) error {
			stream, err := client.CreateChatCompletionStream(ctx, openai.ChatCompletionRequest{
				Model:         "gpt-test",
				Stream:        true,
				StreamOptions: &openai.StreamOptions{IncludeUsage: true},
				Messages: []openai.ChatCompletionMessage{
					{Role: openai.ChatMessageRoleUser, Content: "What does the adapter record?"},
				},
			})
			if err != nil {
				return err
			}
			defer stream.Close()
			for {
				chunk, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return err
				}
				if len(chunk.Choices) > 0 {
					answer += chunk.Choices[0].Delta.Content
				}
			}
			fmt.Println("model answered:", answer)
			fmt.Println("trace:", root.TraceID())
			return nil
		})
	if err != nil {
		return err
	}

	fmt.Println("Open the trace in Langfuse: the generation attempt carries the")
	fmt.Println("response model, exact token usage (including the usage chunk that")
	fmt.Println("arrives after finish), streamed output, time-to-first-token, and")
	fmt.Println("provider metadata; the logical span groups it without duplicating")
	fmt.Println("usage. Nothing in the go-openai call sites changed.")
	return nil
}

// syntheticProvider speaks just enough OpenAI wire format for the
// example to run without provider credentials.
func syntheticProvider() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, chunk := range []string{
			`data: {"choices":[{"index":0,"delta":{"role":"assistant"}}],"model":"gpt-test-2024"}`,
			`data: {"choices":[{"index":0,"delta":{"content":"Model, usage, output, "}}]}`,
			`data: {"choices":[{"index":0,"delta":{"content":"timing, and status."}}]}`,
			`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`data: {"choices":[],"usage":{"prompt_tokens":9,"completion_tokens":7}}`,
			`data: [DONE]`,
		} {
			_, _ = io.WriteString(w, chunk+"\n\n")
			flusher.Flush()
		}
	}))
}
