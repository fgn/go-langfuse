// This example shows what the langfuseopenai adapter buys with the
// official OpenAI Go SDK (github.com/openai/openai-go): a completely
// ordinary streaming chat call whose model, token usage, output,
// time-to-first-token, and status land in Langfuse automatically,
// nested under the application's logical span, with no call-site
// changes and no hand-rolled attribute assembly.
//
// It runs out of the box: without OPENAI_BASE_URL it starts a synthetic
// OpenAI-wire server, so only Langfuse credentials are needed:
//
//	export LANGFUSE_BASE_URL=http://localhost:3000
//	export LANGFUSE_PUBLIC_KEY=pk-lf-... LANGFUSE_SECRET_KEY=sk-lf-...
//	go run ./examples/openaichat
//
// Set OPENAI_BASE_URL and OPENAI_API_KEY to run against a real
// endpoint instead.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	openai "github.com/openai/openai-go"
	"github.com/openai/openai-go/option"

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

	// The complete integration: hand the SDK an instrumented HTTP
	// client. WithMaxRetries(0) keeps one observation per logical call;
	// with the SDK's default retries each HTTP attempt records its own
	// observation instead (both are documented, honest shapes).
	client := openai.NewClient(
		option.WithAPIKey(apiKey),
		option.WithBaseURL(baseURL),
		option.WithHTTPClient(&http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}),
		option.WithMaxRetries(0),
	)

	ctx = lf.WithTraceAttributes(ctx, langfuse.TraceAttributes{
		Name: "answer-question", UserID: "user-123", SessionID: "conversation-456",
	})

	// The logical operation is a span (never a generation: the adapter's
	// attempt carries the model and usage, so metrics are not double
	// counted). The adapter's generation nests under it automatically
	// because the context flows through the SDK into the HTTP request.
	return lf.Observe(ctx, "answer-question", langfuse.TypeSpan,
		langfuse.ObservationAttributes{},
		func(ctx context.Context, root *langfuse.Observation) error {
			stream := client.Chat.Completions.NewStreaming(ctx, openai.ChatCompletionNewParams{
				Model: openai.ChatModel("gpt-test"),
				Messages: []openai.ChatCompletionMessageParamUnion{
					openai.UserMessage("What does the adapter record?"),
				},
				StreamOptions: openai.ChatCompletionStreamOptionsParam{
					IncludeUsage: openai.Bool(true),
				},
			})
			accumulator := openai.ChatCompletionAccumulator{}
			for stream.Next() {
				accumulator.AddChunk(stream.Current())
			}
			if err := stream.Err(); err != nil {
				return err
			}
			if err := stream.Close(); err != nil {
				return err
			}

			if len(accumulator.Choices) > 0 {
				fmt.Println("model answered:", accumulator.Choices[0].Message.Content)
			}
			fmt.Println("trace:", root.TraceID())
			fmt.Println("Open the trace in Langfuse: the generation attempt carries the")
			fmt.Println("response model, exact token usage (including the usage chunk that")
			fmt.Println("arrives after finish), streamed output, time-to-first-token, and")
			fmt.Println("provider metadata; the logical span groups it without duplicating")
			fmt.Println("usage. Nothing in the openai-go call sites changed.")
			return nil
		})
}

// syntheticProvider speaks just enough OpenAI wire format for the
// example to run without provider credentials.
func syntheticProvider() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, chunk := range []string{
			`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant"}}],"model":"gpt-test-2024"}`,
			`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Model, usage, output, "}}]}`,
			`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"timing, and status."}}]}`,
			`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`data: {"id":"c1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":9,"completion_tokens":7}}`,
			`data: [DONE]`,
		} {
			_, _ = io.WriteString(w, chunk+"\n\n")
			flusher.Flush()
		}
	}))
}
