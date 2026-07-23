package integrationtest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"

	openaigo "github.com/openai/openai-go"
	"github.com/openai/openai-go/option"

	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

func officialClient(baseURL string, httpClient *http.Client, extra ...option.RequestOption) openaigo.Client {
	opts := append([]option.RequestOption{
		option.WithAPIKey("synthetic-key"),
		option.WithBaseURL(baseURL),
		option.WithHTTPClient(httpClient),
	}, extra...)
	return openaigo.NewClient(opts...)
}

// TestOfficialOpenAIUnary drives the official openai-go client through
// the instrumented transport: the SDK's result must be untouched and
// the exported generation must carry model, content, and usage.
func TestOfficialOpenAIUnary(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"id":"chatcmpl-1","object":"chat.completion","model":"example-model-002",
			"choices":[{"index":0,"finish_reason":"stop",
				"message":{"role":"assistant","content":"official answer"}}],
			"usage":{"prompt_tokens":7,"completion_tokens":3}
		}`)
	}))
	t.Cleanup(provider.Close)

	client := officialClient(provider.URL+"/v1",
		&http.Client{Transport: langfuseopenai.NewTransport(lf, nil)},
		option.WithMaxRetries(0))
	completion, err := client.Chat.Completions.New(context.Background(), openaigo.ChatCompletionNewParams{
		Model: openaigo.ChatModel("example-model"),
		Messages: []openaigo.ChatCompletionMessageParamUnion{
			openaigo.UserMessage("synthetic question"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if completion.Choices[0].Message.Content != "official answer" {
		t.Fatalf("SDK result altered: %+v", completion.Choices)
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

// TestOfficialOpenAIStreamingWithAccumulator locks the terminal
// behavior against the official SDK's stream reader, which
// deliberately keeps draining after [DONE]: the frozen observation
// must keep serving reads transparently and still complete cleanly
// with the post-finish usage chunk.
func TestOfficialOpenAIStreamingWithAccumulator(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, chunk := range []string{
			`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant"}}],"model":"example-model-002"}`,
			`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"streamed "}}]}`,
			`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"official"}}]}`,
			`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`data: {"id":"c1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":5,"completion_tokens":2}}`,
			`data: [DONE]`,
		} {
			_, _ = io.WriteString(w, chunk+"\n\n")
			flusher.Flush()
		}
	}))
	t.Cleanup(provider.Close)

	client := officialClient(provider.URL+"/v1",
		&http.Client{Transport: langfuseopenai.NewTransport(lf, nil)},
		option.WithMaxRetries(0))
	stream := client.Chat.Completions.NewStreaming(context.Background(), openaigo.ChatCompletionNewParams{
		Model: openaigo.ChatModel("example-model"),
		Messages: []openaigo.ChatCompletionMessageParamUnion{
			openaigo.UserMessage("synthetic question"),
		},
		StreamOptions: openaigo.ChatCompletionStreamOptionsParam{
			IncludeUsage: openaigo.Bool(true),
		},
	})
	accumulator := openaigo.ChatCompletionAccumulator{}
	for stream.Next() {
		accumulator.AddChunk(stream.Current())
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if accumulator.Choices[0].Message.Content != "streamed official" {
		t.Fatalf("SDK accumulation altered: %+v", accumulator.Choices)
	}

	flush(t, lf)
	span := receiver.nextSpan(t)
	if got := attrString(span, "langfuse.observation.output"); got != "streamed official" {
		t.Fatalf("stream output %q", got)
	}
	if got := attrString(span, "langfuse.observation.status_message"); got != "" {
		t.Fatalf("clean official stream carries status %q", got)
	}
	if got := attrString(span, "langfuse.observation.usage_details"); got == "" {
		t.Fatal("post-finish usage lost through official SDK consumption")
	}
	if got := attrString(span, "langfuse.observation.completion_start_time"); got == "" {
		t.Fatal("completion start time missing")
	}
}

// TestOfficialOpenAIDefaultRetriesRecordPerAttempt locks the
// documented attempt semantics with the SDK's default retry loop: a
// transient 500 followed by success yields two observations, the
// failed attempt with its fixed http category and the successful one
// with content.
func TestOfficialOpenAIDefaultRetriesRecordPerAttempt(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf := newTestClient(t, receiver)
	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"error":{"message":"transient"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"example-model-002","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"recovered"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	}))
	t.Cleanup(provider.Close)

	client := officialClient(provider.URL+"/v1",
		&http.Client{Transport: langfuseopenai.NewTransport(lf, nil)})
	completion, err := client.Chat.Completions.New(context.Background(), openaigo.ChatCompletionNewParams{
		Model: openaigo.ChatModel("example-model"),
		Messages: []openaigo.ChatCompletionMessageParamUnion{
			openaigo.UserMessage("q"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if completion.Choices[0].Message.Content != "recovered" {
		t.Fatalf("SDK retry result altered: %+v", completion.Choices)
	}

	// The SDK abandons the failed attempt's body without Close; the
	// adapter's GC safety net finalizes it. Encourage collection so the
	// test is deterministic.
	statuses := map[string]bool{}
	for len(statuses) < 2 {
		runtime.GC()
		flush(t, lf)
		span := receiver.nextSpan(t)
		statuses[attrString(span, "langfuse.observation.status_message")] = true
	}
	if !statuses["http 500"] || !statuses[""] {
		t.Fatalf("per-attempt semantics: statuses %v, want failed attempt and clean success", statuses)
	}
}
