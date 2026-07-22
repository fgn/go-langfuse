package langfuseopenai_test

import (
	"context"
	"log"
	"net/http"

	"github.com/fgn/go-langfuse"
	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

// Attach the transport where the HTTP client is constructed; provider
// SDK call sites do not change.
func ExampleNewTransport() {
	lf, err := langfuse.New(context.Background(), langfuse.ConfigFromEnv())
	if err != nil {
		log.Fatal(err)
	}
	httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}

	// Pass httpClient to any OpenAI-wire SDK, for example:
	//   cfg := openai.DefaultConfig(token)      // sashabaranov/go-openai
	//   cfg.HTTPClient = httpClient
	// or:
	//   openaisdk.NewClient(option.WithHTTPClient(httpClient))
	_ = httpClient
}

// ContextWithCall attaches application knowledge, such as a prompt
// link, to the attempts recorded under a context.
func ExampleContextWithCall() {
	lf, err := langfuse.New(context.Background(), langfuse.ConfigFromEnv())
	if err != nil {
		log.Fatal(err)
	}
	prompt, err := lf.GetPrompt(context.Background(), "response-template", langfuse.PromptQuery{
		Fallback: &langfuse.PromptFallback{Text: "Answer {{question}} briefly."},
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx := langfuseopenai.ContextWithCall(context.Background(), langfuseopenai.CallAttributes{
		Name:   "answer-question",
		Prompt: prompt.Ref(),
	})
	// Provider calls made with ctx now record attempts named
	// "answer-question", linked to the prompt version.
	_ = ctx
}
