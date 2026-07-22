package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fgn/go-langfuse"
)

// This example loads a prompt from Langfuse prompt management, compiles it,
// applies its config, and links the exact prompt version to a generation.
// The fallback keeps the program working when the prompt does not exist yet
// or Langfuse is unreachable.
//
// To see a server-backed result, create a text prompt named
// "summarize-topic" in Langfuse with the body
// "Summarize {{topic}} in {{sentences}} sentences." and the production
// label; an optional config such as {"model": "example-model-large"}
// overrides the defaults below.
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := lf.Shutdown(shutdownCtx); err != nil {
			log.Printf("shut down Langfuse client: %v", err)
		}
	}()

	// Selection defaults to the production label; PromptQuery.Version or
	// PromptQuery.Label pin others. Source reports whether the result came
	// from the server, the cache, a stale entry, or the fallback.
	prompt, err := lf.GetPrompt(ctx, "summarize-topic", langfuse.PromptQuery{
		Type: langfuse.PromptTypeText,
		Fallback: &langfuse.PromptFallback{
			Text: "Summarize {{topic}} in {{sentences}} sentences.",
		},
	})
	if err != nil {
		return fmt.Errorf("get prompt: %w", err)
	}
	fmt.Printf("prompt source=%s version=%d\n", prompt.Source, prompt.Version)

	// CompileStrict names every unresolved {{variable}}; Compile is the
	// lenient variant that leaves unresolved variables verbatim.
	compiled, err := prompt.CompileStrict(map[string]any{
		"topic":     "Go contexts",
		"sentences": 2,
	})
	if err != nil {
		return fmt.Errorf("compile prompt: %w", err)
	}

	// Initialize defaults first; DecodeConfig overrides only the fields the
	// prompt's server-side config actually sets.
	modelCfg := struct {
		Model       string  `json:"model"`
		Temperature float64 `json:"temperature"`
	}{Model: "example-model", Temperature: 0.2}
	if err := prompt.DecodeConfig(&modelCfg); err != nil {
		return fmt.Errorf("decode prompt config: %w", err)
	}

	// Prompt: prompt.Ref() links the generation to the exact prompt version
	// in Langfuse. Ref returns nil for a fallback prompt, safely skipping
	// the link.
	return lf.Observe(ctx, "summarize", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{
			Input:           compiled.Text,
			Model:           modelCfg.Model,
			ModelParameters: map[string]any{"temperature": modelCfg.Temperature},
			Prompt:          prompt.Ref(),
		},
		func(ctx context.Context, generation *langfuse.Observation) error {
			answer, usage, err := callModel(ctx, compiled.Text)
			if err != nil {
				return err // recorded on the generation automatically
			}
			generation.Update(langfuse.ObservationAttributes{Output: answer, Usage: &usage})
			fmt.Println(answer)
			return nil
		})
}

// Replace this stub with a provider SDK call and pass ctx to that SDK.
func callModel(ctx context.Context, prompt string) (string, langfuse.Usage, error) {
	if err := ctx.Err(); err != nil {
		return "", langfuse.Usage{}, err
	}
	return "Contexts carry deadlines and cancellation. They also carry request-scoped values.",
		langfuse.Usage{InputTokens: int64(len(prompt) / 4), OutputTokens: 14}, nil
}
