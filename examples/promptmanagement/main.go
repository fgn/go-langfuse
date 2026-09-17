// This example writes uniquely named synthetic prompts only with -write.
// Use a disposable Langfuse project. It never assigns production or deletes data.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/api"
)

func main() {
	write := flag.Bool("write", false, "allow synthetic prompt writes to the configured project")
	flag.Parse()
	if !*write {
		fmt.Println("No writes performed. Use -write with credentials for a disposable Langfuse project.")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	err := run(ctx, langfuse.ConfigFromEnv(), "sdk-demo/"+rand.Text())
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created text/chat prompts, deployed staging, recorded generations, and rolled staging back.")
}

func run(ctx context.Context, config langfuse.Config, name string) (result error) {
	management, err := api.NewClient(api.Config{
		BaseURL: config.BaseURL, PublicKey: config.PublicKey, SecretKey: config.SecretKey,
	})
	if err != nil {
		return err
	}
	lf, err := langfuse.New(ctx, config)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		result = errors.Join(result, lf.Shutdown(shutdownCtx))
	}()

	first, err := management.Prompts.Create(ctx, api.CreatePromptRequest{
		Name: name, Prompt: api.TextContent("Explain {{topic}}."),
		Labels: api.Set([]string{"staging"}), CommitMessage: api.Set("Synthetic first version"),
	})
	if err != nil {
		return err // Writes are not automatically retried: reconcile ambiguous outcomes.
	}
	_, err = management.Prompts.Create(ctx, api.CreatePromptRequest{
		Name: name + "-chat", Labels: api.Set([]string{"staging"}),
		Prompt: api.ChatContent(api.MessageEntry("system", "Explain {{topic}}."), api.PlaceholderEntry("history")),
	})
	if err != nil {
		return err
	}
	second, err := management.Prompts.Create(ctx, api.CreatePromptRequest{
		Name: name, Prompt: api.TextContent("Explain {{topic}} briefly."),
		Labels: api.Set([]string{}), CommitMessage: api.Set("Synthetic second version"),
	})
	if err != nil {
		return err
	}

	// Prime the cache with v1 to exercise explicit invalidation after deployment.
	initial, err := lf.GetPrompt(ctx, name, langfuse.PromptQuery{Label: "staging"})
	if err != nil {
		return err
	}
	if initial.Version != first.Version {
		return errors.New("staging did not select the first version")
	}
	chat, err := lf.GetPrompt(ctx, name+"-chat", langfuse.PromptQuery{Label: "staging", Type: langfuse.PromptTypeChat})
	if err != nil {
		return err
	}
	if _, err := chat.CompileStrict(map[string]any{
		"topic": "Go contexts", "history": []langfuse.PromptMessage{{Role: "user", Content: "What is cancellation?"}},
	}); err != nil {
		return err
	}

	// Deploy v2, then roll back to v1. Labels move; stored content is immutable.
	for _, version := range []int{second.Version, first.Version} {
		if _, err := management.Prompts.UpdateLabels(ctx, name, version, []string{"staging"}); err != nil {
			return err
		}
		lf.InvalidatePromptCache(name)
		prompt, err := lf.GetPrompt(ctx, name, langfuse.PromptQuery{Label: "staging"})
		if err != nil {
			return err
		}
		if prompt.Version != version || prompt.Source != langfuse.PromptSourceServer {
			return errors.New("deployment returned a stale prompt version")
		}
		compiled, err := prompt.CompileStrict(map[string]any{"topic": "Go contexts"})
		if err != nil {
			return err
		}
		if err := lf.Observe(ctx, "prompt-deployment-demo", langfuse.TypeGeneration,
			langfuse.ObservationAttributes{Input: compiled.Text, Prompt: prompt.Ref()},
			func(ctx context.Context, observation *langfuse.Observation) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				// Deterministic task: no model account or provider SDK is needed.
				observation.Update(langfuse.ObservationAttributes{Output: "Contexts carry deadlines and cancellation."})
				return nil
			}); err != nil {
			return err
		}
	}
	return nil
}
