package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/fgn/go-langfuse"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	client, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(shutdownCtx)
	}()

	ctx, generation := client.StartObservation(ctx, "stream-answer", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{Input: "Explain streaming", Model: "example-model"})

	var answer strings.Builder
	for chunk, streamErr := range streamModel(ctx) {
		if streamErr != nil {
			generation.RecordError(streamErr)
			generation.End()
			return streamErr
		}
		answer.WriteString(chunk)
	}
	generation.Update(langfuse.ObservationAttributes{
		Output: answer.String(),
		Usage:  &langfuse.Usage{InputTokens: 2, OutputTokens: 4},
	})
	// End only after the stream is fully consumed so duration and output are
	// complete and the ended span can be flushed.
	generation.End()
	return nil
}

func streamModel(ctx context.Context) func(func(string, error) bool) {
	return func(yield func(string, error) bool) {
		for _, chunk := range []string{"Streaming ", "returns ", "partial results."} {
			select {
			case <-ctx.Done():
				yield("", fmt.Errorf("stream model: %w", ctx.Err()))
				return
			default:
				if !yield(chunk, nil) {
					return
				}
			}
		}
	}
}
