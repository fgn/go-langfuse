package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fgn/lunte"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	lf, err := lunte.New(ctx, lunte.ConfigFromEnv())
	if err != nil {
		return fmt.Errorf("create Lunte client: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := lf.Shutdown(shutdownCtx); err != nil {
			log.Printf("shut down Lunte: %v", err)
		}
	}()

	ctx = lf.WithTraceAttributes(ctx, lunte.TraceAttributes{
		Name:      "chat-turn",
		UserID:    "user-123",
		SessionID: "conversation-456",
		Tags:      []string{"chat"},
	})

	question := "What is context in Go?"
	rootCtx, root := lf.StartObservation(
		ctx,
		"chat-turn",
		lunte.TypeAgent,
		lunte.ObservationAttributes{Input: question},
	)
	defer root.End()

	messages := []string{question}
	generationCtx, generation := lf.StartObservation(
		rootCtx,
		"generate-answer",
		lunte.TypeGeneration,
		lunte.ObservationAttributes{
			Model: "gemini-2.5-flash",
			Input: messages,
		},
	)
	defer generation.End()

	answer, usage, err := callModel(generationCtx, messages)
	if err != nil {
		generation.RecordError(err)
		root.RecordError(err)
		return err
	}

	generation.Update(lunte.ObservationAttributes{
		Output: answer,
		Usage:  &usage,
	})
	root.Update(lunte.ObservationAttributes{Output: answer})

	return nil
}

// callModel stands in for a provider SDK. Pass ctx to the real provider so
// cancellation and any provider-created child spans retain the generation as
// their parent.
func callModel(ctx context.Context, messages []string) (string, lunte.Usage, error) {
	select {
	case <-ctx.Done():
		return "", lunte.Usage{}, ctx.Err()
	default:
	}

	return "Context carries deadlines, cancellation, and request-scoped values.", lunte.Usage{
		InputTokens:  int64(len(messages) * 6),
		OutputTokens: 10,
	}, nil
}
