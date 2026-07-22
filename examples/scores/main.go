package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fgn/go-langfuse"
)

// This example traces one chat turn and attaches three scores: numeric user
// feedback on the session, a boolean check on the exact generation, and a
// categorical label on the whole trace. RecordScore validates synchronously,
// so every returned error means the score was not accepted; accepted scores
// are delivered asynchronously and drained by Flush and Shutdown.
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

	ctx = lf.WithTraceAttributes(ctx, langfuse.TraceAttributes{
		Name:      "chat-turn",
		UserID:    "user-123",
		SessionID: "conversation-456",
	})

	_, generation := lf.StartObservation(ctx, "generate-answer",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{
			Model: "example-model",
			Input: "How do I cancel a Go context?",
		})
	generation.Update(langfuse.ObservationAttributes{
		Output: "Call the cancel function returned by context.WithCancel.",
		Usage:  &langfuse.Usage{InputTokens: 8, OutputTokens: 10},
	})
	generation.End()

	// User feedback targets the session. A caller-chosen ID makes
	// resubmission an upsert, so a user revising their rating cannot create
	// a duplicate score.
	rating := 4.0
	if err := lf.RecordScore(ctx, langfuse.Score{
		ID:           "feedback-conversation-456",
		Name:         "user-feedback",
		SessionID:    "conversation-456",
		NumericValue: &rating,
		Comment:      "clear and concise answer",
	}); err != nil {
		return fmt.Errorf("record feedback: %w", err)
	}

	// An automated check targets the exact generation through TraceID plus
	// ObservationID. Boolean scores use NumericValue 0 or 1.
	grounded := 1.0
	if err := lf.RecordScore(ctx, langfuse.Score{
		Name:          "grounded",
		TraceID:       generation.TraceID(),
		ObservationID: generation.ID(),
		NumericValue:  &grounded,
		DataType:      langfuse.ScoreTypeBoolean,
	}); err != nil {
		return fmt.Errorf("record grounded check: %w", err)
	}

	// A string value scores the whole trace and is inferred as categorical.
	// Score.Timestamp would backdate a score computed by a later evaluation
	// job; the zero value means "now".
	tone := "helpful"
	if err := lf.RecordScore(ctx, langfuse.Score{
		Name:        "tone",
		TraceID:     generation.TraceID(),
		StringValue: &tone,
		Metadata:    map[string]any{"evaluator": "tone-classifier-v2"},
	}); err != nil {
		return fmt.Errorf("record tone: %w", err)
	}

	// Flush waits for the exported generation and all queued scores while
	// keeping the client usable; the deferred Shutdown would also drain them.
	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := lf.Flush(flushCtx); err != nil {
		return fmt.Errorf("flush telemetry: %w", err)
	}
	fmt.Printf("scored trace %s\n", generation.TraceID())
	return nil
}
