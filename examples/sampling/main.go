package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fgn/go-langfuse"
)

// This example runs a handful of requests on a high-volume route with a 25%
// per-request sample rate and gates an expensive LLM-judge evaluation to a
// 5% fraction. Both decisions are deterministic in the trace ID, and a
// smaller fraction always selects a subset of a larger one, so every judged
// trace is guaranteed to be among the traces kept for export.
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

	for request := 1; request <= 8; request++ {
		if err := classify(ctx, lf, request); err != nil {
			return err
		}
	}
	return nil
}

func classify(ctx context.Context, lf *langfuse.Client, request int) error {
	ctx = lf.WithTraceAttributes(ctx, langfuse.TraceAttributes{
		Name:   "classify-ticket",
		UserID: fmt.Sprintf("user-%d", request),
		Tags:   []string{"high-volume"},
	})
	// Set the rate once per request, before the first observation; the whole
	// trace is then kept or dropped together. Without this override the
	// client-wide Config.SampleRate (default: keep everything) applies.
	ctx = lf.WithSampleRate(ctx, 0.25)

	// Start with minimal fields: start attributes are built before the
	// sampling decision is known.
	_, generation := lf.StartObservation(ctx, "classify-ticket",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{Model: "example-model"})
	defer generation.End()

	category := "billing"
	if generation.Sampled() {
		// Attach expensive payloads only when the trace is kept for export.
		generation.Update(langfuse.ObservationAttributes{
			Input:  fmt.Sprintf("ticket %d: the invoice total looks wrong", request),
			Output: category,
			Usage:  &langfuse.Usage{InputTokens: 12, OutputTokens: 1},
		})
	}

	// TraceSampledAt at 5% selects a subset of the traces kept at 25%, so
	// the judge never evaluates a trace that was dropped from export.
	judged, err := langfuse.TraceSampledAt(generation.TraceID(), 0.05)
	if err != nil {
		return fmt.Errorf("judge gate: %w", err)
	}
	if judged && generation.Sampled() {
		verdict := 0.9 // an LLM judge would grade the classification here
		if err := lf.RecordScore(ctx, langfuse.Score{
			Name:         "judge-accuracy",
			TraceID:      generation.TraceID(),
			NumericValue: &verdict,
		}); err != nil {
			return fmt.Errorf("record judge score: %w", err)
		}
	}

	fmt.Printf("trace %s exported=%v judged=%v\n",
		generation.TraceID(), generation.Sampled(), judged && generation.Sampled())
	return nil
}
