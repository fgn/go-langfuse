package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/fgn/go-langfuse"
)

// This example shows a short-lived job, an event, masking, and an explicit
// flush. Set LANGFUSE_TRACING_ENABLED=false to run the same code as a no-op.
func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) (runErr error) {
	cfg := langfuse.ConfigFromEnv()
	cfg.Mask = redactSDKValue

	client, err := langfuse.New(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create Langfuse client: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		runErr = errors.Join(runErr, client.Shutdown(shutdownCtx))
	}()

	ctx = client.WithTraceAttributes(ctx, langfuse.TraceAttributes{
		Name: "nightly-summary",
		Tags: []string{"job", "nightly"},
	})
	client.Event(ctx, "job-started", langfuse.ObservationAttributes{
		Metadata: map[string]any{"attempt": 1, "customer_id": "secret-customer-123"},
	})

	_, observation := client.StartObservation(ctx, "summarize", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{Input: "secret source text", Model: "example-model"})
	observation.Update(langfuse.ObservationAttributes{
		Output: "summary",
		Usage:  &langfuse.Usage{InputTokens: 3, OutputTokens: 1},
	})
	observation.End()

	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = client.Flush(flushCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("flush telemetry: %w", err)
	}
	return nil
}

// Mask receives the field with each value. This policy removes all observation
// content and recursively redacts selected metadata keys.
func redactSDKValue(field langfuse.MaskField, value any) any {
	switch field {
	case langfuse.MaskObservationInput, langfuse.MaskObservationOutput:
		return "[redacted]"
	case langfuse.MaskTraceMetadata, langfuse.MaskObservationMetadata, langfuse.MaskScoreMetadata:
		return redactMetadata(value)
	default:
		return nil
	}
}

func redactMetadata(value any) any {
	switch value := value.(type) {
	case map[string]any:
		redacted := make(map[string]any, len(value))
		for key, item := range value {
			if strings.EqualFold(key, "customer_id") {
				redacted[key] = "[redacted]"
				continue
			}
			redacted[key] = redactMetadata(item)
		}
		return redacted
	case []any:
		redacted := make([]any, len(value))
		for index, item := range value {
			redacted[index] = redactMetadata(item)
		}
		return redacted
	default:
		return value
	}
}
