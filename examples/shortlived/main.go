package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/fgn/lunte"
)

// This example shows a short-lived job, an event, masking, and an explicit
// flush. Set LANGFUSE_TRACING_ENABLED=false to run the same code as a no-op.
func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) (runErr error) {
	cfg := lunte.ConfigFromEnv()
	cfg.Mask = redactSDKValue

	client, err := lunte.New(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create Lunte client: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		runErr = errors.Join(runErr, client.Shutdown(shutdownCtx))
	}()

	ctx = client.WithTraceAttributes(ctx, lunte.TraceAttributes{
		Name: "nightly-summary",
		Tags: []string{"job", "nightly"},
	})
	client.Event(ctx, "job-started", lunte.ObservationAttributes{
		Metadata: map[string]any{"attempt": 1, "customer_id": "secret-customer-123"},
	})

	_, observation := client.StartObservation(ctx, "summarize", lunte.TypeGeneration,
		lunte.ObservationAttributes{Input: "secret source text", Model: "example-model"})
	observation.Update(lunte.ObservationAttributes{
		Output: "summary",
		Usage:  &lunte.Usage{InputTokens: 3, OutputTokens: 1},
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

// Mask receives input/output values individually and each complete metadata
// map. Return copied collections so caller-owned values are never mutated.
func redactSDKValue(value any) any {
	switch value := value.(type) {
	case string:
		return strings.ReplaceAll(value, "secret", "[redacted]")
	case map[string]any:
		redacted := make(map[string]any, len(value))
		for key, item := range value {
			if strings.EqualFold(key, "customer_id") {
				redacted[key] = "[redacted]"
				continue
			}
			redacted[key] = redactSDKValue(item)
		}
		return redacted
	case []any:
		redacted := make([]any, len(value))
		for index, item := range value {
			redacted[index] = redactSDKValue(item)
		}
		return redacted
	default:
		return value
	}
}
