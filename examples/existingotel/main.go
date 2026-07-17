package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/fgn/lunte"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	// In a real application, this provider already has the application's
	// sampler, resource, and one or more unrelated span processors.
	provider := sdktrace.NewTracerProvider()

	cfg := lunte.ConfigFromEnv()
	cfg.TracerProvider = provider
	lf, err := lunte.New(ctx, cfg)
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(shutdownCtx)
		return fmt.Errorf("attach Lunte: %w", err)
	}

	workErr := handleRequest(ctx, provider, lf)

	// End application work first, then unregister and stop Lunte's processor,
	// then let the application shut down its provider and remaining processors.
	lunteCtx, cancelLunte := context.WithTimeout(context.Background(), 5*time.Second)
	lunteErr := lf.Shutdown(lunteCtx)
	cancelLunte()

	providerCtx, cancelProvider := context.WithTimeout(context.Background(), 5*time.Second)
	providerErr := provider.Shutdown(providerCtx)
	cancelProvider()

	return errors.Join(workErr, lunteErr, providerErr)
}

func handleRequest(ctx context.Context, provider *sdktrace.TracerProvider, lf *lunte.Client) error {
	tracer := provider.Tracer("example.com/backend/http")
	ctx, requestSpan := tracer.Start(ctx, "POST /chat")
	defer requestSpan.End()

	ctx = lf.WithTraceAttributes(ctx, lunte.TraceAttributes{
		Name:      "chat-request",
		UserID:    "user-123",
		SessionID: "conversation-456",
		Metadata:  map[string]any{"route": "/chat"},
	})

	// The already-started requestSpan is not changed retroactively. Spans started
	// from ctx after WithTraceAttributes receive the Langfuse annotations, which
	// are also visible to the application's other exporters.
	requestSpan.SetAttributes(attribute.String("example.request.kind", "interactive"))

	_, generation := lf.StartObservation(
		ctx,
		"generate-answer",
		lunte.TypeGeneration,
		lunte.ObservationAttributes{
			Model: "gemini-2.5-flash",
			Input: "Explain borrowed tracer providers.",
		},
	)
	defer generation.End()

	generation.Update(lunte.ObservationAttributes{
		Output: "Lunte registers one additional processor on the existing provider.",
		Usage: &lunte.Usage{
			InputTokens:  6,
			OutputTokens: 11,
		},
	})

	return nil
}
