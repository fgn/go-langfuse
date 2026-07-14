package existingotel

import (
	"context"
	"os"

	langfuse "github.com/fgn/langfuse-go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// AddLangfuse attaches Langfuse to the application's existing provider.
// Call it once during startup; provider.Shutdown flushes both existing
// processors and Langfuse.
func AddLangfuse(
	ctx context.Context,
	provider *sdktrace.TracerProvider,
) error {
	processor, err := langfuse.NewSpanProcessor(ctx, langfuse.Config{
		BaseURL:   os.Getenv("LANGFUSE_BASE_URL"),
		PublicKey: os.Getenv("LANGFUSE_PUBLIC_KEY"),
		SecretKey: os.Getenv("LANGFUSE_SECRET_KEY"),
	})
	if err != nil {
		return err
	}

	provider.RegisterSpanProcessor(processor)
	return nil
}
