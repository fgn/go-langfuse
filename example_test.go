package langfuse_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fgn/go-langfuse"
)

func ExampleNew() {
	ctx := context.Background()
	lf, err := langfuse.New(ctx, langfuse.Config{
		BaseURL:     "https://cloud.langfuse.com",
		PublicKey:   "pk-lf-...",
		SecretKey:   "sk-lf-...",
		Environment: "production",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = lf.Shutdown(shutdownCtx)
	}()

	_, observation := lf.StartObservation(ctx, "nightly-import", langfuse.TypeSpan,
		langfuse.ObservationAttributes{Input: "42 records"})
	observation.End()
}

func ExampleConfigFromEnv() {
	// ConfigFromEnv reads LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY,
	// LANGFUSE_BASE_URL, LANGFUSE_TRACING_ENVIRONMENT, LANGFUSE_RELEASE,
	// LANGFUSE_SAMPLE_RATE, LANGFUSE_TRACING_ENABLED, and
	// LANGFUSE_CONTENT_CAPTURE_ENABLED.
	lf, err := langfuse.New(context.Background(), langfuse.ConfigFromEnv())
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = lf.Shutdown(shutdownCtx)
	}()
}

func ExampleClient_GetPrompt() {
	// Client setup may be optional in an application. GetPrompt remains safe
	// when the client is nil, so the call site needs no separate nil branch.
	var lf *langfuse.Client
	prompt, err := lf.GetPrompt(context.Background(), "response-template", langfuse.PromptQuery{
		Type:     langfuse.PromptTypeText,
		Fallback: &langfuse.PromptFallback{Text: "Process {{input}}."},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(prompt.Text)
	fmt.Println(prompt.Source)
	// Output:
	// Process {{input}}.
	// fallback
}

func ExampleClient_Observe() {
	ctx := context.Background()
	lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		log.Fatal(err)
	}

	err = lf.Observe(ctx, "retrieve-documents", langfuse.TypeRetriever,
		langfuse.ObservationAttributes{Input: "safe api design"},
		func(ctx context.Context, observation *langfuse.Observation) error {
			// Child observations started from ctx nest under this one. The
			// observation always ends, a returned error is recorded on it,
			// and a panic is marked as a failure before it propagates.
			documents, err := retrieve(ctx, "safe api design")
			if err != nil {
				return err
			}
			observation.Update(langfuse.ObservationAttributes{Output: documents})
			return nil
		})
	if err != nil {
		log.Printf("retrieve: %v", err)
	}
}

func ExampleClient_StartObservation() {
	ctx := context.Background()
	lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		log.Fatal(err)
	}

	// StartObservation suits lifetimes that span functions, such as ending a
	// generation only after a stream is fully consumed.
	generationCtx, generation := lf.StartObservation(ctx, "generate-answer",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{
			Model: "gemini-3.6-flash",
			Input: "What is context in Go?",
		})
	defer generation.End()

	answer, usage, err := callModel(generationCtx, "What is context in Go?")
	if err != nil {
		generation.RecordError(err)
		return
	}
	generation.Update(langfuse.ObservationAttributes{Output: answer, Usage: &usage})
}

func ExampleClient_WithTraceAttributes() {
	ctx := context.Background()
	lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		log.Fatal(err)
	}

	// Stamp request-scoped identity once; observations started from the
	// returned context share the same trace name, user, session, and tags.
	ctx = lf.WithTraceAttributes(ctx, langfuse.TraceAttributes{
		Name:      "chat-turn",
		UserID:    "user-123",
		SessionID: "conversation-456",
		Tags:      []string{"chat"},
	})

	observationCtx, observation := lf.StartObservation(ctx, "chat-turn",
		langfuse.TypeAgent, langfuse.ObservationAttributes{})
	defer observation.End()
	_ = observationCtx
}

func ExampleClient_RecordScore() {
	ctx := context.Background()
	lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		log.Fatal(err)
	}

	rating := 4.0
	err = lf.RecordScore(ctx, langfuse.Score{
		ID:           "feedback-42", // idempotent upsert key
		Name:         "user-feedback",
		SessionID:    "conversation-456",
		NumericValue: &rating,
		Comment:      "clear and concise answer",
	})
	if err != nil {
		log.Printf("record score: %v", err)
	}
}

func ExampleClient_Flush() {
	ctx := context.Background()
	lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		log.Fatal(err)
	}

	lf.Event(ctx, "job-completed", langfuse.ObservationAttributes{
		Metadata: map[string]any{"records": 42},
	})

	// Short-lived jobs and serverless handlers flush before returning so
	// batched observations are exported while the client stays usable.
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lf.Flush(flushCtx); err != nil {
		log.Printf("flush: %v", err)
	}
}

func ExampleClient_WithSampleRate() {
	ctx := context.Background()
	lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		log.Fatal(err)
	}

	// Set the rate once per request, before the first observation. The whole
	// trace is then kept or dropped together, so a high-volume path can run
	// at 2% while other request types keep the configured default.
	ctx = lf.WithSampleRate(ctx, 0.02)

	observationCtx, observation := lf.StartObservation(ctx, "classify-request",
		langfuse.TypeGeneration, langfuse.ObservationAttributes{})
	defer observation.End()

	if observation.Sampled() {
		// Attach expensive payloads only when the trace is kept for export.
		observation.Update(langfuse.ObservationAttributes{Input: "large payload"})
	}
	_ = observationCtx
}

func ExampleTraceSampledAt() {
	// TraceSampledAt is deterministic: every caller agrees on the same trace,
	// and a trace selected at a smaller fraction is always selected at a
	// larger one. Gating an expensive evaluation at 2% therefore evaluates
	// only traces that an export fraction of at least 2% also kept.
	traceID := "0af7651916cd43dd8448eb211c80319c"

	quarter, err := langfuse.TraceSampledAt(traceID, 0.25)
	if err != nil {
		log.Fatal(err)
	}
	majority, err := langfuse.TraceSampledAt(traceID, 0.6)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(quarter, majority)
	// Output: false true
}

func ExamplePrompt_Compile() {
	prompt := langfuse.Prompt{
		Type: langfuse.PromptTypeText,
		Text: "Summarize {{topic}} in one line.",
	}
	compiled := prompt.Compile(map[string]any{"topic": "Go context"})
	fmt.Println(compiled.Text)
	// Output: Summarize Go context in one line.
}

func retrieve(ctx context.Context, query string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []string{"doc-" + query}, nil
}

func callModel(ctx context.Context, question string) (string, langfuse.Usage, error) {
	if err := ctx.Err(); err != nil {
		return "", langfuse.Usage{}, err
	}
	return fmt.Sprintf("answer to %q", question), langfuse.Usage{
		InputTokens:  6,
		OutputTokens: 8,
	}, nil
}
