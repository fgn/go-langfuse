package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/fgn/go-langfuse"
)

// This example traces one classification, prefills its label in the
// annotation queue "go-langfuse-example-review" for a human to confirm or
// correct, and prints the reviews completed so far. The score config and the
// queue are created on the first run; Langfuse cannot delete either through
// its API.
func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

const (
	configName = "example_route"
	queueName  = "go-langfuse-example-review"
)

var routes = []langfuse.ScoreCategory{{Label: "ignore", Value: 0}, {Label: "human", Value: 1}, {Label: "fix", Value: 2}}

func run(ctx context.Context) error {
	lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
	if err != nil {
		return fmt.Errorf("create Langfuse client: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := lf.Shutdown(shutdownCtx); err != nil {
			log.Printf("shut down Langfuse client: %v", err)
		}
	}()

	config, err := ensureConfig(ctx, lf)
	if err != nil {
		return err
	}
	queue, err := ensureQueue(ctx, lf, config.ID)
	if err != nil {
		return err
	}

	// Classify an alert in a trace of its own.
	traceCtx, span := lf.StartObservation(ctx, "classify-alert", langfuse.TypeSpan,
		langfuse.ObservationAttributes{Input: "disk 91% full on db-1", Output: "fix"})
	span.End()
	traceID := oteltrace.SpanFromContext(traceCtx).SpanContext().TraceID().String()
	if err := lf.Flush(ctx); err != nil {
		return fmt.Errorf("export trace: %w", err)
	}

	// Prefill the guess, then queue the trace. Langfuse stores the score a
	// few seconds after accepting it, so a reviewer who opens the item at
	// once can still find the field empty. The fixed ID makes a retry
	// rewrite the same score.
	if _, err := lf.CreateScore(ctx, langfuse.Score{
		ID: traceID + "-route", TraceID: traceID, Name: config.Name, StringValue: new("fix"),
		ConfigID: config.ID, QueueID: queue.ID, Source: langfuse.ScoreSourceAnnotation,
	}); err != nil {
		return fmt.Errorf("prefill route: %w", err)
	}
	if _, err := lf.CreateAnnotationQueueItem(ctx, langfuse.AnnotationQueueItemSpec{
		QueueID: queue.ID, ObjectID: traceID, ObjectType: langfuse.AnnotationObjectTrace,
	}); err != nil {
		return fmt.Errorf("queue trace: %w", err)
	}
	fmt.Printf("queued trace %s for review in %q\n", traceID, queue.Name)
	return printReviews(ctx, lf, queue)
}

// printReviews prints the reviewed route of every completed item.
func printReviews(ctx context.Context, lf *langfuse.Client, queue langfuse.AnnotationQueue) error {
	query := langfuse.AnnotationQueueItemQuery{QueueID: queue.ID, Status: langfuse.AnnotationCompleted}
	for item, err := range lf.AnnotationQueueItems(ctx, query) {
		if err != nil {
			return fmt.Errorf("list reviews: %w", err)
		}
		var latest langfuse.StoredScore
		for score, err := range lf.Scores(ctx, langfuse.ScoreQuery{
			TraceIDs: []string{item.ObjectID}, ConfigIDs: queue.ScoreConfigIDs, QueueIDs: []string{queue.ID},
			Sources: []langfuse.ScoreSource{langfuse.ScoreSourceAnnotation},
		}) {
			if err != nil {
				return fmt.Errorf("read review scores: %w", err)
			}
			if score.StringValue != nil && score.UpdatedAt.After(latest.UpdatedAt) {
				latest = score
			}
		}
		if latest.StringValue != nil {
			fmt.Printf("trace %s reviewed at %s: route %s %s\n", item.ObjectID,
				item.CompletedAt.Format(time.RFC3339), *latest.StringValue, latest.Comment)
		}
	}
	return nil
}

// ensureConfig returns the active route config, creating it if needed.
// Config names are not unique, so it also checks the categories.
func ensureConfig(ctx context.Context, lf *langfuse.Client) (langfuse.ScoreConfig, error) {
	for config, err := range lf.ScoreConfigs(ctx, langfuse.ScoreConfigQuery{}) {
		if err != nil {
			return langfuse.ScoreConfig{}, fmt.Errorf("list score configs: %w", err)
		}
		if config.Name == configName && !config.Archived {
			if !slices.Equal(config.Categories, routes) {
				return langfuse.ScoreConfig{}, errors.New("score config " + configName + " has other categories; archive it")
			}
			return config, nil
		}
	}
	config, err := lf.CreateScoreConfig(ctx, langfuse.ScoreConfigSpec{
		Name: configName, DataType: langfuse.ScoreTypeCategorical, Categories: routes,
		Description: "How an alert should be handled.",
	})
	if err != nil {
		return langfuse.ScoreConfig{}, fmt.Errorf("create score config: %w", err)
	}
	return config, nil
}

// ensureQueue returns the review queue, creating it if needed.
func ensureQueue(ctx context.Context, lf *langfuse.Client, configID string) (langfuse.AnnotationQueue, error) {
	for queue, err := range lf.AnnotationQueues(ctx, langfuse.AnnotationQueueQuery{}) {
		if err != nil {
			return langfuse.AnnotationQueue{}, fmt.Errorf("list annotation queues: %w", err)
		}
		if queue.Name == queueName {
			if !slices.Contains(queue.ScoreConfigIDs, configID) {
				return langfuse.AnnotationQueue{}, errors.New("queue " + queueName + " does not use score config " + configName)
			}
			return queue, nil
		}
	}
	queue, err := lf.CreateAnnotationQueue(ctx, langfuse.AnnotationQueueSpec{
		Name: queueName, Description: "Confirm or correct the suggested route.", ScoreConfigIDs: []string{configID},
	})
	if err != nil {
		return langfuse.AnnotationQueue{}, fmt.Errorf("create annotation queue: %w", err)
	}
	return queue, nil
}
