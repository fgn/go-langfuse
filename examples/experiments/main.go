package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/fgn/go-langfuse"
)

// This example curates a small Langfuse dataset, reads it back as one pinned
// cohort, and runs one experiment over it: every item becomes its own trace
// whose root observation is the item's task, with a child generation and a
// score. Compare runs in the Langfuse UI under the dataset's experiments.
//
// It writes to the configured project: two dataset items in the dataset
// "go-langfuse-example-capitals" and one experiment run per execution.
func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

type question struct {
	id, country, capital string
}

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

	const datasetName = "go-langfuse-example-capitals"
	dataset, err := lf.UpsertDataset(ctx, langfuse.DatasetSpec{Name: datasetName})
	if err != nil {
		return fmt.Errorf("upsert dataset: %w", err)
	}
	// Stable item IDs make these writes updates of the same items on every
	// run. The SDK never repeats a write that may have reached the server;
	// ErrWriteOutcomeUnknown leaves that decision to the caller.
	for _, q := range []question{
		{id: "capital-france", country: "France", capital: "Paris"},
		{id: "capital-japan", country: "Japan", capital: "Tokyo"},
	} {
		_, err := lf.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{
			DatasetName:    datasetName,
			ID:             q.id,
			Input:          map[string]any{"country": q.country},
			ExpectedOutput: q.capital,
			Metadata:       map[string]any{"region": "example"},
		})
		if errors.Is(err, langfuse.ErrWriteOutcomeUnknown) {
			return fmt.Errorf("dataset item %s may or may not be stored: %w", q.id, err)
		}
		if err != nil {
			return fmt.Errorf("upsert dataset item: %w", err)
		}
	}

	// Read the cohort as of one instant and record that instant as each
	// item's version, so the run says exactly which data it used.
	asOf := time.Now()
	var items []langfuse.DatasetItem
	for item, err := range lf.DatasetItems(ctx, langfuse.DatasetItemQuery{DatasetName: datasetName, AsOf: asOf}) {
		if err != nil {
			return fmt.Errorf("read dataset items: %w", err)
		}
		items = append(items, item)
	}

	// One ID for the whole run, created once: a per-item ID would split it.
	experiment := langfuse.Experiment{
		ID:          "capitals-" + time.Now().UTC().Format("20060102T150405Z"),
		Name:        "capitals-baseline",
		Description: "Answers capital-city questions with a fixed lookup.",
		DatasetID:   dataset.ID,
		Metadata:    map[string]any{"workflow": "lookup", "version": 1},
	}
	for _, item := range items {
		if err := runItem(ctx, lf, experiment, asOf, item); err != nil {
			return err
		}
	}
	fmt.Printf("experiment %s ran %d items\n", experiment.ID, len(items))
	return nil
}

func runItem(
	ctx context.Context,
	lf *langfuse.Client,
	experiment langfuse.Experiment,
	asOf time.Time,
	item langfuse.DatasetItem,
) error {
	var input struct {
		Country string `json:"country"`
	}
	if err := json.Unmarshal(item.Input, &input); err != nil {
		return fmt.Errorf("decode item input: %w", err)
	}
	// Decode metadata with UseNumber so large numbers stay exact.
	var metadata map[string]any
	if len(item.Metadata) != 0 {
		decoder := json.NewDecoder(bytes.NewReader(item.Metadata))
		decoder.UseNumber()
		if err := decoder.Decode(&metadata); err != nil {
			return fmt.Errorf("decode item metadata: %w", err)
		}
	}

	itemCtx, task, err := lf.StartExperimentItem(ctx, experiment, langfuse.ExperimentItem{
		ID:      item.ID,
		Version: asOf,
		// The stored JSON string "Paris" is exported as Paris, as the
		// official SDKs do.
		ExpectedOutput: item.ExpectedOutput,
		Metadata:       metadata,
	}, "answer-capital", langfuse.ObservationAttributes{Input: input.Country})
	if err != nil {
		// The task could still run on itemCtx, unlinked; this example stops.
		return fmt.Errorf("start experiment item: %w", err)
	}

	answer := lookupCapital(itemCtx, lf, input.Country)
	task.Update(langfuse.ObservationAttributes{Output: answer})
	task.End() // item latency ends here; evaluation below does not count

	var expected string
	_ = json.Unmarshal(item.ExpectedOutput, &expected)
	correct := 0.0
	if strings.EqualFold(answer, expected) {
		correct = 1
	}
	// Scores attach to the item's root observation. Recorded on the item
	// context, they share the item's sdk-experiment environment.
	return lf.RecordScore(itemCtx, langfuse.Score{
		Name:          "exact-match",
		TraceID:       task.TraceID(),
		ObservationID: task.ID(),
		NumericValue:  &correct,
		DataType:      langfuse.ScoreTypeBoolean,
	})
}

// lookupCapital stands in for a model call; its generation observation is
// linked to the experiment item automatically.
func lookupCapital(ctx context.Context, lf *langfuse.Client, country string) string {
	_, generation := lf.StartObservation(ctx, "capital-lookup", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{Model: "example-lookup", Input: country})
	defer generation.End()
	answer := map[string]string{"France": "Paris", "Japan": "Tokyo"}[country]
	generation.Update(langfuse.ObservationAttributes{
		Output: answer,
		Usage:  &langfuse.Usage{InputTokens: 3, OutputTokens: 1},
	})
	return answer
}
