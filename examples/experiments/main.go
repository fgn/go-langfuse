package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/fgn/go-langfuse"
)

// This example writes two items to the dataset "go-langfuse-example-capitals"
// in the configured project and runs one experiment over them per execution.
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

	var items []langfuse.DatasetItem
	query := langfuse.DatasetItemQuery{DatasetName: datasetName, AsOf: time.Now()}
	for item, err := range lf.DatasetItems(ctx, query) {
		if err != nil {
			return fmt.Errorf("read dataset items: %w", err)
		}
		items = append(items, item)
	}

	// Every item of one run shares its ID.
	experiment := langfuse.Experiment{
		ID:          "capitals-" + time.Now().UTC().Format("20060102T150405Z"),
		Name:        "capitals-baseline",
		Description: "Answers capital-city questions with a fixed lookup.",
		DatasetID:   dataset.ID,
		Metadata:    map[string]any{"workflow": "lookup", "version": 1},
	}
	for _, item := range items {
		if err := runItem(ctx, lf, experiment, item); err != nil {
			return err
		}
	}
	fmt.Printf("experiment %s ran %d items\n", experiment.ID, len(items))
	return nil
}

func runItem(ctx context.Context, lf *langfuse.Client, experiment langfuse.Experiment, item langfuse.DatasetItem) error {
	var input struct {
		Country string `json:"country"`
	}
	if err := json.Unmarshal(item.Input, &input); err != nil {
		return fmt.Errorf("decode item input: %w", err)
	}
	experimentItem, err := item.ExperimentItem()
	if err != nil {
		return fmt.Errorf("convert dataset item: %w", err)
	}
	itemCtx, task, err := lf.StartExperimentItem(ctx, experiment, experimentItem, "answer-capital",
		langfuse.ObservationAttributes{Input: input.Country})
	if err != nil {
		return fmt.Errorf("start experiment item: %w", err)
	}

	answer := lookupCapital(itemCtx, lf, input.Country)
	task.Update(langfuse.ObservationAttributes{Output: answer})
	task.End() // item latency ends here; the evaluation below does not count

	var expected string
	_ = json.Unmarshal(item.ExpectedOutput, &expected)
	correct := 0.0
	if strings.EqualFold(answer, expected) {
		correct = 1
	}
	return lf.RecordScore(itemCtx, langfuse.Score{
		Name:          "exact-match",
		TraceID:       task.TraceID(),
		ObservationID: task.ID(),
		NumericValue:  &correct,
		DataType:      langfuse.ScoreTypeBoolean,
	})
}

// lookupCapital stands in for a model call.
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
