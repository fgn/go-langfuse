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
	if _, err := lf.UpsertDataset(ctx, langfuse.DatasetSpec{Name: datasetName}); err != nil {
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

	// Pin the run to the dataset as it is now.
	var items []langfuse.ExperimentItem
	query := langfuse.DatasetItemQuery{DatasetName: datasetName, AsOf: time.Now()}
	for item, err := range lf.DatasetItems(ctx, query) {
		if err != nil {
			return fmt.Errorf("read dataset items: %w", err)
		}
		items = append(items, item.ExperimentItem())
	}

	result, err := lf.RunExperiment(ctx, langfuse.ExperimentRun{
		Name:        "capitals-baseline",
		Description: "Answers capital-city questions with a fixed lookup.",
		Metadata:    map[string]any{"workflow": "lookup", "version": 1},
		Items:       items,
		Task: func(ctx context.Context, item langfuse.ExperimentItem) (any, error) {
			var input struct {
				Country string `json:"country"`
			}
			raw, _ := item.Input.(json.RawMessage)
			if err := json.Unmarshal(raw, &input); err != nil {
				return nil, fmt.Errorf("decode item input: %w", err)
			}
			return lookupCapital(ctx, lf, input.Country), nil
		},
		Evaluators:    []langfuse.Evaluator{exactMatch},
		RunEvaluators: []langfuse.RunEvaluator{accuracy},
	})
	if err != nil {
		return fmt.Errorf("run experiment: %w", err)
	}
	fmt.Print(result.Summary(true))
	return nil
}

// exactMatch compares the answer with the stored expected output.
func exactMatch(_ context.Context, input langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
	var expected string
	if raw, ok := input.ExpectedOutput.(json.RawMessage); ok {
		if err := json.Unmarshal(raw, &expected); err != nil {
			return nil, fmt.Errorf("decode expected output: %w", err)
		}
	}
	answer, _ := input.Output.(string)
	value := 0.0
	if strings.EqualFold(answer, expected) {
		value = 1
	}
	return []langfuse.Evaluation{{Name: "exact-match", NumericValue: &value, DataType: langfuse.ScoreTypeBoolean}}, nil
}

// accuracy is the share of items whose answer matched.
func accuracy(_ context.Context, results []langfuse.ExperimentItemResult) ([]langfuse.Evaluation, error) {
	matched, total := 0.0, 0.0
	for _, result := range results {
		for _, evaluation := range result.Evaluations {
			if evaluation.Name == "exact-match" && evaluation.NumericValue != nil {
				matched += *evaluation.NumericValue
				total++
			}
		}
	}
	if total == 0 {
		return nil, nil
	}
	value := matched / total
	return []langfuse.Evaluation{{Name: "accuracy", NumericValue: &value}}, nil
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
