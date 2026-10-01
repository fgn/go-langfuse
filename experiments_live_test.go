//go:build live

package langfuse_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

// TestLiveDatasetsAndExperiments needs credentials for a server with
// versioned datasets and the v4 experiment API. It checks that an AsOf read
// survives a later edit and that the server links one experiment item to its
// dataset version, latency, and score.
// Run with: go test -count=1 -tags=live -run TestLiveDatasetsAndExperiments -v .
func TestLiveDatasetsAndExperiments(t *testing.T) {
	if os.Getenv("LANGFUSE_PUBLIC_KEY") == "" || os.Getenv("LANGFUSE_SECRET_KEY") == "" {
		t.Fatal("live Langfuse credentials are required")
	}
	config := langfuse.ConfigFromEnv()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client, err := langfuse.New(ctx, config)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := client.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Shutdown(): %v", err)
		}
	}()

	started := time.Now().UTC()
	marker := fmt.Sprintf("go-langfuse-live-%d", started.UnixNano())
	dataset, err := client.UpsertDataset(ctx, langfuse.DatasetSpec{Name: marker})
	if err != nil {
		t.Fatalf("UpsertDataset(): %v", err)
	}
	itemID := marker + "-item"
	upsertItem := func(expectedOutput string) {
		t.Helper()
		if _, err := client.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{
			DatasetName: marker, ID: itemID, Input: "France", ExpectedOutput: expectedOutput,
		}); err != nil {
			t.Fatalf("UpsertDatasetItem(%q): %v", expectedOutput, err)
		}
	}
	upsertItem("Paris")
	defer func() {
		if err := client.DeleteDatasetItem(context.Background(), itemID); err != nil {
			t.Errorf("DeleteDatasetItem(): %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond) // keep the pin clear of the write's timestamp
	asOf := time.Now()
	time.Sleep(100 * time.Millisecond)
	item := livePinnedItem(ctx, t, client, marker, itemID, asOf)
	upsertItem("Paris (edited)")
	if again := livePinnedItem(ctx, t, client, marker, itemID, asOf); string(again.ExpectedOutput) != `"Paris"` {
		t.Fatalf("pinned expected output after an edit = %s; the server does not honour dataset versions",
			again.ExpectedOutput)
	}

	experiment := langfuse.Experiment{ID: marker + "-run", Name: marker, DatasetID: dataset.ID}
	experimentItem, err := item.ExperimentItem()
	if err != nil {
		t.Fatalf("ExperimentItem(): %v", err)
	}
	itemCtx, task, err := client.StartExperimentItem(ctx, experiment, experimentItem, "live-item-task",
		langfuse.ObservationAttributes{Input: item.Input})
	if err != nil {
		t.Fatalf("StartExperimentItem(): %v", err)
	}
	const taskDuration, evaluatorDuration = 200 * time.Millisecond, 1500 * time.Millisecond
	_, generation := client.StartObservation(itemCtx, "live-item-generation", langfuse.TypeGeneration,
		langfuse.ObservationAttributes{Model: "synthetic-model"})
	time.Sleep(taskDuration)
	generation.End()
	task.End()
	_, evaluator := client.StartObservation(itemCtx, "live-item-evaluator", langfuse.TypeEvaluator,
		langfuse.ObservationAttributes{})
	time.Sleep(evaluatorDuration)
	evaluator.End()
	value := 1.0
	if err := client.RecordScore(itemCtx, langfuse.Score{
		Name: "live-correct", TraceID: task.TraceID(), ObservationID: task.ID(), NumericValue: &value,
	}); err != nil {
		t.Fatalf("RecordScore(): %v", err)
	}
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer flushCancel()
	if err := client.Flush(flushCtx); err != nil {
		t.Fatalf("Flush(): %v", err)
	}

	api := newLiveAPI(t, config.BaseURL)
	deadline := time.Now().Add(2 * time.Minute)
	from := started.Add(-time.Minute).Format(time.RFC3339)
	run := api.awaitExperiment(t, deadline, from, experiment.ID)
	if run.Name != experiment.Name || run.DatasetID == nil || *run.DatasetID != dataset.ID {
		t.Fatalf("experiment = %+v", run)
	}
	got := api.awaitExperimentItem(t, deadline, from, experiment.ID)
	version := asOf.UTC().Truncate(time.Millisecond)
	if got.ExperimentItemID != itemID || got.TraceID != task.TraceID() || got.ID != task.ID() ||
		got.ExperimentDatasetID == nil || *got.ExperimentDatasetID != dataset.ID ||
		got.ExperimentItemVersion == nil || !got.ExperimentItemVersion.Equal(version) {
		t.Fatalf("experiment item = %+v, want item %s on root %s/%s at version %v",
			got, itemID, task.TraceID(), task.ID(), version)
	}
	if got.EndTime == nil {
		t.Fatal("experiment item has no end time")
	}
	if latency := got.EndTime.Sub(got.StartTime); latency < taskDuration || latency >= evaluatorDuration {
		t.Fatalf("item latency = %v, want the task's %v without the evaluator's %v",
			latency, taskDuration, evaluatorDuration)
	}
	if len(got.Scores) != 1 || got.Scores[0]["name"] != "live-correct" || got.Scores[0]["value"] != 1.0 {
		t.Fatalf("experiment item scores = %v, want one live-correct score", got.Scores)
	}
}

func livePinnedItem(
	ctx context.Context, t *testing.T, client *langfuse.Client, dataset, id string, asOf time.Time,
) langfuse.DatasetItem {
	t.Helper()
	var items []langfuse.DatasetItem
	for item, err := range client.DatasetItems(ctx, langfuse.DatasetItemQuery{DatasetName: dataset, AsOf: asOf}) {
		if err != nil {
			t.Fatalf("DatasetItems(): %v", err)
		}
		items = append(items, item)
	}
	if len(items) != 1 || items[0].ID != id {
		t.Fatalf("pinned items = %+v, want only %s", items, id)
	}
	return items[0]
}

type liveExperiment struct {
	Name      string  `json:"name"`
	DatasetID *string `json:"datasetId"`
	ItemCount int     `json:"itemCount"`
}

type liveExperimentItem struct {
	ID                    string           `json:"id"`
	TraceID               string           `json:"traceId"`
	StartTime             time.Time        `json:"startTime"`
	EndTime               *time.Time       `json:"endTime"`
	ExperimentItemID      string           `json:"experimentItemId"`
	ExperimentDatasetID   *string          `json:"experimentDatasetId"`
	ExperimentItemVersion *time.Time       `json:"experimentItemVersion"`
	Scores                []map[string]any `json:"scores"`
}

func (api *liveAPI) awaitExperiment(t *testing.T, deadline time.Time, from, id string) liveExperiment {
	t.Helper()
	query := url.Values{"fromStartTime": {from}, "id": {id}, "fields": {"core"}}
	var page struct {
		Data []liveExperiment `json:"data"`
	}
	api.poll(t, deadline, "/api/public/experiments?"+query.Encode(), &page, func() bool {
		return len(page.Data) == 1 && page.Data[0].ItemCount != 0
	})
	return page.Data[0]
}

func (api *liveAPI) awaitExperimentItem(t *testing.T, deadline time.Time, from, id string) liveExperimentItem {
	t.Helper()
	query := url.Values{"fromStartTime": {from}, "experimentId": {id}, "fields": {"core,dataset,scores"}}
	var page struct {
		Data []liveExperimentItem `json:"data"`
	}
	api.poll(t, deadline, "/api/public/experiment-items?"+query.Encode(), &page, func() bool {
		return len(page.Data) != 0 && len(page.Data[0].Scores) != 0
	})
	if len(page.Data) != 1 {
		t.Fatalf("experiment items = %d, want 1", len(page.Data))
	}
	return page.Data[0]
}

// poll fails on any status other than 200; a 404 means the deployment has no
// v4 experiment API.
func (api *liveAPI) poll(t *testing.T, deadline time.Time, path string, into any, done func() bool) {
	t.Helper()
	for {
		status, err := api.getJSON(deadline, api.baseURL+path, into)
		if err == nil && status != http.StatusOK {
			t.Fatalf("GET %s returned status %d", path, status)
		}
		if err == nil && done() {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("GET %s did not settle before the deadline (last error: %v)", path, err)
		}
		api.waitForNextPoll()
	}
}
