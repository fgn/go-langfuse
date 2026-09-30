//go:build live

package langfuse_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

// TestLiveDatasetsAndExperiments is opt-in. Against the configured project it
// creates a uniquely named dataset with two synthetic items, reads them back
// pinned to one instant, proves that pin survives a later edit, runs a
// two-item experiment with a child generation, a slow evaluator after the
// task ends, and one score per item, and then reads the result through the
// v4 experiment API. It fails, rather than passes, without credentials or
// when the server lacks versioned datasets or the v4 experiment API.
// Run with: go test -count=1 -tags=live -run TestLiveDatasetsAndExperiments -v .
func TestLiveDatasetsAndExperiments(t *testing.T) {
	if os.Getenv("LANGFUSE_PUBLIC_KEY") == "" || os.Getenv("LANGFUSE_SECRET_KEY") == "" {
		t.Fatal("live Langfuse credentials are required; refusing to pass without exporting")
	}
	config := langfuse.ConfigFromEnv()
	if config.Disabled || config.DisableContentCapture {
		t.Fatal("tracing and content capture must be enabled for the live experiment fixture")
	}
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
	description := "synthetic go-langfuse live fixture"
	dataset, err := client.UpsertDataset(ctx, langfuse.DatasetSpec{
		Name: marker, Description: &description, Metadata: map[string]any{"synthetic": true},
	})
	if err != nil {
		t.Fatalf("UpsertDataset(): %v", err)
	}
	if dataset.ID == "" || dataset.Name != marker {
		t.Fatalf("UpsertDataset() = %+v", dataset)
	}
	if got, err := client.GetDataset(ctx, marker); err != nil || got.ID != dataset.ID || got.Description != description {
		t.Fatalf("GetDataset() = %+v, %v", got, err)
	}

	itemA, itemB := marker+"-a", marker+"-b"
	for _, spec := range []langfuse.DatasetItemSpec{
		{
			DatasetName: marker, ID: itemA, Input: map[string]any{"country": "France"},
			ExpectedOutput: "Paris",
			Metadata:       map[string]any{"difficulty": "easy", "rank": json.Number("9007199254740993")},
		},
		{
			DatasetName: marker, ID: itemB, Input: map[string]any{"country": "Japan"},
			ExpectedOutput: map[string]any{"capital": "Tokyo"},
			Metadata:       map[string]any{"difficulty": "hard"},
		},
	} {
		item, err := client.UpsertDatasetItem(ctx, spec)
		if err != nil {
			t.Fatalf("UpsertDatasetItem(%s): %v", spec.ID, err)
		}
		if item.ID != spec.ID || item.DatasetID != dataset.ID || item.Status != langfuse.DatasetItemActive {
			t.Fatalf("UpsertDatasetItem() = %+v", item)
		}
	}
	defer func() {
		// Deletion writes a tombstone; earlier versions and the experiment's
		// events stay in the project by design.
		for _, id := range []string{itemA, itemB} {
			if err := client.DeleteDatasetItem(context.Background(), id); err != nil {
				t.Errorf("DeleteDatasetItem(%s): %v", id, err)
			}
		}
	}()
	if got, err := client.GetDatasetItem(ctx, itemA); err != nil || string(got.ExpectedOutput) != `"Paris"` ||
		!bytes.Contains(got.Metadata, []byte("9007199254740993")) {
		t.Fatalf("GetDatasetItem() = %+v, %v", got, err)
	}

	time.Sleep(100 * time.Millisecond) // let the item versions settle before the pin
	asOf := time.Now()
	time.Sleep(100 * time.Millisecond)
	cohort := liveDatasetCohort(ctx, t, client, marker, asOf)
	if len(cohort) != 2 || cohort[itemA].ID == "" || cohort[itemB].ID == "" {
		t.Fatalf("pinned cohort = %v, want exactly the two fixture items", cohort)
	}
	// An edit after the pin must not change the pinned cohort.
	if _, err := client.UpsertDatasetItem(ctx, langfuse.DatasetItemSpec{
		DatasetName: marker, ID: itemA, ExpectedOutput: "Paris (edited)",
	}); err != nil {
		t.Fatalf("UpsertDatasetItem(edit): %v", err)
	}
	if again := liveDatasetCohort(ctx, t, client, marker, asOf); string(again[itemA].ExpectedOutput) != `"Paris"` {
		t.Fatalf("pinned re-read expected output = %s; the server does not honour dataset versions",
			again[itemA].ExpectedOutput)
	}

	experiment := langfuse.Experiment{
		ID:          marker + "-run",
		Name:        marker,
		Description: "synthetic live run",
		DatasetID:   dataset.ID,
		Metadata:    map[string]any{"workflow": "live", "attempt": 7},
	}
	const taskDuration, evaluatorDuration = 200 * time.Millisecond, 1500 * time.Millisecond
	roots := map[string][2]string{} // item ID -> trace ID, root span ID
	for _, id := range []string{itemA, itemB} {
		item := cohort[id]
		var metadata map[string]any
		decoder := json.NewDecoder(bytes.NewReader(item.Metadata))
		decoder.UseNumber()
		if err := decoder.Decode(&metadata); err != nil {
			t.Fatalf("decode item metadata: %v", err)
		}
		itemCtx, task, err := client.StartExperimentItem(ctx, experiment, langfuse.ExperimentItem{
			ID: item.ID, Version: asOf, ExpectedOutput: item.ExpectedOutput, Metadata: metadata,
		}, "live-item-task", langfuse.ObservationAttributes{Input: item.Input})
		if err != nil {
			t.Fatalf("StartExperimentItem(%s): %v", id, err)
		}
		roots[id] = [2]string{task.TraceID(), task.ID()}
		_, generation := client.StartObservation(itemCtx, "live-item-generation", langfuse.TypeGeneration,
			langfuse.ObservationAttributes{Model: "synthetic-model", Input: "synthetic prompt"})
		time.Sleep(taskDuration)
		generation.Update(langfuse.ObservationAttributes{
			Output: "synthetic answer", Usage: &langfuse.Usage{InputTokens: 10, OutputTokens: 2},
			CostDetails: map[string]float64{"input": 0.001, "output": 0.002},
		})
		generation.End()
		task.Update(langfuse.ObservationAttributes{Output: "synthetic answer"})
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
	}
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer flushCancel()
	if err := client.Flush(flushCtx); err != nil {
		t.Fatalf("Flush(): %v", err)
	}

	api := newLiveAPI(t, config.BaseURL)
	deadline := time.Now().Add(2 * time.Minute)
	from := started.Add(-time.Minute).Format(time.RFC3339)
	summary := api.awaitExperiment(t, deadline, from, experiment.ID)
	if summary.Name != experiment.Name || summary.DatasetID == nil || *summary.DatasetID != dataset.ID ||
		summary.ItemCount != 2 {
		t.Fatalf("experiment summary = %+v", summary)
	}
	items := api.awaitExperimentItems(t, deadline, from, experiment.ID)
	if len(items) != 2 {
		t.Fatalf("experiment items = %d, want exactly 2 (no extra roots)", len(items))
	}
	wantExpected := map[string]string{itemA: "Paris", itemB: `{"capital":"Tokyo"}`}
	wantItemMetadata := map[string]map[string]any{
		itemA: {"difficulty": "easy", "rank": "9007199254740993"},
		itemB: {"difficulty": "hard"},
	}
	for _, item := range items {
		root, found := roots[item.ExperimentItemID]
		if !found {
			t.Fatalf("unexpected experiment item %q", item.ExperimentItemID)
		}
		if item.TraceID != root[0] || item.ID != root[1] || item.ExperimentName != experiment.Name ||
			item.Environment != "sdk-experiment" || item.ExperimentDatasetID == nil ||
			*item.ExperimentDatasetID != dataset.ID {
			t.Fatalf("experiment item identity = %+v, want trace %s root %s", item, root[0], root[1])
		}
		if item.ExperimentItemVersion == nil ||
			!item.ExperimentItemVersion.Equal(asOf.UTC().Truncate(time.Millisecond)) {
			t.Fatalf("experiment item version = %v, want %v", item.ExperimentItemVersion, asOf.UTC().Truncate(time.Millisecond))
		}
		var expected any
		if err := json.Unmarshal(item.ExpectedOutput, &expected); err != nil ||
			expected != wantExpected[item.ExperimentItemID] {
			t.Fatalf("raw expected output = %s, want the string %q", item.ExpectedOutput, wantExpected[item.ExperimentItemID])
		}
		if fmt.Sprint(item.ExperimentItemMetadata) != fmt.Sprint(wantItemMetadata[item.ExperimentItemID]) ||
			fmt.Sprint(item.ExperimentMetadata) != fmt.Sprint(map[string]any{"workflow": "live", "attempt": "7"}) {
			t.Fatalf("metadata = item %v, experiment %v", item.ExperimentItemMetadata, item.ExperimentMetadata)
		}
		if item.EndTime == nil {
			t.Fatal("experiment item has no end time")
		}
		if latency := item.EndTime.Sub(item.StartTime); latency < taskDuration || latency >= evaluatorDuration {
			t.Fatalf("item latency = %v, want the task's duration (%v) without the evaluator's (%v)",
				latency, taskDuration, evaluatorDuration)
		}
		if len(item.Scores) != 1 || item.Scores[0]["name"] != "live-correct" || item.Scores[0]["value"] != 1.0 {
			t.Fatalf("experiment item scores = %v, want one live-correct score", item.Scores)
		}
		if environment, found := item.Scores[0]["environment"]; found && environment != "sdk-experiment" {
			t.Fatalf("score environment = %v, want sdk-experiment", environment)
		}
	}
}

func liveDatasetCohort(
	ctx context.Context,
	t *testing.T,
	client *langfuse.Client,
	datasetName string,
	asOf time.Time,
) map[string]langfuse.DatasetItem {
	t.Helper()
	cohort := map[string]langfuse.DatasetItem{}
	for item, err := range client.DatasetItems(ctx, langfuse.DatasetItemQuery{
		DatasetName: datasetName, AsOf: asOf, PageSize: 1,
	}) {
		if err != nil {
			t.Fatalf("DatasetItems(): %v", err)
		}
		cohort[item.ID] = item
	}
	return cohort
}

type liveExperiment struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	DatasetID *string `json:"datasetId"`
	ItemCount int     `json:"itemCount"`
}

type liveExperimentItem struct {
	ID                     string           `json:"id"`
	TraceID                string           `json:"traceId"`
	StartTime              time.Time        `json:"startTime"`
	EndTime                *time.Time       `json:"endTime"`
	Environment            string           `json:"environment"`
	ExperimentName         string           `json:"experimentName"`
	ExperimentItemID       string           `json:"experimentItemId"`
	ExperimentDatasetID    *string          `json:"experimentDatasetId"`
	ExperimentItemVersion  *time.Time       `json:"experimentItemVersion"`
	ExpectedOutput         json.RawMessage  `json:"expectedOutput"`
	ExperimentItemMetadata map[string]any   `json:"experimentItemMetadata"`
	ExperimentMetadata     map[string]any   `json:"experimentMetadata"`
	Scores                 []map[string]any `json:"scores"`
}

// awaitExperiment polls the v4 experiments API until the run reports both
// items. A 404 means the deployment has no v4 experiment API.
func (api *liveAPI) awaitExperiment(t *testing.T, deadline time.Time, from, id string) liveExperiment {
	t.Helper()
	query := url.Values{"fromStartTime": {from}, "id": {id}, "fields": {"core,metadata,scores"}}
	route := api.baseURL + "/api/public/experiments?" + query.Encode()
	for {
		var page struct {
			Data []liveExperiment `json:"data"`
		}
		status, err := api.getJSON(deadline, route, &page)
		if err == nil && status == http.StatusNotFound {
			t.Fatal("GET /api/public/experiments returned 404; the deployment has no v4 experiment API")
		}
		if err == nil && status != http.StatusOK {
			t.Fatalf("GET /api/public/experiments returned status %d", status)
		}
		if err == nil && len(page.Data) == 1 && page.Data[0].ItemCount == 2 {
			return page.Data[0]
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("experiment %s did not report two items before the deadline (last %+v, err %v)", id, page.Data, err)
		}
		api.waitForNextPoll()
	}
}

func (api *liveAPI) awaitExperimentItems(t *testing.T, deadline time.Time, from, id string) []liveExperimentItem {
	t.Helper()
	query := url.Values{
		"fromStartTime": {from},
		"experimentId":  {id},
		"fields":        {"core,dataset,io,metadata,itemMetadata,experimentMetadata,scores"},
	}
	route := api.baseURL + "/api/public/experiment-items?" + query.Encode()
	for {
		var page struct {
			Data []liveExperimentItem `json:"data"`
		}
		status, err := api.getJSON(deadline, route, &page)
		if err == nil && status != http.StatusOK {
			t.Fatalf("GET /api/public/experiment-items returned status %d", status)
		}
		scored := err == nil && len(page.Data) >= 2
		for _, item := range page.Data {
			scored = scored && len(item.Scores) != 0
		}
		if scored {
			sort.Slice(page.Data, func(i, j int) bool { return page.Data[i].ExperimentItemID < page.Data[j].ExperimentItemID })
			return page.Data
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("experiment %s items were not complete before the deadline (last %d items, err %v)",
				id, len(page.Data), err)
		}
		api.waitForNextPoll()
	}
}
