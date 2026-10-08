package langfuse_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

const storedScores = `[
	{"id":"s1","projectId":"p","name":"exact","source":"API","timestamp":"2026-10-08T10:00:01.000Z",
	 "environment":"sdk-experiment","createdAt":"2026-10-08T10:00:01.000Z","updatedAt":"2026-10-08T10:00:01.000Z",
	 "dataType":"NUMERIC","value":0.75,"comment":"close","metadata":{"n":12345678901234567890},
	 "subject":{"kind":"observation","id":"span-1","traceId":"trace-1"}},
	{"id":"s2","projectId":"p","name":"pass","source":"API","timestamp":"2026-10-08T10:00:02.000Z",
	 "environment":"sdk-experiment","createdAt":"2026-10-08T10:00:02.000Z","updatedAt":"2026-10-08T10:00:02.000Z",
	 "dataType":"BOOLEAN","value":true,"subject":{"kind":"trace","id":"trace-1"}},
	{"id":"s3","projectId":"p","name":"label","source":"API","timestamp":"2026-10-08T10:00:03.000Z",
	 "environment":"default","createdAt":"2026-10-08T10:00:03.000Z","updatedAt":"2026-10-08T10:00:03.000Z",
	 "dataType":"CATEGORICAL","value":"good","comment":null,"configId":null,"subject":{"kind":"experiment","id":"run-1"}}
]`

func storedItemJSON(id string) string {
	return fmt.Sprintf(`{"id":%q,"traceId":"trace-1","startTime":"2026-10-08T10:00:00.000Z",
		"endTime":"2026-10-08T10:00:00.250Z","level":"DEFAULT","environment":"sdk-experiment",
		"experimentId":"run-1","experimentName":"run one","experimentItemId":"item-1","experimentDatasetId":"ds-1",
		"experimentItemVersion":"2026-10-08T09:00:00.123Z","input":{"q":"ü"},"output":"Paris","expectedOutput":null,
		"metadata":{"k":"v"},"experimentItemMetadata":null,"experimentMetadata":{"model":"m"},
		"experimentDescription":"desc","scores":%s}`, id, storedScores)
}

func TestExperimentReadsPageThroughCursors(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var queries []url.Values
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, _ []byte) {
		mu.Lock()
		queries = append(queries, r.URL.Query())
		mu.Unlock()
		switch r.URL.Path + "#" + r.URL.Query().Get("cursor") {
		case "/api/public/experiment-items#":
			_, _ = io.WriteString(w, `{"data":[`+storedItemJSON("span-1")+`],"meta":{"cursor":"c1"}}`)
		case "/api/public/experiment-items#c1":
			_, _ = io.WriteString(w, `{"data":[`+storedItemJSON("span-2")+`],"meta":{}}`)
		case "/api/public/experiments#":
			_, _ = io.WriteString(w, `{"data":[{"id":"run-1","name":"run one","description":null,
				"startTime":"2026-10-08T10:00:00.000Z","endTime":"2026-10-08T10:00:01.000Z","itemCount":2,
				"datasetId":"ds-1","metadata":{"model":"m"},"scores":`+storedScores+`}],"meta":{}}`)
		default:
			http.NotFound(w, r)
		}
	})
	client := newDatasetClient(t, server.URL, nil)
	from := time.Date(2026, 10, 8, 9, 0, 0, 0, time.FixedZone("CEST", 2*3600))
	var items []langfuse.StoredExperimentItem
	for item, err := range client.ExperimentItems(context.Background(), langfuse.ExperimentItemQuery{
		From: from, To: from.Add(time.Hour), ExperimentIDs: []string{"run-1", "run-2"}, ItemIDs: []string{"item-1"},
		PageSize: 100,
	}) {
		if err != nil {
			t.Fatalf("ExperimentItems() error = %v", err)
		}
		items = append(items, item)
	}
	if len(items) != 2 || items[0].ObservationID != "span-1" || items[1].ObservationID != "span-2" {
		t.Fatalf("items = %+v", items)
	}
	wantQuery := url.Values{
		"fields":        {"core,dataset,io,metadata,itemMetadata,experimentMetadata,scores"},
		"limit":         {"100"},
		"fromStartTime": {"2026-10-08T07:00:00.000Z"}, "toStartTime": {"2026-10-08T08:00:00.000Z"},
		"experimentId": {"run-1,run-2"}, "experimentItemId": {"item-1"},
	}
	if !reflect.DeepEqual(queries[0], wantQuery) {
		t.Fatalf("first query = %v, want %v", queries[0], wantQuery)
	}
	if queries[1].Get("cursor") != "c1" {
		t.Fatalf("second query = %v, want cursor c1", queries[1])
	}
	item := items[0]
	if item.TraceID != "trace-1" || item.ExperimentID != "run-1" || item.ExperimentName != "run one" ||
		item.ItemID != "item-1" || item.DatasetID != "ds-1" || item.ExperimentDescription != "desc" ||
		!item.ItemVersion.Equal(time.Date(2026, 10, 8, 9, 0, 0, 123e6, time.UTC)) ||
		item.EndTime.Sub(item.StartTime) != 250*time.Millisecond || item.Level != langfuse.LevelDefault ||
		string(item.Input) != `{"q":"ü"}` || string(item.Output) != `"Paris"` || item.ExpectedOutput != nil ||
		item.ItemMetadata != nil || string(item.ExperimentMetadata) != `{"model":"m"}` {
		t.Fatalf("item = %+v", item)
	}
	if len(item.Scores) != 3 {
		t.Fatalf("scores = %+v", item.Scores)
	}
	numeric, boolean, categorical := item.Scores[0], item.Scores[1], item.Scores[2]
	if *numeric.NumericValue != 0.75 || numeric.DataType != langfuse.ScoreTypeNumeric || numeric.Comment != "close" ||
		numeric.TraceID != "trace-1" || numeric.ObservationID != "span-1" ||
		numeric.Metadata["n"] != json.Number("12345678901234567890") {
		t.Fatalf("numeric score = %+v", numeric)
	}
	if *boolean.NumericValue != 1 || boolean.TraceID != "trace-1" || boolean.ObservationID != "" {
		t.Fatalf("boolean score = %+v", boolean)
	}
	if *categorical.StringValue != "good" || categorical.DatasetRunID != "run-1" || categorical.NumericValue != nil {
		t.Fatalf("categorical score = %+v", categorical)
	}

	var experiments []langfuse.StoredExperiment
	for experiment, err := range client.Experiments(context.Background(), langfuse.ExperimentQuery{
		From: from, Names: []string{"run one"}, DatasetIDs: []string{"ds-1"},
	}) {
		if err != nil {
			t.Fatalf("Experiments() error = %v", err)
		}
		experiments = append(experiments, experiment)
	}
	if len(experiments) != 1 || experiments[0].ID != "run-1" || experiments[0].ItemCount != 2 ||
		experiments[0].DatasetID != "ds-1" || experiments[0].Description != "" || len(experiments[0].Scores) != 3 {
		t.Fatalf("experiments = %+v", experiments)
	}
	if last := queries[len(queries)-1]; last.Get("fields") != "core,metadata,scores" || last.Get("name") != "run one" ||
		last.Get("datasetId") != "ds-1" || last.Get("limit") != "50" {
		t.Fatalf("experiments query = %v", last)
	}
}

func TestExperimentReadsRejectInvalidQueriesAndPages(t *testing.T) {
	t.Parallel()
	from := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	for name, test := range map[string]struct {
		query langfuse.ExperimentItemQuery
		pages map[string]string
	}{
		"missing from":   {query: langfuse.ExperimentItemQuery{}},
		"comma filter":   {query: langfuse.ExperimentItemQuery{From: from, ItemIDs: []string{"a,b"}}},
		"empty filter":   {query: langfuse.ExperimentItemQuery{From: from, ExperimentIDs: []string{""}}},
		"large page":     {query: langfuse.ExperimentItemQuery{From: from, PageSize: 101}},
		"repeated":       {query: langfuse.ExperimentItemQuery{From: from}, pages: map[string]string{"": `{"data":[],"meta":{"cursor":"c"}}`, "c": `{"data":[],"meta":{"cursor":"c"}}`}},
		"missing meta":   {query: langfuse.ExperimentItemQuery{From: from}, pages: map[string]string{"": `{"data":[]}`}},
		"missing data":   {query: langfuse.ExperimentItemQuery{From: from}, pages: map[string]string{"": `{"meta":{}}`}},
		"invalid item":   {query: langfuse.ExperimentItemQuery{From: from}, pages: map[string]string{"": `{"data":[{"id":"x"}],"meta":{}}`}},
		"invalid score":  {query: langfuse.ExperimentItemQuery{From: from}, pages: map[string]string{"": `{"data":[` + strings.Replace(storedItemJSON("x"), `"name":"exact"`, `"name":""`, 1) + `],"meta":{}}`}},
		"not found path": {query: langfuse.ExperimentItemQuery{From: from}, pages: map[string]string{}},
		"object value":   corruptScore(from, `"dataType":"NUMERIC","value":0.75`, `"dataType":"NUMERIC","value":{}`),
		"null value":     corruptScore(from, `"dataType":"NUMERIC","value":0.75`, `"dataType":"NUMERIC","value":null`),
		"string number":  corruptScore(from, `"dataType":"NUMERIC","value":0.75`, `"dataType":"NUMERIC","value":"0.75"`),
		"word boolean":   corruptScore(from, `"dataType":"BOOLEAN","value":true`, `"dataType":"BOOLEAN","value":"yes"`),
		"number label":   corruptScore(from, `"dataType":"CATEGORICAL","value":"good"`, `"dataType":"CATEGORICAL","value":1`),
		"unknown type":   corruptScore(from, `"dataType":"NUMERIC","value":0.75`, `"dataType":"VECTOR","value":0.75`),
		"unknown kind":   corruptScore(from, `"kind":"trace","id":"trace-1"`, `"kind":"dataset","id":"trace-1"`),
		"empty subject":  corruptScore(from, `"kind":"trace","id":"trace-1"`, `"kind":"trace","id":""`),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, _ []byte) {
				page, ok := test.pages[r.URL.Query().Get("cursor")]
				if !ok {
					http.NotFound(w, r)
					return
				}
				_, _ = io.WriteString(w, page)
			})
			client := newDatasetClient(t, server.URL, nil)
			var final error
			count := 0
			for _, err := range client.ExperimentItems(context.Background(), test.query) {
				if err != nil {
					final = err
				}
				count++
			}
			if final == nil || errors.Is(final, langfuse.ErrWriteOutcomeUnknown) {
				t.Fatalf("ExperimentItems() error = %v after %d values, want a terminal error", final, count)
			}
			if test.pages == nil && server.requests.Load() != 0 {
				t.Fatalf("invalid query sent %d requests", server.requests.Load())
			}
		})
	}
}

func TestReadIteratorsCanBeTraversedRepeatedlyAndConcurrently(t *testing.T) {
	t.Parallel()
	server := newDatasetServer(t, func(w http.ResponseWriter, r *http.Request, _ []byte) {
		switch r.URL.Path + "#" + r.URL.Query().Get("cursor") + "#" + r.URL.Query().Get("page") {
		case "/api/public/experiment-items##":
			_, _ = io.WriteString(w, `{"data":[`+storedItemJSON("first")+`],"meta":{"cursor":"c1"}}`)
		case "/api/public/experiment-items#c1#":
			_, _ = io.WriteString(w, `{"data":[`+storedItemJSON("second")+`],"meta":{}}`)
		case "/api/public/dataset-items##1":
			_, _ = io.WriteString(w, itemsPage(2, "a"))
		case "/api/public/dataset-items##2":
			_, _ = io.WriteString(w, itemsPage(2, "b"))
		default:
			http.NotFound(w, r)
		}
	})
	client := newDatasetClient(t, server.URL, nil)
	items := client.ExperimentItems(context.Background(), langfuse.ExperimentItemQuery{From: time.Now().Add(-time.Hour)})
	datasetItems := client.DatasetItems(context.Background(), langfuse.DatasetItemQuery{DatasetName: "set", PageSize: 1})
	collect := func() (stored, listed []string) {
		for item, err := range items {
			if err != nil {
				t.Error(err)
				return
			}
			stored = append(stored, item.ObservationID)
		}
		for item, err := range datasetItems {
			if err != nil {
				t.Error(err)
				return
			}
			listed = append(listed, item.ID)
		}
		return stored, listed
	}
	// An early break must not leave state behind either.
	for range items {
		break
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			stored, listed := collect()
			if !reflect.DeepEqual(stored, []string{"first", "second"}) || !reflect.DeepEqual(listed, []string{"a", "b"}) {
				t.Errorf("traversal = %v / %v, want every page", stored, listed)
			}
		})
	}
	wg.Wait()
}

func corruptScore(from time.Time, valid, corrupt string) struct {
	query langfuse.ExperimentItemQuery
	pages map[string]string
} {
	item := strings.Replace(storedItemJSON("x"), valid, corrupt, 1)
	if item == storedItemJSON("x") {
		panic("corruptScore: " + valid + " not found")
	}
	return struct {
		query langfuse.ExperimentItemQuery
		pages map[string]string
	}{langfuse.ExperimentItemQuery{From: from}, map[string]string{"": `{"data":[` + item + `],"meta":{}}`}}
}
