package langfuse_test

import (
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/fgn/go-langfuse"
)

// runnerServer answers OTLP exports, score ingestion, and dataset run item
// links like Langfuse, and records what it received.
type runnerServer struct {
	*httptest.Server

	mu     sync.Mutex
	spans  []*tracepb.Span
	scores []map[string]any
	links  []map[string]any
	// link answers one dataset run item link; nil answers like an
	// events_only server, with a run ID derived from the run name.
	link func(body map[string]any) (status int, response string)
}

func newRunnerServer(t *testing.T) *runnerServer {
	t.Helper()
	server := &runnerServer{}
	server.Server = httptest.NewServer(server)
	t.Cleanup(server.Close)
	return server
}

func (s *runnerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	switch r.URL.Path {
	case "/api/public/otel/v1/traces":
		request := new(collectortrace.ExportTraceServiceRequest)
		if err := proto.Unmarshal(body, request); err != nil {
			http.Error(w, "decode", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		for _, resource := range request.ResourceSpans {
			for _, scope := range resource.ScopeSpans {
				s.spans = append(s.spans, scope.Spans...)
			}
		}
		s.mu.Unlock()
		response, _ := proto.Marshal(new(collectortrace.ExportTraceServiceResponse))
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(response)
	case "/api/public/ingestion":
		var batch struct {
			Batch []struct {
				ID   string         `json:"id"`
				Body map[string]any `json:"body"`
			} `json:"batch"`
		}
		_ = json.Unmarshal(body, &batch)
		s.mu.Lock()
		for _, event := range batch.Batch {
			s.scores = append(s.scores, event.Body)
		}
		s.mu.Unlock()
		w.WriteHeader(http.StatusMultiStatus)
		_, _ = fmt.Fprintf(w, `{"successes":[{"id":%q,"status":201}],"errors":[]}`, batch.Batch[0].ID)
	case "/api/public/dataset-run-items":
		var link map[string]any
		_ = json.Unmarshal(body, &link)
		s.mu.Lock()
		s.links = append(s.links, link)
		answer := s.link
		s.mu.Unlock()
		status, response := http.StatusOK, ""
		if answer != nil {
			status, response = answer(link)
		}
		if response == "" {
			response = linkResponse(link, nil)
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, response)
	default:
		http.NotFound(w, r)
	}
}

// linkResponse answers a link request like Langfuse, echoing its targets
// with a run ID derived from the run name, then applies overrides.
func linkResponse(request, overrides map[string]any) string {
	response := map[string]any{
		"id": "link-1", "datasetRunId": "run-" + fmt.Sprint(request["runName"]),
		"datasetRunName": request["runName"], "datasetItemId": request["datasetItemId"],
		"traceId": request["traceId"], "observationId": request["observationId"],
		"createdAt": "2026-10-08T10:00:00.000Z", "updatedAt": "2026-10-08T10:00:00.000Z",
	}
	maps.Copy(response, overrides)
	data, err := json.Marshal(response)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func (s *runnerServer) recorded() (spans []*tracepb.Span, scores, links []map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.spans), slices.Clone(s.scores), slices.Clone(s.links)
}

func newRunnerClient(t *testing.T, server *runnerServer, change func(*langfuse.Config)) *langfuse.Client {
	t.Helper()
	config := langfuse.Config{
		BaseURL: server.URL, PublicKey: "pk-lf-runner", SecretKey: "sk-lf-runner", Environment: "runner_env",
	}
	if change != nil {
		change(&config)
	}
	client, err := langfuse.New(context.Background(), config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})
	return client
}

func spanAttributes(t *testing.T, span *tracepb.Span) map[string]any {
	t.Helper()
	return observationWireAttributeMap(t, span.Attributes)
}

func spansNamed(spans []*tracepb.Span, name string) []*tracepb.Span {
	var result []*tracepb.Span
	for _, span := range spans {
		if span.Name == name {
			result = append(result, span)
		}
	}
	return result
}

func spanByID(spans []*tracepb.Span, id string) *tracepb.Span {
	for _, span := range spans {
		if hex.EncodeToString(span.SpanId) == id {
			return span
		}
	}
	return nil
}

// exactMatch is a named evaluator so its observation name is predictable.
func exactMatch(_ context.Context, input langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
	value := 0.0
	if fmt.Sprint(input.Output) == fmt.Sprint(input.ExpectedOutput) {
		value = 1
	}
	return []langfuse.Evaluation{{Name: "exact", NumericValue: &value, Comment: "compared"}}, nil
}

func upperTask(_ context.Context, item langfuse.ExperimentItem) (any, error) {
	if raw, ok := item.Input.(json.RawMessage); ok {
		var input string
		if json.Unmarshal(raw, &input) != nil {
			input = string(raw)
		}
		return strings.ToUpper(input), nil
	}
	return strings.ToUpper(fmt.Sprint(item.Input)), nil
}

func TestRunExperimentLocalItemsWire(t *testing.T) {
	t.Parallel()
	server := newRunnerServer(t)
	client := newRunnerClient(t, server, nil)

	var compositeInput langfuse.EvaluatorInput
	var runInput []langfuse.ExperimentItemResult
	result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
		Name:        "capitals",
		Description: "local data",
		Metadata:    map[string]any{"model": "m-1"},
		Items: []langfuse.ExperimentItem{
			{ID: "q-1", Input: "paris", ExpectedOutput: "PARIS", Metadata: map[string]any{"region": "eu"}},
			{ID: "q-2", Input: "tokyo", ExpectedOutput: "Kyoto"},
		},
		Task: func(ctx context.Context, item langfuse.ExperimentItem) (any, error) {
			_, generation := client.StartObservation(ctx, "model-call", langfuse.TypeGeneration,
				langfuse.ObservationAttributes{Model: "m-1"})
			generation.End()
			return upperTask(ctx, item)
		},
		Evaluators: []langfuse.Evaluator{exactMatch},
		CompositeEvaluator: func(_ context.Context, input langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
			if input.Output == "PARIS" {
				compositeInput = input
			}
			return []langfuse.Evaluation{{Name: "verdict", StringValue: new("ok"), DataType: langfuse.ScoreTypeCategorical}}, nil
		},
		RunEvaluators: []langfuse.RunEvaluator{func(_ context.Context, results []langfuse.ExperimentItemResult) ([]langfuse.Evaluation, error) {
			runInput = results
			return []langfuse.Evaluation{{Name: "accuracy", NumericValue: new(0.5)}}, nil
		}},
	})
	if err != nil {
		t.Fatalf("RunExperiment() error = %v", err)
	}

	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(result.ExperimentID) || result.DatasetRunID != "" {
		t.Fatalf("experiment ID = %q, dataset run ID = %q; want 16 hex and none", result.ExperimentID, result.DatasetRunID)
	}
	if !regexp.MustCompile(`^capitals - \d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$`).MatchString(result.RunName) {
		t.Fatalf("run name = %q, want the name and a millisecond UTC timestamp", result.RunName)
	}
	if len(result.ItemResults) != 2 || result.ItemResults[0].Item.ID != "q-1" || result.ItemResults[1].Item.ID != "q-2" {
		t.Fatalf("item results = %+v, want q-1 then q-2", result.ItemResults)
	}
	for _, item := range result.ItemResults {
		if item.Err != nil || item.EvaluationErr != nil || len(item.Evaluations) != 2 {
			t.Fatalf("item %s = %+v, want two evaluations and no errors", item.Item.ID, item)
		}
	}
	if result.ItemResults[0].Output != "PARIS" || *result.ItemResults[0].Evaluations[0].NumericValue != 1 ||
		*result.ItemResults[1].Evaluations[0].NumericValue != 0 {
		t.Fatalf("outputs and evaluations = %+v", result.ItemResults)
	}
	if len(compositeInput.Evaluations) != 1 || compositeInput.Evaluations[0].Name != "exact" {
		t.Fatalf("composite evaluator input = %+v, want the item evaluation", compositeInput)
	}
	if len(runInput) != 2 || len(result.RunEvaluations) != 1 || result.RunEvaluationErr != nil {
		t.Fatalf("run evaluator saw %d results; run evaluations = %+v, %v", len(runInput), result.RunEvaluations,
			result.RunEvaluationErr)
	}

	spans, scores, links := server.recorded()
	if len(links) != 0 {
		t.Fatalf("local data sent %d dataset run links", len(links))
	}
	roots := spansNamed(spans, "experiment-item-run")
	if len(roots) != 2 {
		t.Fatalf("item roots = %d, want 2", len(roots))
	}
	for _, item := range result.ItemResults {
		root := spanByID(spans, item.ObservationID)
		if root == nil || root.Name != "experiment-item-run" || len(root.ParentSpanId) != 0 ||
			hex.EncodeToString(root.TraceId) != item.TraceID {
			t.Fatalf("item %s root = %v, want a new trace root", item.Item.ID, root)
		}
		attributes := spanAttributes(t, root)
		want := map[string]any{
			experimentIDKey:               result.ExperimentID,
			experimentNameKey:             result.RunName,
			experimentDescKey:             "local data",
			experimentMetaKey:             `{"model":"m-1"}`,
			itemIDKey:                     item.Item.ID,
			itemRootKey:                   item.ObservationID,
			itemExpectedKey:               item.Item.ExpectedOutput,
			environmentKey:                "sdk-experiment",
			"langfuse.observation.input":  item.Item.Input,
			"langfuse.observation.output": item.Output,
			"langfuse.observation.metadata.experiment_name":     "capitals",
			"langfuse.observation.metadata.experiment_run_name": result.RunName,
			"langfuse.observation.metadata.model":               "m-1",
		}
		for key, value := range want {
			if attributes[key] != value {
				t.Errorf("item %s root %s = %#v, want %#v", item.Item.ID, key, attributes[key], value)
			}
		}
		if _, ok := attributes[experimentDatasetKey]; ok {
			t.Errorf("local item root has a dataset ID")
		}
	}
	if attributes := spanAttributes(t, spanByID(spans, result.ItemResults[0].ObservationID)); attributes[itemMetaKey] != `{"region":"eu"}` ||
		attributes["langfuse.observation.metadata.region"] != "eu" {
		t.Errorf("item metadata attributes = %v", attributes)
	}

	evaluators := spansNamed(spans, "exactMatch")
	if len(evaluators) != 2 {
		t.Fatalf("exactMatch evaluator observations = %d, want 2", len(evaluators))
	}
	for _, evaluator := range evaluators {
		attributes := spanAttributes(t, evaluator)
		root := spanByID(spans, hex.EncodeToString(evaluator.ParentSpanId))
		if root == nil || root.Name != "experiment-item-run" || attributes["langfuse.observation.type"] != "evaluator" ||
			attributes[experimentIDKey] != result.ExperimentID || attributes[environmentKey] != "sdk-experiment" ||
			attributes["langfuse.observation.metadata.evaluator_kind"] != "item" {
			t.Fatalf("evaluator observation attributes = %v, parent = %v", attributes, root)
		}
		var output []map[string]any
		if err := json.Unmarshal(fmt.Append(nil, attributes["langfuse.observation.output"]), &output); err != nil ||
			len(output) != 1 || output[0]["name"] != "exact" || output[0]["comment"] != "compared" {
			t.Fatalf("evaluator output = %v", attributes["langfuse.observation.output"])
		}
	}
	if len(spansNamed(spans, "model-call")) != 2 {
		t.Fatalf("task generations = %d, want 2", len(spansNamed(spans, "model-call")))
	}

	// Two item evaluations and one composite per item; no run scores for
	// local data.
	if len(scores) != 4 {
		t.Fatalf("scores = %v, want 4 item scores", scores)
	}
	for _, score := range scores {
		item := result.ItemResults[0]
		if score["traceId"] == result.ItemResults[1].TraceID {
			item = result.ItemResults[1]
		}
		if score["traceId"] != item.TraceID || score["observationId"] != item.ObservationID ||
			score["environment"] != "sdk-experiment" || score["datasetRunId"] != nil {
			t.Errorf("score = %v, want linkage to item %s", score, item.Item.ID)
		}
	}
}

func TestRunExperimentDatasetItemsLinkTheRun(t *testing.T) {
	t.Parallel()
	server := newRunnerServer(t)
	client := newRunnerClient(t, server, func(config *langfuse.Config) {
		config.Mask = func(field langfuse.MaskField, value any) any {
			if field == langfuse.MaskExperimentMetadata {
				return map[string]any{"model": "[masked]"}
			}
			return value
		}
	})
	version := time.Date(2026, 10, 8, 9, 30, 0, 123456789, time.UTC)
	result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
		Name:        "capitals",
		RunName:     "capitals run 1",
		Description: "dataset run",
		Metadata:    map[string]any{"model": "secret"},
		Items: []langfuse.ExperimentItem{
			{ID: "item-a", DatasetID: "ds-1", Version: version, Input: json.RawMessage(`"paris"`), ExpectedOutput: json.RawMessage(`"PARIS"`)},
			{ID: "item-b", DatasetID: "ds-1", Version: version, Input: json.RawMessage(`{"q":"rome"}`)},
		},
		Task:       upperTask,
		Evaluators: []langfuse.Evaluator{exactMatch},
		RunEvaluators: []langfuse.RunEvaluator{func(context.Context, []langfuse.ExperimentItemResult) ([]langfuse.Evaluation, error) {
			return []langfuse.Evaluation{{Name: "accuracy", NumericValue: new(0.5), Comment: "half"}}, nil
		}},
	})
	if err != nil {
		t.Fatalf("RunExperiment() error = %v", err)
	}
	if result.ExperimentID != "run-capitals run 1" || result.DatasetRunID != result.ExperimentID {
		t.Fatalf("experiment ID = %q, dataset run ID = %q, want the linked run", result.ExperimentID, result.DatasetRunID)
	}
	spans, scores, links := server.recorded()
	if len(links) != 2 {
		t.Fatalf("links = %d, want 2", len(links))
	}
	for _, item := range result.ItemResults {
		if item.Err != nil || item.DatasetRunID != result.DatasetRunID {
			t.Fatalf("item %s = %+v", item.Item.ID, item)
		}
		var link map[string]any
		for _, candidate := range links {
			if candidate["datasetItemId"] == item.Item.ID {
				link = candidate
			}
		}
		want := map[string]any{
			"runName": "capitals run 1", "runDescription": "dataset run", "datasetItemId": item.Item.ID,
			"traceId": item.TraceID, "observationId": item.ObservationID,
			"metadata": map[string]any{"model": "[masked]"}, "datasetVersion": "2026-10-08T09:30:00.123Z",
		}
		if fmt.Sprint(link) != fmt.Sprint(want) {
			t.Fatalf("link body = %v, want %v", link, want)
		}
		attributes := spanAttributes(t, spanByID(spans, item.ObservationID))
		if attributes[experimentIDKey] != result.ExperimentID || attributes[experimentDatasetKey] != "ds-1" ||
			attributes[itemVersionKey] != "2026-10-08T09:30:00.123Z" ||
			attributes["langfuse.observation.metadata.dataset_id"] != "ds-1" ||
			attributes["langfuse.observation.metadata.dataset_item_id"] != item.Item.ID {
			t.Fatalf("item %s root attributes = %v", item.Item.ID, attributes)
		}
	}
	// Raw JSON input is exported verbatim; Langfuse decodes the JSON string.
	if attributes := spanAttributes(t, spanByID(spans, result.ItemResults[0].ObservationID)); attributes[itemExpectedKey] != "PARIS" ||
		attributes["langfuse.observation.input"] != `"paris"` || attributes["langfuse.observation.output"] != "PARIS" ||
		attributes["langfuse.observation.metadata.model"] != "[masked]" {
		t.Fatalf("root attributes = %v, want raw input, decoded expected output, and masked run metadata", attributes)
	}
	for _, evaluator := range spansNamed(spans, "exactMatch") {
		if input := spanAttributes(t, evaluator)["langfuse.observation.input"]; !strings.Contains(fmt.Sprint(input), `"expected_output":"PARIS"`) &&
			!strings.Contains(fmt.Sprint(input), `"expected_output":null`) {
			t.Fatalf("evaluator input = %v", input)
		}
	}
	var runScores []map[string]any
	for _, score := range scores {
		if score["datasetRunId"] != nil {
			runScores = append(runScores, score)
		}
	}
	if len(runScores) != 1 || runScores[0]["datasetRunId"] != result.DatasetRunID || runScores[0]["traceId"] != nil ||
		runScores[0]["name"] != "accuracy" || runScores[0]["comment"] != "half" || runScores[0]["environment"] != "runner_env" {
		t.Fatalf("run scores = %v, want one accuracy score on the dataset run", runScores)
	}
}

func TestRunExperimentFailedLinkFailsOnlyThatItem(t *testing.T) {
	t.Parallel()
	server := newRunnerServer(t)
	server.link = func(body map[string]any) (int, string) {
		if body["datasetItemId"] == "missing" {
			return http.StatusNotFound, `{"message":"Dataset item not found"}`
		}
		return http.StatusOK, ""
	}
	client := newRunnerClient(t, server, nil)
	var ran []string
	var mu sync.Mutex
	result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
		Name: "links", RunName: "links",
		Items: []langfuse.ExperimentItem{
			{ID: "missing", DatasetID: "ds-1", Input: "a"},
			{ID: "present", DatasetID: "ds-1", Input: "b"},
		},
		Task: func(ctx context.Context, item langfuse.ExperimentItem) (any, error) {
			mu.Lock()
			ran = append(ran, item.ID)
			mu.Unlock()
			return "out", nil
		},
	})
	if err != nil {
		t.Fatalf("RunExperiment() error = %v", err)
	}
	failed, linked := result.ItemResults[0], result.ItemResults[1]
	if !errors.Is(failed.Err, langfuse.ErrDatasetItemNotFound) || failed.DatasetRunID != "" || failed.TraceID != "" {
		t.Fatalf("failed link item = %+v, want ErrDatasetItemNotFound and no IDs", failed)
	}
	if linked.Err != nil || result.DatasetRunID != "run-links" || !slices.Equal(ran, []string{"present"}) {
		t.Fatalf("linked item = %+v, dataset run = %q, tasks run = %v", linked, result.DatasetRunID, ran)
	}
	spans, _, _ := server.recorded()
	roots := spansNamed(spans, "experiment-item-run")
	if len(roots) != 2 {
		t.Fatalf("item roots = %d, want 2", len(roots))
	}
	for _, root := range roots {
		attributes := spanAttributes(t, root)
		if hex.EncodeToString(root.SpanId) == linked.ObservationID {
			continue
		}
		if _, ok := attributes[experimentIDKey]; ok || attributes["langfuse.observation.level"] != "ERROR" ||
			attributes["langfuse.observation.status_message"] != "langfuse: dataset run link failed" {
			t.Fatalf("unlinked root attributes = %v, want an error without an experiment ID", attributes)
		}
	}
}

func TestRunExperimentTaskAndEvaluatorFailures(t *testing.T) {
	t.Parallel()
	server := newRunnerServer(t)
	client := newRunnerClient(t, server, nil)
	taskErr := errors.New("model unavailable")
	evaluatorErr := errors.New("judge unavailable")
	var evaluated atomic.Int32
	result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
		Name: "failures",
		Items: []langfuse.ExperimentItem{
			{ID: "error", Input: "a"},
			{ID: "panic", Input: "b"},
			{ID: "ok", Input: "c"},
			{ID: "no-input"},
			{ID: "null-input", Input: json.RawMessage(` null `)},
		},
		Task: func(_ context.Context, item langfuse.ExperimentItem) (any, error) {
			switch item.ID {
			case "error":
				return nil, taskErr
			case "panic":
				panic("secret panic value")
			}
			return "out", nil
		},
		Evaluators: []langfuse.Evaluator{
			func(context.Context, langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
				evaluated.Add(1)
				return nil, evaluatorErr
			},
			func(context.Context, langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) { panic("judge panic") },
			func(context.Context, langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
				return []langfuse.Evaluation{{Name: "invalid"}, {Name: "valid", NumericValue: new(1.0)}}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("RunExperiment() error = %v", err)
	}
	byID := map[string]langfuse.ExperimentItemResult{}
	for _, item := range result.ItemResults {
		byID[item.Item.ID] = item
	}
	if !errors.Is(byID["error"].Err, taskErr) || !strings.Contains(fmt.Sprint(byID["panic"].Err), "secret panic value") ||
		!errors.Is(byID["no-input"].Err, langfuse.ErrInvalidExperiment) || byID["no-input"].TraceID != "" ||
		!errors.Is(byID["null-input"].Err, langfuse.ErrInvalidExperiment) {
		t.Fatalf("item errors = %v / %v / %v", byID["error"].Err, byID["panic"].Err, byID["no-input"].Err)
	}
	ok := byID["ok"]
	if evaluated.Load() != 1 || ok.Err != nil || !errors.Is(ok.EvaluationErr, evaluatorErr) ||
		!strings.Contains(fmt.Sprint(ok.EvaluationErr), "judge panic") ||
		!strings.Contains(fmt.Sprint(ok.EvaluationErr), `evaluation "invalid" score`) || len(ok.Evaluations) != 2 {
		t.Fatalf("evaluated %d times; ok item = %+v", evaluated.Load(), ok)
	}
	spans, scores, _ := server.recorded()
	if len(scores) != 1 || scores[0]["name"] != "valid" {
		t.Fatalf("scores = %v, want only the valid evaluation", scores)
	}
	messages := map[string]string{}
	for _, span := range spans {
		attributes := spanAttributes(t, span)
		if attributes["langfuse.observation.level"] == "ERROR" {
			messages[fmt.Sprint(attributes[itemIDKey])+"/"+span.Name] = fmt.Sprint(attributes["langfuse.observation.status_message"])
		}
	}
	for key, want := range map[string]string{
		"error/experiment-item-run": "model unavailable",
		"panic/experiment-item-run": "langfuse: experiment task panicked",
	} {
		if messages[key] != want {
			t.Errorf("status %s = %q, want %q (all: %v)", key, messages[key], want, messages)
		}
	}
	evaluatorStatuses := 0
	for key, message := range messages {
		if strings.HasPrefix(key, "ok/") {
			evaluatorStatuses++
			if strings.Contains(message, "judge panic") {
				t.Errorf("evaluator status %q exports the panic value", message)
			}
		}
	}
	if evaluatorStatuses != 2 {
		t.Fatalf("failed evaluator observations = %d, want 2 (all: %v)", evaluatorStatuses, messages)
	}
}

func TestRunExperimentBoundsConcurrencyAndKeepsOrder(t *testing.T) {
	t.Parallel()
	server := newRunnerServer(t)
	client := newRunnerClient(t, server, nil)
	var running, peak atomic.Int32
	items := make([]langfuse.ExperimentItem, 9)
	for index := range items {
		items[index] = langfuse.ExperimentItem{ID: fmt.Sprintf("item-%d", index), Input: index}
	}
	result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
		Name: "concurrency", Items: items, MaxConcurrency: 3,
		Task: func(_ context.Context, item langfuse.ExperimentItem) (any, error) {
			now := running.Add(1)
			for {
				old := peak.Load()
				if now <= old || peak.CompareAndSwap(old, now) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			running.Add(-1)
			return item.Input, nil
		},
	})
	if err != nil {
		t.Fatalf("RunExperiment() error = %v", err)
	}
	if peak.Load() != 3 {
		t.Fatalf("peak concurrency = %d, want 3", peak.Load())
	}
	for index, item := range result.ItemResults {
		if item.Item.ID != items[index].ID || item.Output != index {
			t.Fatalf("result %d = %+v, want item %d in order", index, item, index)
		}
	}
}

func TestRunExperimentCancellationStopsStartingItems(t *testing.T) {
	t.Parallel()
	server := newRunnerServer(t)
	client := newRunnerClient(t, server, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runEvaluated := false
	result, err := client.RunExperiment(ctx, langfuse.ExperimentRun{
		Name: "cancel", MaxConcurrency: 1,
		Items: []langfuse.ExperimentItem{{ID: "first", Input: 1}, {ID: "second", Input: 2}, {ID: "third", Input: 3}},
		Task: func(ctx context.Context, item langfuse.ExperimentItem) (any, error) {
			cancel()
			return nil, ctx.Err()
		},
		RunEvaluators: []langfuse.RunEvaluator{func(context.Context, []langfuse.ExperimentItemResult) ([]langfuse.Evaluation, error) {
			runEvaluated = true
			return nil, nil
		}},
	})
	if !errors.Is(err, context.Canceled) || runEvaluated {
		t.Fatalf("RunExperiment() error = %v, run evaluated = %v; want context.Canceled and no run evaluation", err, runEvaluated)
	}
	if len(result.ItemResults) != 3 || !errors.Is(result.ItemResults[0].Err, context.Canceled) ||
		result.ItemResults[0].TraceID == "" {
		t.Fatalf("first item = %+v, want a started item canceled in its task", result.ItemResults[0])
	}
	for _, item := range result.ItemResults[1:] {
		if !errors.Is(item.Err, context.Canceled) || item.TraceID != "" || item.Item.ID == "" {
			t.Fatalf("unstarted item = %+v, want context.Canceled without a trace", item)
		}
	}
}

func TestRunExperimentRejectsInvalidRunsBeforeRunning(t *testing.T) {
	t.Parallel()
	server := newRunnerServer(t)
	client := newRunnerClient(t, server, nil)
	task := func(context.Context, langfuse.ExperimentItem) (any, error) {
		t.Error("task ran for an invalid run")
		return "", nil
	}
	valid := func() langfuse.ExperimentRun {
		return langfuse.ExperimentRun{Name: "valid", Task: task, Items: []langfuse.ExperimentItem{{ID: "a", Input: 1}}}
	}
	for name, change := range map[string]func(*langfuse.ExperimentRun){
		"empty name":         func(run *langfuse.ExperimentRun) { run.Name = "" },
		"control name":       func(run *langfuse.ExperimentRun) { run.Name = "a\nb" },
		"long run name":      func(run *langfuse.ExperimentRun) { run.RunName = strings.Repeat("r", 256) },
		"long default name":  func(run *langfuse.ExperimentRun) { run.Name = strings.Repeat("n", 240) },
		"nil task":           func(run *langfuse.ExperimentRun) { run.Task = nil },
		"negative limit":     func(run *langfuse.ExperimentRun) { run.MaxConcurrency = -1 },
		"nil evaluator":      func(run *langfuse.ExperimentRun) { run.Evaluators = []langfuse.Evaluator{nil} },
		"nil run evaluator":  func(run *langfuse.ExperimentRun) { run.RunEvaluators = []langfuse.RunEvaluator{nil} },
		"missing item ID":    func(run *langfuse.ExperimentRun) { run.Items = append(run.Items, langfuse.ExperimentItem{Input: 1}) },
		"invalid dataset ID": func(run *langfuse.ExperimentRun) { run.Items[0].DatasetID = "\xff" },
		"long description":   func(run *langfuse.ExperimentRun) { run.Description = strings.Repeat("d", 16<<10+1) },
		"no items, bad name": func(run *langfuse.ExperimentRun) { run.Items, run.RunName = nil, "a\x00b" },
	} {
		run := valid()
		change(&run)
		if _, err := client.RunExperiment(context.Background(), run); !errors.Is(err, langfuse.ErrInvalidExperiment) {
			t.Errorf("%s: error = %v, want ErrInvalidExperiment", name, err)
		}
	}
	//nolint:staticcheck // A nil context is the case under test.
	if _, err := client.RunExperiment(nil, valid()); err == nil {
		t.Error("nil context accepted")
	}
	flushClient(t, client)
	if spans, scores, links := server.recorded(); len(spans)+len(scores)+len(links) != 0 {
		t.Fatalf("invalid runs sent %d spans, %d scores, %d links", len(spans), len(scores), len(links))
	}
}

func TestRunExperimentOnDisabledClientsRunsWithoutExport(t *testing.T) {
	t.Parallel()
	disabled, err := langfuse.New(context.Background(), langfuse.Config{Disabled: true})
	if err != nil {
		t.Fatal(err)
	}
	for name, client := range map[string]*langfuse.Client{"nil": nil, "disabled": disabled} {
		result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
			Name:       "offline",
			Items:      []langfuse.ExperimentItem{{ID: "a", DatasetID: "ds-1", Input: "x", ExpectedOutput: "X"}},
			Task:       upperTask,
			Evaluators: []langfuse.Evaluator{exactMatch},
			RunEvaluators: []langfuse.RunEvaluator{func(context.Context, []langfuse.ExperimentItemResult) ([]langfuse.Evaluation, error) {
				return []langfuse.Evaluation{{Name: "run", NumericValue: new(1.0)}}, nil
			}},
		})
		item := result.ItemResults[0]
		if err != nil || item.Err != nil || item.EvaluationErr != nil || item.Output != "X" || item.TraceID != "" ||
			*item.Evaluations[0].NumericValue != 1 || len(result.RunEvaluations) != 1 || result.DatasetRunID != "" {
			t.Fatalf("%s client: result = %+v, %v", name, result, err)
		}
	}
}

func TestExperimentResultSummary(t *testing.T) {
	t.Parallel()
	result := langfuse.ExperimentResult{
		ExperimentID: "exp-1", Name: "capitals", RunName: "capitals run", Description: "desc",
		ItemResults: []langfuse.ExperimentItemResult{
			{
				Item:   langfuse.ExperimentItem{ID: "a", Input: strings.Repeat("é", 60), ExpectedOutput: json.RawMessage(`"Paris"`)},
				Output: "Paris", TraceID: "trace-a",
				Evaluations: []langfuse.Evaluation{
					{Name: "exact", NumericValue: new(1.0), Comment: "match"},
					{Name: "label", StringValue: new("good")},
				},
			},
			{Item: langfuse.ExperimentItem{ID: "b", Input: map[string]any{"q": 1}}, Err: errors.New("boom")},
			{Item: langfuse.ExperimentItem{ID: "d", Input: 4}, Output: 4, EvaluationErr: errors.New("judge down")},
			{Item: langfuse.ExperimentItem{ID: "c", Input: 2}, Output: 3, Evaluations: []langfuse.Evaluation{{Name: "exact", NumericValue: new(0.0)}}},
		},
		RunEvaluations:   []langfuse.Evaluation{{Name: "accuracy", NumericValue: new(0.5), Comment: "half"}},
		RunEvaluationErr: errors.New("aggregate failed"),
	}
	summary := result.Summary(false)
	for _, want := range []string{
		"Experiment: capitals\n", "Run: capitals run\n", "Description: desc\n", "Experiment ID: exp-1\n",
		"Items: 4 (1 failed, 1 with evaluation errors)\n", "Average evaluation values:\n", "  exact: 0.500\n",
		"  label: no numeric values\n", "  accuracy: 0.500\n    half\n", "Run evaluation errors: aggregate failed\n",
	} {
		if !strings.Contains(summary, want) {
			t.Errorf("Summary(false) lacks %q:\n%s", want, summary)
		}
	}
	if strings.Contains(summary, "Trace ID") {
		t.Errorf("Summary(false) includes items:\n%s", summary)
	}
	detailed := result.Summary(true)
	for _, want := range []string{
		"1. Item a\n   Input:    " + strings.Repeat("é", 47) + "...\n", "   Expected: \"Paris\"\n",
		"   Actual:   Paris\n", "     exact: 1.000\n       match\n", "     label: good\n", "   Trace ID: trace-a\n",
		"2. Item b\n   Input:    {\"q\":1}\n   Error:    boom\n", "4. Item c\n   Input:    2\n   Actual:   3\n",
		"3. Item d\n   Input:    4\n   Actual:   4\n   Evaluation errors: judge down\n",
	} {
		if !strings.Contains(detailed, want) {
			t.Errorf("Summary(true) lacks %q:\n%s", want, detailed)
		}
	}
	if !strings.HasSuffix(detailed, summary) {
		t.Errorf("Summary(true) does not end with the summary")
	}
}

func TestRunExperimentLinkFailuresAreItemErrors(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		status    int
		overrides map[string]any
		raw       string
		want      error
	}{
		"server error":      {status: http.StatusInternalServerError, raw: `{}`, want: langfuse.ErrWriteOutcomeUnknown},
		"other item":        {overrides: map[string]any{"datasetItemId": "other"}, want: langfuse.ErrWriteOutcomeUnknown},
		"other trace":       {overrides: map[string]any{"traceId": "0123456789abcdef0123456789abcdef"}, want: langfuse.ErrWriteOutcomeUnknown},
		"other observation": {overrides: map[string]any{"observationId": "0123456789abcdef"}, want: langfuse.ErrWriteOutcomeUnknown},
		"null observation":  {overrides: map[string]any{"observationId": nil}, want: langfuse.ErrWriteOutcomeUnknown},
		"empty observation": {overrides: map[string]any{"observationId": ""}, want: langfuse.ErrWriteOutcomeUnknown},
		"other run name":    {overrides: map[string]any{"datasetRunName": "another run"}, want: langfuse.ErrWriteOutcomeUnknown},
		"missing run ID":    {overrides: map[string]any{"datasetRunId": ""}, want: langfuse.ErrWriteOutcomeUnknown},
		"invalid run ID":    {overrides: map[string]any{"datasetRunId": "run\n1"}, want: langfuse.ErrWriteOutcomeUnknown},
		"oversized run ID":  {overrides: map[string]any{"datasetRunId": strings.Repeat("r", 256)}, want: langfuse.ErrWriteOutcomeUnknown},
		"not JSON response": {raw: `not json`, want: langfuse.ErrWriteOutcomeUnknown},
		"rejected request":  {status: http.StatusBadRequest, raw: `{}`},
		"missing item":      {status: http.StatusNotFound, raw: `{}`, want: langfuse.ErrDatasetItemNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := newRunnerServer(t)
			server.link = func(request map[string]any) (int, string) {
				status := cmp.Or(test.status, http.StatusOK)
				if test.raw != "" {
					return status, test.raw
				}
				return status, linkResponse(request, test.overrides)
			}
			client := newRunnerClient(t, server, nil)
			result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
				Name: "links", Items: []langfuse.ExperimentItem{{ID: "item", DatasetID: "ds", Input: 1}},
				Task: func(context.Context, langfuse.ExperimentItem) (any, error) {
					t.Error("task ran after a failed link")
					return "", nil
				},
			})
			item := result.ItemResults[0]
			if err != nil || item.Err == nil || (test.want != nil && !errors.Is(item.Err, test.want)) ||
				(test.want == nil && errors.Is(item.Err, langfuse.ErrWriteOutcomeUnknown)) ||
				item.DatasetRunID != "" || result.DatasetRunID != "" {
				t.Fatalf("result = %+v, %v; want item error %v", item, err, test.want)
			}
		})
	}
}

func TestRunExperimentExportsOnlyMaskedContent(t *testing.T) {
	t.Parallel()
	for name, capture := range map[string]bool{"content capture": true, "no content capture": false} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := newRunnerServer(t)
			client := newRunnerClient(t, server, func(config *langfuse.Config) {
				config.DisableContentCapture = !capture
				config.Mask = func(field langfuse.MaskField, value any) any {
					switch field { //nolint:exhaustive // Other fields pass through.
					case langfuse.MaskExperimentItemMetadata:
						return map[string]any{"patient": "[masked]"}
					case langfuse.MaskExperimentItemExpectedOutput:
						return "[masked expected]"
					default:
						return value
					}
				}
			})
			var seen langfuse.EvaluatorInput
			_, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
				Name: "masking",
				Items: []langfuse.ExperimentItem{{
					ID: "a", Input: "question", ExpectedOutput: "secret answer",
					Metadata: map[string]any{"patient": "secret name"},
				}},
				Task: func(context.Context, langfuse.ExperimentItem) (any, error) { return "answer", nil },
				Evaluators: []langfuse.Evaluator{func(_ context.Context, input langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
					seen = input
					return nil, nil
				}},
			})
			if err != nil {
				t.Fatalf("RunExperiment() error = %v", err)
			}
			if metadata, _ := seen.Metadata.(map[string]any); seen.ExpectedOutput != "secret answer" || metadata["patient"] != "secret name" {
				t.Fatalf("evaluator input = %+v, want the unmasked local values", seen)
			}
			spans, _, _ := server.recorded()
			for _, span := range spans {
				for key, value := range spanAttributes(t, span) {
					if strings.Contains(fmt.Sprint(value), "secret") {
						t.Errorf("%s %s exports unmasked content: %v", span.Name, key, value)
					}
					if !capture && (key == "langfuse.observation.input" || key == "langfuse.observation.output" ||
						key == itemExpectedKey) {
						t.Errorf("%s exports %s with content capture off", span.Name, key)
					}
				}
			}
			if capture {
				// A closure's observation is named after its compiler name.
				var evaluator []*tracepb.Span
				for _, span := range spans {
					if spanAttributes(t, span)["langfuse.observation.type"] == "evaluator" {
						evaluator = append(evaluator, span)
					}
				}
				if len(evaluator) != 1 || !strings.HasPrefix(evaluator[0].Name, "TestRunExperimentExportsOnlyMaskedContent.func") {
					t.Fatalf("evaluator observations = %v", evaluator)
				}
				input := fmt.Sprint(spanAttributes(t, evaluator[0])["langfuse.observation.input"])
				if !strings.Contains(input, `"expected_output":"[masked expected]"`) || !strings.Contains(input, `"patient":"[masked]"`) {
					t.Fatalf("evaluator observation input = %s, want masked expected output and metadata", input)
				}
			}
		})
	}
}

func TestScoreDatasetRunTarget(t *testing.T) {
	t.Parallel()
	client, receiver := newScoreWireClient(t, nil)
	value := 0.75
	if err := client.RecordScore(context.Background(), langfuse.Score{
		Name: "accuracy", DatasetRunID: "run-1", NumericValue: &value,
	}); err != nil {
		t.Fatalf("RecordScore() error = %v", err)
	}
	for name, score := range map[string]langfuse.Score{
		"run and trace":       {Name: "s", DatasetRunID: "run-1", TraceID: "0123456789abcdef0123456789abcdef", NumericValue: &value},
		"run and session":     {Name: "s", DatasetRunID: "run-1", SessionID: "session", NumericValue: &value},
		"run and observation": {Name: "s", DatasetRunID: "run-1", ObservationID: "0123456789abcdef", NumericValue: &value},
		"correction on run":   {Name: "s", DatasetRunID: "run-1", StringValue: new("fix"), DataType: langfuse.ScoreTypeCorrection},
		"invalid run ID":      {Name: "s", DatasetRunID: "\xff", NumericValue: &value},
	} {
		if err := client.RecordScore(context.Background(), score); err == nil {
			t.Errorf("%s: RecordScore() accepted an invalid target", name)
		}
	}
	flushClient(t, client)
	var bodies []map[string]any
	for _, request := range receiver.all() {
		if strings.HasSuffix(request.path, "/api/public/ingestion") {
			_, body := scoreWireEvent(t, request)
			bodies = append(bodies, body)
		}
	}
	if len(bodies) != 1 || bodies[0]["datasetRunId"] != "run-1" || bodies[0]["traceId"] != nil ||
		bodies[0]["sessionId"] != nil || bodies[0]["value"] != 0.75 || bodies[0]["environment"] != "score_wire" {
		t.Fatalf("score bodies = %v, want one dataset run score", bodies)
	}
}

func TestRunExperimentSurvivesShutdownFromATask(t *testing.T) {
	t.Parallel()
	server := newRunnerServer(t)
	config := langfuse.Config{BaseURL: server.URL, PublicKey: "pk-lf-runner", SecretKey: "sk-lf-runner"}
	client, err := langfuse.New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var result langfuse.ExperimentResult
	go func() {
		defer close(done)
		result, err = client.RunExperiment(context.Background(), langfuse.ExperimentRun{
			Name: "shutdown", MaxConcurrency: 1,
			Items: []langfuse.ExperimentItem{
				{ID: "first", DatasetID: "ds", Input: 1}, {ID: "second", DatasetID: "ds", Input: 2},
			},
			Task: func(ctx context.Context, item langfuse.ExperimentItem) (any, error) {
				if item.ID == "first" {
					shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					if err := client.Shutdown(shutdownCtx); err != nil {
						t.Errorf("Shutdown() from a task error = %v", err)
					}
				}
				return "out", nil
			},
			Evaluators: []langfuse.Evaluator{exactMatch},
		})
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("RunExperiment deadlocked after Shutdown from a task")
	}
	if err != nil {
		t.Fatalf("RunExperiment() error = %v", err)
	}
	first, second := result.ItemResults[0], result.ItemResults[1]
	if first.Err != nil || first.Output != "out" || first.EvaluationErr == nil {
		t.Fatalf("first item = %+v, want its output and a rejected score after shutdown", first)
	}
	if second.Err == nil || second.TraceID != "" {
		t.Fatalf("second item = %+v, want a start failure after shutdown", second)
	}
}

// exportedText renders everything a runner server received, for leak scans.
func (s *runnerServer) exportedText(t *testing.T) string {
	t.Helper()
	spans, scores, links := s.recorded()
	var b strings.Builder
	for _, span := range spans {
		fmt.Fprintf(&b, "%s %v %s\n", span.Name, spanAttributes(t, span), span.Status.GetMessage())
		for _, event := range span.Events {
			fmt.Fprintf(&b, "%s %v\n", event.Name, observationWireAttributeMap(t, event.Attributes))
		}
	}
	for _, body := range append(scores, links...) {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(data)
		b.WriteByte('\n')
	}
	return b.String()
}

func TestRunExperimentMasksEveryExportedCopyOnce(t *testing.T) {
	t.Parallel()
	sentinels := map[langfuse.MaskField]string{
		langfuse.MaskObservationInput:             "SENT_INPUT",
		langfuse.MaskObservationOutput:            "SENT_OUTPUT",
		langfuse.MaskExperimentItemExpectedOutput: "SENT_EXPECTED",
		langfuse.MaskExperimentItemMetadata:       "SENT_ITEM_META",
		langfuse.MaskExperimentMetadata:           "SENT_RUN_META",
		langfuse.MaskScoreMetadata:                "SENT_SCORE_META",
	}
	for field, sentinel := range sentinels {
		for _, mode := range []string{"replace", "nil", "panic"} {
			t.Run(string(field)+"/"+mode, func(t *testing.T) {
				t.Parallel()
				server := newRunnerServer(t)
				var calls sync.Map
				client := newRunnerClient(t, server, func(config *langfuse.Config) {
					config.Mask = func(got langfuse.MaskField, value any) any {
						if got != field {
							return value
						}
						count, _ := calls.LoadOrStore(got, new(atomic.Int32))
						count.(*atomic.Int32).Add(1)
						switch mode {
						case "nil":
							return nil
						case "panic":
							panic("mask " + sentinel)
						}
						if _, ok := value.(map[string]any); ok {
							return map[string]any{"redacted": true}
						}
						return "[redacted]"
					}
				})
				evaluation := func(name string) langfuse.Evaluation {
					return langfuse.Evaluation{Name: name, NumericValue: new(1.0), Metadata: map[string]any{"sk": "SENT_SCORE_META"}}
				}
				result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
					Name: "privacy", RunName: "privacy", Metadata: map[string]any{"rk": "SENT_RUN_META"},
					Items: []langfuse.ExperimentItem{{
						ID: "item", DatasetID: "ds", Input: "SENT_INPUT", ExpectedOutput: "SENT_EXPECTED",
						Metadata: map[string]any{"ik": "SENT_ITEM_META"},
					}},
					Task: func(context.Context, langfuse.ExperimentItem) (any, error) { return "SENT_OUTPUT", nil },
					Evaluators: []langfuse.Evaluator{func(context.Context, langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
						return []langfuse.Evaluation{evaluation("item")}, nil
					}},
					CompositeEvaluator: func(context.Context, langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
						return []langfuse.Evaluation{evaluation("composite")}, nil
					},
					RunEvaluators: []langfuse.RunEvaluator{func(context.Context, []langfuse.ExperimentItemResult) ([]langfuse.Evaluation, error) {
						return []langfuse.Evaluation{evaluation("run")}, nil
					}},
				})
				if err != nil {
					t.Fatalf("RunExperiment() error = %v", err)
				}
				exported := server.exportedText(t)
				if strings.Contains(exported, sentinel) {
					t.Fatalf("%s sentinel exported with a %s mask:\n%s", field, mode, exported)
				}
				// A panicking experiment-field masker fails the item start.
				startFails := mode == "panic" && (field == langfuse.MaskExperimentMetadata ||
					field == langfuse.MaskExperimentItemMetadata || field == langfuse.MaskExperimentItemExpectedOutput)
				if startFails != errors.Is(result.ItemResults[0].Err, langfuse.ErrInvalidExperiment) {
					t.Fatalf("item error = %v, want a start failure only for a panicking experiment masker", result.ItemResults[0].Err)
				}
				// Otherwise every other sentinel is exported: masks are per field.
				for other, otherSentinel := range sentinels {
					if !startFails && other != field && !strings.Contains(exported, otherSentinel) {
						t.Errorf("%s sentinel missing while masking %s", other, field)
					}
				}
				if count, ok := calls.Load(field); ok && field != langfuse.MaskScoreMetadata && count.(*atomic.Int32).Load() != 1 {
					t.Errorf("%s masked %d times, want once", field, count.(*atomic.Int32).Load())
				}
			})
		}
	}
}

func TestRunExperimentCancellationDuringRunEvaluators(t *testing.T) {
	t.Parallel()
	server := newRunnerServer(t)
	enabled := newRunnerClient(t, server, nil)
	disabled, err := langfuse.New(context.Background(), langfuse.Config{Disabled: true})
	if err != nil {
		t.Fatal(err)
	}
	for name, client := range map[string]*langfuse.Client{"enabled": enabled, "disabled": disabled, "nil": nil} {
		ctx, cancel := context.WithCancel(context.Background())
		var second atomic.Bool
		result, err := client.RunExperiment(ctx, langfuse.ExperimentRun{
			Name: "cancel-run", Items: []langfuse.ExperimentItem{{ID: "a", DatasetID: "ds", Input: 1}},
			Task: func(context.Context, langfuse.ExperimentItem) (any, error) { return "out", nil },
			RunEvaluators: []langfuse.RunEvaluator{
				func(context.Context, []langfuse.ExperimentItemResult) ([]langfuse.Evaluation, error) {
					cancel()
					return []langfuse.Evaluation{{Name: "first", NumericValue: new(1.0)}}, nil
				},
				func(context.Context, []langfuse.ExperimentItemResult) ([]langfuse.Evaluation, error) {
					second.Store(true)
					return nil, nil
				},
			},
		})
		if !errors.Is(err, context.Canceled) || second.Load() || len(result.RunEvaluations) != 1 ||
			result.ItemResults[0].Err != nil {
			t.Errorf("%s client: result %+v, err %v, second evaluator ran %v", name, result, err, second.Load())
		}
		cancel()
	}
	flushClient(t, enabled)
	if _, scores, _ := server.recorded(); slices.ContainsFunc(scores, func(score map[string]any) bool { return score["name"] == "first" }) {
		t.Errorf("a run score was recorded after cancellation: %v", scores)
	}
}

func TestRunExperimentExportsFrozenMaskedSnapshots(t *testing.T) {
	t.Parallel()
	server := newRunnerServer(t)
	containsMarker := func(value any) bool {
		data, err := json.Marshal(value)
		return err != nil || strings.Contains(string(data), "PRIVATE")
	}
	client := newRunnerClient(t, server, func(config *langfuse.Config) {
		// A valid masker: it replaces marked content and returns other
		// values unchanged, without copying them.
		config.Mask = func(field langfuse.MaskField, value any) any {
			if !containsMarker(value) {
				return value
			}
			if _, ok := value.(map[string]any); ok {
				return map[string]any{"text": "[redacted]"}
			}
			return "[redacted]"
		}
	})
	input := map[string]any{"text": "safe input"}
	expected := map[string]any{"text": "safe expected"}
	metadata := map[string]any{"text": "safe metadata"}
	output := map[string]any{"text": "safe output"}
	first := true
	_, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
		Name:  "frozen",
		Items: []langfuse.ExperimentItem{{ID: "a", Input: input, ExpectedOutput: expected, Metadata: metadata}},
		Task: func(context.Context, langfuse.ExperimentItem) (any, error) {
			input["text"], expected["text"], metadata["text"] = "PRIVATE_INPUT", "PRIVATE_EXPECTED", "PRIVATE_METADATA"
			return output, nil
		},
		Evaluators: []langfuse.Evaluator{
			func(context.Context, langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
				if first {
					first = false
					output["text"] = "PRIVATE_OUTPUT"
				}
				return []langfuse.Evaluation{{Name: "one", NumericValue: new(1.0)}}, nil
			},
			func(context.Context, langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
				return []langfuse.Evaluation{{Name: "two", NumericValue: new(1.0)}}, nil
			},
		},
		CompositeEvaluator: func(context.Context, langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
			return []langfuse.Evaluation{{Name: "composite", NumericValue: new(1.0)}}, nil
		},
	})
	if err != nil {
		t.Fatalf("RunExperiment() error = %v", err)
	}
	exported := server.exportedText(t)
	if strings.Contains(exported, "PRIVATE") {
		t.Fatalf("content changed after masking was exported:\n%s", exported)
	}
	if strings.Count(exported, `"input":{"text":"safe input"}`) != 3 ||
		strings.Count(exported, `"output":{"text":"safe output"}`) != 3 {
		t.Fatalf("every evaluator observation should export the snapshot taken before the callbacks:\n%s", exported)
	}
}

// flippingJSON serializes as "safe" once, then as a private marker.
type flippingJSON struct{ calls atomic.Int32 }

func (f *flippingJSON) MarshalJSON() ([]byte, error) {
	if f.calls.Add(1) == 1 {
		return []byte(`"safe"`), nil
	}
	return []byte(`"PRIVATE_SECOND_SERIALIZATION"`), nil
}

func TestRunExperimentFreezesMaskedMetadataAtTheMaskBoundary(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, mask func(langfuse.MaskField, any) any) string {
		t.Helper()
		server := newRunnerServer(t)
		client := newRunnerClient(t, server, func(config *langfuse.Config) { config.Mask = mask })
		result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
			Name: "metadata", MaxConcurrency: 1, Metadata: map[string]any{"run": "value"},
			Items: []langfuse.ExperimentItem{{ID: "a", Input: 1, Metadata: map[string]any{"item": "value"}}},
			Task:  func(context.Context, langfuse.ExperimentItem) (any, error) { return 1, nil },
			Evaluators: []langfuse.Evaluator{func(context.Context, langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
				return []langfuse.Evaluation{
					{Name: "one", NumericValue: new(1.0), Metadata: map[string]any{"x": 1}},
					{Name: "two", NumericValue: new(1.0), Metadata: map[string]any{"x": 2}},
				}, nil
			}},
		})
		if err != nil || result.ItemResults[0].Err != nil {
			t.Fatalf("RunExperiment() = %+v, %v", result.ItemResults, err)
		}
		return server.exportedText(t)
	}
	// A masker may reuse its own result buffer; a later Mask call must not
	// change what an earlier one approved.
	t.Run("reused experiment metadata buffer", func(t *testing.T) {
		shared := map[string]any{}
		exported := run(t, func(field langfuse.MaskField, value any) any {
			switch field { //nolint:exhaustive // Other fields pass through.
			case langfuse.MaskExperimentMetadata:
				shared["text"] = "safe"
				return shared
			case langfuse.MaskExperimentItemMetadata:
				shared["text"] = "PRIVATE_SHARED"
				return nil
			default:
				return value
			}
		})
		if strings.Contains(exported, "PRIVATE") || !strings.Contains(exported, "langfuse.observation.metadata.text:safe") {
			t.Fatalf("root metadata did not keep the approved value:\n%s", exported)
		}
	})
	t.Run("reused score metadata buffer", func(t *testing.T) {
		shared := map[string]any{}
		var calls atomic.Int32
		exported := run(t, func(field langfuse.MaskField, value any) any {
			if field != langfuse.MaskScoreMetadata {
				return value
			}
			if calls.Add(1) == 1 {
				shared["text"] = "safe"
				return shared
			}
			shared["text"] = "PRIVATE_SHARED"
			return nil
		})
		if strings.Contains(exported, "PRIVATE") || strings.Count(exported, `"metadata":{"text":"safe"}`) != 2 {
			t.Fatalf("score metadata did not keep the approved value in the output and the score:\n%s", exported)
		}
	})
	for _, field := range []langfuse.MaskField{langfuse.MaskExperimentMetadata, langfuse.MaskScoreMetadata} {
		t.Run("stateful "+string(field), func(t *testing.T) {
			var mu sync.Mutex
			var flips []*flippingJSON
			exported := run(t, func(got langfuse.MaskField, value any) any {
				if got != field {
					return value
				}
				flip := &flippingJSON{}
				mu.Lock()
				flips = append(flips, flip)
				mu.Unlock()
				return map[string]any{"text": flip}
			})
			if strings.Contains(exported, "PRIVATE") || len(flips) == 0 {
				t.Fatalf("a masked value was serialized again:\n%s", exported)
			}
			for index, flip := range flips {
				if calls := flip.calls.Load(); calls != 1 {
					t.Errorf("mask result %d serialized %d times, want once", index, calls)
				}
			}
		})
	}
}

type namedString string

type stringMarshaler struct{}

func (stringMarshaler) MarshalJSON() ([]byte, error) { return []byte(`"safe"`), nil }

func TestRunExperimentRootMetadataKeepsEachEntrysWireForm(t *testing.T) {
	t.Parallel()
	metadata := func(prefix string) map[string]any {
		text := "safe"
		return map[string]any{
			prefix + "native":    "safe",
			prefix + "raw":       json.RawMessage(`"safe"`),
			prefix + "named":     namedString("safe"),
			prefix + "pointer":   &text,
			prefix + "marshaler": stringMarshaler{},
			prefix + "flip":      &flippingJSON{},
			prefix + "rawnull":   json.RawMessage(`null`),
			prefix + "nil":       nil,
			prefix + "big":       json.Number("12345678901234567890"),
			prefix + "exp":       json.Number("1.2300e+04"),
			prefix + "array":     []any{"a", nil, json.Number("-0")},
			prefix + "nested":    map[string]any{"s": "x", "n": nil, "b": json.Number("12345678901234567890")},
		}
	}
	server := newRunnerServer(t)
	client := newRunnerClient(t, server, nil)
	// The control is an ordinary observation given the same metadata.
	_, control := client.StartObservation(context.Background(), "control", langfuse.TypeSpan,
		langfuse.ObservationAttributes{Metadata: func() map[string]any {
			result := metadata("item_")
			maps.Copy(result, metadata("run_"))
			return result
		}()})
	control.End()
	result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
		Name: "wire", Metadata: metadata("run_"),
		Items: []langfuse.ExperimentItem{{ID: "a", Input: 1, Metadata: metadata("item_")}},
		Task:  func(context.Context, langfuse.ExperimentItem) (any, error) { return 1, nil },
	})
	if err != nil || result.ItemResults[0].Err != nil {
		t.Fatalf("RunExperiment() = %+v, %v", result.ItemResults, err)
	}
	flushClient(t, client)
	spans, _, _ := server.recorded()
	want := spanAttributes(t, spansNamed(spans, "control")[0])
	got := spanAttributes(t, spansNamed(spans, "experiment-item-run")[0])
	checked := 0
	for key, value := range want {
		if !strings.HasPrefix(key, "langfuse.observation.metadata.") {
			continue
		}
		checked++
		if got[key] != value {
			t.Errorf("%s = %#v, want %#v as an ordinary observation exports it", key, got[key], value)
		}
	}
	for _, key := range []string{"item_nil", "run_nil"} {
		if _, ok := got["langfuse.observation.metadata."+key]; ok {
			t.Errorf("nil entry %s exported", key)
		}
	}
	if checked != 22 {
		t.Fatalf("control exported %d metadata entries, want 22: %v", checked, want)
	}
}

func TestRunExperimentRejectsScoresWhoseMetadataCannotBeSent(t *testing.T) {
	t.Parallel()
	server := newRunnerServer(t)
	client := newRunnerClient(t, server, nil)
	evaluate := func(context.Context, langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
		return []langfuse.Evaluation{
			{Name: "unsupported", NumericValue: new(1.0), Metadata: map[string]any{"channel": make(chan int)}},
			{Name: "oversized", NumericValue: new(1.0), Metadata: map[string]any{"text": strings.Repeat("x", 129<<10)}},
			{
				Name: "envelope", NumericValue: new(1.0), Comment: strings.Repeat("c", 100<<10),
				Metadata: map[string]any{"text": strings.Repeat("x", 100<<10)},
			},
			{Name: "valid", NumericValue: new(1.0), Metadata: map[string]any{"ok": true}},
		}, nil
	}
	result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
		Name: "metadata", Items: []langfuse.ExperimentItem{{ID: "a", Input: 1}},
		Task:               func(context.Context, langfuse.ExperimentItem) (any, error) { return 1, nil },
		Evaluators:         []langfuse.Evaluator{evaluate},
		CompositeEvaluator: evaluate,
	})
	if err != nil {
		t.Fatalf("RunExperiment() error = %v", err)
	}
	item := result.ItemResults[0]
	message := fmt.Sprint(item.EvaluationErr)
	// The envelope case fails like RecordScore: the bounded serialization of
	// the whole event stops at its limit.
	if len(item.Evaluations) != 8 || strings.Count(message, "score could not be serialized") != 6 ||
		strings.Contains(message, "xxxx") {
		t.Fatalf("evaluations %d; evaluation errors: %v", len(item.Evaluations), item.EvaluationErr)
	}
	_, scores, _ := server.recorded()
	if len(scores) != 2 || scores[0]["name"] != "valid" || scores[1]["name"] != "valid" ||
		fmt.Sprint(scores[0]["metadata"]) != "map[ok:true]" {
		t.Fatalf("scores = %v, want only the two valid ones with their metadata", scores)
	}
	if summary := result.Summary(false); !strings.Contains(summary, "1 with evaluation errors") {
		t.Fatalf("Summary() = %s", summary)
	}
}

// countingJSON counts its serializations.
type countingJSON struct{ calls atomic.Int32 }

func (c *countingJSON) MarshalJSON() ([]byte, error) {
	c.calls.Add(1)
	return []byte(`"value"`), nil
}

func TestRunExperimentPreparesNoTelemetryOnDisabledClients(t *testing.T) {
	t.Parallel()
	disabled, err := langfuse.New(context.Background(), langfuse.Config{Disabled: true})
	if err != nil {
		t.Fatal(err)
	}
	for name, client := range map[string]*langfuse.Client{"nil": nil, "disabled": disabled} {
		counter := &countingJSON{}
		var evaluated atomic.Int32
		result, err := client.RunExperiment(context.Background(), langfuse.ExperimentRun{
			Name: "offline", Metadata: map[string]any{"run": counter},
			Items: []langfuse.ExperimentItem{{ID: "a", Input: counter, ExpectedOutput: counter, Metadata: map[string]any{"item": counter}}},
			Task:  func(context.Context, langfuse.ExperimentItem) (any, error) { return counter, nil },
			Evaluators: []langfuse.Evaluator{func(context.Context, langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
				evaluated.Add(1)
				return []langfuse.Evaluation{{Name: "e", NumericValue: new(1.0), Metadata: map[string]any{"score": counter}}}, nil
			}},
		})
		item := result.ItemResults[0]
		if err != nil || item.Err != nil || item.EvaluationErr != nil || evaluated.Load() != 1 ||
			item.Evaluations[0].Metadata["score"] != counter {
			t.Fatalf("%s client: %+v, %v", name, item, err)
		}
		if calls := counter.calls.Load(); calls != 0 {
			t.Errorf("%s client serialized discarded content %d times", name, calls)
		}
	}
}
