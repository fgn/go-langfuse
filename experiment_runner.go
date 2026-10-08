package langfuse

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
	"github.com/fgn/go-langfuse/internal/transport"
)

const defaultExperimentConcurrency = 50

var (
	// errTaskPanicked and errEvaluatorPanicked are the exported statuses of
	// observations whose callback panicked; the returned error carries the
	// panic value.
	errTaskPanicked      = errors.New("langfuse: experiment task panicked")
	errEvaluatorPanicked = errors.New("langfuse: evaluator panicked")
)

// ExperimentTask produces the output of one item. ctx carries the item's root
// observation, so observations started from it join the item trace.
type ExperimentTask func(ctx context.Context, item ExperimentItem) (output any, err error)

// Evaluator scores the output of one item. ctx carries the evaluator's own
// observation in the item trace.
type Evaluator func(ctx context.Context, input EvaluatorInput) ([]Evaluation, error)

// RunEvaluator scores a whole run from its item results, including failed
// items.
type RunEvaluator func(ctx context.Context, results []ExperimentItemResult) ([]Evaluation, error)

// EvaluatorInput is what an [Evaluator] judges.
type EvaluatorInput struct {
	Input          any
	Output         any
	ExpectedOutput any
	Metadata       any
	// Evaluations holds the item's evaluations so far; it is set only for
	// ExperimentRun.CompositeEvaluator.
	Evaluations []Evaluation
}

// Evaluation is one score produced by an evaluator. The value and type rules
// are those of [Score].
type Evaluation struct {
	Name         string
	NumericValue *float64
	StringValue  *string
	DataType     ScoreDataType
	ConfigID     string
	Comment      string
	Metadata     map[string]any
}

// ExperimentRun configures [Client.RunExperiment].
type ExperimentRun struct {
	// Name labels the experiment and may repeat across runs. Required; same
	// rules as Experiment.ID.
	Name string
	// RunName names this run and is exported as the experiment name; for
	// dataset items it names the dataset run, so reusing it adds items to
	// that run. Empty selects Name followed by the start time.
	RunName string
	// Description and Metadata follow the rules of [Experiment]; Metadata is
	// also sent with each dataset run link.
	Description string
	Metadata    map[string]any
	// Items are the data; each needs an ID, and an item without Input fails.
	// Items from [DatasetItem.ExperimentItem] also link the run to their
	// dataset.
	Items []ExperimentItem
	// Task is required.
	Task ExperimentTask
	// Evaluators run in order after a successful task.
	Evaluators []Evaluator
	// CompositeEvaluator runs after Evaluators when they produced at least one
	// evaluation, with those evaluations in its input.
	CompositeEvaluator Evaluator
	// RunEvaluators run in order once every item has finished.
	RunEvaluators []RunEvaluator
	// MaxConcurrency bounds how many items run at once; 0 selects 50.
	MaxConcurrency int
}

// ExperimentItemResult is the outcome of one item.
type ExperimentItemResult struct {
	Item   ExperimentItem
	Output any
	// Evaluations holds the evaluations returned by the evaluators, including
	// any whose score was rejected.
	Evaluations []Evaluation
	// TraceID and ObservationID identify the item root; empty when the item
	// did not start or the client is disabled.
	TraceID       string
	ObservationID string
	// DatasetRunID is the dataset run the item was linked to, if any.
	DatasetRunID string
	// Err reports why the item has no output: its start, dataset run link,
	// or task failed, or the run was canceled before it started.
	Err error
	// EvaluationErr joins evaluator failures and rejected scores.
	EvaluationErr error
}

// ExperimentResult is the outcome of [Client.RunExperiment].
type ExperimentResult struct {
	// ExperimentID is the first dataset run ID among the items, or a random
	// ID shared by the run's local items.
	ExperimentID string
	Name         string
	RunName      string
	Description  string
	// DatasetRunID is the first item's dataset run ID; empty for local data.
	DatasetRunID string
	// ItemResults are in the order of ExperimentRun.Items.
	ItemResults    []ExperimentItemResult
	RunEvaluations []Evaluation
	// RunEvaluationErr joins run evaluator failures and rejected scores.
	RunEvaluationErr error
}

// RunExperiment runs Task on every item, scores each output with the
// evaluators, scores the run with the run evaluators, and flushes.
//
// Each item runs as [Client.StartExperimentItem] would, in a root
// observation named "experiment-item-run" that ends when the task returns.
// A dataset item is first linked to the dataset run named RunName, and that
// run's ID becomes the item's experiment ID; Langfuse derives it from the
// dataset and run name. A failed link fails the item. Each evaluator runs in
// its own evaluator observation, and its evaluations become scores on the
// item root. Run evaluations become scores on the dataset run, so local runs
// keep them only in the result. Task and evaluator errors are recorded like
// [Observation.RecordError], and panics are recovered as errors.
//
// It returns an error, without running anything, for an invalid run, and
// otherwise reports failures per item in the result. Canceling ctx stops
// starting items and running run evaluators, skips their scores and the
// flush, and returns the context error with the partial result; a flush
// failure is returned with the complete result. A nil or disabled
// client runs the tasks and evaluators without exporting anything.
func (c *Client) RunExperiment(ctx context.Context, run ExperimentRun) (ExperimentResult, error) {
	if ctx == nil {
		return ExperimentResult{}, errors.New("langfuse: experiment context is nil")
	}
	if run.RunName == "" {
		run.RunName = run.Name + " - " + time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	}
	if err := validateExperimentRun(run); err != nil {
		return ExperimentResult{}, err
	}
	localID, err := newExperimentID()
	if err != nil {
		return ExperimentResult{}, err
	}
	result := ExperimentResult{
		Name:        run.Name,
		RunName:     run.RunName,
		Description: run.Description,
		ItemResults: make([]ExperimentItemResult, len(run.Items)),
	}
	concurrency := run.MaxConcurrency
	if concurrency == 0 {
		concurrency = defaultExperimentConcurrency
	}
	var next atomic.Int64
	var workers sync.WaitGroup
	for range min(concurrency, len(run.Items)) {
		workers.Go(func() {
			for {
				index := int(next.Add(1) - 1)
				if index >= len(run.Items) {
					return
				}
				if err := ctx.Err(); err != nil {
					result.ItemResults[index] = ExperimentItemResult{
						Item: run.Items[index],
						Err:  fmt.Errorf("langfuse: experiment item not started: %w", err),
					}
					continue
				}
				result.ItemResults[index] = c.runExperimentItem(ctx, &run, localID, run.Items[index])
			}
		})
	}
	workers.Wait()

	result.ExperimentID = localID
	for _, item := range result.ItemResults {
		if item.DatasetRunID != "" {
			result.DatasetRunID, result.ExperimentID = item.DatasetRunID, item.DatasetRunID
			break
		}
	}
	canceled := func() (ExperimentResult, error) {
		return result, fmt.Errorf("langfuse: experiment run canceled: %w", ctx.Err())
	}
	if ctx.Err() != nil {
		return canceled()
	}
	var runErrs []error
	for _, evaluator := range run.RunEvaluators {
		evaluations, err := callRunEvaluator(ctx, evaluator, result.ItemResults)
		if err != nil {
			runErrs = append(runErrs, err)
		} else {
			result.RunEvaluations = append(result.RunEvaluations, evaluations...)
		}
		if ctx.Err() != nil {
			result.RunEvaluationErr = errors.Join(runErrs...)
			return canceled()
		}
	}
	if result.DatasetRunID != "" {
		for _, evaluation := range result.RunEvaluations {
			score := evaluation.score()
			score.DatasetRunID = result.DatasetRunID
			if err := c.RecordScore(ctx, score); err != nil {
				runErrs = append(runErrs, fmt.Errorf("langfuse: run evaluation %q score: %w", evaluation.Name, err))
			}
		}
	}
	result.RunEvaluationErr = errors.Join(runErrs...)
	if err := c.Flush(ctx); err != nil {
		return result, fmt.Errorf("langfuse: experiment flush: %w", err)
	}
	if ctx.Err() != nil {
		return canceled()
	}
	return result, nil
}

func validateExperimentRun(run ExperimentRun) error {
	if err := validateExperimentIdentifier("experiment name", run.Name); err != nil {
		return err
	}
	if run.Task == nil {
		return fmt.Errorf("%w: task is required", ErrInvalidExperiment)
	}
	if run.MaxConcurrency < 0 {
		return fmt.Errorf("%w: max concurrency is negative", ErrInvalidExperiment)
	}
	for _, evaluator := range run.Evaluators {
		if evaluator == nil {
			return fmt.Errorf("%w: evaluator is nil", ErrInvalidExperiment)
		}
	}
	for _, evaluator := range run.RunEvaluators {
		if evaluator == nil {
			return fmt.Errorf("%w: run evaluator is nil", ErrInvalidExperiment)
		}
	}
	if err := validateExperiment(Experiment{Name: run.RunName, Description: run.Description}, false); err != nil {
		return err
	}
	for index, item := range run.Items {
		if err := validateExperimentItem(item); err != nil {
			return fmt.Errorf("item %d: %w", index, err)
		}
	}
	return nil
}

// newExperimentID returns 16 random hex characters, the official SDKs' form.
func newExperimentID() (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", errors.New("langfuse: generate experiment ID")
	}
	return hex.EncodeToString(raw[:]), nil
}

func (c *Client) runExperimentItem(
	ctx context.Context, run *ExperimentRun, localID string, item ExperimentItem,
) ExperimentItemResult {
	result := ExperimentItemResult{Item: item}
	if raw, ok := item.Input.(json.RawMessage); isNilValue(item.Input) || ok && string(bytes.TrimSpace(raw)) == "null" {
		result.Err = fmt.Errorf("%w: item input is missing", ErrInvalidExperiment)
		return result
	}
	experiment := Experiment{ID: localID, Name: run.RunName, Description: run.Description, Metadata: run.Metadata}
	start := experimentStart{
		rootMetadata: func(experimentMetadata, itemMetadata map[string]any) map[string]any {
			return experimentRootMetadata(run, item, experimentMetadata, itemMetadata)
		},
	}
	if item.DatasetID != "" {
		start.link = func(root *Observation, metadata string) (string, error) {
			id, err := c.linkDatasetRunItem(ctx, run, item, root, metadata)
			result.DatasetRunID = id
			return id, err
		}
	}
	// Each value is masked once and frozen before the next callback can change
	// it; the root and the evaluator observations export the same snapshots.
	capture := c != nil && !c.isDisabled() && c.contentCaptureEnabled(ctx)
	var exportedInput, exportedOutput contentSnapshot
	if capture {
		exportedInput = c.snapshotContent(MaskObservationInput, item.Input, "observation input")
	}
	itemCtx, root, masked, err := c.startExperimentItem(ctx, experiment, item, "experiment-item-run",
		ObservationAttributes{Input: exportedInput.premasked}, start)
	if err != nil {
		result.DatasetRunID = ""
		result.Err = err
		return result
	}
	result.TraceID, result.ObservationID = root.TraceID(), root.ID()

	output, err := callExperimentTask(itemCtx, run.Task, item)
	if err != nil {
		root.RecordError(exportedCallbackError(err, errTaskPanicked))
		root.End()
		result.Err = err
		return result
	}
	if capture {
		exportedOutput = c.snapshotContent(MaskObservationOutput, output, "observation output")
	}
	root.Update(ObservationAttributes{Output: exportedOutput.premasked})
	root.End()
	result.Output = output

	input := EvaluatorInput{
		Input: item.Input, Output: output, ExpectedOutput: item.ExpectedOutput, Metadata: item.Metadata,
	}
	var telemetry premasked
	if capture {
		telemetry = frozenJSON(map[string]json.RawMessage{
			"input": exportedInput.json, "output": exportedOutput.json,
			"expected_output": masked.expectedOutput, "metadata": masked.itemMetadataJSON,
		}, "evaluator input")
	}
	var errs []error
	evaluate := func(evaluator Evaluator, input EvaluatorInput, metadata map[string]any) {
		evaluations, frozen, err := c.runEvaluator(itemCtx, evaluator, input, telemetry, metadata)
		if err != nil {
			errs = append(errs, err)
			return
		}
		result.Evaluations = append(result.Evaluations, evaluations...)
		for index, evaluation := range evaluations {
			score := evaluation.score()
			score.TraceID, score.ObservationID, score.Metadata = result.TraceID, result.ObservationID, frozen[index].metadata
			err := frozen[index].err
			if err == nil {
				err = c.recordScore(itemCtx, score, true)
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("langfuse: evaluation %q score: %w", evaluation.Name, err))
			}
		}
	}
	for index, evaluator := range run.Evaluators {
		evaluate(evaluator, input, map[string]any{"evaluator_kind": "item", "evaluator_index": index})
	}
	if run.CompositeEvaluator != nil && len(result.Evaluations) != 0 {
		input.Evaluations = append([]Evaluation(nil), result.Evaluations...)
		evaluate(run.CompositeEvaluator, input, map[string]any{"evaluator_kind": "composite"})
	}
	result.EvaluationErr = errors.Join(errs...)
	return result
}

// experimentRootMetadata is the item root's observation metadata, as the
// official SDKs set it, from the masked experiment and item metadata.
func experimentRootMetadata(
	run *ExperimentRun, item ExperimentItem, experimentMetadata, itemMetadata map[string]any,
) map[string]any {
	metadata := make(map[string]any, len(itemMetadata)+len(experimentMetadata)+4)
	maps.Copy(metadata, itemMetadata)
	maps.Copy(metadata, experimentMetadata)
	metadata["experiment_name"] = run.Name
	metadata["experiment_run_name"] = run.RunName
	if item.DatasetID != "" {
		metadata["dataset_id"] = item.DatasetID
		metadata["dataset_item_id"] = item.ID
	}
	return metadata
}

// linkDatasetRunItem links a started item root to the dataset run named
// RunName and returns the run's ID. metadata is the masked experiment
// metadata JSON, or empty.
func (c *Client) linkDatasetRunItem(
	ctx context.Context, run *ExperimentRun, item ExperimentItem, root *Observation, metadata string,
) (string, error) {
	body := map[string]any{
		"runName":       run.RunName,
		"datasetItemId": item.ID,
		"traceId":       root.TraceID(),
		"observationId": root.ID(),
	}
	if run.Description != "" {
		body["runDescription"] = run.Description
	}
	if metadata != "" {
		body["metadata"] = json.RawMessage(metadata)
	}
	if !item.Version.IsZero() {
		body["datasetVersion"] = formatDatasetInstant(item.Version)
	}
	payload, err := marshalDatasetBody(body, maxDatasetBodyBytes, "dataset run item")
	if err != nil {
		return "", err
	}
	if err := c.datasetUnavailable(); err != nil {
		return "", err
	}
	want := transport.DatasetRunItem{
		DatasetRunName: run.RunName, DatasetItemID: item.ID, TraceID: root.TraceID(), ObservationID: root.ID(),
	}
	link, err := runDatasetOperation(ctx, c, ErrDatasetItemNotFound,
		func(ctx context.Context) (string, error) {
			link, err := c.datasetTransport.CreateRunItem(ctx, payload, want)
			return link.DatasetRunID, err
		})
	return link, err
}

// runEvaluator runs evaluator in its own observation, whose input is the
// item's masked telemetry. It returns each evaluation's metadata masked once
// as MaskScoreMetadata, shared by the observation output and the score.
func (c *Client) runEvaluator(
	ctx context.Context, evaluator Evaluator, input EvaluatorInput, telemetry premasked,
	metadata map[string]any,
) ([]Evaluation, []frozenEvaluation, error) {
	evalCtx, observation := c.StartObservation(ctx, evaluatorName(evaluator), TypeEvaluator,
		ObservationAttributes{Input: telemetry, Metadata: metadata})
	defer observation.End()
	evaluations, err := callEvaluator(evalCtx, evaluator, input)
	if err != nil {
		observation.RecordError(exportedCallbackError(err, errEvaluatorPanicked))
		return nil, nil, err
	}
	frozen := make([]frozenEvaluation, len(evaluations))
	if c == nil || c.isDisabled() {
		// Nothing is exported, so no metadata is masked or serialized.
		return evaluations, frozen, nil
	}
	for index, evaluation := range evaluations {
		// Freeze each result before the next Mask call can reuse it.
		frozen[index] = freezeMetadata(lfattr.ScoreMetadata(evaluation.Metadata, c.mask))
	}
	if c.contentCaptureEnabled(ctx) {
		observation.Update(ObservationAttributes{Output: frozenJSON(evaluationOutput(evaluations, frozen), "evaluator output")})
	}
	return evaluations, frozen, nil
}

// frozenEvaluation is an evaluation's metadata as its score sends it, or why
// the score cannot be sent.
type frozenEvaluation struct {
	metadata map[string]any
	err      error
}

// errScoreSerialization is RecordScore's error for a score it cannot
// serialize.
var errScoreSerialization = errors.New("langfuse: score could not be serialized")

// freezeMetadata serializes masked metadata once into fresh plain values, so
// neither a reused map nor a stateful marshaler changes a later copy. Metadata
// that cannot be serialized or exceeds the score limit fails the score, as
// RecordScore would; a masker that omits metadata does not.
func freezeMetadata(metadata map[string]any) frozenEvaluation {
	if len(metadata) == 0 {
		return frozenEvaluation{}
	}
	data, err, panicked := lfattr.MarshalJSON(metadata, maxScoreMetadataBytes)
	if panicked || err != nil || len(data) > maxScoreMetadataBytes {
		return frozenEvaluation{err: errScoreSerialization}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var frozen map[string]any
	if err := decoder.Decode(&frozen); err != nil {
		return frozenEvaluation{err: errScoreSerialization}
	}
	return frozenEvaluation{metadata: frozen}
}

// evaluationOutput is the evaluator observation's output, in the official
// SDKs' snake_case form, with each evaluation's frozen metadata.
func evaluationOutput(evaluations []Evaluation, frozen []frozenEvaluation) []map[string]any {
	output := make([]map[string]any, len(evaluations))
	for index, evaluation := range evaluations {
		entry := map[string]any{"name": evaluation.Name}
		if evaluation.NumericValue != nil {
			entry["value"] = *evaluation.NumericValue
		} else if evaluation.StringValue != nil {
			entry["value"] = *evaluation.StringValue
		}
		if evaluation.Comment != "" {
			entry["comment"] = evaluation.Comment
		}
		if frozen[index].metadata != nil {
			entry["metadata"] = frozen[index].metadata
		}
		if evaluation.DataType != "" {
			entry["data_type"] = string(evaluation.DataType)
		}
		if evaluation.ConfigID != "" {
			entry["config_id"] = evaluation.ConfigID
		}
		output[index] = entry
	}
	return output
}

func (e Evaluation) score() Score {
	return Score{
		Name:         e.Name,
		NumericValue: e.NumericValue,
		StringValue:  e.StringValue,
		DataType:     e.DataType,
		ConfigID:     e.ConfigID,
		Comment:      e.Comment,
		Metadata:     e.Metadata,
	}
}

// panicError is a recovered callback panic; observations record its fixed
// sentinel instead of the panic value.
type panicError struct {
	sentinel error
	value    any
}

func (e *panicError) Error() string { return fmt.Sprintf("%v: %v", e.sentinel, e.value) }

func (e *panicError) Unwrap() error { return e.sentinel }

func exportedCallbackError(err, sentinel error) error {
	var panicked *panicError
	if errors.As(err, &panicked) {
		return sentinel
	}
	return err
}

func callExperimentTask(ctx context.Context, task ExperimentTask, item ExperimentItem) (output any, err error) {
	defer recoverCallback(&err, errTaskPanicked)
	return task(ctx, item)
}

func callEvaluator(ctx context.Context, evaluator Evaluator, input EvaluatorInput) (evaluations []Evaluation, err error) {
	defer recoverCallback(&err, errEvaluatorPanicked)
	return evaluator(ctx, input)
}

func callRunEvaluator(
	ctx context.Context, evaluator RunEvaluator, results []ExperimentItemResult,
) (evaluations []Evaluation, err error) {
	defer recoverCallback(&err, errEvaluatorPanicked)
	return evaluator(ctx, results)
}

func recoverCallback(err *error, sentinel error) {
	if value := recover(); value != nil {
		*err = &panicError{sentinel: sentinel, value: value}
	}
}

// evaluatorName names an evaluator observation after the function, as the
// Python SDK does: "exactMatch" for main.exactMatch, "run.func1" for a
// closure.
func evaluatorName(evaluator Evaluator) string {
	function := runtime.FuncForPC(reflect.ValueOf(evaluator).Pointer())
	if function == nil {
		return "evaluator"
	}
	name := function.Name()
	name = name[strings.LastIndex(name, "/")+1:]
	if index := strings.Index(name, "."); index >= 0 {
		name = name[index+1:]
	}
	name = strings.TrimSuffix(name, "-fm")
	if name == "" || !utf8.ValidString(name) {
		return "evaluator"
	}
	return name
}

// Summary renders the result for a terminal or log: the run, its task and
// evaluation failure counts, each evaluation's mean over numeric values, the
// run evaluations and their errors, and, when includeItems is set, every item
// with its input, output, evaluations, and errors.
func (r ExperimentResult) Summary(includeItems bool) string {
	var b strings.Builder
	if includeItems {
		for index, item := range r.ItemResults {
			fmt.Fprintf(&b, "%d. Item %s\n", index+1, item.Item.ID)
			fmt.Fprintf(&b, "   Input:    %s\n", summaryValue(item.Item.Input))
			if !isNilValue(item.Item.ExpectedOutput) {
				fmt.Fprintf(&b, "   Expected: %s\n", summaryValue(item.Item.ExpectedOutput))
			}
			if item.Err != nil {
				fmt.Fprintf(&b, "   Error:    %v\n", item.Err)
			} else {
				fmt.Fprintf(&b, "   Actual:   %s\n", summaryValue(item.Output))
			}
			if item.EvaluationErr != nil {
				fmt.Fprintf(&b, "   Evaluation errors: %v\n", item.EvaluationErr)
			}
			if len(item.Evaluations) != 0 {
				b.WriteString("   Scores:\n")
				for _, evaluation := range item.Evaluations {
					writeSummaryEvaluation(&b, "     ", evaluation)
				}
			}
			if item.TraceID != "" {
				fmt.Fprintf(&b, "   Trace ID: %s\n", item.TraceID)
			}
		}
	}
	failed, evaluationFailed := 0, 0
	for _, item := range r.ItemResults {
		if item.Err != nil {
			failed++
		}
		if item.EvaluationErr != nil {
			evaluationFailed++
		}
	}
	fmt.Fprintf(&b, "Experiment: %s\nRun: %s\n", r.Name, r.RunName)
	if r.Description != "" {
		fmt.Fprintf(&b, "Description: %s\n", r.Description)
	}
	fmt.Fprintf(&b, "Experiment ID: %s\n", r.ExperimentID)
	fmt.Fprintf(&b, "Items: %d (%d failed, %d with evaluation errors)\n", len(r.ItemResults), failed, evaluationFailed)
	var names []string
	sums := map[string]float64{}
	counts := map[string]int{}
	for _, item := range r.ItemResults {
		for _, evaluation := range item.Evaluations {
			if _, seen := counts[evaluation.Name]; !seen {
				names = append(names, evaluation.Name)
				counts[evaluation.Name] = 0
			}
			if evaluation.NumericValue != nil {
				sums[evaluation.Name] += *evaluation.NumericValue
				counts[evaluation.Name]++
			}
		}
	}
	if len(names) != 0 {
		// Local values: a rejected score still counts here.
		b.WriteString("Average evaluation values:\n")
		for _, name := range names {
			if counts[name] == 0 {
				fmt.Fprintf(&b, "  %s: no numeric values\n", name)
				continue
			}
			fmt.Fprintf(&b, "  %s: %.3f\n", name, sums[name]/float64(counts[name]))
		}
	}
	if len(r.RunEvaluations) != 0 {
		b.WriteString("Run evaluations:\n")
		for _, evaluation := range r.RunEvaluations {
			writeSummaryEvaluation(&b, "  ", evaluation)
		}
	}
	if r.RunEvaluationErr != nil {
		fmt.Fprintf(&b, "Run evaluation errors: %v\n", r.RunEvaluationErr)
	}
	return b.String()
}

func writeSummaryEvaluation(b *strings.Builder, indent string, evaluation Evaluation) {
	value := ""
	switch {
	case evaluation.NumericValue != nil:
		value = strconv.FormatFloat(*evaluation.NumericValue, 'f', 3, 64)
	case evaluation.StringValue != nil:
		value = *evaluation.StringValue
	}
	fmt.Fprintf(b, "%s%s: %s\n", indent, evaluation.Name, value)
	if evaluation.Comment != "" {
		fmt.Fprintf(b, "%s  %s\n", indent, evaluation.Comment)
	}
}

// summaryValue shortens a value to 50 characters, as the official SDKs do.
func summaryValue(value any) string {
	var text string
	switch value := value.(type) {
	case string:
		text = value
	case json.RawMessage:
		text = string(value)
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			text = fmt.Sprint(value)
		} else {
			text = string(encoded)
		}
	}
	if runes := []rune(text); len(runes) > 50 {
		return string(runes[:47]) + "..."
	}
	return text
}
