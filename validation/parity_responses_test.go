//go:build validation && parity

package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	openaigo "github.com/openai/openai-go"
	"github.com/openai/openai-go/responses"
	"github.com/openai/openai-go/shared"
)

const responsesGoldenPath = "testdata/parity/azure_responses.golden.json"

// responsesParityInput bridges the two SDKs' deliberate input-shape
// divergence onto one projection: the Python wrapper rewrites
// instructions plus a list input into [{role: system}, *input], while
// the Go adapter exports {"instructions": ..., "input": [...]}. Both
// land as one item list with a leading system message, then pass the
// closed item subset.
func responsesParityInput(value any) (any, error) {
	switch typed := value.(type) {
	case []any:
		// Python's rewritten form.
		return projectResponsesItems(typed), nil
	case map[string]any:
		// Go's canonical export.
		items := []any{}
		if instructions, ok := typed["instructions"].(string); ok && instructions != "" {
			items = append(items, map[string]any{"role": "system", "content": instructions})
		}
		list, ok := typed["input"].([]any)
		if !ok {
			return nil, fmt.Errorf("go responses input has no item list (keys %v)", keysOfAny(typed))
		}
		items = append(items, list...)
		return projectResponsesItems(items), nil
	default:
		return nil, fmt.Errorf("unsupported responses input form %T", value)
	}
}

// responsesParityOutput re-wraps Python's collapsed singleton, treats
// Python's null-for-empty and an absent field as the same no-output
// fact, and projects every item through the closed subset so raw
// Python fields (ids, statuses, logprobs, annotations) and sanitized
// Go items compare on the same surface.
func responsesParityOutput(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil // Python serializes output: [] as null
	case map[string]any:
		return projectResponsesItems([]any{typed}), nil
	case []any:
		if len(typed) == 0 {
			return nil, nil
		}
		return projectResponsesItems(typed), nil
	default:
		return nil, fmt.Errorf("unsupported responses output form %T", value)
	}
}

// responsesItemSubset is the closed compared surface for one item or
// content part; anything outside it is language-local detail dropped
// by the projection (the parity doc records tools and these raw
// fields as scoped exclusions).
var responsesItemSubset = map[string]bool{
	"type": true, "role": true, "content": true, "text": true,
	"refusal": true, "name": true, "arguments": true, "call_id": true,
	"summary": true, "thought": true, "omitted": true, "output": true,
	"instructions": true, "input": true, "id": false,
}

func projectResponsesItems(items []any) []any {
	projected := make([]any, 0, len(items))
	for _, item := range items {
		projected = append(projected, projectResponsesValue(item))
	}
	return projected
}

func projectResponsesValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			if !responsesItemSubset[key] {
				continue
			}
			result[key] = projectResponsesValue(item)
		}
		return result
	case []any:
		result := make([]any, 0, len(typed))
		for _, item := range typed {
			result = append(result, projectResponsesValue(item))
		}
		return result
	default:
		return value
	}
}

func keysOfAny(value map[string]any) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// canonicalizeResponses mirrors canonicalize with the responses
// operation and the projection bridge applied before shapes are taken.
func canonicalizeResponses(obs observation, stream bool) (canonical, error) {
	if !enumKinds[strings.ToUpper(obs.Type)] {
		return canonical{}, fmt.Errorf("unknown observation type %q", obs.Type)
	}
	level := obs.Level
	if level == "" {
		level = "DEFAULT"
	}
	if !enumLevels[level] {
		return canonical{}, fmt.Errorf("unknown level %q", level)
	}
	if obs.Name != "openai.responses" && obs.Name != "OpenAI-responses-parity" {
		return canonical{}, fmt.Errorf("unknown responses operation name %q", obs.Name)
	}
	var buckets []string
	for bucket, value := range obs.UsageDetails {
		if value == 0 {
			continue
		}
		mapped, ok := usageAliases[bucket]
		if !ok {
			return canonical{}, fmt.Errorf("unknown non-zero usage bucket %q (value %d)", bucket, value)
		}
		buckets = append(buckets, mapped)
	}
	sort.Strings(buckets)
	mode := "unary"
	if stream {
		mode = "stream"
	}
	projectedShape := func(raw json.RawMessage, project func(any) (any, error)) (shape, error) {
		if len(raw) == 0 {
			return shape{Kind: "absent"}, nil
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return shape{}, err
		}
		projected, err := project(value)
		if err != nil {
			return shape{}, err
		}
		if projected == nil {
			return shape{Kind: "absent"}, nil
		}
		return shapeOf(projected), nil
	}
	inputShape, err := projectedShape(obs.Input, responsesParityInput)
	if err != nil {
		return canonical{}, fmt.Errorf("input projection: %w", err)
	}
	outputShape, err := projectedShape(obs.Output, responsesParityOutput)
	if err != nil {
		return canonical{}, fmt.Errorf("output projection: %w", err)
	}
	return canonical{
		SchemaVersion: canonicalSchemaVersion,
		Kind:          "generation",
		Level:         level,
		Operation:     "responses",
		Mode:          mode,
		UsageBuckets:  buckets,
		InputShape:    inputShape,
		OutputShape:   outputShape,
	}, nil
}

func goResponsesParityObservation(t *testing.T) observation {
	t.Helper()
	r := newRun(t)
	env := requireEnv(t, "AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY", "AZURE_OPENAI_RESPONSES_DEPLOYMENT")
	client := azureResponsesClient(r, env["AZURE_OPENAI_ENDPOINT"], env["AZURE_OPENAI_API_KEY"])
	traceID, err := r.call(t, "parity-azure-responses", func(ctx context.Context) error {
		response, callErr := client.Responses.New(ctx, responses.ResponseNewParams{
			Model:           shared.ResponsesModel(env["AZURE_OPENAI_RESPONSES_DEPLOYMENT"]),
			Temperature:     openaigo.Float(0),
			MaxOutputTokens: openaigo.Int(64),
			Instructions:    openaigo.String("Reply with one short word."),
			Input: responses.ResponseNewParamsInputUnion{
				OfInputItemList: responses.ResponseInputParam{
					responses.ResponseInputItemParamOfMessage(
						responses.ResponseInputMessageContentListParam{
							responses.ResponseInputContentParamOfInputText("Say ok. Marker: " + r.marker),
						},
						responses.EasyInputMessageRoleUser,
					),
				},
			},
		})
		if callErr == nil && response.OutputText() == "" {
			return fmt.Errorf("inconclusive: no comparable output")
		}
		return callErr
	})
	if err != nil {
		t.Fatalf("parity Go responses call: %v", err)
	}
	return r.observation(t, traceID, "openai.responses", ingested)
}

func TestParityAzureResponses(t *testing.T) {
	golden, err := os.ReadFile(responsesGoldenPath)
	if os.IsNotExist(err) {
		t.Fatalf("no golden at %s; run `task parity:regen` with Azure and Langfuse credentials before shipping parity", responsesGoldenPath)
	}
	if err != nil {
		t.Fatal(err)
	}
	want, err := decodeResponsesGolden(golden)
	if err != nil {
		t.Fatalf("golden rejected: %v", err)
	}

	got := goResponsesParityObservation(t)
	projected, err := canonicalizeResponses(got, false)
	if err != nil {
		t.Fatalf("canonicalize Go observation: %v", err)
	}
	diffCanonical(t, projected, want)
}

// decodeResponsesGolden applies the shared sealed-schema decoder
// (whose operation enum includes responses) and additionally requires
// the responses operation, so a chat golden cannot stand in.
func decodeResponsesGolden(raw []byte) (canonical, error) {
	golden, err := decodeGolden(raw)
	if err != nil {
		return canonical{}, err
	}
	if golden.Operation != "responses" {
		return canonical{}, fmt.Errorf("operation %q, want responses", golden.Operation)
	}
	return golden, nil
}

// TestParityRegenResponses mirrors TestParityRegen for the Responses
// route: PARITY_PYTHON_TRACE_RESPONSES names the trace written by
// parity/log_responses.py.
func TestParityRegenResponses(t *testing.T) {
	pythonTrace := os.Getenv("PARITY_PYTHON_TRACE_RESPONSES")
	if os.Getenv("PARITY_REGEN") == "" {
		t.Skip("regeneration only; run via `task parity:regen`")
	}
	if !traceIDShape.MatchString(pythonTrace) {
		t.Fatalf("PARITY_PYTHON_TRACE_RESPONSES %q is not a lowercase 32-hex trace ID", pythonTrace)
	}
	r := newRun(t)
	pythonObs := r.observation(t, pythonTrace, "OpenAI-responses-parity", ingested)
	pythonCanonical, err := canonicalizeResponses(pythonObs, false)
	if err != nil {
		t.Fatalf("canonicalize Python observation: %v", err)
	}

	goObs := goResponsesParityObservation(t)
	goCanonical, err := canonicalizeResponses(goObs, false)
	if err != nil {
		t.Fatalf("canonicalize Go observation: %v", err)
	}
	diffCanonical(t, goCanonical, pythonCanonical)
	if t.Failed() {
		t.Fatal("Python and Go projections disagree; candidate rejected")
	}

	pythonCanonical.Provenance = provenance{
		Note:           "normalized Python langfuse.openai Responses trace; regenerate via task parity:regen",
		PythonLangfuse: "4.14.1",
		PythonOpenAI:   "2.47.0",
	}
	candidate, err := json.MarshalIndent(pythonCanonical, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeResponsesGolden(candidate); err != nil {
		t.Fatalf("candidate rejected by the sealed schema: %v", err)
	}
	candidatePath := responsesGoldenPath + ".candidate"
	if err := os.MkdirAll(filepath.Dir(candidatePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(candidatePath, candidate, 0o644); err != nil {
		t.Fatal(err)
	}
	if previous, err := os.ReadFile(responsesGoldenPath); err == nil {
		if old, err := decodeResponsesGolden(previous); err == nil {
			oldJSON, _ := json.MarshalIndent(old, "", "  ")
			if !reflect.DeepEqual(old, pythonCanonical) {
				t.Logf("old golden differs from candidate; previous:\n%s", oldJSON)
			}
		}
	}
	t.Logf("candidate golden:\n%s", candidate)

	if os.Getenv("PARITY_REGEN") != "accept" {
		t.Logf("candidate at %s; rerun with ACCEPT=accept to replace %s", candidatePath, responsesGoldenPath)
		return
	}
	replacement := responsesGoldenPath + ".tmp"
	if err := os.WriteFile(replacement, candidate, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, responsesGoldenPath); err != nil {
		t.Fatal(err)
	}
	t.Logf("golden replaced: %s", responsesGoldenPath)
}

// TestResponsesCanonicalizerFixtures locks the projection semantics
// credential-free: singleton re-wrap, the two zero-output forms, raw
// Python field dropping, reasoning and placeholder shapes, the
// instructions bridge, and asymmetric unknown fields.
func TestResponsesCanonicalizerFixtures(t *testing.T) {
	obs := func(name, input, output string) observation {
		result := observation{Name: name, Type: "GENERATION"}
		if input != "" {
			result.Input = json.RawMessage(input)
		}
		if output != "" {
			result.Output = json.RawMessage(output)
		}
		return result
	}
	pythonInput := `[{"role":"system","content":"be terse"},{"role":"user","content":[{"type":"input_text","text":"q"}]}]`
	goInput := `{"instructions":"be terse","input":[{"role":"user","content":[{"type":"input_text","text":"q"}]}]}`

	t.Run("instructions-bridge", func(t *testing.T) {
		python, err := canonicalizeResponses(obs("OpenAI-responses-parity", pythonInput, `null`), false)
		if err != nil {
			t.Fatal(err)
		}
		goSide, err := canonicalizeResponses(obs("openai.responses", goInput, `null`), false)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(python.InputShape, goSide.InputShape) {
			t.Errorf("bridged input shapes differ:\npython %+v\ngo     %+v", python.InputShape, goSide.InputShape)
		}
	})
	t.Run("zero-output-forms-agree", func(t *testing.T) {
		asNull, err := canonicalizeResponses(obs("openai.responses", goInput, `null`), false)
		if err != nil {
			t.Fatal(err)
		}
		asAbsent, err := canonicalizeResponses(obs("openai.responses", goInput, ""), false)
		if err != nil {
			t.Fatal(err)
		}
		asEmpty, err := canonicalizeResponses(obs("openai.responses", goInput, `[]`), false)
		if err != nil {
			t.Fatal(err)
		}
		if asNull.OutputShape.Kind != "absent" || asAbsent.OutputShape.Kind != "absent" || asEmpty.OutputShape.Kind != "absent" {
			t.Errorf("zero-output forms diverge: null=%v absent=%v empty=%v",
				asNull.OutputShape, asAbsent.OutputShape, asEmpty.OutputShape)
		}
	})
	t.Run("singleton-rewrap-and-raw-field-drop", func(t *testing.T) {
		pythonRaw := `{"id":"msg-1","type":"message","status":"completed","role":"assistant",` +
			`"content":[{"type":"output_text","text":"ok","annotations":[],"logprobs":[{"token":"SECRET"}]}]}`
		goSanitized := `{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}`
		python, err := canonicalizeResponses(obs("OpenAI-responses-parity", pythonInput, pythonRaw), false)
		if err != nil {
			t.Fatal(err)
		}
		goSide, err := canonicalizeResponses(obs("openai.responses", goInput, goSanitized), false)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(python.OutputShape, goSide.OutputShape) {
			t.Errorf("projected output shapes differ:\npython %+v\ngo     %+v", python.OutputShape, goSide.OutputShape)
		}
	})
	t.Run("reasoning-and-placeholder-shapes", func(t *testing.T) {
		output := `[{"type":"reasoning","thought":true,"summary":["s"]},` +
			`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]},` +
			`{"type":"image_generation_call","omitted":true}]`
		projected, err := canonicalizeResponses(obs("openai.responses", goInput, output), false)
		if err != nil {
			t.Fatal(err)
		}
		if len(projected.OutputShape.Elems) != 3 {
			t.Fatalf("want 3 projected items, got %+v", projected.OutputShape)
		}
	})
	t.Run("meaningful-asymmetry-still-fails", func(t *testing.T) {
		one, err := canonicalizeResponses(obs("openai.responses", goInput,
			`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}`), false)
		if err != nil {
			t.Fatal(err)
		}
		two, err := canonicalizeResponses(obs("openai.responses", goInput,
			`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"},{"type":"refusal","refusal":"no"}]}`), false)
		if err != nil {
			t.Fatal(err)
		}
		if reflect.DeepEqual(one.OutputShape, two.OutputShape) {
			t.Error("an extra content part must diverge")
		}
	})
	t.Run("unknown-operation-rejected", func(t *testing.T) {
		if _, err := canonicalizeResponses(obs("OpenAI-generation", goInput, `null`), false); err == nil {
			t.Error("the chat alias must not canonicalize as responses")
		}
	})
}
