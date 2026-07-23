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

	gopenai "github.com/sashabaranov/go-openai"
)

// canonicalSchemaVersion changes whenever the projection semantics
// change; goldens record it and mismatches force regeneration.
const canonicalSchemaVersion = 1

const goldenPath = "testdata/parity/azure.golden.json"

// canonical is the common semantic projection of one provider
// observation. Topology is deliberately absent (Go wraps calls in a
// core span, the Python wrapper does not), and no field can hold an
// arbitrary string: every scalar is either from a closed enum or a
// typed shape descriptor, so goldens cannot leak deployments, models,
// projects, or regions.
type canonical struct {
	SchemaVersion int        `json:"schemaVersion"`
	Kind          string     `json:"kind"`      // enum: generation
	Level         string     `json:"level"`     // enum: DEFAULT, DEBUG, WARNING, ERROR
	Operation     string     `json:"operation"` // enum: chat.completion
	Mode          string     `json:"mode"`      // enum: unary, stream
	UsageBuckets  []string   `json:"usageBuckets"`
	InputShape    shape      `json:"inputShape"`
	OutputShape   shape      `json:"outputShape"`
	Provenance    provenance `json:"provenance,omitempty"`
}

type provenance struct {
	Note            string `json:"note,omitempty"`
	LangfuseServer  string `json:"langfuseServer,omitempty"`
	PythonLangfuse  string `json:"pythonLangfuse,omitempty"`
	PythonOpenAI    string `json:"pythonOpenAI,omitempty"`
	AzureAPIVersion string `json:"azureApiVersion,omitempty"`
}

// shape is a recursive structural descriptor: object keys and scalar
// kinds are semantic; values are not retained.
type shape struct {
	Kind string           `json:"kind"` // object, array, string, number, bool, null, absent
	Keys map[string]shape `json:"keys,omitempty"`
	Elem *shape           `json:"elem,omitempty"`
	Len  int              `json:"len,omitempty"` // arrays: length is semantic
}

func shapeOf(value any) shape {
	switch value := value.(type) {
	case nil:
		return shape{Kind: "null"}
	case bool:
		return shape{Kind: "bool"}
	case float64:
		return shape{Kind: "number"}
	case string:
		return shape{Kind: "string"}
	case []any:
		s := shape{Kind: "array", Len: len(value)}
		if len(value) > 0 {
			elem := shapeOf(value[0])
			s.Elem = &elem
		}
		return s
	case map[string]any:
		s := shape{Kind: "object", Keys: map[string]shape{}}
		for key, item := range value {
			s.Keys[key] = shapeOf(item)
		}
		return s
	default:
		return shape{Kind: fmt.Sprintf("unsupported:%T", value)}
	}
}

// usageAliases maps language-specific usage bucket names onto the
// canonical set before comparison.
var usageAliases = map[string]string{
	"input": "input", "output": "output", "total": "total",
	"input_cached_tokens": "input_cached", "input_cache_read": "input_cached",
	"output_reasoning_tokens": "output_reasoning",
}

// operationAliases maps SDK-specific observation names to the
// canonical operation.
var operationAliases = map[string]string{
	"openai.chat.completions": "chat.completion", // Go adapter route
	"OpenAI-generation":       "chat.completion", // Python wrapper default
}

var (
	enumKinds  = map[string]bool{"GENERATION": true}
	enumLevels = map[string]bool{"DEFAULT": true, "DEBUG": true, "WARNING": true, "ERROR": true}
)

// canonicalize projects a readback observation. Unknown inputs are
// rejected, never dropped.
func canonicalize(obs observation, stream bool) (canonical, error) {
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
	operation, ok := operationAliases[obs.Name]
	if !ok {
		return canonical{}, fmt.Errorf("unknown operation name %q", obs.Name)
	}
	var buckets []string
	for bucket := range obs.UsageDetails {
		mapped, ok := usageAliases[bucket]
		if !ok {
			return canonical{}, fmt.Errorf("unknown usage bucket %q", bucket)
		}
		buckets = append(buckets, mapped)
	}
	sort.Strings(buckets)
	mode := "unary"
	if stream {
		mode = "stream"
	}
	var input, output any
	if len(obs.Input) > 0 {
		if err := json.Unmarshal(obs.Input, &input); err != nil {
			return canonical{}, fmt.Errorf("input decode: %w", err)
		}
	}
	if len(obs.Output) > 0 {
		if err := json.Unmarshal(obs.Output, &output); err != nil {
			return canonical{}, fmt.Errorf("output decode: %w", err)
		}
	}
	return canonical{
		SchemaVersion: canonicalSchemaVersion,
		Kind:          "generation",
		Level:         level,
		Operation:     operation,
		Mode:          mode,
		UsageBuckets:  buckets,
		InputShape:    shapeOf(input),
		OutputShape:   shapeOf(output),
	}, nil
}

// diffCanonical reports every divergence in both directions.
func diffCanonical(t *testing.T, got, want canonical) {
	t.Helper()
	got.Provenance, want.Provenance = provenance{}, provenance{}
	if reflect.DeepEqual(got, want) {
		return
	}
	gotJSON, _ := json.MarshalIndent(got, "", "  ")
	wantJSON, _ := json.MarshalIndent(want, "", "  ")
	t.Errorf("canonical projection diverges\n--- got (Go, this run)\n%s\n--- want (pinned snapshot)\n%s", gotJSON, wantJSON)
}

// TestParityAzure asserts Go-versus-pinned-snapshot conformance: one
// Go inference call, projected and compared against the committed
// normalized Python snapshot. Python-side drift is detected only by
// regeneration, which actually executes the pinned oracle.
func TestParityAzure(t *testing.T) {
	golden, err := os.ReadFile(goldenPath)
	if os.IsNotExist(err) {
		t.Skipf("no golden at %s yet; run `task parity:regen` with Azure and Langfuse credentials", goldenPath)
	}
	if err != nil {
		t.Fatal(err)
	}
	var want canonical
	if err := json.Unmarshal(golden, &want); err != nil {
		t.Fatalf("golden decode: %v", err)
	}
	if want.SchemaVersion != canonicalSchemaVersion {
		t.Fatalf("golden schema v%d, canonicalizer v%d; regenerate the golden", want.SchemaVersion, canonicalSchemaVersion)
	}

	got := goParityObservation(t)
	projected, err := canonicalize(got, false)
	if err != nil {
		t.Fatalf("canonicalize Go observation: %v", err)
	}
	diffCanonical(t, projected, want)
}

// goParityObservation makes the parity call on the Go side and returns
// the readback observation.
func goParityObservation(t *testing.T) observation {
	t.Helper()
	r := newRun(t)
	env := azureEnv(t)
	client := azureClient(r, env, env["AZURE_OPENAI_DEPLOYMENT"])
	traceID, err := r.call(t, "parity-azure", func(ctx context.Context) error {
		response, callErr := client.CreateChatCompletion(ctx, gopenai.ChatCompletionRequest{
			Model:       "azure-mapped",
			Temperature: 0,
			MaxTokens:   24,
			Messages: []gopenai.ChatCompletionMessage{
				{
					Role:    gopenai.ChatMessageRoleUser,
					Content: "Reply with one short word. Marker: " + r.marker,
				},
			},
		})
		if callErr == nil && (len(response.Choices) == 0 || response.Choices[0].Message.Content == "") {
			return fmt.Errorf("inconclusive: no comparable output")
		}
		return callErr
	})
	if err != nil {
		t.Fatalf("parity Go call: %v", err)
	}
	return r.observation(t, traceID, "openai.chat.completions")
}

// TestParityRegen is the regeneration flow: PARITY_PYTHON_TRACE names
// the trace written by parity/log_call.py; the Python and Go
// projections must agree before the candidate is accepted, and the
// golden is replaced only with PARITY_REGEN=accept.
func TestParityRegen(t *testing.T) {
	pythonTrace := os.Getenv("PARITY_PYTHON_TRACE")
	if pythonTrace == "" {
		t.Skip("regeneration only; run via `task parity:regen`")
	}
	r := newRun(t)
	pythonObs := r.observation(t, pythonTrace, "OpenAI-generation")
	pythonCanonical, err := canonicalize(pythonObs, false)
	if err != nil {
		t.Fatalf("canonicalize Python observation: %v", err)
	}

	goObs := goParityObservation(t)
	goCanonical, err := canonicalize(goObs, false)
	if err != nil {
		t.Fatalf("canonicalize Go observation: %v", err)
	}
	diffCanonical(t, goCanonical, pythonCanonical)
	if t.Failed() {
		t.Fatal("Python and Go projections disagree; candidate rejected")
	}

	pythonCanonical.Provenance = provenance{
		Note:            "normalized Python langfuse.openai trace; regenerate via task parity:regen",
		PythonLangfuse:  "4.14.1",
		PythonOpenAI:    "2.47.0",
		AzureAPIVersion: os.Getenv("AZURE_OPENAI_API_VERSION"),
	}
	candidate, err := json.MarshalIndent(pythonCanonical, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(t.TempDir(), "azure.golden.json")
	if err := os.WriteFile(tmp, candidate, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("candidate golden:\n%s", candidate)

	if os.Getenv("PARITY_REGEN") != "accept" {
		t.Logf("candidate written to %s; rerun with PARITY_REGEN=accept to replace %s", tmp, goldenPath)
		return
	}
	if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(goldenPath, candidate, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("golden replaced: %s", goldenPath)
}

// TestCanonicalizerFixtures locks the projection semantics without any
// credentials or provider: pure-function tests of the canonicalizer
// itself (the no-mock rule governs the live smoke and parity tests,
// not unit tests of projection code).
func TestCanonicalizerFixtures(t *testing.T) {
	obs := observation{
		Name:         "openai.chat.completions",
		Type:         "GENERATION",
		Input:        json.RawMessage(`[{"role":"user","content":"hi"}]`),
		Output:       json.RawMessage(`{"role":"assistant","content":"hello"}`),
		UsageDetails: map[string]int64{"input": 3, "output": 2, "total": 5},
	}
	got, err := canonicalize(obs, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Operation != "chat.completion" || got.Mode != "unary" {
		t.Fatalf("projection %+v", got)
	}
	if got.InputShape.Kind != "array" || got.InputShape.Len != 1 ||
		got.InputShape.Elem.Keys["content"].Kind != "string" {
		t.Fatalf("input shape %+v", got.InputShape)
	}
	if !reflect.DeepEqual(got.UsageBuckets, []string{"input", "output", "total"}) {
		t.Fatalf("usage buckets %v", got.UsageBuckets)
	}

	// Unknown inputs are rejected, never dropped.
	if _, err := canonicalize(observation{Name: "mystery", Type: "GENERATION"}, false); err == nil {
		t.Fatal("unknown operation accepted")
	}
	unknownBucket := observation{
		Name: "OpenAI-generation", Type: "GENERATION",
		UsageDetails: map[string]int64{"exotic_bucket": 1},
	}
	if _, err := canonicalize(unknownBucket, false); err == nil {
		t.Fatal("unknown usage bucket accepted")
	}

	// The Python wrapper name projects onto the same operation.
	python := observation{
		Name: "OpenAI-generation", Type: "GENERATION",
		UsageDetails: map[string]int64{"input": 1, "output": 1, "total": 2},
	}
	pc, err := canonicalize(python, false)
	if err != nil {
		t.Fatal(err)
	}
	if pc.Operation != "chat.completion" {
		t.Fatalf("python operation %q", pc.Operation)
	}
}
