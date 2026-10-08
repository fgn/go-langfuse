//go:build datasetinterop

package validation

// The dataset and experiment interop harness: the local Go SDK, the pinned
// official Python SDK (datasetinterop/python, uv-locked), and the pinned
// official TypeScript SDK (datasetinterop/ts, npm-locked) write and read the
// same synthetic records on one live Langfuse project, in both directions.
// It needs LANGFUSE_BASE_URL, LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY,
// LANGFUSE_INTEROP_PROJECT (the project name the keys must belong to), uv,
// and node (INTEROP_NODE overrides the binary). Missing prerequisites fail;
// nothing is skipped. See validation/README.md.

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
)

// ---------------------------------------------------------------------------
// Fixtures. Every value is synthetic. Each SDK receives the same JSON; Go
// decodes it to native values (json.Number keeps exact tokens).

type fixtureItem struct {
	key            string
	input          string // raw JSON, "" omits the field
	expectedOutput string
	metadata       string
	sourceTraceID  string
	// sourceObservationID is set with sourceTraceID.
	sourceObservationID string
	status              string
	// storedInput, when set, is the input as the server returns it, where
	// it differs beyond storedByServer.
	storedInput string
}

var datasetFixtures = []fixtureItem{
	{
		key: "rich",
		input: `{"question":"Hauptstadt von Österreich? 🏔️","context":["Wien","Graz"],` +
			`"flags":{"strict":true,"retries":0,"temperature":0.25,"neg":-3,"small":1.5e-7},` +
			`"nothing":null,"escapes":"He said \"hi\"\\n\ttab","rtl":"مرحبا","cjk":"東京","combining":"é"}`,
		expectedOutput: `"Wien"`,
		metadata:       `{"source":"synthetic","tags":["geo","de"],"difficulty":2,"verified":false,"nested":{"a":{"b":null}}}`,
	},
	{key: "string", input: `"plain string input"`, expectedOutput: `{"answer":"Paris","confidence":1}`, metadata: `["alpha",2,false]`},
	{key: "json-looking", input: `"{\"a\":1}"`, expectedOutput: `"true"`, metadata: `"label"`},
	{key: "scalars", input: `42`, expectedOutput: `false`},
	{key: "empty", input: `""`, metadata: `{}`},
	// Observed, not a general rule: the server returns 12345678901234570000
	// to every SDK, a different double.
	{
		key: "big", input: `{"big":12345678901234567890,"maxSafePlusTwo":9007199254740993,"tiny":-0.000001}`,
		storedInput: `{"big":12345678901234570000,"maxSafePlusTwo":9007199254740992,"tiny":-0.000001}`,
	},
	{key: "archived", input: `"hidden"`, status: "ARCHIVED"},
	{key: "sourced", input: `"with source"`, sourceTraceID: "0123456789abcdef0123456789abcdef", sourceObservationID: "0123456789abcdef"},
	{key: "array", input: `[1,"two",{"three":3},[]]`, expectedOutput: `[true,null,"x"]`, metadata: `[{"k":"v"}]`},
}

const (
	datasetDescription   = "synthetic interop dataset"
	datasetMetadata      = `{"purpose":"interop","schemaVersion":1}`
	arrayDatasetMetadata = `["interop",1,{"nested":null}]`
	inputSchema          = `{"description":"any input"}`
	expectedOutputSchema = `{"description":"any expected output"}`
)

// experimentFixtures share one deterministic task across SDKs: Japan is
// answered wrongly, so exact_match is 1,1,0,1 and accuracy is 0.75.
var experimentFixtures = []fixtureItem{
	{key: "austria", input: `{"country":"Austria"}`, expectedOutput: `"Wien"`, metadata: `{"region":"eu"}`},
	{key: "france", input: `{"country":"France"}`, expectedOutput: `"Paris"`, metadata: `{"region":"eu"}`},
	{key: "japan", input: `{"country":"Japan"}`, expectedOutput: `"Tokyo"`, metadata: `{"region":"asia"}`},
	{key: "peru", input: `{"country":"Peru"}`, expectedOutput: `{"capital":"Lima"}`, metadata: `{"region":"sa"}`},
}

var experimentAnswers = map[string]any{
	"Austria": "Wien", "France": "Paris", "Japan": "Kyoto", "Peru": map[string]any{"capital": "Lima"},
}

const (
	taskDelay      = 200 * time.Millisecond
	evaluatorDelay = 1500 * time.Millisecond
)

// ---------------------------------------------------------------------------
// Harness.

type interopHarness struct {
	lf       *langfuse.Client
	baseURL  string
	public   string
	secret   string
	node     string
	marker   string
	started  time.Time
	versions map[string]any
	// environment is the Go client's score environment for run scores.
	environment string

	mu      sync.Mutex
	results []caseResult
}

type caseResult struct {
	Case      string   `json:"case"`
	Direction string   `json:"direction"`
	Status    string   `json:"status"`
	Evidence  []string `json:"evidence,omitempty"`
}

func newInteropHarness(t *testing.T) *interopHarness {
	t.Helper()
	var missing []string
	for _, name := range []string{"LANGFUSE_BASE_URL", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_INTEROP_PROJECT"} {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		t.Fatalf("dataset interop needs %s; a missing prerequisite is a failure, not a skip", strings.Join(missing, ", "))
	}
	node := os.Getenv("INTEROP_NODE")
	if node == "" {
		var err error
		if node, err = exec.LookPath("node"); err != nil {
			t.Fatal("node is not on PATH; set INTEROP_NODE")
		}
	}
	if _, err := exec.LookPath("uv"); err != nil {
		t.Fatal("uv is not on PATH")
	}
	// Every client inherits one environment; a non-default one makes the
	// environment assertions meaningful.
	if os.Getenv("LANGFUSE_TRACING_ENVIRONMENT") == "" {
		t.Setenv("LANGFUSE_TRACING_ENVIRONMENT", "interop")
	}
	config := langfuse.ConfigFromEnv()
	if config.Disabled || config.DisableContentCapture || (config.SampleRate != nil && *config.SampleRate != 1) {
		t.Fatal("tracing, content capture, and a sample rate of 1 are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	lf, err := langfuse.New(ctx, config)
	if err != nil {
		t.Fatalf("langfuse.New(): %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := lf.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown(): %v", err)
		}
	})
	started := time.Now().UTC()
	h := &interopHarness{
		lf: lf, baseURL: strings.TrimRight(config.BaseURL, "/"), public: config.PublicKey, secret: config.SecretKey,
		node: node, marker: fmt.Sprintf("go-interop-%d", started.UnixNano()), started: started,
		versions: map[string]any{}, environment: cmp.Or(config.Environment, "default"),
	}
	var health struct {
		Status  string `json:"status"`
		Version string `json:"version"`
	}
	h.getJSON(t, "/api/public/health", &health)
	var projects struct {
		Data []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"data"`
	}
	h.getJSON(t, "/api/public/projects", &projects)
	if len(projects.Data) != 1 || projects.Data[0].Name != os.Getenv("LANGFUSE_INTEROP_PROJECT") {
		t.Fatalf("the keys belong to %d projects (%+v), want only %q; refusing to write", len(projects.Data),
			projects.Data, os.Getenv("LANGFUSE_INTEROP_PROJECT"))
	}
	h.versions["server"] = health.Version
	h.versions["project"] = projects.Data[0].Name
	h.versions["project_id"] = projects.Data[0].ID
	h.versions["python"] = h.python(t, map[string]any{"op": "meta"})
	h.versions["typescript"] = h.ts(t, map[string]any{"op": "meta"})
	h.versions["go_sources_before"] = sourceManifest(t)
	h.versions["client_environment"] = h.environment
	h.versions["marker"] = h.marker
	t.Logf("versions: %v", h.versions)
	return h
}

// sourceManifest identifies the code under test: the git HEAD and a SHA-256
// over every tracked and untracked Go source, module file, agent, and lock
// file of the Go SDK and this harness, by path and content.
func sourceManifest(t *testing.T) string {
	t.Helper()
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("git rev-parse: %v", err)
	}
	listing, err := exec.Command("git", "-C", "..", "ls-files", "--cached", "--others", "--exclude-standard").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	var paths []string
	for _, path := range strings.Split(strings.TrimSpace(string(listing)), "\n") {
		base := filepath.Base(path)
		if strings.HasSuffix(path, ".go") || base == "go.mod" || base == "go.sum" ||
			strings.HasPrefix(path, "validation/datasetinterop/") {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	digest := sha256.New()
	for _, path := range paths {
		data, err := os.ReadFile(filepath.Join("..", path))
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		fmt.Fprintf(digest, "%s %d\n", path, len(data))
		digest.Write(data)
	}
	return fmt.Sprintf("HEAD %s, %d files, sha256 %x", strings.TrimSpace(string(head))[:12], len(paths), digest.Sum(nil))
}

func (h *interopHarness) getJSON(t *testing.T, path string, into any) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, h.baseURL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.SetBasicAuth(h.public, h.secret)
	client := &http.Client{Timeout: 30 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("GET %s failed", path)
	}
	defer func() { _ = response.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if response.StatusCode != http.StatusOK || json.Unmarshal(body, into) != nil {
		t.Fatalf("GET %s returned status %d", path, response.StatusCode)
	}
}

// agentTimeout bounds one agent process, polling included.
const agentTimeout = 6 * time.Minute

// agent runs one request through an official SDK agent. Credentials reach
// it only through the inherited environment, and its failure output is
// shown with the credential values redacted.
func (h *interopHarness) agent(t *testing.T, dir string, name string, args []string, request any) any {
	t.Helper()
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), agentTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = dir
	var stdout, stderr bytes.Buffer
	command.Stdin = bytes.NewReader(payload)
	command.Stdout, command.Stderr = &stdout, &stderr
	command.Env = os.Environ()
	if err := command.Run(); err != nil {
		t.Fatalf("%s agent %v failed: %v\n%s", filepath.Base(dir), requestOp(request), err, h.redact(tail(stderr.String())))
	}
	return decodeJSON(t, stdout.Bytes())
}

func (h *interopHarness) redact(text string) string {
	for _, secret := range []string{h.secret, h.public} {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "[redacted]")
		}
	}
	return text
}

func requestOp(request any) any {
	if m, ok := request.(map[string]any); ok {
		return m["op"]
	}
	return request
}

func tail(text string) string {
	if len(text) > 4000 {
		return text[len(text)-4000:]
	}
	return text
}

func (h *interopHarness) python(t *testing.T, request any) any {
	t.Helper()
	return h.agent(t, "datasetinterop/python", "uv", []string{"run", "--locked", "--quiet", "python", "-I", "agent.py"}, request)
}

func (h *interopHarness) ts(t *testing.T, request any) any {
	t.Helper()
	return h.agent(t, "datasetinterop/ts", h.node, []string{"agent.mjs"}, request)
}

// sdk runs one request through the named official SDK.
func (h *interopHarness) sdk(t *testing.T, name string, request any) any {
	t.Helper()
	if name == "python" {
		return h.python(t, request)
	}
	return h.ts(t, request)
}

func (h *interopHarness) record(t *testing.T, name, direction string, evidence *[]string, body func(t *testing.T)) {
	h.recordAs(t, name, direction, "pass", evidence, body)
}

// recordAs records a case whose successful assertions mean success.
func (h *interopHarness) recordAs(
	t *testing.T, name, direction, success string, evidence *[]string, body func(t *testing.T),
) {
	t.Run(name+" "+direction, func(t *testing.T) {
		defer func() {
			status := success
			switch {
			case t.Failed():
				status = "fail"
			case t.Skipped():
				status = "skip"
			}
			h.mu.Lock()
			h.results = append(h.results, caseResult{Case: name, Direction: direction, Status: status, Evidence: *evidence})
			h.mu.Unlock()
		}()
		body(t)
	})
}

func (h *interopHarness) writeReport(t *testing.T) {
	directory := os.Getenv("INTEROP_REPORT_DIR")
	if directory == "" {
		directory = filepath.Join(os.TempDir(), h.marker)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Errorf("report directory: %v", err)
		return
	}
	after := sourceManifest(t)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.versions["go_sources_after"] = after
	if after != h.versions["go_sources_before"] {
		t.Error("sources changed during the run; the evidence does not identify one tree")
	}
	totals := map[string]int{}
	for _, result := range h.results {
		totals[result.Status]++
	}
	report := map[string]any{
		"versions": h.versions, "started": h.started, "finished": time.Now().UTC(), "cases": h.results, "totals": totals,
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Errorf("encode report: %v", err)
		return
	}
	path := filepath.Join(directory, "dataset-interop-report.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Errorf("write report: %v", err)
		return
	}
	var markdown strings.Builder
	fmt.Fprintf(&markdown, "# Dataset and experiment interop report\n\nMarker `%s`; started %s.\n\n", h.marker,
		h.started.Format(time.RFC3339))
	keys := make([]string, 0, len(h.versions))
	for key := range h.versions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(&markdown, "- %s: `%v`\n", key, h.versions[key])
	}
	fmt.Fprintf(&markdown, "\nFunctional cases: %d pass, %d fail, %d skip. Server limitations recorded as unsupported (not passes): %d.\n",
		totals["pass"], totals["fail"], totals["skip"], totals["unsupported"])
	markdown.WriteString("\n| Case | Direction | Status | Evidence |\n| --- | --- | --- | --- |\n")
	for _, result := range h.results {
		fmt.Fprintf(&markdown, "| %s | %s | %s | %s |\n", result.Case, result.Direction, strings.ToUpper(result.Status),
			strings.ReplaceAll(strings.Join(result.Evidence, "; "), "|", "/"))
	}
	if err := os.WriteFile(filepath.Join(directory, "dataset-interop-report.md"), []byte(markdown.String()), 0o600); err != nil {
		t.Errorf("write report: %v", err)
	}
	t.Logf("report: %s", path)
}

// ---------------------------------------------------------------------------
// JSON normalization. Objects compare by key presence and value, arrays by
// order, and strings, booleans, and null exactly. Numbers compare as IEEE-754 doubles, because
// the Langfuse server parses every request with JavaScript's JSON.parse. A
// missing value equals JSON null, as Langfuse stores omitted content.
//
// storedByServer applies the server's documented write normalization of
// dataset item content (web/src/.../DatasetItemValidator.ts normalize, the
// same in v4.48.0 and v4.55.0): "" becomes null and a top-level string that
// parses as JSON is stored parsed. Numbers become doubles. The harness
// expects these for every SDK, so they are server behavior, not SDK gaps.

// exportedContent reads an experiment item's input, output, or expected
// output: the experiments API returns the exported attribute text as a
// string, so a string holding JSON is that JSON value. Fixtures avoid plain
// strings that would also parse as JSON.
func exportedContent(value any) any {
	if text, ok := value.(string); ok && json.Valid([]byte(text)) {
		return rawValueNoT(text)
	}
	return value
}

// flattenedByServer is how the server stores experiment and experiment item
// metadata (packages/shared/src/server/otel/utils.ts flattenJsonToPathArrays
// in v4.48.0): nested objects become dotted keys and leaves strings, with
// other JSON values as their JSON text and null as "". Absent metadata reads
// back as {}. v4.55.0 also keeps only the first of colliding dotted paths;
// the fixtures have no collisions.
func flattenedByServer(value any) map[string]any {
	result := map[string]any{}
	var flatten func(prefix string, object map[string]any)
	flatten = func(prefix string, object map[string]any) {
		for key, value := range object {
			path := key
			if prefix != "" {
				path = prefix + "." + key
			}
			switch value := value.(type) {
			case map[string]any:
				flatten(path, value)
			case nil:
				result[path] = ""
			case string:
				result[path] = value
			default:
				data, _ := json.Marshal(value)
				result[path] = string(data)
			}
		}
	}
	if object, ok := value.(map[string]any); ok {
		flatten("", object)
	}
	return result
}

func storedByServer(value any) any {
	if text, ok := value.(string); ok {
		if text == "" {
			return nil
		}
		if json.Valid([]byte(text)) {
			return rawValueNoT(text)
		}
	}
	return value
}

func decodeJSON(t *testing.T, data []byte) any {
	t.Helper()
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode %q: %v", tail(string(data)), err)
	}
	return value
}

func rawValue(t *testing.T, raw string) any {
	t.Helper()
	if raw == "" {
		return nil
	}
	return decodeJSON(t, []byte(raw))
}

// jsonDiff returns "" when got and want are the same JSON value, else the
// path of the first difference.
func jsonDiff(path string, got, want any) string {
	switch want := want.(type) {
	case map[string]any:
		object, ok := got.(map[string]any)
		if !ok {
			return fmt.Sprintf("%s: got %s, want object %s", path, describe(got), describe(want))
		}
		keys := map[string]bool{}
		for key := range object {
			keys[key] = true
		}
		for key := range want {
			keys[key] = true
		}
		for _, key := range sortedKeys(keys) {
			gotValue, inGot := object[key]
			wantValue, inWant := want[key]
			if inGot != inWant {
				return fmt.Sprintf("%s.%s: present %v, want present %v", path, key, inGot, inWant)
			}
			if diff := jsonDiff(path+"."+key, gotValue, wantValue); diff != "" {
				return diff
			}
		}
		return ""
	case []any:
		array, ok := got.([]any)
		if !ok || len(array) != len(want) {
			return fmt.Sprintf("%s: got %s, want %s", path, describe(got), describe(want))
		}
		for index := range want {
			if diff := jsonDiff(fmt.Sprintf("%s[%d]", path, index), array[index], want[index]); diff != "" {
				return diff
			}
		}
		return ""
	case json.Number:
		gotNumber, ok := got.(json.Number)
		if !ok {
			return fmt.Sprintf("%s: got %s, want number %s", path, describe(got), want)
		}
		a, errA := strconv.ParseFloat(gotNumber.String(), 64)
		b, errB := strconv.ParseFloat(want.String(), 64)
		if errA != nil || errB != nil || a != b {
			return fmt.Sprintf("%s: got number %s, want %s", path, gotNumber, want)
		}
		return ""
	default:
		if !reflect.DeepEqual(got, want) {
			return fmt.Sprintf("%s: got %s, want %s", path, describe(got), describe(want))
		}
		return ""
	}
}

func describe(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%T", value)
	}
	if len(data) > 300 {
		return string(data[:300]) + "..."
	}
	return string(data)
}

func sortedKeys(keys map[string]bool) []string {
	result := make([]string, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func rawOf(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	return decodeJSON(t, raw)
}

func field(value any, keys ...string) any {
	for _, key := range keys {
		object, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = object[key]
	}
	return value
}

func text(value any) string {
	if value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprint(value)
}

// ---------------------------------------------------------------------------
// Writers. Each writes the fixture through one SDK and returns the item IDs
// by fixture key.

func (h *interopHarness) itemID(dataset, key string) string { return dataset + "-" + key }

func (h *interopHarness) writeWithGo(t *testing.T, dataset, metadata string, fixtures []fixtureItem) string {
	t.Helper()
	ctx := context.Background()
	description := datasetDescription
	stored, err := h.lf.UpsertDataset(ctx, langfuse.DatasetSpec{
		Name: dataset, Description: &description, Metadata: rawValue(t, metadata),
		InputSchema: json.RawMessage(inputSchema), ExpectedOutputSchema: json.RawMessage(expectedOutputSchema),
	})
	if err != nil {
		t.Fatalf("Go UpsertDataset(): %v", err)
	}
	for _, fixture := range fixtures {
		if _, err := h.lf.UpsertDatasetItem(ctx, h.goItemSpec(t, dataset, fixture)); err != nil {
			t.Fatalf("Go UpsertDatasetItem(%s): %v", fixture.key, err)
		}
	}
	return stored.ID
}

func (h *interopHarness) goItemSpec(t *testing.T, dataset string, fixture fixtureItem) langfuse.DatasetItemSpec {
	spec := langfuse.DatasetItemSpec{
		DatasetName: dataset, ID: h.itemID(dataset, fixture.key), SourceTraceID: fixture.sourceTraceID,
		SourceObservationID: fixture.sourceObservationID, Status: langfuse.DatasetItemStatus(fixture.status),
	}
	if fixture.input != "" {
		spec.Input = rawValue(t, fixture.input)
	}
	if fixture.expectedOutput != "" {
		spec.ExpectedOutput = rawValue(t, fixture.expectedOutput)
	}
	if fixture.metadata != "" {
		spec.Metadata = rawValue(t, fixture.metadata)
	}
	return spec
}

// writeWithSDK writes the fixture through the named official SDK; Python
// takes snake_case fields and TypeScript camelCase.
// sdkKeys maps the agents' snake_case request keys to TypeScript's.
var sdkKeys = map[string]string{
	"dataset_name": "datasetName", "expected_output": "expectedOutput", "source_trace_id": "sourceTraceId",
	"source_observation_id": "sourceObservationId", "input_schema": "inputSchema",
	"expected_output_schema": "expectedOutputSchema",
}

func sdkKey(sdk string) func(string) string {
	return func(key string) string {
		if sdk == "typescript" && sdkKeys[key] != "" {
			return sdkKeys[key]
		}
		return key
	}
}

func (h *interopHarness) writeWithSDK(t *testing.T, sdk, dataset, metadata string, fixtures []fixtureItem) string {
	t.Helper()
	name := sdkKey(sdk)
	created := h.sdk(t, sdk, map[string]any{
		"op": "create_dataset", "name": dataset, "description": datasetDescription,
		"metadata": rawValue(t, metadata), name("input_schema"): rawValue(t, inputSchema),
		name("expected_output_schema"): rawValue(t, expectedOutputSchema),
	})
	items := make([]any, 0, len(fixtures))
	for _, fixture := range fixtures {
		items = append(items, h.sdkItem(t, sdk, dataset, fixture, name))
	}
	if len(items) != 0 {
		h.sdk(t, sdk, map[string]any{"op": "upsert_items", "items": items})
	}
	return text(field(created, "id"))
}

func (h *interopHarness) sdkItem(t *testing.T, sdk, dataset string, fixture fixtureItem, name func(string) string) map[string]any {
	item := map[string]any{name("dataset_name"): dataset, "id": h.itemID(dataset, fixture.key)}
	for key, raw := range map[string]string{
		"input": fixture.input, name("expected_output"): fixture.expectedOutput, "metadata": fixture.metadata,
	} {
		if raw != "" {
			item[key] = rawValue(t, raw)
		}
	}
	if fixture.sourceTraceID != "" {
		item[name("source_trace_id")] = fixture.sourceTraceID
	}
	if fixture.sourceObservationID != "" {
		item[name("source_observation_id")] = fixture.sourceObservationID
	}
	if fixture.status != "" {
		item["status"] = fixture.status
	}
	return item
}

// ---------------------------------------------------------------------------
// Readers return items normalized to {id, status, input, expectedOutput,
// metadata, sourceTraceId} JSON values.

type readItem struct {
	ID, Status, SourceTraceID, SourceObservationID string
	Input, ExpectedOutput                          any
	Metadata                                       any
}

func (h *interopHarness) readWithGo(t *testing.T, dataset string, asOf time.Time, pageSize int) (map[string]any, []readItem) {
	t.Helper()
	ctx := context.Background()
	stored, err := h.lf.GetDataset(ctx, dataset)
	if err != nil {
		t.Fatalf("Go GetDataset(): %v", err)
	}
	header := map[string]any{
		"id": stored.ID, "name": stored.Name, "description": stored.Description,
		"metadata": rawOf(t, stored.Metadata), "inputSchema": rawOf(t, stored.InputSchema),
		"expectedOutputSchema": rawOf(t, stored.ExpectedOutputSchema),
	}
	var items []readItem
	for item, err := range h.lf.DatasetItems(ctx, langfuse.DatasetItemQuery{DatasetName: dataset, AsOf: asOf, PageSize: pageSize}) {
		if err != nil {
			t.Fatalf("Go DatasetItems(): %v", err)
		}
		items = append(items, readItem{
			ID: item.ID, Status: string(item.Status), SourceTraceID: item.SourceTraceID, SourceObservationID: item.SourceObservationID,
			Input: rawOf(t, item.Input), ExpectedOutput: rawOf(t, item.ExpectedOutput), Metadata: rawOf(t, item.Metadata),
		})
	}
	return header, items
}

func (h *interopHarness) readWithSDK(t *testing.T, sdk, dataset string, asOf time.Time, pageSize int) (map[string]any, []readItem) {
	t.Helper()
	request := map[string]any{"op": "get_dataset", "name": dataset, "page_size": pageSize}
	if !asOf.IsZero() {
		request["version"] = asOf.UTC().Format("2006-01-02T15:04:05.000Z")
	}
	response := h.sdk(t, sdk, request)
	stored := field(response, "dataset")
	header := map[string]any{
		"id": field(stored, "id"), "name": field(stored, "name"), "description": field(stored, "description"),
		"metadata": field(stored, "metadata"), "inputSchema": field(stored, "inputSchema"),
		"expectedOutputSchema": field(stored, "expectedOutputSchema"),
	}
	var items []readItem
	for _, item := range field(response, "items").([]any) {
		items = append(items, sdkReadItem(item))
	}
	return header, items
}

func sdkReadItem(item any) readItem {
	return readItem{
		ID: text(field(item, "id")), Status: text(field(item, "status")), SourceTraceID: text(field(item, "sourceTraceId")),
		SourceObservationID: text(field(item, "sourceObservationId")),
		Input:               field(item, "input"), ExpectedOutput: field(item, "expectedOutput"), Metadata: field(item, "metadata"),
	}
}

func (h *interopHarness) getItem(t *testing.T, reader, id string) (readItem, int) {
	t.Helper()
	if reader == "go" {
		item, err := h.lf.GetDatasetItem(context.Background(), id)
		if errors.Is(err, langfuse.ErrDatasetItemNotFound) {
			return readItem{}, http.StatusNotFound
		}
		if err != nil {
			t.Fatalf("Go GetDatasetItem(%s): %v", id, err)
		}
		return readItem{
			ID: item.ID, Status: string(item.Status), SourceTraceID: item.SourceTraceID, SourceObservationID: item.SourceObservationID,
			Input: rawOf(t, item.Input), ExpectedOutput: rawOf(t, item.ExpectedOutput), Metadata: rawOf(t, item.Metadata),
		}, http.StatusOK
	}
	response := h.sdk(t, reader, map[string]any{"op": "get_item", "id": id})
	if status := field(response, "error", "status"); status != nil {
		number, _ := status.(json.Number).Int64()
		return readItem{}, int(number)
	}
	return sdkReadItem(response), http.StatusOK
}

func (h *interopHarness) read(t *testing.T, reader, dataset string, asOf time.Time, pageSize int) (map[string]any, []readItem) {
	if reader == "go" {
		return h.readWithGo(t, dataset, asOf, pageSize)
	}
	return h.readWithSDK(t, reader, dataset, asOf, pageSize)
}

// write creates the dataset and its items through writer and returns the
// created dataset's ID.
func (h *interopHarness) write(t *testing.T, writer, dataset string, fixtures []fixtureItem) string {
	return h.writeWithMetadata(t, writer, dataset, datasetMetadata, fixtures)
}

func (h *interopHarness) writeWithMetadata(t *testing.T, writer, dataset, metadata string, fixtures []fixtureItem) string {
	if writer == "go" {
		return h.writeWithGo(t, dataset, metadata, fixtures)
	}
	return h.writeWithSDK(t, writer, dataset, metadata, fixtures)
}

// upsert applies one item change through the named SDK; empty fixture fields
// are omitted, so the stored values must survive.
func (h *interopHarness) upsert(t *testing.T, writer, dataset string, fixture fixtureItem) {
	t.Helper()
	if writer == "go" {
		if _, err := h.lf.UpsertDatasetItem(context.Background(), h.goItemSpec(t, dataset, fixture)); err != nil {
			t.Fatalf("Go UpsertDatasetItem(%s): %v", fixture.key, err)
		}
		return
	}
	h.sdk(t, writer, map[string]any{"op": "upsert_items", "items": []any{h.sdkItem(t, writer, dataset, fixture, sdkKey(writer))}})
}

func (h *interopHarness) deleteItem(t *testing.T, writer, id string) {
	t.Helper()
	if writer == "go" {
		if err := h.lf.DeleteDatasetItem(context.Background(), id); err != nil {
			t.Fatalf("Go DeleteDatasetItem(%s): %v", id, err)
		}
		return
	}
	if response := h.sdk(t, writer, map[string]any{"op": "delete_item", "id": id}); field(response, "deleted") != true {
		t.Fatalf("%s delete_item(%s) = %v", writer, id, response)
	}
}

// listDatasets enumerates the project's datasets through reader in pages
// of pageSize and returns their IDs by name. Offset pages can repeat a
// dataset while others are being created; a name listed with two IDs is an
// error.
func (h *interopHarness) listDatasets(t *testing.T, reader string, pageSize int) map[string]string {
	t.Helper()
	listed := map[string]string{}
	add := func(name, id string) {
		if previous, ok := listed[name]; ok && previous != id {
			t.Errorf("%s listed dataset %s with IDs %s and %s", reader, name, previous, id)
		}
		listed[name] = id
	}
	if reader == "go" {
		ctx, cancel := context.WithTimeout(context.Background(), agentTimeout)
		defer cancel()
		for dataset, err := range h.lf.Datasets(ctx, langfuse.DatasetQuery{PageSize: pageSize}) {
			if err != nil {
				t.Fatalf("Go Datasets(): %v", err)
			}
			add(dataset.Name, dataset.ID)
		}
		return listed
	}
	response := h.sdk(t, reader, map[string]any{"op": "list_datasets", "page_size": pageSize})
	for _, dataset := range asSlice(field(response, "datasets")) {
		add(text(field(dataset, "name")), text(field(dataset, "id")))
	}
	return listed
}

// missingOwned lists the owned datasets that a listing lacks or names with
// another ID.
func missingOwned(listed, owned map[string]string) []string {
	var missing []string
	for name, id := range owned {
		if id == "" || listed[name] != id {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	return missing
}

func fixtureByKey(key string) fixtureItem {
	for _, fixture := range datasetFixtures {
		if fixture.key == key {
			return fixture
		}
	}
	panic("no fixture " + key)
}

// listFiltered lists the dataset filtered by the item's source trace and by
// its source observation through reader.
func (h *interopHarness) listFiltered(t *testing.T, reader, dataset string, item readItem) map[string][]readItem {
	t.Helper()
	result := map[string][]readItem{}
	for name, filter := range map[string][2]string{
		"source trace": {item.SourceTraceID, ""}, "source observation": {"", item.SourceObservationID},
	} {
		if reader == "go" {
			var items []readItem
			for stored, err := range h.lf.DatasetItems(context.Background(), langfuse.DatasetItemQuery{
				DatasetName: dataset, SourceTraceID: filter[0], SourceObservationID: filter[1],
			}) {
				if err != nil {
					t.Fatalf("Go DatasetItems(filtered): %v", err)
				}
				items = append(items, readItem{
					ID: stored.ID, Status: string(stored.Status), SourceTraceID: stored.SourceTraceID,
					SourceObservationID: stored.SourceObservationID, Input: rawOf(t, stored.Input),
					ExpectedOutput: rawOf(t, stored.ExpectedOutput), Metadata: rawOf(t, stored.Metadata),
				})
			}
			result[name] = items
			continue
		}
		request := map[string]any{"op": "list_items", "dataset_name": dataset}
		if filter[0] != "" {
			request["source_trace_id"] = filter[0]
		} else {
			request["source_observation_id"] = filter[1]
		}
		for _, stored := range asSlice(field(h.sdk(t, reader, request), "items")) {
			result[name] = append(result[name], sdkReadItem(stored))
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Assertions.

func (h *interopHarness) expectedItem(t *testing.T, dataset string, fixture fixtureItem) readItem {
	status := fixture.status
	if status == "" {
		status = "ACTIVE"
	}
	return readItem{
		ID: h.itemID(dataset, fixture.key), Status: status, SourceTraceID: fixture.sourceTraceID,
		SourceObservationID: fixture.sourceObservationID,
		Input:               storedByServer(rawValue(t, cmp.Or(fixture.storedInput, fixture.input))),
		ExpectedOutput:      storedByServer(rawValue(t, fixture.expectedOutput)),
		Metadata:            storedByServer(rawValue(t, fixture.metadata)),
	}
}

// serverNormalized lists the fixture fields the server stores differently
// from what was sent, for the report.
func serverNormalized(t *testing.T, fixtures []fixtureItem) []string {
	var changed []string
	for _, fixture := range fixtures {
		for name, raw := range map[string]string{
			"input": fixture.input, "expectedOutput": fixture.expectedOutput, "metadata": fixture.metadata,
		} {
			if raw == "" {
				continue
			}
			sent := rawValue(t, raw)
			if !reflect.DeepEqual(sent, storedByServer(sent)) || name == "input" && fixture.storedInput != "" {
				changed = append(changed, fixture.key+"."+name)
			}
		}
	}
	sort.Strings(changed)
	return changed
}

func compareItem(t *testing.T, label string, got, want readItem) {
	t.Helper()
	if got.ID != want.ID || got.Status != want.Status || got.SourceTraceID != want.SourceTraceID ||
		got.SourceObservationID != want.SourceObservationID {
		t.Errorf("%s: identity = %s/%s/%q/%q, want %s/%s/%q/%q", label, got.ID, got.Status, got.SourceTraceID,
			got.SourceObservationID, want.ID, want.Status, want.SourceTraceID, want.SourceObservationID)
	}
	for name, pair := range map[string][2]any{
		"input": {got.Input, want.Input}, "expectedOutput": {got.ExpectedOutput, want.ExpectedOutput},
		"metadata": {got.Metadata, want.Metadata},
	} {
		if diff := jsonDiff(name, pair[0], pair[1]); diff != "" {
			t.Errorf("%s %s: %s", label, want.ID, diff)
		}
	}
}

func compareListing(t *testing.T, label string, got []readItem, want []readItem) {
	t.Helper()
	byID := map[string]readItem{}
	for _, item := range got {
		if _, duplicate := byID[item.ID]; duplicate {
			t.Errorf("%s: item %s listed twice", label, item.ID)
		}
		byID[item.ID] = item
	}
	if len(got) != len(want) {
		var ids []string
		for _, item := range got {
			ids = append(ids, item.ID)
		}
		t.Errorf("%s: %d items %v, want %d", label, len(got), ids, len(want))
	}
	for _, item := range want {
		found, ok := byID[item.ID]
		if !ok {
			t.Errorf("%s: item %s missing", label, item.ID)
			continue
		}
		compareItem(t, label, found, item)
	}
}

func (h *interopHarness) expectedListing(t *testing.T, dataset string, fixtures []fixtureItem) []readItem {
	var want []readItem
	for _, fixture := range fixtures {
		if fixture.status != "ARCHIVED" {
			want = append(want, h.expectedItem(t, dataset, fixture))
		}
	}
	return want
}

func compareHeader(t *testing.T, label string, got map[string]any, id, dataset, metadata string) {
	t.Helper()
	want := map[string]any{
		"id": id, "name": dataset, "description": datasetDescription, "metadata": rawValueNoT(metadata),
		"inputSchema": rawValueNoT(inputSchema), "expectedOutputSchema": rawValueNoT(expectedOutputSchema),
	}
	if diff := jsonDiff("dataset", got, want); diff != "" || id == "" {
		t.Errorf("%s: %s (created ID %q)", label, diff, id)
	}
}

func rawValueNoT(raw string) any {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var value any
	_ = decoder.Decode(&value)
	return value
}

// ---------------------------------------------------------------------------

var directions = [][2]string{{"python", "go"}, {"go", "python"}, {"typescript", "go"}, {"go", "typescript"}}

func TestDatasetInterop(t *testing.T) {
	h := newInteropHarness(t)
	defer h.writeReport(t)

	t.Run("E1 create and read", func(t *testing.T) {
		for _, direction := range directions {
			writer, reader := direction[0], direction[1]
			dataset := fmt.Sprintf("%s-e1-%s-%s", h.marker, writer, reader)
			var evidence []string
			h.record(t, "E1 create/read/list", writer+"→"+reader, &evidence, func(t *testing.T) {
				// Two older owned datasets put at least one owned dataset past
				// the first page of 2 (newest first), whatever else the
				// project holds.
				owned := map[string]string{}
				for index := range 2 {
					name := fmt.Sprintf("%s-list-%d", dataset, index)
					owned[name] = h.writeWithMetadata(t, writer, name, datasetMetadata, nil)
				}
				id := h.write(t, writer, dataset, datasetFixtures)
				owned[dataset] = id
				header, items := h.read(t, reader, dataset, time.Time{}, 3)
				compareHeader(t, reader+" dataset", header, id, dataset, datasetMetadata)
				compareListing(t, reader+" listing (page size 3)", items, h.expectedListing(t, dataset, datasetFixtures))
				archived, status := h.getItem(t, reader, h.itemID(dataset, "archived"))
				if status != http.StatusOK {
					t.Errorf("archived item read status %d", status)
				}
				compareItem(t, reader+" archived item", archived, h.expectedItem(t, dataset, fixtureByKey("archived")))
				sourced := h.expectedItem(t, dataset, fixtureByKey("sourced"))
				for name, filtered := range h.listFiltered(t, reader, dataset, sourced) {
					compareListing(t, reader+" listing filtered by "+name, filtered, []readItem{sourced})
				}
				if missing := missingOwned(h.listDatasets(t, reader, 2), owned); len(missing) != 0 {
					t.Errorf("%s dataset listing lacks or misidentifies %v", reader, missing)
				}
				evidence = append(evidence, fmt.Sprintf("%s listing in pages of 2 returned all 3 owned datasets with their IDs "+
					"(at least one past the first page)", reader))
				evidence = append(evidence, fmt.Sprintf("dataset %s (ID %s); %d active items listed in pages of 3; archived item read by ID; "+
					"source trace and observation filters; "+
					"expected server normalization of %v (\"\" → null, JSON-looking strings parsed, observed large-number change)",
					dataset, id, len(items), serverNormalized(t, datasetFixtures)))
			})
		}
	})

	t.Run("E2 mutate", func(t *testing.T) {
		for _, direction := range directions {
			creator, mutator := direction[1], direction[0]
			dataset := fmt.Sprintf("%s-e2-%s-%s", h.marker, creator, mutator)
			var evidence []string
			h.record(t, "E2 mutate", mutator+" mutates "+creator+"-created, "+creator+" reads", &evidence, func(t *testing.T) {
				id := h.writeWithMetadata(t, creator, dataset, arrayDatasetMetadata, datasetFixtures)
				changes := []fixtureItem{
					// Input omitted: the stored input must survive.
					{key: "rich", expectedOutput: `"Wien (updated) ✓"`, metadata: `{"source":"mutation","list":[1,2.5,"drei"]}`},
					{key: "string", status: "ARCHIVED"},
					{key: "archived", status: "ACTIVE"},
					{
						key: "sourced", input: `{"replaced":true}`,
						sourceTraceID: "fedcba9876543210fedcba9876543210", sourceObservationID: "fedcba9876543210",
					},
					// Explicit null: Python and TypeScript send it, Go omits the
					// field; the server keeps the stored value either way.
					{key: "json-looking", expectedOutput: `null`},
				}
				for _, change := range changes {
					h.upsert(t, mutator, dataset, change)
				}
				if mutator == "go" {
					// Go rejects explicit JSON null locally rather than send a
					// value the server would ignore.
					if _, err := h.lf.UpsertDatasetItem(context.Background(), langfuse.DatasetItemSpec{
						DatasetName: dataset, ID: h.itemID(dataset, "rich"), Input: json.RawMessage(`null`),
					}); err == nil {
						t.Error("Go sent an explicit JSON null item input")
					}
				}
				h.deleteItem(t, mutator, h.itemID(dataset, "scalars"))

				final := map[string]fixtureItem{}
				for _, fixture := range datasetFixtures {
					final[fixture.key] = fixture
				}
				rich := final["rich"]
				rich.expectedOutput, rich.metadata = changes[0].expectedOutput, changes[0].metadata
				final["rich"] = rich
				str := final["string"]
				str.status = "ARCHIVED"
				final["string"] = str
				archived := final["archived"]
				archived.status = ""
				final["archived"] = archived
				sourced := final["sourced"]
				sourced.input, sourced.sourceTraceID = changes[3].input, changes[3].sourceTraceID
				sourced.sourceObservationID = changes[3].sourceObservationID
				final["sourced"] = sourced
				delete(final, "scalars")
				var fixtures []fixtureItem
				for _, fixture := range datasetFixtures {
					if f, ok := final[fixture.key]; ok {
						fixtures = append(fixtures, f)
					}
				}
				header, items := h.read(t, creator, dataset, time.Time{}, 50)
				compareHeader(t, creator+" dataset with array metadata", header, id, dataset, arrayDatasetMetadata)
				compareListing(t, creator+" listing after mutation", items, h.expectedListing(t, dataset, fixtures))
				gotArchived, status := h.getItem(t, creator, h.itemID(dataset, "string"))
				if status != http.StatusOK {
					t.Errorf("archived item status %d", status)
				}
				compareItem(t, creator+" newly archived item", gotArchived, h.expectedItem(t, dataset, final["string"]))
				if _, status := h.getItem(t, creator, h.itemID(dataset, "scalars")); status != http.StatusNotFound {
					t.Errorf("deleted item read status %d, want 404", status)
				}
				evidence = append(evidence, fmt.Sprintf("dataset %s (array metadata): content/metadata update with omitted input kept, explicit null kept "+
					"(Go rejects it locally), archive, unarchive, source trace and observation change, delete → 404", dataset))
			})
		}
	})

	t.Run("E3 versions and pages", func(t *testing.T) {
		for _, pair := range [][2]string{{"python", "go"}, {"go", "python"}, {"typescript", "go"}, {"go", "typescript"}} {
			writer, reader := pair[0], pair[1]
			dataset := fmt.Sprintf("%s-e3-%s-%s", h.marker, writer, reader)
			var evidence []string
			h.record(t, "E3 as-of version and pagination", writer+"→"+reader, &evidence, func(t *testing.T) {
				var original []fixtureItem
				for index := range 7 {
					original = append(original, fixtureItem{
						key: fmt.Sprintf("p%d", index), input: fmt.Sprintf(`{"n":%d}`, index),
						expectedOutput: fmt.Sprintf(`"v%d"`, index),
					})
				}
				h.write(t, writer, dataset, original)
				time.Sleep(1500 * time.Millisecond)
				pin := time.Now().UTC().Truncate(time.Millisecond)
				time.Sleep(1500 * time.Millisecond)
				h.upsert(t, writer, dataset, fixtureItem{key: "p1", expectedOutput: `"v1 changed"`})
				h.upsert(t, writer, dataset, fixtureItem{key: "p2", status: "ARCHIVED"})
				h.deleteItem(t, writer, h.itemID(dataset, "p3"))
				added := fixtureItem{key: "p7", input: `{"n":7}`, expectedOutput: `"v7"`}
				h.upsert(t, writer, dataset, added)

				_, pinned := h.read(t, reader, dataset, pin, 2)
				compareListing(t, reader+" as-of listing (page size 2)", pinned, h.expectedListing(t, dataset, original))
				latest := []fixtureItem{original[0], original[1], original[4], original[5], original[6], added}
				latest[1].expectedOutput = `"v1 changed"`
				_, current := h.read(t, reader, dataset, time.Time{}, 3)
				compareListing(t, reader+" latest listing (page size 3)", current, h.expectedListing(t, dataset, latest))
				evidence = append(evidence, fmt.Sprintf("pin %s: %d items over %d pages of 2; latest %d items over pages of 3",
					pin.Format(time.RFC3339Nano), len(pinned), (len(pinned)+1)/2, len(current)))
			})
		}
	})

	t.Run("E4 dataset experiments", func(t *testing.T) {
		for _, pair := range [][2]string{{"python", "go"}, {"go", "python"}, {"typescript", "go"}, {"go", "typescript"}} {
			runner, reader := pair[0], pair[1]
			dataset := fmt.Sprintf("%s-e4-%s-%s", h.marker, runner, reader)
			var evidence []string
			h.record(t, "E4 dataset experiment", runner+" runs, "+reader+" reads", &evidence, func(t *testing.T) {
				id := h.write(t, runner, dataset, experimentFixtures)
				time.Sleep(1500 * time.Millisecond)
				pin := time.Now().UTC().Truncate(time.Millisecond)
				run := h.runExperiment(t, runner, dataset, id, pin)
				h.verifyExperiment(t, reader, run, &evidence)
			})
		}
	})

	t.Run("E5 local experiments", func(t *testing.T) {
		for _, pair := range [][2]string{{"python", "go"}, {"go", "python"}, {"typescript", "go"}, {"go", "typescript"}} {
			runner, reader := pair[0], pair[1]
			var evidence []string
			h.record(t, "E5 local experiment", runner+" runs, "+reader+" reads", &evidence, func(t *testing.T) {
				run := h.runExperiment(t, runner, "", "", time.Time{})
				h.verifyExperiment(t, reader, run, &evidence)
			})
		}
	})

	t.Run("E7 any metadata", func(t *testing.T) {
		var evidence []string
		h.record(t, "E7 Go experiment over non-object item metadata", "python creates, go runs, python reads", &evidence, func(t *testing.T) {
			dataset := fmt.Sprintf("%s-e7-python-go", h.marker)
			h.write(t, "python", dataset, datasetFixtures)
			h.verifyMetadataShapes(t, dataset, &evidence)
		})
	})

	t.Run("E6 legacy dataset runs", func(t *testing.T) {
		// Not an interoperability pass: the case only classifies the
		// server's mode. Its status is UNSUPPORTED when the server rejects
		// legacy dataset-run reads for events_only mode, and fail otherwise.
		var evidence []string
		h.recordAs(t, "E6 legacy dataset-run reads", "python", "unsupported", &evidence, func(t *testing.T) {
			dataset := fmt.Sprintf("%s-e4-python-go", h.marker)
			response := h.python(t, map[string]any{"op": "legacy_runs", "dataset_name": dataset})
			evidence = append(evidence, "server limitation (v4 events_only): python get_dataset_runs → "+describe(response))
			if field(response, "error", "status") != json.Number("404") ||
				!strings.Contains(text(field(response, "error", "message")), "events_only") {
				t.Errorf("legacy dataset runs answered %s, not the events_only rejection", describe(response))
			}
		})
	})
}

// ---------------------------------------------------------------------------
// Experiments.

// failingFixture fails every runner's task; local runs include it.
var failingFixture = fixtureItem{key: "fail", input: `{"country":"Fail"}`, expectedOutput: `"none"`, metadata: `{"region":"none"}`}

// experimentRun is one run as its runner reported it, with the items the
// server must store for it.
type experimentRun struct {
	runner, name, runName, experimentID, datasetRunID string
	datasetID                                         string
	pin                                               time.Time
	local                                             bool
	// expected maps every item ID the run must store to its fixture.
	expected map[string]fixtureItem
	// items holds what the runner reported; Python and TypeScript report
	// successful items only.
	items          map[string]runItem
	runEvaluations map[string]any
}

type runItem struct {
	traceID, observationID string
	output                 any
	evaluations            map[string]any
	failed                 bool
}

func (run experimentRun) metadata() map[string]any {
	return map[string]any{"suite": "interop", "sdk": run.runner}
}

// itemScores is the scores each successful item gets: name to data type.
// Python and Go also run a composite evaluator; TypeScript has none.
func (run experimentRun) itemScores() map[string]string {
	scores := map[string]string{"exact_match": "NUMERIC", "verdict": "CATEGORICAL"}
	if run.runner != "typescript" {
		scores["overall"] = "NUMERIC"
	}
	return scores
}

func (h *interopHarness) runExperiment(t *testing.T, runner, dataset, datasetID string, pin time.Time) experimentRun {
	t.Helper()
	kind := map[bool]string{true: "local", false: "dataset"}[dataset == ""]
	run := experimentRun{
		runner: runner, name: "interop-" + runner, runName: fmt.Sprintf("%s-%s-run-%s", h.marker, runner, kind),
		datasetID: datasetID, pin: pin, local: dataset == "",
		expected: map[string]fixtureItem{}, items: map[string]runItem{}, runEvaluations: map[string]any{},
	}
	fixtures := experimentFixtures
	if run.local {
		fixtures = append(slices.Clone(experimentFixtures), failingFixture)
	}
	for _, fixture := range fixtures {
		switch {
		case !run.local:
			run.expected[h.itemID(dataset, fixture.key)] = fixture
		case runner == "go":
			run.expected[run.runName+"-"+fixture.key] = fixture
		default:
			run.expected[localItemID(runner, rawValue(t, fixture.input))] = fixture
		}
	}
	if runner == "go" {
		run.fromGo(t, h, dataset, fixtures)
		return run
	}
	request := map[string]any{
		"op": "run_experiment", "name": run.name, "run_name": run.runName, "description": "interop " + runner,
		"metadata": run.metadata(), "task_delay": taskDelay.Seconds(), "evaluator_delay": evaluatorDelay.Seconds(),
	}
	if dataset != "" {
		request["dataset"] = dataset
		request["version"] = pin.Format("2006-01-02T15:04:05.000Z")
	} else {
		var items []any
		for _, fixture := range fixtures {
			items = append(items, map[string]any{
				"input": rawValue(t, fixture.input), runnerKey(runner, "expected_output"): rawValue(t, fixture.expectedOutput),
				"metadata": rawValue(t, fixture.metadata),
			})
		}
		request["local_items"] = items
	}
	response := h.sdk(t, runner, request)
	run.experimentID = text(field(response, "experiment_id"))
	run.datasetRunID = text(field(response, "dataset_run_id"))
	for _, result := range asSlice(field(response, "item_results")) {
		itemID := text(field(result, "item_id"))
		if itemID == "" {
			itemID = localItemID(runner, field(result, "input"))
		}
		evaluations := map[string]any{}
		for _, evaluation := range asSlice(field(result, "evaluations")) {
			evaluations[text(field(evaluation, "name"))] = field(evaluation, "value")
		}
		if _, duplicate := run.items[itemID]; duplicate {
			t.Errorf("%s reported item %s twice", runner, itemID)
		}
		run.items[itemID] = runItem{
			traceID: text(field(result, "trace_id")), output: field(result, "output"), evaluations: evaluations,
		}
	}
	for _, evaluation := range asSlice(field(response, "run_evaluations")) {
		run.runEvaluations[text(field(evaluation, "name"))] = field(evaluation, "value")
	}
	return run
}

func runnerKey(runner, key string) string {
	if runner == "typescript" && key == "expected_output" {
		return "expectedOutput"
	}
	return key
}

// localItemID is the official SDKs' item ID for local data: the first 16 hex
// characters of the SHA-256 of the serialized input. Python serializes with
// json.dumps defaults and TypeScript with JSON.stringify.
func localItemID(runner string, input any) string {
	var serialized string
	switch value := input.(type) {
	case string:
		serialized = value
	default:
		if runner == "python" {
			serialized = pythonJSON(value)
		} else {
			data, _ := json.Marshal(value)
			serialized = string(data)
		}
	}
	sum := sha256.Sum256([]byte(serialized))
	return hex.EncodeToString(sum[:])[:16]
}

// pythonJSON renders the one-key string-valued objects of the experiment
// fixtures the way Python's json.dumps does: ", " and ": " separators and
// ASCII escapes.
func pythonJSON(value any) string {
	object, ok := value.(map[string]any)
	if !ok {
		data, _ := json.Marshal(value)
		return string(data)
	}
	var parts []string
	for _, key := range sortedKeys(func() map[string]bool {
		keys := map[string]bool{}
		for key := range object {
			keys[key] = true
		}
		return keys
	}()) {
		k, _ := json.Marshal(key)
		v, _ := json.Marshal(object[key])
		parts = append(parts, string(k)+": "+string(v))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func (run *experimentRun) fromGo(t *testing.T, h *interopHarness, dataset string, fixtures []fixtureItem) {
	ctx := context.Background()
	var items []langfuse.ExperimentItem
	if dataset != "" {
		for item, err := range h.lf.DatasetItems(ctx, langfuse.DatasetItemQuery{DatasetName: dataset, AsOf: run.pin}) {
			if err != nil {
				t.Fatalf("Go DatasetItems(): %v", err)
			}
			items = append(items, item.ExperimentItem())
		}
	} else {
		for _, fixture := range fixtures {
			items = append(items, langfuse.ExperimentItem{
				ID: run.runName + "-" + fixture.key, Input: rawValue(t, fixture.input),
				ExpectedOutput: rawValue(t, fixture.expectedOutput), Metadata: rawValue(t, fixture.metadata),
			})
		}
	}
	decode := func(value any) any {
		if raw, ok := value.(json.RawMessage); ok {
			return decodeJSON(t, raw)
		}
		return value
	}
	same := func(a, b any) bool { return jsonDiff("", decode(a), decode(b)) == "" }
	exactMatch := func(_ context.Context, input langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
		time.Sleep(evaluatorDelay)
		value := 0.0
		if same(input.Output, input.ExpectedOutput) {
			value = 1
		}
		return []langfuse.Evaluation{{Name: "exact_match", NumericValue: &value, Comment: "go"}}, nil
	}
	verdict := func(_ context.Context, input langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
		value := "miss"
		if same(input.Output, input.ExpectedOutput) {
			value = "match"
		}
		return []langfuse.Evaluation{{Name: "verdict", StringValue: &value, DataType: langfuse.ScoreTypeCategorical}}, nil
	}
	overall := func(_ context.Context, input langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
		for _, evaluation := range input.Evaluations {
			if evaluation.Name == "exact_match" {
				return []langfuse.Evaluation{{Name: "overall", NumericValue: evaluation.NumericValue, Comment: "composite"}}, nil
			}
		}
		return nil, errors.New("no exact_match evaluation")
	}
	accuracy := func(_ context.Context, results []langfuse.ExperimentItemResult) ([]langfuse.Evaluation, error) {
		sum, count := 0.0, 0.0
		for _, result := range results {
			for _, evaluation := range result.Evaluations {
				if evaluation.Name == "exact_match" {
					sum += *evaluation.NumericValue
					count++
				}
			}
		}
		value := sum / count
		return []langfuse.Evaluation{{Name: "accuracy", NumericValue: &value, Comment: "mean"}}, nil
	}
	result, err := h.lf.RunExperiment(ctx, langfuse.ExperimentRun{
		Name: run.name, RunName: run.runName, Description: "interop go", Metadata: run.metadata(), Items: items,
		Task: func(_ context.Context, item langfuse.ExperimentItem) (any, error) {
			time.Sleep(taskDelay)
			country := text(field(decode(item.Input), "country"))
			if country == "Fail" {
				return nil, errors.New("synthetic task failure")
			}
			return experimentAnswers[country], nil
		},
		Evaluators:         []langfuse.Evaluator{exactMatch, verdict},
		CompositeEvaluator: overall,
		RunEvaluators:      []langfuse.RunEvaluator{accuracy},
	})
	if err != nil {
		t.Fatalf("Go RunExperiment(): %v", err)
	}
	if result.RunEvaluationErr != nil {
		t.Fatalf("Go run evaluation error: %v", result.RunEvaluationErr)
	}
	run.experimentID, run.datasetRunID = result.ExperimentID, result.DatasetRunID
	for _, item := range result.ItemResults {
		failed := item.Item.ID == run.runName+"-fail"
		if failed != (item.Err != nil) || item.EvaluationErr != nil {
			t.Fatalf("Go item %s: %v / %v", item.Item.ID, item.Err, item.EvaluationErr)
		}
		evaluations := map[string]any{}
		for _, evaluation := range item.Evaluations {
			if evaluation.NumericValue != nil {
				evaluations[evaluation.Name] = json.Number(fmt.Sprint(*evaluation.NumericValue))
			} else {
				evaluations[evaluation.Name] = *evaluation.StringValue
			}
		}
		output, _ := json.Marshal(item.Output)
		run.items[item.Item.ID] = runItem{
			traceID: item.TraceID, observationID: item.ObservationID, output: decodeJSON(t, output),
			evaluations: evaluations, failed: failed,
		}
	}
	for _, evaluation := range result.RunEvaluations {
		run.runEvaluations[evaluation.Name] = json.Number(fmt.Sprint(*evaluation.NumericValue))
	}
}

// storedExperiment is the readback of one run, normalized from the Go SDK
// reader or an official SDK's experiment API response.
type storedExperiment struct {
	found               bool
	id, name, datasetID string
	itemCount           int
	metadata            any
	runScores           []storedScore
	items               []storedItem
}

type storedItem struct {
	observationID, traceID, itemID, datasetID, environment, experimentID, experimentName, level string
	start, end, version                                                                         time.Time
	input, output, expectedOutput, experimentMetadata, itemMetadata                             any
	scores                                                                                      []storedScore
}

// storedScore keeps every score record; environment is empty from the Go
// reader, whose Score has no environment.
type storedScore struct {
	id, name, dataType, kind, subjectID, subjectTraceID, environment string
	value                                                            any
}

// uniqueScores fails on a missing ID, or a repeated score ID or name.
func uniqueScores(scores []storedScore) error {
	ids, names := map[string]bool{}, map[string]bool{}
	for _, score := range scores {
		if score.id == "" {
			return fmt.Errorf("score %s has no ID", score.name)
		}
		if ids[score.id] || names[score.name] {
			return fmt.Errorf("score %s (%s) stored twice", score.name, score.id)
		}
		ids[score.id], names[score.name] = true, true
	}
	return nil
}

// validHex reports whether id is length lowercase hex characters.
func validHex(id string, length int) bool {
	if len(id) != length {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil && strings.ToLower(id) == id
}

// scoreEnvironments reads the stored environment of every score of an
// experiment directly from the experiment API, because the Go reader's
// Score has no environment field.
func (h *interopHarness) scoreEnvironments(t *testing.T, experimentID string) map[string]string {
	t.Helper()
	environments := map[string]string{}
	from := url.QueryEscape(h.started.Add(-time.Minute).Format(time.RFC3339))
	var experiments struct {
		Data []struct {
			Scores []struct {
				ID          string `json:"id"`
				Environment string `json:"environment"`
			} `json:"scores"`
		} `json:"data"`
	}
	h.getJSON(t, "/api/public/experiments?fields=core,scores&fromStartTime="+from+"&id="+url.QueryEscape(experimentID), &experiments)
	var items struct {
		Data []struct {
			Scores []struct {
				ID          string `json:"id"`
				Environment string `json:"environment"`
			} `json:"scores"`
		} `json:"data"`
		Meta struct {
			Cursor string `json:"cursor"`
		} `json:"meta"`
	}
	h.getJSON(t, "/api/public/experiment-items?fields=core,scores&limit=100&fromStartTime="+from+
		"&experimentId="+url.QueryEscape(experimentID), &items)
	if items.Meta.Cursor != "" {
		t.Fatalf("experiment %s has more than 100 items", experimentID)
	}
	for _, experiment := range experiments.Data {
		for _, score := range experiment.Scores {
			environments[score.ID] = score.Environment
		}
	}
	for _, item := range items.Data {
		for _, score := range item.Scores {
			environments[score.ID] = score.Environment
		}
	}
	return environments
}

func (h *interopHarness) readExperiment(t *testing.T, reader string, run experimentRun, itemScores, runScores int) storedExperiment {
	t.Helper()
	from := h.started.Add(-time.Minute)
	if reader != "go" {
		response := h.sdk(t, reader, map[string]any{
			"op": "read_experiment", "experiment_id": run.experimentID, "from": from.Format(time.RFC3339),
			"items": len(run.expected), "item_scores": itemScores, "run_scores": runScores, "timeout": 180,
			"page_size": 2, "settle": 5,
		})
		if field(response, "settled") != true {
			t.Errorf("%s readback did not settle on exact counts: %v", reader, field(response, "counts"))
		}
		return normalizeSDKExperiment(t, response)
	}
	counts := func(stored storedExperiment) [4]int {
		scores, open := 0, 0
		for _, item := range stored.items {
			scores += len(item.scores)
			if item.end.IsZero() {
				open++
			}
		}
		return [4]int{len(stored.items), scores, len(stored.runScores), open}
	}
	want := [4]int{len(run.expected), itemScores, runScores, 0}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		stored := h.readWithGoSDK(t, run, from)
		if stored.found && counts(stored) == want {
			time.Sleep(5 * time.Second)
			stored = h.readWithGoSDK(t, run, from)
			if got := counts(stored); got != want {
				t.Errorf("Go readback changed after settling: %v, want %v", got, want)
			}
			return stored
		}
		if time.Now().After(deadline) {
			t.Errorf("Go readback did not settle: %v, want %v", counts(stored), want)
			return stored
		}
		time.Sleep(2 * time.Second)
	}
}

func (h *interopHarness) readWithGoSDK(t *testing.T, run experimentRun, from time.Time) storedExperiment {
	ctx := context.Background()
	var stored storedExperiment
	for experiment, err := range h.lf.Experiments(ctx, langfuse.ExperimentQuery{
		From: from, IDs: []string{run.experimentID}, PageSize: 1,
	}) {
		if err != nil {
			t.Fatalf("Go Experiments(): %v", err)
		}
		if stored.found {
			t.Errorf("Go Experiments() returned experiment %s twice", experiment.ID)
		}
		stored = storedExperiment{
			found: true, id: experiment.ID, name: experiment.Name, datasetID: experiment.DatasetID,
			itemCount: experiment.ItemCount, metadata: rawOf(t, experiment.Metadata),
		}
		for _, score := range experiment.Scores {
			stored.runScores = append(stored.runScores, goStoredScore(score))
		}
	}
	for item, err := range h.lf.ExperimentItems(ctx, langfuse.ExperimentItemQuery{
		From: from, ExperimentIDs: []string{run.experimentID}, PageSize: 2,
	}) {
		if err != nil {
			t.Fatalf("Go ExperimentItems(): %v", err)
		}
		converted := storedItem{
			observationID: item.ObservationID, traceID: item.TraceID, itemID: item.ItemID, datasetID: item.DatasetID,
			environment: item.Environment, experimentID: item.ExperimentID, experimentName: item.ExperimentName,
			level: string(item.Level), start: item.StartTime, end: item.EndTime, version: item.ItemVersion,
			input: rawOf(t, item.Input), output: rawOf(t, item.Output), expectedOutput: rawOf(t, item.ExpectedOutput),
			experimentMetadata: rawOf(t, item.ExperimentMetadata), itemMetadata: rawOf(t, item.ItemMetadata),
		}
		for _, score := range item.Scores {
			converted.scores = append(converted.scores, goStoredScore(score))
		}
		stored.items = append(stored.items, converted)
	}
	return stored
}

func goStoredScore(score langfuse.Score) storedScore {
	result := storedScore{id: score.ID, name: score.Name, dataType: string(score.DataType)}
	switch {
	case score.ObservationID != "":
		result.kind, result.subjectID, result.subjectTraceID = "observation", score.ObservationID, score.TraceID
	case score.TraceID != "":
		result.kind, result.subjectID = "trace", score.TraceID
	case score.SessionID != "":
		result.kind, result.subjectID = "session", score.SessionID
	case score.DatasetRunID != "":
		result.kind, result.subjectID = "experiment", score.DatasetRunID
	}
	if score.NumericValue != nil {
		result.value = json.Number(fmt.Sprint(*score.NumericValue))
	} else if score.StringValue != nil {
		result.value = *score.StringValue
	}
	return result
}

func sdkStoredScore(score any) storedScore {
	value := field(score, "value")
	switch value {
	case true:
		value = json.Number("1")
	case false:
		value = json.Number("0")
	}
	return storedScore{
		id: text(field(score, "id")), name: text(field(score, "name")), dataType: text(field(score, "dataType")),
		kind: text(field(score, "subject", "kind")), subjectID: text(field(score, "subject", "id")),
		subjectTraceID: text(field(score, "subject", "traceId")), environment: text(field(score, "environment")),
		value: value,
	}
}

func normalizeSDKExperiment(t *testing.T, response any) storedExperiment {
	var stored storedExperiment
	experiments := asSlice(field(response, "experiments"))
	if len(experiments) == 1 {
		experiment := experiments[0]
		count, _ := field(experiment, "itemCount").(json.Number).Int64()
		stored = storedExperiment{
			found: true, id: text(field(experiment, "id")), name: text(field(experiment, "name")),
			datasetID: text(field(experiment, "datasetId")), itemCount: int(count), metadata: field(experiment, "metadata"),
		}
		for _, score := range asSlice(field(experiment, "scores")) {
			stored.runScores = append(stored.runScores, sdkStoredScore(score))
		}
	} else if len(experiments) > 1 {
		t.Errorf("experiment read returned %d experiments for one ID", len(experiments))
	}
	for _, item := range asSlice(field(response, "items")) {
		converted := storedItem{
			observationID: text(field(item, "id")), traceID: text(field(item, "traceId")),
			itemID: text(field(item, "experimentItemId")), datasetID: text(field(item, "experimentDatasetId")),
			environment: text(field(item, "environment")), experimentID: text(field(item, "experimentId")),
			experimentName: text(field(item, "experimentName")), level: text(field(item, "level")),
			start: parseTime(text(field(item, "startTime"))), end: parseTime(text(field(item, "endTime"))),
			version: parseTime(text(field(item, "experimentItemVersion"))),
			input:   field(item, "input"), output: field(item, "output"), expectedOutput: field(item, "expectedOutput"),
			experimentMetadata: field(item, "experimentMetadata"), itemMetadata: field(item, "experimentItemMetadata"),
		}
		for _, score := range asSlice(field(item, "scores")) {
			converted.scores = append(converted.scores, sdkStoredScore(score))
		}
		stored.items = append(stored.items, converted)
	}
	return stored
}

func asSlice(value any) []any {
	slice, _ := value.([]any)
	return slice
}

func parseTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func (h *interopHarness) verifyExperiment(t *testing.T, reader string, run experimentRun, evidence *[]string) {
	t.Helper()
	itemScores := 0
	for _, fixture := range run.expected {
		if fixture.key != failingFixture.key {
			itemScores += len(run.itemScores())
		}
	}
	runScores := 0
	if !run.local {
		runScores = 1
	}
	stored := h.readExperiment(t, reader, run, itemScores, runScores)
	*evidence = append(*evidence, fmt.Sprintf("experiment %s (%s): %d/%d items, %d item scores, %d run scores read by %s in pages of 2",
		run.experimentID, run.runName, len(stored.items), len(run.expected), itemScores, len(stored.runScores), reader))
	if !stored.found {
		t.Fatalf("%s did not find experiment %s", reader, run.experimentID)
	}
	if stored.id != run.experimentID || stored.name != run.runName || stored.itemCount != len(run.expected) ||
		stored.datasetID != run.datasetID {
		t.Errorf("experiment = %s %q (%d items, dataset %q), want %s %q (%d, dataset %q)", stored.id, stored.name,
			stored.itemCount, stored.datasetID, run.experimentID, run.runName, len(run.expected), run.datasetID)
	}
	if run.local != (run.datasetRunID == "") || (!run.local && run.datasetRunID != run.experimentID) {
		t.Errorf("dataset run ID %q for experiment %q (local %v)", run.datasetRunID, run.experimentID, run.local)
	}
	if diff := jsonDiff("experiment metadata", stored.metadata, flattenedByServer(run.metadata())); diff != "" {
		t.Errorf("%s", diff)
	}
	if err := uniqueScores(stored.runScores); err != nil {
		t.Errorf("run scores: %v", err)
	}
	// Score IDs are unique across the run, and environments are exact: Go
	// scores items in sdk-experiment (D4); every runner scores the run, and
	// the official SDKs score items, in the configured client environment,
	// which every client inherits.
	environments := map[string]string{}
	if reader == "go" {
		environments = h.scoreEnvironments(t, run.experimentID)
	}
	allIDs := map[string]bool{}
	for _, scores := range append([][]storedScore{stored.runScores}, func() [][]storedScore {
		var lists [][]storedScore
		for _, item := range stored.items {
			lists = append(lists, item.scores)
		}
		return lists
	}()...) {
		for _, score := range scores {
			if score.id == "" || allIDs[score.id] {
				t.Errorf("score %q has an empty or repeated ID %q", score.name, score.id)
			}
			allIDs[score.id] = true
			if score.environment == "" {
				score.environment = environments[score.id]
			}
			want := h.environment
			if score.kind == "observation" && run.runner == "go" {
				want = "sdk-experiment"
			}
			if score.environment != want {
				t.Errorf("score %s (%s) environment %q, want %q", score.name, score.id, score.environment, want)
			}
		}
	}
	if run.local {
		if len(stored.runScores) != 0 {
			t.Errorf("local run has run scores %+v; the official runners keep them only in the result", stored.runScores)
		}
	} else {
		if len(stored.runScores) != 1 {
			t.Fatalf("run scores = %+v, want exactly one", stored.runScores)
		}
		score := stored.runScores[0]
		if score.name != "accuracy" || score.dataType != "NUMERIC" || score.kind != "experiment" ||
			score.subjectID != run.experimentID || jsonDiff("", score.value, json.Number("0.75")) != "" ||
			reader != "go" && score.environment != h.environment && run.runner == "go" {
			t.Errorf("run score = %+v, want accuracy 0.75 on experiment %s", score, run.experimentID)
		}
	}
	if diff := jsonDiff("runner run evaluation", run.runEvaluations["accuracy"], json.Number("0.75")); diff != "" {
		t.Errorf("%s", diff)
	}
	reported := 0
	for _, fixture := range run.expected {
		if fixture.key != failingFixture.key || run.runner == "go" {
			reported++
		}
	}
	if len(run.items) != reported {
		t.Errorf("%s reported %d items, want %d", run.runner, len(run.items), reported)
	}

	seen := map[string]bool{}
	for _, item := range stored.items {
		label := reader + " item " + item.itemID
		fixture, ok := run.expected[item.itemID]
		if !ok || seen[item.itemID] {
			t.Errorf("%s: unexpected or duplicate item", label)
			continue
		}
		seen[item.itemID] = true
		if item.experimentID != run.experimentID || item.experimentName != run.runName || item.environment != "sdk-experiment" {
			t.Errorf("%s: identity %s %q %q", label, item.experimentID, item.experimentName, item.environment)
		}
		if run.local {
			if item.datasetID != "" {
				t.Errorf("%s: local item has dataset %q", label, item.datasetID)
			}
		} else {
			if item.datasetID != run.datasetID {
				t.Errorf("%s: dataset %q, want %q", label, item.datasetID, run.datasetID)
			}
			// Only the Go runner exports the pinned version as an attribute.
			if run.runner == "go" && !item.version.Equal(run.pin) || run.runner != "go" && !item.version.IsZero() {
				t.Errorf("%s: item version %v (runner %s, pin %v)", label, item.version, run.runner, run.pin)
			}
		}
		wantInput := rawValue(t, fixture.input)
		if fixture.key == failingFixture.key && run.runner == "typescript" {
			// TypeScript sets the root's input with its output after the task
			// (ExperimentManager.ts:453-458), so a failed item has none.
			wantInput = ""
		}
		for name, pair := range map[string][2]any{
			"input":           {exportedContent(item.input), wantInput},
			"expected output": {exportedContent(item.expectedOutput), rawValue(t, fixture.expectedOutput)},
			"item metadata":   {item.itemMetadata, flattenedByServer(rawValue(t, fixture.metadata))},
			"run metadata":    {item.experimentMetadata, flattenedByServer(run.metadata())},
		} {
			if diff := jsonDiff(name, pair[0], pair[1]); diff != "" {
				t.Errorf("%s: %s", label, diff)
			}
		}
		if !validHex(item.traceID, 32) || !validHex(item.observationID, 16) {
			t.Errorf("%s: root %q/%q is not a valid trace and span ID", label, item.traceID, item.observationID)
		}
		// Go reports both root IDs, failed items included; the official
		// runners report the trace ID of successful items only.
		want, reported := run.items[item.itemID]
		if reported && (item.traceID != want.traceID || want.observationID != "" && item.observationID != want.observationID) {
			t.Errorf("%s: root %s/%s, want %s/%s", label, item.traceID, item.observationID, want.traceID, want.observationID)
		}
		if run.runner == "go" && (!reported || want.observationID == "") {
			t.Errorf("%s: the Go runner did not report the root", label)
		}
		if fixture.key == failingFixture.key {
			if item.level != "ERROR" || len(item.scores) != 0 || run.runner == "go" && !want.failed {
				t.Errorf("%s: failed item level %q with %d scores, want ERROR and none", label, item.level, len(item.scores))
			}
			continue
		}
		if !reported {
			t.Errorf("%s: the runner did not report this successful item", label)
		}
		country := text(field(rawValue(t, fixture.input), "country"))
		answer := decodeJSONValue(t, experimentAnswers[country])
		if diff := jsonDiff("output", exportedContent(item.output), answer); diff != "" || item.level != "DEFAULT" {
			t.Errorf("%s: level %s; %s", label, item.level, diff)
		}
		if latency := item.end.Sub(item.start); latency < taskDelay || latency >= evaluatorDelay {
			t.Errorf("%s: latency %v, want the task's %v without the evaluator's %v", label, latency, taskDelay, evaluatorDelay)
		}
		matched := jsonDiff("", answer, rawValue(t, fixture.expectedOutput)) == ""
		wantValues := map[string]any{"exact_match": json.Number("0"), "verdict": "miss", "overall": json.Number("0")}
		if matched {
			wantValues = map[string]any{"exact_match": json.Number("1"), "verdict": "match", "overall": json.Number("1")}
		}
		if err := uniqueScores(item.scores); err != nil {
			t.Errorf("%s: %v", label, err)
		}
		gotScores := map[string]any{}
		for _, score := range item.scores {
			gotScores[score.name] = score.value
			if score.dataType != run.itemScores()[score.name] || score.kind != "observation" ||
				score.subjectID != item.observationID || (score.subjectTraceID != "" && score.subjectTraceID != item.traceID) {
				t.Errorf("%s: score %+v, want type %s on the item root %s", label, score, run.itemScores()[score.name], item.observationID)
			}
		}
		wantScores := map[string]any{}
		for name := range run.itemScores() {
			wantScores[name] = wantValues[name]
		}
		if diff := jsonDiff("scores", gotScores, wantScores); diff != "" {
			t.Errorf("%s: %s", label, diff)
		}
		if diff := jsonDiff("runner evaluations", want.evaluations, wantScores); diff != "" {
			t.Errorf("%s: %s", label, diff)
		}
		if diff := jsonDiff("runner output", want.output, answer); diff != "" {
			t.Errorf("%s: %s", label, diff)
		}
	}
	if len(seen) != len(run.expected) {
		t.Errorf("%s read %d of %d items", reader, len(seen), len(run.expected))
	}
	if reader == "python" && run.runner == "go" {
		h.verifyEvaluatorObservations(t, run, stored, evidence)
	}
	checks := fmt.Sprintf("absolute dataset and item IDs, input/output/expected output, item and run metadata, "+
		"%d typed scores per item on the root, exact run scores, latency excludes evaluators", len(run.itemScores()))
	if run.local {
		checks += ", failed local item stored as ERROR without scores"
	}
	*evidence = append(*evidence, checks)
}

// verifyEvaluatorObservations reads a Go run's item traces with Python and
// checks that each successful item has one evaluator observation per
// evaluator under its root.
func (h *interopHarness) verifyEvaluatorObservations(t *testing.T, run experimentRun, stored storedExperiment, evidence *[]string) {
	checked := 0
	for _, item := range stored.items {
		if run.expected[item.itemID].key == failingFixture.key {
			continue
		}
		response := h.python(t, map[string]any{
			"op": "trace_observations", "trace_id": item.traceID, "from": h.started.Add(-time.Minute).Format(time.RFC3339),
		})
		evaluators := 0
		for _, observation := range asSlice(field(response, "observations")) {
			if text(field(observation, "type")) == "EVALUATOR" {
				evaluators++
				if parent := text(field(observation, "parentObservationId")); parent != item.observationID {
					t.Errorf("item %s: evaluator %q parent %q, want the root %s", item.itemID,
						text(field(observation, "name")), parent, item.observationID)
				}
			}
		}
		if evaluators != len(run.itemScores()) {
			t.Errorf("item %s: %d evaluator observations, want %d (observations %s)", item.itemID, evaluators,
				len(run.itemScores()), describe(field(response, "observations")))
		}
		checked++
	}
	*evidence = append(*evidence, fmt.Sprintf("python read %d evaluator observations under each of %d item roots",
		len(run.itemScores()), checked))
}

func decodeJSONValue(t *testing.T, value any) any {
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return decodeJSON(t, data)
}

// verifyMetadataShapes runs a Go experiment over every active item of a
// dataset whose item metadata takes every JSON shape, then reads it back with
// Python: object metadata is exported as experiment item metadata, and other
// shapes reach the evaluator without being exported, as the Python runner
// does.
func (h *interopHarness) verifyMetadataShapes(t *testing.T, dataset string, evidence *[]string) {
	ctx := context.Background()
	var items []langfuse.ExperimentItem
	for item, err := range h.lf.DatasetItems(ctx, langfuse.DatasetItemQuery{DatasetName: dataset}) {
		if err != nil {
			t.Fatalf("Go DatasetItems(): %v", err)
		}
		items = append(items, item.ExperimentItem())
	}
	shape := func(value any) string {
		raw, _ := value.(json.RawMessage)
		switch decoded := decodeJSON(t, raw).(type) {
		case nil:
			return "none"
		case map[string]any:
			return "object"
		case []any:
			return "array"
		default:
			return fmt.Sprintf("%T", decoded)
		}
	}
	result, err := h.lf.RunExperiment(ctx, langfuse.ExperimentRun{
		Name: "interop-go-metadata", RunName: dataset + "-run", Items: items,
		Task: func(context.Context, langfuse.ExperimentItem) (any, error) { return "ok", nil },
		Evaluators: []langfuse.Evaluator{func(_ context.Context, input langfuse.EvaluatorInput) ([]langfuse.Evaluation, error) {
			kind := shape(input.Metadata)
			return []langfuse.Evaluation{{Name: "metadata_kind", StringValue: &kind, DataType: langfuse.ScoreTypeCategorical}}, nil
		}},
	})
	if err != nil {
		t.Fatalf("Go RunExperiment(): %v", err)
	}
	// The server stores the "empty" item's "" input as null, and an item
	// without input fails, as in the Python runner.
	started := 0
	for _, item := range result.ItemResults {
		missingInput := item.Item.ID == h.itemID(dataset, "empty")
		if missingInput != errors.Is(item.Err, langfuse.ErrInvalidExperiment) || item.EvaluationErr != nil ||
			(item.Err != nil && !missingInput) {
			t.Errorf("item %s: %v / %v", item.Item.ID, item.Err, item.EvaluationErr)
		}
		if item.Err == nil {
			started++
		}
	}
	response := h.python(t, map[string]any{
		"op": "read_experiment", "experiment_id": result.ExperimentID, "from": h.started.Add(-time.Minute).Format(time.RFC3339),
		"items": started, "item_scores": started, "run_scores": 0, "timeout": 180, "page_size": 2, "settle": 5,
	})
	if field(response, "settled") != true {
		t.Errorf("python readback did not settle")
	}
	fixtures := map[string]fixtureItem{}
	for _, fixture := range datasetFixtures {
		fixtures[h.itemID(dataset, fixture.key)] = fixture
	}
	shapes := map[string]int{}
	seen := map[string]bool{}
	for _, item := range asSlice(field(response, "items")) {
		id := text(field(item, "experimentItemId"))
		fixture, ok := fixtures[id]
		if !ok || fixture.status == "ARCHIVED" || seen[id] {
			t.Errorf("unexpected or duplicate item %s", id)
			continue
		}
		seen[id] = true
		metadata := storedByServer(rawValue(t, fixture.metadata))
		if diff := jsonDiff("item metadata", field(item, "experimentItemMetadata"), flattenedByServer(metadata)); diff != "" {
			t.Errorf("%s: %s", id, diff)
		}
		kind := "none"
		switch metadata.(type) {
		case map[string]any:
			kind = "object"
		case []any:
			kind = "array"
		case string:
			kind = "string"
		}
		scores := asSlice(field(item, "scores"))
		if len(scores) != 1 || field(scores[0], "value") != kind {
			t.Errorf("%s: scores %s, want metadata_kind %s", id, describe(scores), kind)
		}
		shapes[kind]++
	}
	if len(asSlice(field(response, "items"))) != started || started != len(items)-1 {
		t.Errorf("python read %d items, want %d of %d", len(asSlice(field(response, "items"))), started, len(items))
	}
	*evidence = append(*evidence, fmt.Sprintf("experiment %s: %d of %d items ran (the null-input item fails as in Python); metadata shapes %v; "+
		"only object metadata exported, as the server's flattened string map", result.ExperimentID, started, len(items), shapes))
}

// TestDatasetInteropComparator checks that the comparison helpers reject
// deliberate corruptions; it needs no server.
func TestDatasetInteropComparator(t *testing.T) {
	value := func(raw string) any { return rawValueNoT(raw) }
	for name, pair := range map[string][2]string{
		"missing nested null": {`{}`, `{"nested":null}`},
		"extra key":           {`{"a":1,"b":2}`, `{"a":1}`},
		"string vs number":    {`{"a":"1"}`, `{"a":1}`},
		"string vs bool":      {`"true"`, `true`},
		"array order":         {`[1,2]`, `[2,1]`},
		"array length":        {`[1]`, `[1,null]`},
		"different number":    {`0.1`, `0.10000001`},
		"unicode":             {`"e\u0301"`, `"\u00e9"`},
	} {
		if diff := jsonDiff("", value(pair[0]), value(pair[1])); diff == "" {
			t.Errorf("%s: jsonDiff accepted %s as %s", name, pair[0], pair[1])
		}
	}
	for name, pair := range map[string][2]string{
		"key order":       {`{"a":1,"b":[true,null]}`, `{"b":[true,null],"a":1}`},
		"number spelling": {`1.5e-7`, `0.00000015`},
	} {
		if diff := jsonDiff("", value(pair[0]), value(pair[1])); diff != "" {
			t.Errorf("%s: %s", name, diff)
		}
	}
	scores := []storedScore{{id: "s1", name: "accuracy"}, {id: "s2", name: "accuracy"}}
	if err := uniqueScores(scores); err == nil {
		t.Error("uniqueScores accepted two scores with one name")
	}
	if err := uniqueScores([]storedScore{{id: "s1", name: "a"}, {id: "s1", name: "b"}}); err == nil {
		t.Error("uniqueScores accepted a repeated score ID")
	}
	if err := uniqueScores([]storedScore{{name: "a"}}); err == nil {
		t.Error("uniqueScores accepted a score without an ID")
	}
	for id, length := range map[string]int{"": 16, "0123456789ABCDEF": 16, "0123456789abcde": 16, "0123456789abcdeg": 16} {
		if validHex(id, length) {
			t.Errorf("validHex accepted %q", id)
		}
	}
	if !validHex("0123456789abcdef", 16) {
		t.Error("validHex rejected a span ID")
	}
	owned := map[string]string{"new": "3", "list-1": "2", "list-0": "1"}
	pages := map[string][]map[string]string{
		// Newest first in pages of 2; a fresh project holds only the owned
		// datasets.
		"fresh project":  {{"new": "3", "list-1": "2"}, {"list-0": "1"}},
		"busy project":   {{"other": "9", "new": "3"}, {"list-1": "2", "list-0": "1"}, {"older": "0"}},
		"repeated entry": {{"new": "3", "list-1": "2"}, {"list-1": "2", "list-0": "1"}},
	}
	for name, listing := range pages {
		listed := map[string]string{}
		for _, page := range listing {
			maps.Copy(listed, page)
		}
		if missing := missingOwned(listed, owned); len(missing) != 0 {
			t.Errorf("%s: missingOwned = %v, want none", name, missing)
		}
	}
	for name, listed := range map[string]map[string]string{
		"first page replayed": {"new": "3", "list-1": "2"},
		"wrong ID":            {"new": "3", "list-1": "2", "list-0": "7"},
	} {
		if missing := missingOwned(listed, owned); len(missing) == 0 {
			t.Errorf("%s: missingOwned accepted the listing", name)
		}
	}
}
