//go:build interop

// The baggage interop corpus: credential-free, deterministic evidence
// that Go's baggage protocol v1 round-trips with the uv-locked Python
// SDK (langfuse + opentelemetry pins recorded in the golden). Four
// projections are sealed per bidirectional fixture: the raw members
// each producer injects, Go's accepted state after standard extraction
// plus explicit import, and Python's propagated span attributes after
// standard extraction. Inbound-only fixtures additionally seal Go's
// re-injection after import, proving namespace consumption. Run via
// `task interop`; reseal with ACCEPT=accept after reviewing a diff.
package validation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	langfuse "github.com/fgn/go-langfuse"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const interopGoldenPath = "testdata/interop/corpus.golden.json"

// corpusDocument is the sealed golden. Every value is deterministic:
// attribute fixtures inject without a root span (no random IDs), and
// the smokes assert live identities instead of sealing them.
type corpusDocument struct {
	Pins       map[string]string         `json:"pins"`
	Fixtures   map[string]*fixtureResult `json:"fixtures"`
	ValueSweep map[string]*sweepResult   `json:"value_sweep"`
	KeySweep   map[string]*sweepResult   `json:"key_sweep"`
}

type fixtureResult struct {
	ByteIdentical bool `json:"byte_identical,omitempty"`

	// Bidirectional attribute fixtures.
	PythonRaw              []string       `json:"python_raw,omitempty"`
	GoRaw                  []string       `json:"go_raw,omitempty"`
	GoAcceptedFromPython   map[string]any `json:"go_accepted_from_python,omitempty"`
	PythonAttributesFromGo map[string]any `json:"python_attributes_from_go,omitempty"`

	// Inbound-only raw-header fixtures, on unmarked and marked branches.
	GoAcceptedFromHeader       map[string]any `json:"go_accepted_from_header,omitempty"`
	GoReinjectedAfterImport    []string       `json:"go_reinjected_after_import,omitempty"`
	GoReinjectedMarked         []string       `json:"go_reinjected_marked,omitempty"`
	PythonAttributesFromHeader map[string]any `json:"python_attributes_from_header,omitempty"`
}

type sweepResult struct {
	GoExported      bool   `json:"go_exported"`
	GoRawMember     string `json:"go_raw_member,omitempty"`
	PythonReceived  string `json:"python_received,omitempty"`
	PythonRawMember string `json:"python_raw_member,omitempty"`
	GoAccepted      bool   `json:"go_accepted"`
	GoReceived      string `json:"go_received,omitempty"`
}

// interopAttributes is the shared fixture input shape (also the oracle
// inject payload).
type interopAttributes struct {
	UserID      string            `json:"user_id,omitempty"`
	SessionID   string            `json:"session_id,omitempty"`
	TraceName   string            `json:"trace_name,omitempty"`
	Version     string            `json:"version,omitempty"`
	Environment string            `json:"environment,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
}

type attributeFixture struct {
	name          string
	byteIdentical bool
	attributes    interopAttributes
}

type headerFixture struct {
	name    string
	headers []string
}

var attributeFixtures = []attributeFixture{
	{
		name:          "scalars-basic",
		byteIdentical: true,
		attributes: interopAttributes{
			UserID: "alice", SessionID: "s-1", TraceName: "checkout",
			Version: "v1", Environment: "staging",
			Metadata: map[string]string{"tenant": "acme"},
		},
	},
	{
		// Semantically stable but raw-divergent characters: Python
		// percent-encodes them, Go emits them raw.
		name: "value-specials",
		attributes: interopAttributes{
			UserID:    "alice@example.com:8080/x",
			SessionID: "s[1]&(2)*'3'",
			TraceName: "a=b%25c",
			Metadata:  map[string]string{"quoted": `q"w,x;y\z`},
		},
	},
	{
		name:          "tags-are-excluded-from-go",
		byteIdentical: false,
		attributes: interopAttributes{
			UserID: "alice",
			Tags:   []string{"t1", "t2"},
		},
	},
	{
		name:       "limit-200-chars",
		attributes: interopAttributes{UserID: strings.Repeat("u", 200)},
	},
	{
		name:       "limit-201-chars-dropped",
		attributes: interopAttributes{UserID: strings.Repeat("u", 201)},
	},
	{
		name:       "multibyte-rejected",
		attributes: interopAttributes{UserID: "héllo-wörld"},
	},
	{
		name:       "space-and-plus-out-of-domain",
		attributes: interopAttributes{UserID: "alice smith", SessionID: "s+1"},
	},
	{
		name: "metadata-33-keys-capped",
		attributes: interopAttributes{
			Metadata: numberedMetadata(33),
		},
	},
}

var headerFixtures = []headerFixture{
	{
		name:    "unknown-member-terminates-at-go",
		headers: []string{"langfuse_user_id=alice,langfuse_region=eu,foo=bar"},
	},
	{
		name: "excluded-rows-ignored-by-go",
		headers: []string{
			"langfuse_tags=%5B%27a%27%5D,langfuse_prompt_name=greet,langfuse_prompt_version=3,langfuse_user_id=alice",
		},
	},
	{
		name:    "encoded-plus-value-rejected-by-go",
		headers: []string{"langfuse_user_id=a%2Bb,langfuse_session_id=ok"},
	},
	{
		name:    "python-space-becomes-plus-rejected-by-go",
		headers: []string{"langfuse_trace_name=checkout+flow,langfuse_user_id=alice"},
	},
	{
		name:    "encoded-metadata-key-rejected-by-go",
		headers: []string{"langfuse_metadata_a%2Bb=v,langfuse_metadata_ok=v"},
	},
	{
		name:    "duplicate-member-keys",
		headers: []string{"langfuse_user_id=first,langfuse_user_id=second"},
	},
	{
		name: "multiple-header-lines",
		headers: []string{
			"langfuse_user_id=alice",
			"langfuse_session_id=s-2",
		},
	},
	{
		name:    "foreign-member-with-property",
		headers: []string{"foo=bar;prop=1,langfuse_user_id=alice"},
	},
	{
		name:    "empty-value-ignored",
		headers: []string{"langfuse_user_id=,langfuse_session_id=s-3"},
	},
	{
		name:    "python-70-metadata-keys-vs-go-64-member-parse-limit",
		headers: []string{seventyMetadataHeader()},
	},
}

func numberedMetadata(count int) map[string]string {
	result := make(map[string]string, count)
	for index := range count {
		result[fmt.Sprintf("k%02d", index)] = fmt.Sprintf("v%02d", index)
	}
	return result
}

func seventyMetadataHeader() string {
	parts := make([]string, 0, 70)
	for index := range 70 {
		parts = append(parts, fmt.Sprintf("langfuse_metadata_k%02d=v%02d", index, index))
	}
	return strings.Join(parts, ",")
}

// --- oracle plumbing ---

type oracleCase struct {
	ID          string             `json:"id"`
	Op          string             `json:"op"`
	Attributes  *interopAttributes `json:"attributes,omitempty"`
	WithRoot    *bool              `json:"with_root,omitempty"`
	Headers     []string           `json:"headers,omitempty"`
	Traceparent string             `json:"traceparent,omitempty"`
}

type oracleResult struct {
	Baggage      string         `json:"baggage"`
	Traceparent  string         `json:"traceparent"`
	TraceID      string         `json:"trace_id"`
	SpanID       string         `json:"span_id"`
	Attributes   map[string]any `json:"attributes"`
	ParentSpanID string         `json:"parent_span_id"`
}

func runOracle(t *testing.T, cases []oracleCase) map[string]oracleResult {
	t.Helper()
	input, err := json.Marshal(map[string]any{"cases": cases})
	if err != nil {
		t.Fatalf("marshal oracle input: %v", err)
	}
	command := exec.Command("uv", "run", "--locked", "python", "interop_oracle.py")
	command.Dir = "parity"
	command.Stdin = bytes.NewReader(input)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("python oracle failed: %v\nstderr:\n%s", err, stderr.String())
	}
	var response struct {
		Results map[string]oracleResult `json:"results"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		t.Fatalf("decode oracle output: %v\n%s", err, stdout.String())
	}
	return response.Results
}

func oraclePins(t *testing.T) map[string]string {
	t.Helper()
	lock, err := os.ReadFile("parity/uv.lock")
	if err != nil {
		t.Fatalf("read uv.lock: %v", err)
	}
	pins := make(map[string]string)
	// opentelemetry-api carries the baggage codec under audit;
	// opentelemetry-sdk drives span processing. Both are pinned.
	for _, name := range []string{"langfuse", "opentelemetry-api", "opentelemetry-sdk"} {
		pattern := regexp.MustCompile(`name = "` + name + `"\nversion = "([^"]+)"`)
		match := pattern.FindSubmatch(lock)
		if match == nil {
			t.Fatalf("pin for %s not found in uv.lock", name)
		}
		pins[name] = string(match[1])
	}
	return pins
}

// --- Go-side harness ---

// interopGoClient is a borrowed-provider client whose exported spans
// land in an in-memory exporter, mirroring the oracle's setup.
type interopGoClient struct {
	client   *langfuse.Client
	exporter *tracetest.InMemoryExporter
}

func newInteropGoClient(t *testing.T, publicKey string) *interopGoClient {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
	)
	client, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL:        "http://127.0.0.1:9",
		PublicKey:      publicKey,
		SecretKey:      "sk-interop",
		Environment:    "go-default",
		TracerProvider: provider,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(ctx) // the unroutable exporter fails fast
		_ = provider.Shutdown(ctx)
	})
	return &interopGoClient{client: client, exporter: exporter}
}

func goInjectHeader(client *langfuse.Client, attrs interopAttributes) string {
	ctx := client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
		Name:        attrs.TraceName,
		UserID:      attrs.UserID,
		SessionID:   attrs.SessionID,
		Version:     attrs.Version,
		Environment: attrs.Environment,
		Metadata:    anyMetadata(attrs.Metadata),
		Tags:        attrs.Tags,
	})
	ctx = client.WithBaggagePropagation(ctx)
	carrier := propagation.MapCarrier{}
	propagation.Baggage{}.Inject(ctx, carrier)
	return carrier.Get("baggage")
}

func anyMetadata(values map[string]string) map[string]any {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]any, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

// goAcceptedAttributes imports the given headers and returns the
// receiver span's langfuse-owned attributes, plus the raw members a
// standard inject would forward after import (namespace consumption).
func (h *interopGoClient) goAcceptedAttributes(t *testing.T, name string, headers []string) (map[string]any, []string) {
	return h.goAcceptedAttributesOn(t, context.Background(), name, headers)
}

// goReinjectedMarked runs extract-import-inject on an already marked
// branch and returns the rebuilt members: accepted values reappear,
// while invalid, excluded, and unknown members stay terminated.
func (h *interopGoClient) goReinjectedMarked(t *testing.T, name string, headers []string) []string {
	t.Helper()
	marked := h.client.WithBaggagePropagation(context.Background())
	_, reinjected := h.goAcceptedAttributesOn(t, marked, name, headers)
	return reinjected
}

func (h *interopGoClient) goAcceptedAttributesOn(t *testing.T, base context.Context, name string, headers []string) (map[string]any, []string) {
	t.Helper()
	httpHeader := http.Header{}
	for _, value := range headers {
		httpHeader.Add("Baggage", value)
	}
	ctx := propagation.Baggage{}.Extract(base, propagation.HeaderCarrier(httpHeader))
	ctx = h.client.WithTraceAttributesFromBaggage(ctx)

	carrier := propagation.MapCarrier{}
	propagation.Baggage{}.Inject(ctx, carrier)
	reinjected := splitMembers(carrier.Get("baggage"))

	h.exporter.Reset()
	_, observation := h.client.StartObservation(ctx, name, langfuse.TypeSpan, langfuse.ObservationAttributes{})
	observation.End()
	spans := h.exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("%s: expected one receiver span, got %d", name, len(spans))
	}
	return langfuseOwnedAttributes(spans[0]), reinjected
}

func langfuseOwnedAttributes(span tracetest.SpanStub) map[string]any {
	result := make(map[string]any)
	for _, item := range span.Attributes {
		key := string(item.Key)
		if strings.HasPrefix(key, "langfuse.") || key == "user.id" || key == "session.id" {
			result[key] = item.Value.AsInterface()
		}
	}
	return result
}

func splitMembers(header string) []string {
	if header == "" {
		return nil
	}
	members := strings.Split(header, ",")
	for index, member := range members {
		members[index] = strings.TrimSpace(member)
	}
	sort.Strings(members)
	return members
}

func memberValue(members []string, key string) (string, bool) {
	for _, member := range members {
		if value, found := strings.CutPrefix(member, key+"="); found {
			return value, true
		}
	}
	return "", false
}

// --- the corpus test ---

func TestInteropCorpus(t *testing.T) {
	document := &corpusDocument{
		Pins:       oraclePins(t),
		Fixtures:   make(map[string]*fixtureResult),
		ValueSweep: make(map[string]*sweepResult),
		KeySweep:   make(map[string]*sweepResult),
	}

	// Phase 1: Go exports and the first oracle batch (Python injections
	// plus Python readings of raw-header fixtures).
	withoutRoot := false
	cases := make([]oracleCase, 0, 512)
	goHeaders := make(map[string]string)

	exporterClient := newInteropGoClient(t, "pk-interop-export")
	for _, fixture := range attributeFixtures {
		header := goInjectHeader(exporterClient.client, fixture.attributes)
		goHeaders["fixture:"+fixture.name] = header
		attrs := fixture.attributes
		cases = append(cases,
			oracleCase{ID: "pyraw:" + fixture.name, Op: "inject", Attributes: &attrs, WithRoot: &withoutRoot},
			oracleCase{ID: "pyattr:" + fixture.name, Op: "extract", Headers: []string{header}},
		)
	}
	for _, fixture := range headerFixtures {
		cases = append(cases,
			oracleCase{ID: "pyattr-header:" + fixture.name, Op: "extract", Headers: fixture.headers})
	}

	valueSweepInputs := make(map[string]string)
	for b := 0x21; b <= 0x7E; b++ {
		id := fmt.Sprintf("0x%02x", b)
		value := "a" + string(rune(b)) + "b"
		valueSweepInputs[id] = value
		header := goInjectHeader(exporterClient.client, interopAttributes{UserID: value})
		goHeaders["value:"+id] = header
		attrs := interopAttributes{UserID: value}
		cases = append(cases,
			oracleCase{ID: "value-pyraw:" + id, Op: "inject", Attributes: &attrs, WithRoot: &withoutRoot})
		if header != "" {
			cases = append(cases,
				oracleCase{ID: "value-pyattr:" + id, Op: "extract", Headers: []string{header}})
		}
	}
	keySweepInputs := make(map[string]string)
	for b := 0x21; b <= 0x7E; b++ {
		id := fmt.Sprintf("0x%02x", b)
		suffix := "a" + string(rune(b)) + "b"
		keySweepInputs[id] = suffix
		header := goInjectHeader(exporterClient.client,
			interopAttributes{Metadata: map[string]string{suffix: "v"}})
		goHeaders["key:"+id] = header
		attrs := interopAttributes{Metadata: map[string]string{suffix: "v"}}
		cases = append(cases,
			oracleCase{ID: "key-pyraw:" + id, Op: "inject", Attributes: &attrs, WithRoot: &withoutRoot})
		if header != "" {
			cases = append(cases,
				oracleCase{ID: "key-pyattr:" + id, Op: "extract", Headers: []string{header}})
		}
	}

	results := runOracle(t, cases)

	// Phase 2: Go readings of the Python-injected headers.
	importClient := newInteropGoClient(t, "pk-interop-import")
	for _, fixture := range attributeFixtures {
		pythonHeader := results["pyraw:"+fixture.name].Baggage
		accepted, _ := importClient.goAcceptedAttributes(t, "fx-"+fixture.name, []string{pythonHeader})
		goRaw := splitMembers(goHeaders["fixture:"+fixture.name])
		pythonRaw := splitMembers(pythonHeader)
		document.Fixtures[fixture.name] = &fixtureResult{
			ByteIdentical:          fixture.byteIdentical,
			PythonRaw:              pythonRaw,
			GoRaw:                  goRaw,
			GoAcceptedFromPython:   accepted,
			PythonAttributesFromGo: results["pyattr:"+fixture.name].Attributes,
		}
		if fixture.byteIdentical && !equalStringSlices(pythonRaw, goRaw) {
			t.Errorf("%s: byte-identical fixture diverged:\npython: %v\ngo:     %v",
				fixture.name, pythonRaw, goRaw)
		}
	}
	for _, fixture := range headerFixtures {
		accepted, reinjected := importClient.goAcceptedAttributes(t, "hdr-"+fixture.name, fixture.headers)
		document.Fixtures[fixture.name] = &fixtureResult{
			GoAcceptedFromHeader:       accepted,
			GoReinjectedAfterImport:    reinjected,
			GoReinjectedMarked:         importClient.goReinjectedMarked(t, "hdrm-"+fixture.name, fixture.headers),
			PythonAttributesFromHeader: results["pyattr-header:"+fixture.name].Attributes,
		}
		for _, member := range reinjected {
			if strings.HasPrefix(member, "langfuse_") {
				t.Errorf("%s: import must consume the langfuse_* namespace; reinjected %s",
					fixture.name, member)
			}
		}
	}

	// Phase 3: the sweeps, with the domain invariants asserted live.
	for id, value := range valueSweepInputs {
		entry := &sweepResult{}
		goMembers := splitMembers(goHeaders["value:"+id])
		entry.GoRawMember, entry.GoExported = memberValue(goMembers, "langfuse_user_id")
		if entry.GoExported {
			entry.PythonReceived, _ = results["value-pyattr:"+id].Attributes["user.id"].(string)
		}
		pythonMembers := splitMembers(results["value-pyraw:"+id].Baggage)
		entry.PythonRawMember, _ = memberValue(pythonMembers, "langfuse_user_id")
		accepted, _ := importClient.goAcceptedAttributes(t, "value-"+id,
			[]string{results["value-pyraw:"+id].Baggage})
		received, found := accepted["user.id"].(string)
		entry.GoReceived, entry.GoAccepted = received, found
		document.ValueSweep[id] = entry

		inDomain := value[1] != '+'
		if entry.GoExported != inDomain {
			t.Errorf("value %s: go_exported = %v, want %v", id, entry.GoExported, inDomain)
		}
		if inDomain {
			if entry.PythonReceived != value {
				t.Errorf("value %s: python received %q, want %q (Go-to-Python corruption)",
					id, entry.PythonReceived, value)
			}
			if !entry.GoAccepted || entry.GoReceived != value {
				t.Errorf("value %s: go received %q (accepted=%v), want %q (Python-to-Go corruption)",
					id, entry.GoReceived, entry.GoAccepted, value)
			}
		} else if entry.GoAccepted {
			t.Errorf("value %s: out-of-domain value must be rejected on import", id)
		}
	}
	for id, suffix := range keySweepInputs {
		entry := &sweepResult{}
		goMembers := splitMembers(goHeaders["key:"+id])
		entry.GoRawMember, entry.GoExported = memberValue(goMembers, "langfuse_metadata_"+suffix)
		if entry.GoExported {
			entry.PythonReceived, _ = results["key-pyattr:"+id].Attributes["langfuse.trace.metadata."+suffix].(string)
		}
		pythonMembers := splitMembers(results["key-pyraw:"+id].Baggage)
		for _, member := range pythonMembers {
			if strings.HasPrefix(member, "langfuse_metadata_") {
				entry.PythonRawMember = member
			}
		}
		accepted, _ := importClient.goAcceptedAttributes(t, "key-"+id,
			[]string{results["key-pyraw:"+id].Baggage})
		received, found := accepted["langfuse.trace.metadata."+suffix].(string)
		entry.GoReceived, entry.GoAccepted = received, found
		document.KeySweep[id] = entry

		b := suffix[1]
		inAlphabet := b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' ||
			b >= '0' && b <= '9' || b == '.' || b == '_' || b == '~' || b == '-'
		if entry.GoExported != inAlphabet {
			t.Errorf("key %s: go_exported = %v, want %v", id, entry.GoExported, inAlphabet)
		}
		if inAlphabet {
			if entry.PythonReceived != "v" {
				t.Errorf("key %s: python received %q under the same key, want v", id, entry.PythonReceived)
			}
			if !entry.GoAccepted || entry.GoReceived != "v" {
				t.Errorf("key %s: go must accept python's member under the same key", id)
			}
		} else if entry.GoAccepted {
			t.Errorf("key %s: out-of-alphabet suffix must be rejected on import", id)
		}
	}

	sealCorpus(t, document)
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}

func sealCorpus(t *testing.T, document *corpusDocument) {
	t.Helper()
	rendered, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatalf("marshal corpus: %v", err)
	}
	rendered = append(rendered, '\n')

	sealed, err := os.ReadFile(interopGoldenPath)
	if os.Getenv("INTEROP_REGEN") == "accept" {
		if err := os.MkdirAll("testdata/interop", 0o755); err != nil {
			t.Fatalf("create golden dir: %v", err)
		}
		if err := os.WriteFile(interopGoldenPath, rendered, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("sealed %s (%d bytes)", interopGoldenPath, len(rendered))
		return
	}
	if err != nil {
		t.Fatalf("read sealed corpus (run `task interop` with ACCEPT=accept once to seal): %v", err)
	}
	if !bytes.Equal(sealed, rendered) {
		t.Fatalf("interop corpus diverged from the sealed golden.\n"+
			"This means the Go implementation, the pinned Python SDK behavior, or the fixtures changed.\n"+
			"Review the diff and reseal deliberately with ACCEPT=accept:\n%s",
			renderCorpusDiff(sealed, rendered))
	}
}

func renderCorpusDiff(sealed, rendered []byte) string {
	sealedLines := strings.Split(string(sealed), "\n")
	renderedLines := strings.Split(string(rendered), "\n")
	var diff []string
	max := len(sealedLines)
	if len(renderedLines) > max {
		max = len(renderedLines)
	}
	for index := range max {
		var s, r string
		if index < len(sealedLines) {
			s = sealedLines[index]
		}
		if index < len(renderedLines) {
			r = renderedLines[index]
		}
		if s != r {
			diff = append(diff, fmt.Sprintf("line %d:\n  sealed:  %s\n  current: %s", index+1, s, r))
		}
		if len(diff) >= 20 {
			diff = append(diff, "... (more differences elided)")
			break
		}
	}
	return strings.Join(diff, "\n")
}

// TestInteropSmokes drives both real cross-language process boundaries
// with live identity assertions: trace continuity, parentage, claim
// non-authority, and exactly one application root.
func TestInteropSmokes(t *testing.T) {
	t.Run("PythonToGo", func(t *testing.T) {
		attrs := interopAttributes{
			UserID: "py-user", SessionID: "py-session", Environment: "staging",
			TraceName: "py-flow", Version: "v7",
			Metadata: map[string]string{"tenant": "acme"},
		}
		results := runOracle(t, []oracleCase{
			{ID: "root", Op: "inject", Attributes: &attrs},
		})
		root := results["root"]
		if root.TraceID == "" || root.Baggage == "" || root.Traceparent == "" {
			t.Fatalf("oracle root incomplete: %+v", root)
		}
		if value, found := memberValue(splitMembers(root.Baggage), "langfuse_trace_id"); !found || value != root.TraceID {
			t.Fatalf("python must claim its root trace; members: %v", splitMembers(root.Baggage))
		}
		// The Python producer's exported root carries the single
		// app-root marker for this trace.
		if _, isRoot := root.Attributes["langfuse.internal.is_app_root"]; !isRoot {
			t.Fatalf("python root must be the app root; attributes: %v", root.Attributes)
		}

		harness := newInteropGoClient(t, "pk-interop-pygo")
		httpHeader := http.Header{}
		httpHeader.Set("Baggage", root.Baggage)
		httpHeader.Set("Traceparent", root.Traceparent)
		ctx := propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		).Extract(context.Background(), propagation.HeaderCarrier(httpHeader))
		ctx = harness.client.WithTraceAttributesFromBaggage(ctx)

		harness.exporter.Reset()
		_, observation := harness.client.StartObservation(ctx, "go-receiver", langfuse.TypeSpan, langfuse.ObservationAttributes{})
		observation.End()

		if observation.TraceID() != root.TraceID {
			t.Errorf("go trace = %s, want python's %s", observation.TraceID(), root.TraceID)
		}
		spans := harness.exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected one exported span, got %d", len(spans))
		}
		span := spans[0]
		if got := span.Parent.SpanID().String(); got != root.SpanID {
			t.Errorf("go parent span = %s, want python root %s", got, root.SpanID)
		}
		attributes := langfuseOwnedAttributes(span)
		if attributes["user.id"] != "py-user" || attributes["session.id"] != "py-session" {
			t.Errorf("propagated identity missing: %v", attributes)
		}
		if attributes["langfuse.environment"] != "staging" {
			t.Errorf("environment = %v, want the propagated staging", attributes["langfuse.environment"])
		}
		if attributes["langfuse.trace.name"] != "py-flow" || attributes["langfuse.version"] != "v7" {
			t.Errorf("trace name/version missing: %v", attributes)
		}
		if attributes["langfuse.trace.metadata.tenant"] != "acme" {
			t.Errorf("metadata missing: %v", attributes)
		}
		if _, isRoot := attributes["langfuse.internal.is_app_root"]; isRoot {
			t.Error("the python-claimed trace must not gain a second app root in Go")
		}
	})

	// The claim never seeds trace identity: with a mismatched claim the
	// traceparent still decides, and without a traceparent a fresh trace
	// ID is generated; both make the Go receiver the app root.
	t.Run("MismatchedClaimIsNotAuthority", func(t *testing.T) {
		harness := newInteropGoClient(t, "pk-interop-claim1")
		httpHeader := http.Header{}
		httpHeader.Set("Baggage", "langfuse_trace_id=ffffffffffffffffffffffffffffffff,langfuse_user_id=alice")
		httpHeader.Set("Traceparent", "00-0123456789abcdef0123456789abcdef-00f067aa0ba902b7-01")
		ctx := propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		).Extract(context.Background(), propagation.HeaderCarrier(httpHeader))
		ctx = harness.client.WithTraceAttributesFromBaggage(ctx)
		harness.exporter.Reset()
		_, observation := harness.client.StartObservation(ctx, "mismatch", langfuse.TypeSpan, langfuse.ObservationAttributes{})
		observation.End()
		if observation.TraceID() != "0123456789abcdef0123456789abcdef" {
			t.Errorf("trace ID must come from traceparent, got %s", observation.TraceID())
		}
		spans := harness.exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected one span, got %d", len(spans))
		}
		attributes := langfuseOwnedAttributes(spans[0])
		if _, isRoot := attributes["langfuse.internal.is_app_root"]; !isRoot {
			t.Error("a rejected claim must leave the receiver as the app root")
		}
		if attributes["user.id"] != "alice" {
			t.Error("attribute acceptance is independent of the claim outcome")
		}
	})
	t.Run("ClaimWithoutTraceparentIsNotAuthority", func(t *testing.T) {
		harness := newInteropGoClient(t, "pk-interop-claim2")
		httpHeader := http.Header{}
		httpHeader.Set("Baggage", "langfuse_trace_id=0123456789abcdef0123456789abcdef")
		ctx := propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		).Extract(context.Background(), propagation.HeaderCarrier(httpHeader))
		ctx = harness.client.WithTraceAttributesFromBaggage(ctx)
		harness.exporter.Reset()
		_, observation := harness.client.StartObservation(ctx, "no-parent", langfuse.TypeSpan, langfuse.ObservationAttributes{})
		observation.End()
		if observation.TraceID() == "0123456789abcdef0123456789abcdef" {
			t.Error("a claim must never seed the trace ID")
		}
		spans := harness.exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected one span, got %d", len(spans))
		}
		if _, isRoot := langfuseOwnedAttributes(spans[0])["langfuse.internal.is_app_root"]; !isRoot {
			t.Error("a fresh trace under a rejected claim is its own app root")
		}
	})

	// Python-side claim non-authority, through the real pinned
	// receiver: a mismatched claim leaves trace identity to the
	// traceparent, a claim without a traceparent never seeds a trace,
	// and both leave the receiver as its own app root.
	t.Run("PythonMismatchedClaimIsNotAuthority", func(t *testing.T) {
		results := runOracle(t, []oracleCase{{
			ID: "rx", Op: "extract",
			Headers:     []string{"langfuse_trace_id=ffffffffffffffffffffffffffffffff,langfuse_user_id=alice"},
			Traceparent: "00-0123456789abcdef0123456789abcdef-00f067aa0ba902b7-01",
		}})
		receiver := results["rx"]
		if receiver.TraceID != "0123456789abcdef0123456789abcdef" {
			t.Errorf("python trace = %s, want the traceparent's", receiver.TraceID)
		}
		if receiver.ParentSpanID != "00f067aa0ba902b7" {
			t.Errorf("python parent = %s, want the traceparent's span", receiver.ParentSpanID)
		}
		if _, isRoot := receiver.Attributes["langfuse.internal.is_app_root"]; !isRoot {
			t.Errorf("a mismatched claim must not suppress python's app root: %v", receiver.Attributes)
		}
		if receiver.Attributes["user.id"] != "alice" {
			t.Errorf("attribute acceptance is independent of the claim: %v", receiver.Attributes)
		}
	})
	t.Run("PythonClaimWithoutTraceparentIsNotAuthority", func(t *testing.T) {
		results := runOracle(t, []oracleCase{{
			ID: "rx", Op: "extract",
			Headers: []string{"langfuse_trace_id=0123456789abcdef0123456789abcdef"},
		}})
		receiver := results["rx"]
		if receiver.TraceID == "0123456789abcdef0123456789abcdef" {
			t.Error("python must never seed trace identity from the claim")
		}
		if len(receiver.TraceID) != 32 || receiver.TraceID == strings.Repeat("0", 32) {
			t.Errorf("python trace ID must be a fresh nonzero 32-hex ID, got %q", receiver.TraceID)
		}
		if receiver.ParentSpanID != "" {
			t.Errorf("a claim without a traceparent must leave the receiver parentless, got parent %q", receiver.ParentSpanID)
		}
		if _, isRoot := receiver.Attributes["langfuse.internal.is_app_root"]; !isRoot {
			t.Errorf("a parentless rejected claim leaves python as its own app root: %v", receiver.Attributes)
		}
	})

	t.Run("GoToPython", func(t *testing.T) {
		harness := newInteropGoClient(t, "pk-interop-gopy")
		ctx := harness.client.WithTraceAttributes(context.Background(), langfuse.TraceAttributes{
			UserID:      "go-user",
			SessionID:   "go-session",
			Environment: "staging",
			Name:        "go-flow",
			Version:     "v9",
			Metadata:    map[string]any{"tenant": "acme"},
		})
		ctx = harness.client.WithBaggagePropagation(ctx)
		rootCtx, root := harness.client.StartObservation(ctx, "go-root", langfuse.TypeSpan, langfuse.ObservationAttributes{})
		defer root.End()

		carrier := propagation.MapCarrier{}
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		).Inject(rootCtx, carrier)
		if value, found := memberValue(splitMembers(carrier.Get("baggage")), "langfuse_trace_id"); !found || value != root.TraceID() {
			t.Fatalf("go must claim its root trace; header: %s", carrier.Get("baggage"))
		}

		results := runOracle(t, []oracleCase{{
			ID:          "receiver",
			Op:          "extract",
			Headers:     []string{carrier.Get("baggage")},
			Traceparent: carrier.Get("traceparent"),
		}})
		receiver := results["receiver"]

		if receiver.TraceID != root.TraceID() {
			t.Errorf("python trace = %s, want go's %s", receiver.TraceID, root.TraceID())
		}
		if receiver.ParentSpanID != root.ID() {
			t.Errorf("python parent = %s, want go root %s", receiver.ParentSpanID, root.ID())
		}
		if receiver.Attributes["user.id"] != "go-user" || receiver.Attributes["session.id"] != "go-session" {
			t.Errorf("propagated identity missing on python side: %v", receiver.Attributes)
		}
		if receiver.Attributes["langfuse.environment"] != "staging" {
			t.Errorf("python environment = %v, want staging", receiver.Attributes["langfuse.environment"])
		}
		if receiver.Attributes["langfuse.trace.name"] != "go-flow" || receiver.Attributes["langfuse.version"] != "v9" {
			t.Errorf("trace name/version missing on python side: %v", receiver.Attributes)
		}
		if receiver.Attributes["langfuse.trace.metadata.tenant"] != "acme" {
			t.Errorf("metadata missing on python side: %v", receiver.Attributes)
		}
		if _, isRoot := receiver.Attributes["langfuse.internal.is_app_root"]; isRoot {
			t.Error("the go-claimed trace must not gain a second app root in Python")
		}

		// One app root exists in total: the Go root carries the marker.
		harness.exporter.Reset()
		root.End()
		spans := harness.exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected the go root span, got %d", len(spans))
		}
		if _, isRoot := langfuseOwnedAttributes(spans[0])["langfuse.internal.is_app_root"]; !isRoot {
			t.Error("the go root must carry the single app-root marker")
		}
	})
}
