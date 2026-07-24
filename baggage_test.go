package langfuse

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelbaggage "go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
	"github.com/fgn/go-langfuse/internal/otlpreceiver"
)

func TestWireBaggageValueDomainCoversEveryASCIICodePoint(t *testing.T) {
	for b := range 0x80 {
		value := "a" + string(rune(b)) + "b"
		accepted := wireBaggageValue(value)
		inDomain := b > 0x20 && b < 0x7F && b != '+'
		if accepted != inDomain {
			t.Errorf("wireBaggageValue(0x%02x) = %v, want %v", b, accepted, inDomain)
		}
	}
	if wireBaggageValue("") {
		t.Error("empty value must be outside the wire domain")
	}
	if wireBaggageValue("héllo") {
		t.Error("non-ASCII value must be outside the wire domain")
	}
	if !wireBaggageValue(strings.Repeat("x", 200)) {
		t.Error("200-byte value must be inside the wire domain")
	}
	if wireBaggageValue(strings.Repeat("x", 201)) {
		t.Error("201-byte value must be outside the wire domain")
	}
}

func TestWireBaggageMetadataKeyAlphabet(t *testing.T) {
	for b := range 0x80 {
		suffix := "a" + string(rune(b)) + "b"
		accepted := wireBaggageMetadataKey(suffix)
		inAlphabet := b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' ||
			b >= '0' && b <= '9' || b == '.' || b == '_' || b == '~' || b == '-'
		if accepted != inAlphabet {
			t.Errorf("wireBaggageMetadataKey(0x%02x) = %v, want %v", b, accepted, inAlphabet)
		}
	}
	if wireBaggageMetadataKey("") {
		t.Error("empty suffix must be rejected")
	}
	if wireBaggageMetadataKey("a%2Bb") {
		t.Error("percent-encoded suffix must be rejected: Go never decodes member names")
	}
}

func TestParseBaggageTraceID(t *testing.T) {
	valid := "0123456789abcdef0123456789abcdef"
	if _, ok := parseBaggageTraceID(valid); !ok {
		t.Errorf("parseBaggageTraceID(%q) rejected a valid ID", valid)
	}
	for _, invalid := range []string{
		"0123456789ABCDEF0123456789ABCDEF", // uppercase
		"0123456789abcdef0123456789abcde",  // 31 chars
		"0123456789abcdef0123456789abcdef0",
		"00000000000000000000000000000000", // all-zero is not a valid trace ID
		"0123456789abcdef0123456789abcdeg",
		"",
	} {
		if _, ok := parseBaggageTraceID(invalid); ok {
			t.Errorf("parseBaggageTraceID(%q) accepted an invalid ID", invalid)
		}
	}
}

// extractBaggage runs the standard W3C propagator over a raw header,
// exactly as HTTP ingress middleware would.
func extractBaggage(t *testing.T, ctx context.Context, header string) context.Context {
	t.Helper()
	return propagation.Baggage{}.Extract(ctx, propagation.MapCarrier{"baggage": header})
}

// injectedBaggage returns the header the standard W3C propagator would
// send from ctx, decoded into a key -> raw member map.
func injectedMembers(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	propagation.Baggage{}.Inject(ctx, carrier)
	result := make(map[string]string)
	header := carrier.Get("baggage")
	if header == "" {
		return result
	}
	for member := range strings.SplitSeq(header, ",") {
		key, rest, _ := strings.Cut(member, "=")
		result[strings.TrimSpace(key)] = rest
	}
	return result
}

func remoteSpanContext(t *testing.T, traceHex string) oteltrace.SpanContext {
	t.Helper()
	return oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    mustInteropTraceID(t, traceHex),
		SpanID:     mustInteropSpanID(t, "00f067aa0ba902b7"),
		TraceFlags: oteltrace.FlagsSampled,
		Remote:     true,
	})
}

func captureBaggageDiagnostics(t *testing.T) *[]string {
	t.Helper()
	var mu sync.Mutex
	var messages []string
	previous := otel.GetErrorHandler()
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		mu.Lock()
		defer mu.Unlock()
		messages = append(messages, err.Error())
	}))
	t.Cleanup(func() { otel.SetErrorHandler(previous) })
	return &messages
}

func TestBaggageVerbsAreNoOpsOnNilDisabledAndStoppedClients(t *testing.T) {
	inbound := extractBaggage(t, context.Background(), "langfuse_user_id=alice,foo=bar")

	var nilClient *Client
	if got := nilClient.WithBaggagePropagation(inbound); got != inbound {
		t.Error("nil client must return the context unchanged")
	}
	if got := nilClient.WithTraceAttributesFromBaggage(inbound); got != inbound {
		t.Error("nil client import must return the context unchanged")
	}

	disabled, err := New(context.Background(), Config{Disabled: true})
	if err != nil {
		t.Fatalf("New(disabled): %v", err)
	}
	if got := disabled.WithBaggagePropagation(inbound); got != inbound {
		t.Error("disabled client must return the context unchanged")
	}
	if got := disabled.WithTraceAttributesFromBaggage(inbound); got != inbound {
		t.Error("disabled client import must return the context unchanged")
	}

	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	stopped := newInteropClient(t, receiver, Config{})
	shutdownClient(t, stopped)
	if got := stopped.WithBaggagePropagation(inbound); got != inbound {
		t.Error("stopped client must return the context unchanged")
	}
	if got := stopped.WithTraceAttributesFromBaggage(inbound); got != inbound {
		t.Error("stopped client import must return the context unchanged")
	}
}

func TestWithBaggagePropagationExportsProtocolMembersOnly(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{Environment: "prod"})
	t.Cleanup(func() { shutdownClient(t, client) })

	ctx := client.WithTraceAttributes(context.Background(), TraceAttributes{
		Name:        "checkout",
		UserID:      "alice",
		SessionID:   "s-1",
		Version:     "v1",
		Environment: "staging",
		Tags:        []string{"tag-a"},
		Metadata: map[string]any{
			"tenant": "acme",
			"count":  7, // non-string: never propagated cross-process
		},
	})
	ctx = client.WithBaggagePropagation(ctx)

	members := injectedMembers(ctx)
	want := map[string]string{
		baggageKeyTraceName:              "checkout",
		baggageKeyUserID:                 "alice",
		baggageKeySessionID:              "s-1",
		baggageKeyVersion:                "v1",
		baggageKeyEnvironment:            "staging",
		baggageMetadataPrefix + "tenant": "acme",
	}
	for key, value := range want {
		if members[key] != value {
			t.Errorf("member %s = %q, want %q (members: %v)", key, members[key], value, members)
		}
	}
	for _, absent := range []string{"langfuse_tags", baggageMetadataPrefix + "count"} {
		if _, exists := members[absent]; exists {
			t.Errorf("member %s must not be exported", absent)
		}
	}
}

func TestConfigEnvironmentIsNeverSerialized(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{Environment: "prod"})
	t.Cleanup(func() { shutdownClient(t, client) })

	ctx := client.WithBaggagePropagation(
		client.WithTraceAttributes(context.Background(), TraceAttributes{UserID: "alice"}),
	)
	members := injectedMembers(ctx)
	if _, exists := members[baggageKeyEnvironment]; exists {
		t.Errorf("Config.Environment must never masquerade as a request-scoped member; members: %v", members)
	}
	if members[baggageKeyUserID] != "alice" {
		t.Errorf("user member missing; members: %v", members)
	}
}

func TestBaggageExportDropsOutOfDomainValuesFromBaggageOnly(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })
	diagnostics := captureBaggageDiagnostics(t)

	ctx := client.WithBaggagePropagation(context.Background())
	ctx = client.WithTraceAttributes(ctx, TraceAttributes{
		UserID:    "alice smith", // space: locally legal, outside the wire domain
		SessionID: "s+1",         // '+': ambiguous across the pinned codecs
		Name:      "checkout",
	})

	members := injectedMembers(ctx)
	if _, exists := members[baggageKeyUserID]; exists {
		t.Error("out-of-domain user ID must be dropped from baggage")
	}
	if _, exists := members[baggageKeySessionID]; exists {
		t.Error("out-of-domain session ID must be dropped from baggage")
	}
	if members[baggageKeyTraceName] != "checkout" {
		t.Errorf("in-domain member missing; members: %v", members)
	}

	// Local propagation is unaffected: the exported span still carries both.
	obsCtx, observation := client.StartObservation(ctx, "op", TypeSpan, ObservationAttributes{})
	_ = obsCtx
	observation.End()
	flushClient(t, client)
	span := interopSpanMap(t, receiver)["op"]
	if span == nil {
		t.Fatal("span op not exported")
	}
	assertInteropStringAttribute(t, span, lfattr.TraceUserIDKey, "alice smith")
	assertInteropStringAttribute(t, span, lfattr.TraceSessionIDKey, "s+1")

	found := false
	for _, message := range *diagnostics {
		if strings.Contains(message, "wire domain") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a wire-domain diagnostic; got %v", *diagnostics)
	}
}

func TestImportConsumesLangfuseNamespaceOnUnmarkedBranch(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })
	diagnostics := captureBaggageDiagnostics(t)

	header := strings.Join([]string{
		"langfuse_user_id=alice",     // accepted
		"langfuse_session_id=s%2B1",  // decodes to s+1: rejected by the domain
		"langfuse_tags=%5B'a'%5D",    // excluded row
		"langfuse_region=eu",         // unknown/future member: terminates here
		"langfuse_prompt_name=greet", // excluded row
		"foo=bar;prop=1",             // foreign: preserved with its property
	}, ",")
	inbound := extractBaggage(t, context.Background(), header)

	imported := client.WithTraceAttributesFromBaggage(inbound)

	members := injectedMembers(imported)
	if len(members) != 1 || !strings.HasPrefix(members["foo"], "bar") {
		t.Errorf("unmarked import must strip every langfuse_* member and keep foreign; members: %v", members)
	}

	state, _ := imported.Value(traceStateContextKey{client: client}).(traceState)
	if state.userID != "alice" {
		t.Errorf("accepted user ID = %q, want alice", state.userID)
	}
	if state.sessionID != "" {
		t.Errorf("rejected session ID must not land in trace state; got %q", state.sessionID)
	}

	found := false
	for _, message := range *diagnostics {
		if strings.Contains(message, "ignored") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected an ignored-members diagnostic; got %v", *diagnostics)
	}
}

func TestImportRebuildsAcceptedMembersOnMarkedBranch(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	// The realistic marked-branch import: a long-lived marked base context
	// (e.g. a message consumer) extracts fresh inbound baggage per message,
	// then imports after authenticating it.
	marked := client.WithBaggagePropagation(context.Background())
	inbound := extractBaggage(t, marked, "langfuse_user_id=alice,langfuse_region=eu,foo=bar")
	imported := client.WithTraceAttributesFromBaggage(inbound)

	members := injectedMembers(imported)
	if members[baggageKeyUserID] != "alice" {
		t.Errorf("accepted member must be rebuilt on a marked branch; members: %v", members)
	}
	if _, exists := members["langfuse_region"]; exists {
		t.Error("unknown member must terminate at import even on a marked branch")
	}
	if !strings.HasPrefix(members["foo"], "bar") {
		t.Errorf("foreign member must be preserved; members: %v", members)
	}
}

func TestMarkingBeforeImportDropsInboundMembersWithDiagnostic(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })
	diagnostics := captureBaggageDiagnostics(t)

	// The documented anti-pattern: marking replaces the langfuse_*
	// namespace from private state, so un-imported inbound members are
	// gone before a later import can accept them — loudly.
	inbound := extractBaggage(t, context.Background(), "langfuse_user_id=alice,foo=bar")
	marked := client.WithBaggagePropagation(inbound)
	imported := client.WithTraceAttributesFromBaggage(marked)

	state, _ := imported.Value(traceStateContextKey{client: client}).(traceState)
	if state.userID != "" {
		t.Errorf("marking before import must have dropped the member; state user = %q", state.userID)
	}
	found := false
	for _, message := range *diagnostics {
		if strings.Contains(message, "import") && strings.Contains(message, baggageKeyUserID) {
			found = true
		}
	}
	if !found {
		t.Errorf("expected an import-before-mark diagnostic naming the member; got %v", *diagnostics)
	}
}

func TestImportPrecedenceIsPerFieldAndLocalWins(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	inbound := extractBaggage(t, context.Background(),
		"langfuse_user_id=remote-user,langfuse_session_id=remote-session")

	// Local set before import: the local value wins, the other field is
	// still accepted from baggage.
	ctx := client.WithTraceAttributes(inbound, TraceAttributes{UserID: "local-user"})
	ctx = client.WithTraceAttributesFromBaggage(ctx)
	state, _ := ctx.Value(traceStateContextKey{client: client}).(traceState)
	if state.userID != "local-user" || state.sessionID != "remote-session" {
		t.Errorf("state = {user: %q, session: %q}, want local-user/remote-session",
			state.userID, state.sessionID)
	}

	// Local set after import overwrites the accepted value.
	ctx = client.WithTraceAttributes(ctx, TraceAttributes{SessionID: "local-session"})
	state, _ = ctx.Value(traceStateContextKey{client: client}).(traceState)
	if state.sessionID != "local-session" {
		t.Errorf("later local session = %q, want local-session", state.sessionID)
	}
}

func TestImportClaimRequiresAmbientTraceMatch(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	traceHex := "0123456789abcdef0123456789abcdef"
	remote := remoteSpanContext(t, traceHex)

	matching := oteltrace.ContextWithRemoteSpanContext(context.Background(), remote)
	matching = extractBaggage(t, matching, "langfuse_trace_id="+traceHex)
	matching = client.WithTraceAttributesFromBaggage(matching)
	if !client.hasTraceClaim(matching, remote.TraceID()) {
		t.Error("matching claim must be installed")
	}

	mismatched := oteltrace.ContextWithRemoteSpanContext(context.Background(), remote)
	mismatched = extractBaggage(t, mismatched,
		"langfuse_trace_id=ffffffffffffffffffffffffffffffff")
	mismatched = client.WithTraceAttributesFromBaggage(mismatched)
	if client.hasTraceClaim(mismatched, remote.TraceID()) ||
		client.hasTraceClaim(mismatched, mustInteropTraceID(t, "ffffffffffffffffffffffffffffffff")) {
		t.Error("mismatched claim must be ignored")
	}

	// The honored claim suppresses app-root marking for the continued trace.
	_, observation := client.StartObservation(matching, "continued", TypeSpan, ObservationAttributes{})
	observation.End()
	_, orphan := client.StartObservation(mismatched, "reclassified", TypeSpan, ObservationAttributes{})
	orphan.End()
	flushClient(t, client)
	spans := interopSpanMap(t, receiver)
	if _, isRoot := interopProtoAttribute(spans["continued"], lfattr.AppRootKey); isRoot {
		t.Error("continued span must not be marked as an application root")
	}
	if _, isRoot := interopProtoAttribute(spans["reclassified"], lfattr.AppRootKey); !isRoot {
		t.Error("span under a rejected claim must become an application root")
	}
}

func TestExportedClaimFollowsSampledRootsAndDetachedContexts(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	marked := client.WithBaggagePropagation(context.Background())
	if _, exists := injectedMembers(marked)[baggageKeyTraceID]; exists {
		t.Error("no claim member before any root")
	}

	rootCtx, root := client.StartObservation(marked, "root", TypeSpan, ObservationAttributes{})
	defer root.End()
	members := injectedMembers(rootCtx)
	if members[baggageKeyTraceID] != root.TraceID() {
		t.Errorf("claim member = %q, want the new root's trace ID %q", members[baggageKeyTraceID], root.TraceID())
	}

	// A detached context drops the stale member at the next synchronizing
	// transition.
	detached := oteltrace.ContextWithSpanContext(rootCtx, oteltrace.SpanContext{})
	detached = client.WithTraceAttributes(detached, TraceAttributes{Name: "background"})
	if _, exists := injectedMembers(detached)[baggageKeyTraceID]; exists {
		t.Error("detached context must stop exporting the old claim")
	}
}

func TestSampledOutRootRemovesNonMatchingClaim(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	marked := client.WithBaggagePropagation(context.Background())
	firstCtx, first := client.StartObservation(marked, "kept", TypeSpan, ObservationAttributes{})
	first.End()
	if injectedMembers(firstCtx)[baggageKeyTraceID] != first.TraceID() {
		t.Fatal("sampled root must export its claim")
	}

	// A sampled-out sibling root starts a new trace; the old claim no longer
	// matches its ambient trace and must disappear from its branch.
	sampledOut := client.WithSampleRate(firstCtx, 0)
	droppedCtx, dropped := client.StartObservation(
		oteltrace.ContextWithSpanContext(sampledOut, oteltrace.SpanContext{}),
		"dropped", TypeSpan, ObservationAttributes{},
	)
	dropped.End()
	if dropped.Sampled() {
		t.Fatal("test setup: the second root must be sampled out")
	}
	if _, exists := injectedMembers(droppedCtx)[baggageKeyTraceID]; exists {
		t.Error("a sampled-out root must remove a claim that does not match its trace")
	}
}

func TestBaggageSynchronizingTransitionsRebuildAndAliasesStayStale(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	marked := client.WithBaggagePropagation(context.Background())
	aliceCtx := client.WithTraceAttributes(marked, TraceAttributes{UserID: "alice"})
	bobCtx := client.WithTraceAttributes(aliceCtx, TraceAttributes{UserID: "bob"})

	if got := injectedMembers(bobCtx)[baggageKeyUserID]; got != "bob" {
		t.Errorf("latest branch member = %q, want bob", got)
	}
	// Contexts are immutable: the earlier alias keeps its old member. This
	// is the documented anti-pattern, asserted so the behavior is a contract.
	if got := injectedMembers(aliceCtx)[baggageKeyUserID]; got != "alice" {
		t.Errorf("stale alias member = %q, want alice", got)
	}
}

func TestBaggageConcurrentBranchesFromOneMarkedInput(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	marked := client.WithBaggagePropagation(
		client.WithTraceAttributes(context.Background(), TraceAttributes{UserID: "alice"}),
	)

	branchMembers := make([]map[string]string, 2)
	branchTraceIDs := make([]string, 2)
	var group sync.WaitGroup
	for index := range branchMembers {
		group.Add(1)
		go func(slot int) {
			defer group.Done()
			ctx, observation := client.StartObservation(marked,
				fmt.Sprintf("root-%d", slot), TypeSpan, ObservationAttributes{})
			observation.End()
			branchMembers[slot] = injectedMembers(ctx)
			branchTraceIDs[slot] = observation.TraceID()
		}(index)
	}
	group.Wait()

	if branchTraceIDs[0] == branchTraceIDs[1] {
		t.Fatal("sibling branches must carry distinct traces")
	}
	for index, members := range branchMembers {
		if members[baggageKeyTraceID] != branchTraceIDs[index] {
			t.Errorf("branch %d claim = %q, want %q", index, members[baggageKeyTraceID], branchTraceIDs[index])
		}
		if members[baggageKeyUserID] != "alice" {
			t.Errorf("branch %d user member missing", index)
		}
	}
	if _, exists := injectedMembers(marked)[baggageKeyTraceID]; exists {
		t.Error("the shared input must remain unchanged")
	}
}

func TestImportedEnvironmentUpdatesCurrentSpanAndFutureSpans(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{Environment: "prod"})
	t.Cleanup(func() { shutdownClient(t, client) })

	// The server observation is already running when authentication
	// completes and the import runs: the processor has stamped the client
	// default, and the accepted value must overwrite it.
	serverCtx, server := client.StartObservation(context.Background(), "server", TypeSpan, ObservationAttributes{})
	inbound := extractBaggage(t, serverCtx, "langfuse_environment=staging")
	imported := client.WithTraceAttributesFromBaggage(inbound)

	childCtx, child := client.StartObservation(imported, "child", TypeSpan, ObservationAttributes{})
	_ = childCtx
	child.End()
	server.End()
	flushClient(t, client)

	spans := interopSpanMap(t, receiver)
	assertInteropStringAttribute(t, spans["server"], lfattr.EnvironmentKey, "staging")
	assertInteropStringAttribute(t, spans["child"], lfattr.EnvironmentKey, "staging")
}

func TestLocalEnvironmentBeatsAcceptedBaggageEnvironment(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{Environment: "prod"})
	t.Cleanup(func() { shutdownClient(t, client) })

	inbound := extractBaggage(t, context.Background(), "langfuse_environment=staging")
	ctx := client.WithTraceAttributes(inbound, TraceAttributes{Environment: "canary"})
	ctx = client.WithTraceAttributesFromBaggage(ctx)

	state, _ := ctx.Value(traceStateContextKey{client: client}).(traceState)
	if state.environment != "canary" {
		t.Errorf("environment = %q, want the explicit local value canary", state.environment)
	}

	_, observation := client.StartObservation(ctx, "op", TypeSpan, ObservationAttributes{})
	observation.End()
	flushClient(t, client)
	assertInteropStringAttribute(t, interopSpanMap(t, receiver)["op"], lfattr.EnvironmentKey, "canary")
}

func TestInvalidTraceAttributesEnvironmentIsIgnored(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })
	diagnostics := captureBaggageDiagnostics(t)

	ctx := client.WithTraceAttributes(context.Background(),
		TraceAttributes{Environment: "Not-Valid!"})
	state, _ := ctx.Value(traceStateContextKey{client: client}).(traceState)
	if state.environment != "" {
		t.Errorf("invalid environment must be ignored; got %q", state.environment)
	}
	found := false
	for _, message := range *diagnostics {
		if strings.Contains(message, "environment") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected an environment diagnostic; got %v", *diagnostics)
	}
}

func TestImportAppliesMaskToInboundMetadataExactlyOnce(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{
		// The Mask receives whole field values; for metadata that is the
		// merged map, exactly as on the local WithTraceAttributes path.
		Mask: func(value any) any {
			if fields, ok := value.(map[string]any); ok {
				masked := make(map[string]any, len(fields))
				for key, field := range fields {
					if text, isString := field.(string); isString {
						masked[key] = strings.ReplaceAll(text, "secret", "***")
					} else {
						masked[key] = field
					}
				}
				return masked
			}
			return value
		},
	})
	t.Cleanup(func() { shutdownClient(t, client) })

	inbound := extractBaggage(t, context.Background(), "langfuse_metadata_note=secret-plan")
	ctx := client.WithTraceAttributesFromBaggage(inbound)
	state, _ := ctx.Value(traceStateContextKey{client: client}).(traceState)
	if state.metadata["note"] != "***-plan" {
		t.Errorf("imported metadata = %q, want masked ***-plan", state.metadata["note"])
	}
}

func TestBaggageBudgetKeepsForeignFirstAndDropsLangfuseTail(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })
	diagnostics := captureBaggageDiagnostics(t)

	// One large foreign member leaves room for only the leading protocol
	// members: the published priority order decides which survive.
	foreign := "foreign_payload=" + strings.Repeat("x", 7975)
	inbound := extractBaggage(t, context.Background(), foreign)
	ctx := client.WithTraceAttributes(inbound, TraceAttributes{
		Environment: "staging",                // priority 3, fits
		SessionID:   strings.Repeat("s", 120), // priority 4, fits
		UserID:      strings.Repeat("u", 120), // priority 5, does not fit: drops with the tail
		Name:        "checkout",
	})
	ctx = client.WithBaggagePropagation(ctx)

	members := injectedMembers(ctx)
	if _, exists := members["foreign_payload"]; !exists {
		t.Error("foreign member must be preserved ahead of Langfuse members")
	}
	if members[baggageKeyEnvironment] != "staging" {
		t.Errorf("environment must survive before identity; members: %v", keysOf(members))
	}
	if _, exists := members[baggageKeySessionID]; !exists {
		t.Errorf("session must survive before user; members: %v", keysOf(members))
	}
	if _, exists := members[baggageKeyUserID]; exists {
		t.Error("user must be dropped when it no longer fits")
	}
	if _, exists := members[baggageKeyTraceName]; exists {
		t.Error("everything after the first non-fitting member drops with it")
	}

	found := false
	for _, message := range *diagnostics {
		if strings.Contains(message, "budget") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a budget diagnostic; got %v", *diagnostics)
	}
}

func TestBaggageBudgetMemberCountBoundary(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	// 63 foreign members leave exactly one slot: the highest-priority
	// Langfuse member takes it and everything else drops.
	parts := make([]string, 0, 63)
	for index := range 63 {
		parts = append(parts, fmt.Sprintf("f%02d=v", index))
	}
	inbound := extractBaggage(t, context.Background(), strings.Join(parts, ","))
	ctx := client.WithTraceAttributes(inbound, TraceAttributes{
		Environment: "staging",
		UserID:      "alice",
	})
	ctx = client.WithBaggagePropagation(ctx)

	members := injectedMembers(ctx)
	if len(members) != 64 {
		t.Fatalf("member count = %d, want exactly 64", len(members))
	}
	if members[baggageKeyEnvironment] != "staging" {
		t.Error("the single free slot belongs to the highest-priority member present")
	}
	if _, exists := members[baggageKeyUserID]; exists {
		t.Error("user must not fit into a full header")
	}
}

func TestBaggageMultipleClientsLastWriterWins(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	first := newInteropClient(t, receiver, Config{PublicKey: "pk-first"})
	t.Cleanup(func() { shutdownClient(t, first) })
	second := newInteropClient(t, receiver, Config{PublicKey: "pk-second"})
	t.Cleanup(func() { shutdownClient(t, second) })

	ctx := first.WithBaggagePropagation(
		first.WithTraceAttributes(context.Background(), TraceAttributes{UserID: "alice"}),
	)
	if injectedMembers(ctx)[baggageKeyUserID] != "alice" {
		t.Fatal("first client must export alice")
	}

	ctx = second.WithBaggagePropagation(
		second.WithTraceAttributes(ctx, TraceAttributes{UserID: "bob"}),
	)
	if got := injectedMembers(ctx)[baggageKeyUserID]; got != "bob" {
		t.Errorf("shared namespace member = %q, want the last writer's bob", got)
	}
}

func TestImportThenDirectProviderSpanReceivesPropagatedState(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	inbound := extractBaggage(t, context.Background(), "langfuse_user_id=alice")
	imported := client.WithTraceAttributesFromBaggage(inbound)

	// A span started directly on the provider's tracer (not through
	// StartObservation) still receives imported state via the processor's
	// context callback — the 1.0 borrowed-receipt contract.
	tracer := client.provider.Tracer("application")
	_, span := tracer.Start(imported, "direct",
		oteltrace.WithAttributes(attribute.String("gen_ai.request.model", "m")))
	span.End()
	flushClient(t, client)
	assertInteropStringAttribute(t, interopSpanMap(t, receiver)["direct"], lfattr.TraceUserIDKey, "alice")
}

func TestBaggageRoundTripsThroughRawHeaderUnchanged(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	ctx := client.WithBaggagePropagation(
		client.WithTraceAttributes(context.Background(), TraceAttributes{
			UserID:   "alice-1",
			Metadata: map[string]any{"tenant": "acme~1.0"},
		}),
	)
	carrier := propagation.MapCarrier{}
	propagation.Baggage{}.Inject(ctx, carrier)

	received := propagation.Baggage{}.Extract(context.Background(), carrier)
	got := otelbaggage.FromContext(received)
	if got.Member(baggageKeyUserID).Value() != "alice-1" {
		t.Errorf("user round-trip = %q", got.Member(baggageKeyUserID).Value())
	}
	if got.Member(baggageMetadataPrefix+"tenant").Value() != "acme~1.0" {
		t.Errorf("metadata round-trip = %q", got.Member(baggageMetadataPrefix+"tenant").Value())
	}
}

func keysOf(members map[string]string) []string {
	keys := make([]string, 0, len(members))
	for key := range members {
		keys = append(keys, key)
	}
	return keys
}

// TestBaggageCrossProcessGoToGo drives a full producer-to-consumer hop
// over real HTTP with the standard W3C propagators: trace identity
// continues via traceparent, attributes and the app-root claim arrive
// via baggage, and exactly one application root exists across both
// services.
func TestBaggageCrossProcessGoToGo(t *testing.T) {
	producerReceiver := otlpreceiver.New()
	t.Cleanup(producerReceiver.Close)
	producer := newInteropClient(t, producerReceiver, Config{PublicKey: "pk-producer"})
	t.Cleanup(func() { shutdownClient(t, producer) })
	consumerReceiver := otlpreceiver.New()
	t.Cleanup(consumerReceiver.Close)
	consumer := newInteropClient(t, consumerReceiver, Config{
		PublicKey:   "pk-consumer",
		Environment: "consumer-default",
	})
	t.Cleanup(func() { shutdownClient(t, consumer) })

	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{})

	type consumerResult struct {
		traceID    string
		parentSpan string
		spanID     string
	}
	resultCh := make(chan consumerResult, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		// Authentication happens here; only then is inbound baggage trusted.
		ctx = consumer.WithTraceAttributesFromBaggage(ctx)
		// The consumer's own local value must beat the propagated one for
		// exactly this field.
		ctx = consumer.WithTraceAttributes(ctx, TraceAttributes{Version: "consumer-v2"})
		_, observation := consumer.StartObservation(ctx, "consume", TypeSpan, ObservationAttributes{})
		observation.End()
		resultCh <- consumerResult{
			traceID:    observation.TraceID(),
			parentSpan: oteltrace.SpanFromContext(ctx).SpanContext().SpanID().String(),
			spanID:     observation.ID(),
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	ctx := producer.WithTraceAttributes(context.Background(), TraceAttributes{
		UserID:      "alice",
		SessionID:   "s-1",
		Version:     "producer-v1",
		Environment: "staging",
		Metadata:    map[string]any{"tenant": "acme"},
	})
	ctx = producer.WithBaggagePropagation(ctx)
	rootCtx, root := producer.StartObservation(ctx, "produce", TypeSpan, ObservationAttributes{})

	request, err := http.NewRequestWithContext(rootCtx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	propagator.Inject(rootCtx, propagation.HeaderCarrier(request.Header))
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("cross-process request: %v", err)
	}
	response.Body.Close()
	root.End()

	received := <-resultCh
	if received.traceID != root.TraceID() {
		t.Fatalf("consumer trace = %s, want the producer trace %s", received.traceID, root.TraceID())
	}

	flushClient(t, producer)
	flushClient(t, consumer)
	producerSpans := interopSpanMap(t, producerReceiver)
	consumerSpans := interopSpanMap(t, consumerReceiver)
	produce, consume := producerSpans["produce"], consumerSpans["consume"]
	if produce == nil || consume == nil {
		t.Fatalf("spans missing: producer=%v consumer=%v",
			sortedInteropSpanNames(producerSpans), sortedInteropSpanNames(consumerSpans))
	}

	// Trace identity and parentage continue across the wire.
	if got := hex.EncodeToString(consume.TraceId); got != root.TraceID() {
		t.Errorf("consumer exported trace = %s, want %s", got, root.TraceID())
	}
	if got := hex.EncodeToString(consume.ParentSpanId); got != root.ID() {
		t.Errorf("consumer parent span = %s, want the producer root %s", got, root.ID())
	}

	// Exactly one application root across both services.
	if _, isRoot := interopProtoAttribute(produce, lfattr.AppRootKey); !isRoot {
		t.Error("producer root must carry the app-root marker")
	}
	if _, isRoot := interopProtoAttribute(consume, lfattr.AppRootKey); isRoot {
		t.Error("consumer span must not become a second application root")
	}

	// Per-field precedence: propagated values apply, the consumer's own
	// local version wins, and the propagated environment overrides the
	// consumer's client default.
	assertInteropStringAttribute(t, consume, lfattr.TraceUserIDKey, "alice")
	assertInteropStringAttribute(t, consume, lfattr.TraceSessionIDKey, "s-1")
	assertInteropStringAttribute(t, consume, lfattr.TraceMetadataKey+".tenant", "acme")
	assertInteropStringAttribute(t, consume, lfattr.VersionKey, "consumer-v2")
	assertInteropStringAttribute(t, consume, lfattr.EnvironmentKey, "staging")
}

func TestReImportUpdatesAcceptedValuesWhileLocalWins(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	// Hop 1 accepts alice/s-1; hop 2 must replace the ACCEPTED user with
	// bob (old accepted baggage has no local precedence) while the
	// session, absent from hop 2, keeps its earlier accepted value.
	ctx := client.WithTraceAttributesFromBaggage(
		extractBaggage(t, context.Background(), "langfuse_user_id=alice,langfuse_session_id=s-1,langfuse_metadata_note=n1,langfuse_environment=staging"))
	ctx = client.WithTraceAttributesFromBaggage(
		extractBaggage(t, ctx, "langfuse_user_id=bob,langfuse_metadata_note=n2,langfuse_environment=canary"))

	state, _ := ctx.Value(traceStateContextKey{client: client}).(traceState)
	if state.userID != "bob" {
		t.Errorf("re-imported user = %q, want bob (stale accepted value must not win)", state.userID)
	}
	if state.sessionID != "s-1" {
		t.Errorf("session = %q, want the earlier accepted s-1", state.sessionID)
	}
	if state.metadata["note"] != "n2" {
		t.Errorf("re-imported metadata = %q, want n2", state.metadata["note"])
	}
	if state.environment != "canary" {
		t.Errorf("re-imported environment = %q, want canary", state.environment)
	}

	// A local value set between imports keeps winning over hop 3.
	ctx = client.WithTraceAttributes(ctx, TraceAttributes{UserID: "local-user"})
	ctx = client.WithTraceAttributesFromBaggage(
		extractBaggage(t, ctx, "langfuse_user_id=carol"))
	state, _ = ctx.Value(traceStateContextKey{client: client}).(traceState)
	if state.userID != "local-user" {
		t.Errorf("user after local set = %q, want local-user", state.userID)
	}
}

func TestImportTwiceOnConsumedContextIsStable(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	once := client.WithTraceAttributesFromBaggage(
		extractBaggage(t, context.Background(), "langfuse_user_id=alice"))
	twice := client.WithTraceAttributesFromBaggage(once)
	stateOnce, _ := once.Value(traceStateContextKey{client: client}).(traceState)
	stateTwice, _ := twice.Value(traceStateContextKey{client: client}).(traceState)
	if stateTwice.userID != stateOnce.userID {
		t.Errorf("second import on a consumed context changed state: %q -> %q",
			stateOnce.userID, stateTwice.userID)
	}
}

func TestImportedClaimIsReplacedAndLocalClaimSurvives(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	traceHex := "0123456789abcdef0123456789abcdef"
	remote := remoteSpanContext(t, traceHex)

	// Import 1 installs the matching claim; import 2, whose namespace
	// carries a mismatched claim, must clear that imported authority.
	ctx := oteltrace.ContextWithRemoteSpanContext(context.Background(), remote)
	ctx = client.WithTraceAttributesFromBaggage(
		extractBaggage(t, ctx, "langfuse_trace_id="+traceHex))
	if !client.hasTraceClaim(ctx, remote.TraceID()) {
		t.Fatal("first import must install the matching claim")
	}
	ctx = client.WithTraceAttributesFromBaggage(
		extractBaggage(t, ctx, "langfuse_trace_id=ffffffffffffffffffffffffffffffff"))
	if client.hasTraceClaim(ctx, remote.TraceID()) {
		t.Error("a later import with a mismatched claim must clear imported authority")
	}

	// An import whose namespace has NO claim also clears imported
	// authority.
	ctx = client.WithTraceAttributesFromBaggage(
		extractBaggage(t, oteltrace.ContextWithRemoteSpanContext(context.Background(), remote),
			"langfuse_trace_id="+traceHex))
	ctx = client.WithTraceAttributesFromBaggage(
		extractBaggage(t, ctx, "langfuse_user_id=alice"))
	if client.hasTraceClaim(ctx, remote.TraceID()) {
		t.Error("an import without a claim member must clear imported authority")
	}

	// A claim installed by a locally started root is never cleared by a
	// later import.
	marked := client.WithBaggagePropagation(context.Background())
	rootCtx, root := client.StartObservation(marked, "root", TypeSpan, ObservationAttributes{})
	defer root.End()
	imported := client.WithTraceAttributesFromBaggage(
		extractBaggage(t, rootCtx, "langfuse_user_id=alice"))
	if got := injectedMembers(imported)[baggageKeyTraceID]; got != root.TraceID() {
		t.Errorf("local root claim = %q, want %q (imports must not clear local claims)", got, root.TraceID())
	}
}

func TestUnserializableForeignMembersConsumeNoBudget(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	// OTel accepts Unicode member names through NewMemberRaw, but W3C
	// cannot serialize them: Member.String() is empty. They must consume
	// neither member slots nor bytes.
	bag := otelbaggage.FromContext(context.Background())
	phantoms := 0
	for index := range 64 {
		member, err := otelbaggage.NewMemberRaw(fmt.Sprintf("kéy-%02d", index), "v")
		if err != nil {
			continue
		}
		if member.String() != "" {
			t.Skip("this OTel version serializes Unicode names; phantom case not constructible")
		}
		next, err := bag.SetMember(member)
		if err != nil {
			continue
		}
		bag = next
		phantoms++
	}
	if phantoms == 0 {
		t.Skip("no phantom members constructible on this OTel version")
	}
	ctx := otelbaggage.ContextWithBaggage(context.Background(), bag)
	ctx = client.WithTraceAttributes(ctx, TraceAttributes{UserID: "alice"})
	ctx = client.WithBaggagePropagation(ctx)
	if got := injectedMembers(ctx)[baggageKeyUserID]; got != "alice" {
		t.Errorf("phantom foreign members must not crowd out real members; got %v", keysOf(injectedMembers(ctx)))
	}
}

func TestBaggageBudgetExactByteBoundary(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	// The langfuse_user_id member serializes as "langfuse_user_id=" + v
	// (17+N bytes); one foreign member "f=" + x*M is 2+M bytes; total
	// header = foreign + comma + member. Solve for exactly 8192.
	user := strings.Repeat("u", 100) // member size 117
	for _, boundary := range []struct {
		total int
		kept  bool
	}{
		{8192, true},
		{8193, false},
	} {
		foreignSize := boundary.total - 1 - (17 + len(user))
		foreign := "f=" + strings.Repeat("x", foreignSize-2)
		ctx := extractBaggage(t, context.Background(), foreign)
		ctx = client.WithTraceAttributes(ctx, TraceAttributes{UserID: user})
		ctx = client.WithBaggagePropagation(ctx)

		carrier := propagation.MapCarrier{}
		propagation.Baggage{}.Inject(ctx, carrier)
		header := carrier.Get("baggage")
		_, kept := injectedMembers(ctx)[baggageKeyUserID]
		if kept != boundary.kept {
			t.Errorf("total %d: user kept = %v, want %v (header %d bytes)",
				boundary.total, kept, boundary.kept, len(header))
		}
		if boundary.kept && len(header) != boundary.total {
			t.Errorf("exact header = %d bytes, want %d", len(header), boundary.total)
		}
	}
}

func TestBaggageMemberCountMatrix(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })

	// Foreign-only 65 members (built through SetMember, which permits
	// exceeding the parse limits): the lexicographic tail is dropped.
	bag := otelbaggage.FromContext(context.Background())
	for index := range 65 {
		member, err := otelbaggage.NewMemberRaw(fmt.Sprintf("f%02d", index), "v")
		if err != nil {
			t.Fatalf("NewMemberRaw: %v", err)
		}
		if next, err := bag.SetMember(member); err == nil {
			bag = next
		}
	}
	if bag.Len() < 65 {
		t.Skipf("SetMember capped at %d members on this OTel version", bag.Len())
	}
	ctx := otelbaggage.ContextWithBaggage(context.Background(), bag)
	ctx = client.WithTraceAttributes(ctx, TraceAttributes{UserID: "alice"})
	ctx = client.WithBaggagePropagation(ctx)
	members := injectedMembers(ctx)
	if len(members) != 64 {
		t.Fatalf("member count = %d, want 64", len(members))
	}
	if _, exists := members["f64"]; exists {
		t.Error("the lexicographic tail must drop first")
	}
	if _, exists := members[baggageKeyUserID]; exists {
		t.Error("foreign members are preserved ahead of Langfuse members")
	}

	// Langfuse-heavy: 60 foreign leave four slots; the priority order
	// (claim absent here) fills environment, session, user, name and
	// drops version and metadata.
	parts := make([]string, 0, 60)
	for index := range 60 {
		parts = append(parts, fmt.Sprintf("g%02d=v", index))
	}
	ctx = extractBaggage(t, context.Background(), strings.Join(parts, ","))
	ctx = client.WithTraceAttributes(ctx, TraceAttributes{
		Environment: "staging", SessionID: "s-1", UserID: "alice",
		Name: "checkout", Version: "v1",
		Metadata: map[string]any{"k": "v"},
	})
	ctx = client.WithBaggagePropagation(ctx)
	members = injectedMembers(ctx)
	if len(members) != 64 {
		t.Fatalf("langfuse-heavy count = %d, want 64", len(members))
	}
	for _, want := range []string{baggageKeyEnvironment, baggageKeySessionID, baggageKeyUserID, baggageKeyTraceName} {
		if _, exists := members[want]; !exists {
			t.Errorf("priority member %s missing", want)
		}
	}
	for _, dropped := range []string{baggageKeyVersion, baggageMetadataPrefix + "k"} {
		if _, exists := members[dropped]; exists {
			t.Errorf("member %s must drop with the tail", dropped)
		}
	}
}

func TestBaggageDiagnosticsNeverNameUserControlledKeys(t *testing.T) {
	receiver := otlpreceiver.New()
	t.Cleanup(receiver.Close)
	client := newInteropClient(t, receiver, Config{})
	t.Cleanup(func() { shutdownClient(t, client) })
	diagnostics := captureBaggageDiagnostics(t)

	secret := "sk-hunter2-topsecret"
	inbound := extractBaggage(t, context.Background(),
		"langfuse_metadata_"+secret+"=x,langfuse_"+secret+"=y")
	_ = client.WithTraceAttributesFromBaggage(inbound)

	ctx := client.WithTraceAttributes(context.Background(), TraceAttributes{
		Metadata: map[string]any{secret: "value with spaces"},
	})
	_ = client.WithBaggagePropagation(ctx)

	for _, message := range *diagnostics {
		if strings.Contains(message, secret) {
			t.Fatalf("diagnostic leaked a user-controlled key: %s", message)
		}
	}
	if len(*diagnostics) == 0 {
		t.Fatal("expected diagnostics for the rejected members")
	}
}
