package langfuse

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	otelbaggage "go.opentelemetry.io/otel/baggage"
	oteltrace "go.opentelemetry.io/otel/trace"

	lfattr "github.com/fgn/go-langfuse/internal/attributes"
	"github.com/fgn/go-langfuse/internal/diagnostic"
)

// Baggage protocol v1: the closed set of W3C baggage members this SDK
// exchanges with the official Langfuse SDKs. Unknown or excluded
// langfuse_* members terminate at the import verb; they are neither
// applied nor forwarded. New members are additive protocol revisions.
const (
	baggageKeyPrefix      = "langfuse_"
	baggageKeyTraceID     = "langfuse_trace_id"
	baggageKeyEnvironment = "langfuse_environment"
	baggageKeySessionID   = "langfuse_session_id"
	baggageKeyUserID      = "langfuse_user_id"
	baggageKeyTraceName   = "langfuse_trace_name"
	baggageKeyVersion     = "langfuse_version"
	baggageMetadataPrefix = "langfuse_metadata_"
)

// The W3C limits are enforced against the exact serialized header size
// (member bytes plus one comma between adjacent members) before baggage
// construction, so the OpenTelemetry builder can never truncate
// nondeterministically.
const (
	maxBaggageMembers    = 64
	maxBaggageBytes      = 8192
	maxBaggageValueBytes = 200
)

// diagnosticMemberLimit bounds how many member keys one dropped-member
// diagnostic names.
const diagnosticMemberLimit = 8

type baggageMarkerContextKey struct{ client *Client }

// WithBaggagePropagation marks the returned context so this Client's
// trace attributes travel as W3C baggage to every downstream service.
// From the returned context on, WithTraceAttributes,
// WithTraceAttributesFromBaggage, and StartObservation each return a
// context whose langfuse_* baggage members are rebuilt from the state
// visible at that point; callers must propagate the latest returned
// context, because earlier contexts and aliases keep their old members.
//
// Propagated members are the Langfuse trace attributes (user ID,
// session ID, trace name, version, request-scoped environment, and
// string metadata values) plus an application-root marker for the
// current trace. Values must be printable ASCII without spaces or '+'
// and at most 200 bytes; values outside that domain stay on the local
// trace but are dropped from baggage with a diagnostic. Tags never
// propagate cross-process.
//
// Baggage is delivered by whatever propagator the application has
// installed, so these values are sent to EVERY destination the context
// reaches — third-party APIs included — not only to services that use
// Langfuse. Enable it only on paths where that disclosure is intended.
//
// The trace claim carried in baggage is bound to the trace that was
// current at the last synchronizing call; a root started directly on
// the application's tracer cannot refresh it. Start roots through
// StartObservation on propagation-enabled paths when application-root
// continuity matters downstream.
func (c *Client) WithBaggagePropagation(ctx context.Context) context.Context {
	if c == nil || c.isDisabled() || ctx == nil {
		return ctx
	}
	if c.stopped.Load() {
		c.reportStoppedOnce()
		return ctx
	}
	marked := context.WithValue(ctx, baggageMarkerContextKey{client: c}, true)
	return c.syncBaggage(marked, true, true)
}

// WithTraceAttributesFromBaggage applies Langfuse trace attributes
// received as W3C baggage to the returned context. Receipt is explicit
// and never automatic: call it only after the application has
// authenticated the request, because inbound baggage is caller-
// controlled and would otherwise set tenant attribution.
//
// Only the protocol v1 members are applied, each validated against the
// wire domain: user ID, session ID, trace name, version, environment,
// and string metadata values land in propagated trace state with local
// WithTraceAttributes values taking precedence per field; an accepted
// environment also updates the current recording span. The
// application-root marker is honored only when it names the ambient
// span context's trace ID. Inbound metadata passes through the
// configured Mask exactly once.
//
// The returned context's baggage always has the entire langfuse_*
// namespace removed — accepted, invalid, excluded, and unknown members
// alike — so a standard inject forwards nothing of Langfuse's unless
// WithBaggagePropagation re-enables export from the accepted state.
// Foreign baggage members are preserved untouched.
func (c *Client) WithTraceAttributesFromBaggage(ctx context.Context) context.Context {
	if c == nil || c.isDisabled() || ctx == nil {
		return ctx
	}
	if c.stopped.Load() {
		c.reportStoppedOnce()
		return ctx
	}

	bag := otelbaggage.FromContext(ctx)
	previous, _ := ctx.Value(traceStateContextKey{client: c}).(traceState)
	state := previous.clone()
	ambient := oteltrace.SpanFromContext(ctx).SpanContext()

	var (
		claimID             oteltrace.TraceID
		haveClaim           bool
		hasNamespace        bool
		environmentAccepted bool
		importedMetadata    map[string]any
		ignored             []string
	)
	scalars := map[string]*string{
		acceptedSessionID: &state.sessionID,
		acceptedUserID:    &state.userID,
		acceptedName:      &state.name,
		acceptedVersion:   &state.version,
	}
	confirmed := make(map[string]struct{})
	// A field is replaceable when it is empty or its current value came
	// from an earlier import: each import rebuilds the accepted layer
	// from the CURRENT namespace, while local values keep winning.
	acceptScalar := func(target *string, originKey, memberKey, value string) {
		if !wireBaggageValue(value) {
			ignored = append(ignored, memberKey)
			return
		}
		if *target == "" || state.isAccepted(originKey) {
			*target = value
			state.markAccepted(originKey)
			confirmed[originKey] = struct{}{}
		}
	}
	for _, member := range bag.Members() {
		key := member.Key()
		if !strings.HasPrefix(key, baggageKeyPrefix) {
			continue
		}
		hasNamespace = true
		value := member.Value()
		switch {
		case key == baggageKeyTraceID:
			if id, ok := parseBaggageTraceID(value); ok &&
				ambient.IsValid() && id == ambient.TraceID() {
				claimID, haveClaim = id, true
			} else {
				ignored = append(ignored, key)
			}
		case key == baggageKeyEnvironment:
			if wireBaggageValue(value) && validateEnvironment(value) == nil {
				if state.environment == "" || state.isAccepted(acceptedEnvironment) {
					state.environment = value
					state.markAccepted(acceptedEnvironment)
					confirmed[acceptedEnvironment] = struct{}{}
					environmentAccepted = true
				}
			} else {
				ignored = append(ignored, key)
			}
		case key == baggageKeySessionID:
			acceptScalar(&state.sessionID, acceptedSessionID, key, value)
		case key == baggageKeyUserID:
			acceptScalar(&state.userID, acceptedUserID, key, value)
		case key == baggageKeyTraceName:
			acceptScalar(&state.name, acceptedName, key, value)
		case key == baggageKeyVersion:
			acceptScalar(&state.version, acceptedVersion, key, value)
		case strings.HasPrefix(key, baggageMetadataPrefix):
			suffix := key[len(baggageMetadataPrefix):]
			if !wireBaggageMetadataKey(suffix) || !wireBaggageValue(value) {
				ignored = append(ignored, key)
				continue
			}
			if _, exists := state.metadata[suffix]; exists && !state.isAccepted(acceptedMetadata+suffix) {
				continue // local metadata wins
			}
			if importedMetadata == nil {
				importedMetadata = make(map[string]any)
			}
			importedMetadata[suffix] = value
		default:
			// Excluded rows (tags, prompt name/version) and unknown or
			// future members terminate here by design.
			ignored = append(ignored, key)
		}
	}
	if hasNamespace {
		// The accepted layer is a projection of the CURRENT namespace:
		// accepted fields whose member is absent or invalid in it are
		// retired, so stale attribution from an earlier hop can never
		// outlive the message that carried it. Locally originated
		// fields are untouched, and a consumed context (no namespace)
		// changes nothing.
		for originKey, target := range scalars {
			if _, kept := confirmed[originKey]; !kept && state.isAccepted(originKey) {
				*target = ""
				delete(state.accepted, originKey)
			}
		}
		if _, kept := confirmed[acceptedEnvironment]; !kept && state.isAccepted(acceptedEnvironment) {
			state.environment = ""
			delete(state.accepted, acceptedEnvironment)
		}
	}
	state.mergeImportedMetadata(importedMetadata, c.mask, hasNamespace)
	if len(ignored) != 0 {
		reportIgnoredBaggageMembers(ignored)
	}

	result := context.WithValue(ctx, traceStateContextKey{client: c}, state)
	// Claim replacement: a matching claim in the current namespace
	// installs imported authority — unless an identical LOCAL claim is
	// already held, which keeps its local origin so no later import can
	// clear it. Anything else (absent, invalid, or mismatched) clears
	// authority a previous import installed; a claim from a locally
	// started root is never cleared by an import.
	prior := c.traceClaimState(ctx)
	switch {
	case haveClaim && prior.id == claimID && !prior.imported:
		// Keep the stronger local origin.
	case haveClaim:
		result = c.withImportedTraceClaim(result, claimID)
	case prior.imported:
		result = c.withClearedTraceClaim(result)
	}
	result = c.syncBaggage(result, true, false)
	if environmentAccepted {
		stampEnvironmentAttribute(result, state.environment)
	}
	return result
}

// syncBaggage consumes the langfuse_* namespace in the context's OTel
// baggage and, when the context carries this client's propagation
// marker, rebuilds the protocol members from private state within the
// W3C budgets. force consumes the namespace even without the marker
// (the import contract); otherwise an unmarked context is returned
// unchanged, since plain local attribute calls never touch baggage.
// reportReplaced is set only by the mark verb, where replacing inbound
// members reveals an import-before-mark ordering mistake worth naming.
func (c *Client) syncBaggage(ctx context.Context, force, reportReplaced bool) context.Context {
	marked, _ := ctx.Value(baggageMarkerContextKey{client: c}).(bool)
	if !marked && !force {
		return ctx
	}
	bag := otelbaggage.FromContext(ctx)
	members, changed := c.wireBaggageMembers(ctx, bag, marked, reportReplaced)
	if !changed {
		return ctx
	}
	next, err := otelbaggage.New(members...)
	if err != nil {
		// The member set is validated and within budget by construction;
		// failing here is an invariant violation, and the previous baggage
		// is left untouched rather than partially rewritten.
		diagnostic.Report("baggage rebuild failed unexpectedly; outbound baggage left unchanged")
		return ctx
	}
	return otelbaggage.ContextWithBaggage(ctx, next)
}

// wireBaggageMembers returns the outbound member set: foreign members
// first (preserved, lexicographically ordered for deterministic budget
// accounting), then, when rebuild is set, the Langfuse members in the
// documented priority order. Members are kept whole or dropped whole;
// the first member that cannot fit drops itself and everything after
// it in its group.
func (c *Client) wireBaggageMembers(
	ctx context.Context,
	bag otelbaggage.Baggage,
	rebuild bool,
	reportReplaced bool,
) ([]otelbaggage.Member, bool) {
	type sizedMember struct {
		member otelbaggage.Member
		size   int
	}
	var foreign []sizedMember
	hadLangfuse := false
	for _, member := range bag.Members() {
		if strings.HasPrefix(member.Key(), baggageKeyPrefix) {
			hadLangfuse = true
			continue
		}
		rendered := member.String()
		if rendered == "" {
			// Not W3C-serializable (e.g. a NewMemberRaw Unicode name):
			// no wire can carry it, so it must not consume budget or a
			// member slot either.
			continue
		}
		foreign = append(foreign, sizedMember{member: member, size: len(rendered)})
	}
	sort.Slice(foreign, func(i, j int) bool {
		return foreign[i].member.Key() < foreign[j].member.Key()
	})

	members := make([]otelbaggage.Member, 0, len(foreign)+8)
	totalBytes := 0
	fits := func(size int) bool {
		need := size
		if len(members) > 0 {
			need++ // the comma separating adjacent members
		}
		return len(members) < maxBaggageMembers && totalBytes+need <= maxBaggageBytes
	}
	add := func(member otelbaggage.Member, size int) {
		if len(members) > 0 {
			size++
		}
		totalBytes += size
		members = append(members, member)
	}

	droppedForeign := 0
	for index, item := range foreign {
		if !fits(item.size) {
			droppedForeign = len(foreign) - index
			break
		}
		add(item.member, item.size)
	}
	if droppedForeign != 0 {
		diagnostic.Report("foreign baggage members exceed the W3C budget; dropped " +
			itoa(droppedForeign) + " member(s)")
	}

	added := false
	if rebuild {
		var droppedDomain, droppedBudget []string
		emitted := make(map[string]string)
		budgetExhausted := false
		emit := func(key, value string) {
			if value == "" {
				return
			}
			if !wireBaggageValue(value) {
				droppedDomain = append(droppedDomain, key)
				return
			}
			if budgetExhausted {
				droppedBudget = append(droppedBudget, key)
				return
			}
			member, err := otelbaggage.NewMemberRaw(key, value)
			if err != nil {
				droppedDomain = append(droppedDomain, key)
				return
			}
			size := len(member.String())
			if !fits(size) {
				budgetExhausted = true
				droppedBudget = append(droppedBudget, key)
				return
			}
			add(member, size)
			emitted[key] = value
			added = true
		}

		state, _ := ctx.Value(traceStateContextKey{client: c}).(traceState)
		ambient := oteltrace.SpanFromContext(ctx).SpanContext()
		if claim := c.traceClaimState(ctx); claim.id.IsValid() &&
			ambient.IsValid() && claim.id == ambient.TraceID() {
			emit(baggageKeyTraceID, claim.id.String())
		}
		emit(baggageKeyEnvironment, state.environment)
		emit(baggageKeySessionID, state.sessionID)
		emit(baggageKeyUserID, state.userID)
		emit(baggageKeyTraceName, state.name)
		emit(baggageKeyVersion, state.version)
		metadataKeys := make([]string, 0, len(state.wireMetadata))
		for key := range state.wireMetadata {
			metadataKeys = append(metadataKeys, key)
		}
		sort.Strings(metadataKeys)
		for _, key := range metadataKeys {
			if !wireBaggageMetadataKey(key) {
				droppedDomain = append(droppedDomain, baggageMetadataPrefix+key)
				continue
			}
			emit(baggageMetadataPrefix+key, state.metadata[key])
		}
		if len(droppedDomain) != 0 {
			diagnostic.Report("trace attributes outside the baggage wire domain propagate locally only; dropped from baggage: " +
				summarizeMemberKeys(droppedDomain))
		}
		if len(droppedBudget) != 0 {
			diagnostic.Report("baggage members exceed the W3C budget; dropped: " +
				summarizeMemberKeys(droppedBudget))
		}
		// A rebuild replaces the langfuse_* namespace wholesale. At marking
		// time, members that were present but are not re-emitted are
		// un-imported inbound values (or another writer's members): dropping
		// them silently would hide the import-before-mark ordering
		// requirement. Routine syncs replace members by design and stay
		// quiet.
		var droppedInbound []string
		if !reportReplaced {
			bag = otelbaggage.Baggage{}
		}
		for _, member := range bag.Members() {
			key := member.Key()
			if !strings.HasPrefix(key, baggageKeyPrefix) {
				continue
			}
			// The claim is included deliberately: marking before
			// importing silently discards inbound app-root suppression,
			// which is exactly the ordering mistake worth naming. The
			// wording below states content replacement only, because a
			// detached re-mark also lands here legitimately.
			if value, kept := emitted[key]; !kept || value != member.Value() {
				droppedInbound = append(droppedInbound, key)
			}
		}
		if len(droppedInbound) != 0 {
			sort.Strings(droppedInbound)
			diagnostic.Report("existing langfuse_* baggage members were replaced from this client's state (" +
				summarizeMemberKeys(droppedInbound) +
				"); if they were inbound, import with WithTraceAttributesFromBaggage before enabling propagation")
		}
	}

	return members, hadLangfuse || added || droppedForeign != 0
}

// wireBaggageValue reports whether value lies in the protocol v1 value
// domain: printable US-ASCII excluding space and '+', at most 200
// bytes. Space and '+' are excluded because the pinned Python decoder
// form-decodes '+' to a space while Go preserves it, so both spellings
// are ambiguous on the wire; the rejection is a domain rule and makes
// no claim about which spelling produced the byte.
func wireBaggageValue(value string) bool {
	if value == "" || len(value) > maxBaggageValueBytes {
		return false
	}
	for index := range len(value) {
		b := value[index]
		if b <= 0x20 || b >= 0x7F || b == '+' {
			return false
		}
	}
	return true
}

// wireBaggageMetadataKey reports whether a metadata key suffix lies in
// the cross-SDK raw-name alphabet: ASCII letters, digits, '.', '_',
// '~', and '-'. Both pinned producers emit these bytes raw in member
// names, and Go never percent-decodes names, so raw name agreement
// holds exactly on this set.
func wireBaggageMetadataKey(suffix string) bool {
	if suffix == "" || len(suffix) > maxBaggageValueBytes {
		return false
	}
	for index := range len(suffix) {
		b := suffix[index]
		switch {
		case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9',
			b == '.', b == '_', b == '~', b == '-':
		default:
			return false
		}
	}
	return true
}

// parseBaggageTraceID accepts exactly 32 lowercase hex characters
// naming a valid (non-zero) trace ID.
func parseBaggageTraceID(value string) (oteltrace.TraceID, bool) {
	if len(value) != 32 {
		return oteltrace.TraceID{}, false
	}
	for index := range len(value) {
		b := value[index]
		if (b < '0' || b > '9') && (b < 'a' || b > 'f') {
			return oteltrace.TraceID{}, false
		}
	}
	id, err := oteltrace.TraceIDFromHex(value)
	return id, err == nil && id.IsValid()
}

func stampEnvironmentAttribute(ctx context.Context, environment string) {
	if environment == "" {
		return
	}
	if span := oteltrace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attribute.String(lfattr.EnvironmentKey, environment))
	}
}

func reportIgnoredBaggageMembers(keys []string) {
	diagnostic.Report("inbound langfuse_* baggage members ignored (invalid or outside protocol v1): " +
		summarizeMemberKeys(keys))
}

// summarizeMemberKeys renders a payload-free summary: the seven fixed
// protocol member names appear literally; metadata suffixes, unknown
// langfuse_* members, and anything else are user- or wire-controlled
// text and are reported as bounded counts only.
func summarizeMemberKeys(keys []string) string {
	fixed := map[string]bool{
		baggageKeyTraceID: true, baggageKeyEnvironment: true,
		baggageKeySessionID: true, baggageKeyUserID: true,
		baggageKeyTraceName: true, baggageKeyVersion: true,
		"langfuse_tags": true, "langfuse_prompt_name": true,
		"langfuse_prompt_version": true,
	}
	var named []string
	metadataCount, otherCount := 0, 0
	for _, key := range keys {
		switch {
		case fixed[key]:
			named = append(named, key)
		case strings.HasPrefix(key, baggageMetadataPrefix):
			metadataCount++
		default:
			otherCount++
		}
	}
	sort.Strings(named)
	if len(named) > diagnosticMemberLimit {
		otherCount += len(named) - diagnosticMemberLimit
		named = named[:diagnosticMemberLimit]
	}
	parts := named
	if metadataCount > 0 {
		parts = append(parts, itoa(metadataCount)+" metadata member(s)")
	}
	if otherCount > 0 {
		parts = append(parts, itoa(otherCount)+" other member(s)")
	}
	return strings.Join(parts, ", ")
}

func itoa(value int) string {
	return strconv.Itoa(value)
}
