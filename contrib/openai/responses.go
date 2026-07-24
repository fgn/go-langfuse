package langfuseopenai

import (
	"bytes"
	"encoding/json"
	"slices"
	"sort"
	"strings"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/contrib/openai/internal/wiretap"
)

// Bounds for the Responses route. Wire fields are untrusted input and
// never size allocations; items are kept whole or dropped whole.
const (
	maxResponseItems = 128
	maxContentParts  = 32
	maxItemBytes     = 64 << 10
)

// responsesParameterAllowlist is the closed numeric/boolean set for
// /responses. Operational fields (background, store, stream,
// service_tier, truncation), content and identifiers (tools,
// tool_choice, text, reasoning, include, metadata,
// previous_response_id, prompt_cache_key, safety_identifier, user),
// and string-typed controls are excluded by design.
var responsesParameterAllowlist = map[string]bool{
	"temperature":         true,
	"top_p":               true,
	"max_output_tokens":   true,
	"max_tool_calls":      true,
	"top_logprobs":        true,
	"parallel_tool_calls": true,
}

// knownResponsesOutputTypes are the pinned output-item discriminators.
// Retained: message, function_call, reasoning. Everything else exports
// the fixed omitted placeholder; discriminators outside this set export
// the literal "unknown", never the raw wire string.
var knownResponsesOutputTypes = map[string]bool{
	"message":               true,
	"function_call":         true,
	"reasoning":             true,
	"image_generation_call": true,
	"computer_call":         true,
	"code_interpreter_call": true,
	"file_search_call":      true,
	"web_search_call":       true,
	"mcp_call":              true,
	"mcp_list_tools":        true,
	"mcp_approval_request":  true,
	"local_shell_call":      true,
	"custom_tool_call":      true,
}

// knownResponsesInputTypes are the pinned input-item discriminators
// beyond the retained message/function/reasoning/reference forms.
var knownResponsesInputTypes = map[string]bool{
	"computer_call":           true,
	"computer_call_output":    true,
	"function_call":           true,
	"function_call_output":    true,
	"file_search_call":        true,
	"web_search_call":         true,
	"image_generation_call":   true,
	"code_interpreter_call":   true,
	"local_shell_call":        true,
	"local_shell_call_output": true,
	"mcp_list_tools":          true,
	"mcp_approval_request":    true,
	"mcp_approval_response":   true,
	"mcp_call":                true,
	"custom_tool_call":        true,
	"custom_tool_call_output": true,
	"item_reference":          true,
	"message":                 true,
	"reasoning":               true,
}

// responsesCall parses one /responses attempt: request, unary body,
// typed SSE events with terminal-authoritative output, and the bounded
// over-cap salvage scanner.
type responsesCall struct {
	route      wiretap.Route
	captureCap int

	// Request.
	input           any
	requestModel    string
	modelParameters map[string]any

	// Response control plane.
	responseModel string
	usage         *langfuse.Usage
	incomplete    bool
	errorCategory string
	errorOutput   any // sanitized provider error, Mask/content-governed
	partial       bool

	// Unary output.
	unaryOutput any
	haveUnary   bool

	// Streaming incremental fallback state (semantic TTFT and output
	// when EOF arrives before a terminal or the terminal was over-cap).
	sawEvents    bool
	items        map[int]*responseItemState
	itemOrder    []int
	idToIndex    map[string]int
	audioPresent bool
	outputBytes  int

	// Terminal-authoritative output.
	finalOutput []any
	haveFinal   bool

	// Over-cap salvage.
	scanner *controlScanner
}

// responseItemState is the bounded per-output_index fallback state.
type responseItemState struct {
	id        string
	kind      string // known discriminator or "unknown"
	tombstone bool
	// done, when set, is the sanitized finalized item from
	// output_item.done and replaces every accumulated value below.
	done      any
	texts     map[int]*strings.Builder
	refusals  map[int]*strings.Builder
	partOrder []int
	name      string
	callID    string
	args      strings.Builder
	summary   strings.Builder
	imaged    bool
}

func newResponsesCall(route wiretap.Route, captureCap int) *responsesCall {
	return &responsesCall{route: route, captureCap: captureCap}
}

// --- request ---

func (c *responsesCall) ParseRequest(body []byte) {
	var request map[string]json.RawMessage
	if err := json.Unmarshal(body, &request); err != nil {
		c.partial = true
		return
	}
	if raw, ok := request["model"]; ok {
		_ = json.Unmarshal(raw, &c.requestModel)
	}
	c.modelParameters = allowedParametersFrom(request, responsesParameterAllowlist)

	input := map[string]any{}
	if raw, ok := request["instructions"]; ok {
		var instructions string
		if json.Unmarshal(raw, &instructions) == nil && instructions != "" {
			input["instructions"] = instructions
		}
	}
	if raw, ok := request["input"]; ok {
		if sanitized, ok := c.sanitizeRequestInput(raw); ok {
			input["input"] = sanitized
		}
	}
	if raw, ok := request["prompt"]; ok {
		if prompt := sanitizePromptReference(raw); prompt != nil {
			input["prompt"] = prompt
		} else {
			c.partial = true
		}
	}
	if len(input) != 0 {
		c.input = input
	}
}

// sanitizeRequestInput normalizes the request input field: a string is
// kept verbatim; a list runs every pinned input-item variant through
// the closed policy; anything else is omitted with partial telemetry.
func (c *responsesCall) sanitizeRequestInput(raw json.RawMessage) (any, bool) {
	var asString string
	if json.Unmarshal(raw, &asString) == nil {
		return asString, true
	}
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		c.partial = true
		return nil, false
	}
	sanitized := make([]any, 0, len(items))
	for _, item := range items {
		sanitized = append(sanitized, c.sanitizeInputItem(item))
	}
	return sanitized, true
}

func (c *responsesCall) sanitizeInputItem(raw json.RawMessage) any {
	var item struct {
		Type      string          `json:"type"`
		Role      string          `json:"role"`
		Content   json.RawMessage `json:"content"`
		Name      string          `json:"name"`
		Arguments string          `json:"arguments"`
		CallID    string          `json:"call_id"`
		Output    json.RawMessage `json:"output"`
		Summary   json.RawMessage `json:"summary"`
		ID        string          `json:"id"`
	}
	if err := json.Unmarshal(raw, &item); err != nil {
		c.partial = true
		return map[string]any{"type": "unknown", "omitted": true}
	}
	switch {
	case item.Type == "" && item.Role != "":
		// EasyInputMessage: all four pinned roles, string or list content.
		return map[string]any{"role": item.Role, "content": sanitizeInputContent(item.Content)}
	case item.Type == "message":
		if item.Role == "assistant" {
			// A prior output message travels through the output sanitizer.
			sanitized, _, _ := sanitizeResponsesOutputItem(raw)
			return sanitized
		}
		return map[string]any{"role": item.Role, "content": sanitizeInputContent(item.Content)}
	case item.Type == "function_call":
		return map[string]any{
			"type": "function_call", "name": item.Name,
			"arguments": item.Arguments, "call_id": item.CallID,
		}
	case item.Type == "function_call_output":
		var output any = map[string]any{"omitted": true}
		var text string
		if json.Unmarshal(item.Output, &text) == nil {
			output = text
		}
		return map[string]any{"type": "function_call_output", "call_id": item.CallID, "output": output}
	case item.Type == "reasoning":
		return map[string]any{
			"type": "reasoning", "thought": true,
			"summary": sanitizeReasoningSummary(item.Summary),
		}
	case item.Type == "item_reference":
		return map[string]any{"type": "item_reference", "id": item.ID}
	case knownResponsesInputTypes[item.Type]:
		c.partial = true
		return map[string]any{"type": item.Type, "omitted": true}
	default:
		c.partial = true
		return map[string]any{"type": "unknown", "omitted": true}
	}
}

// sanitizeInputContent handles the string-or-content-list union with
// the fixed input-content shapes: input_text retained, media replaced.
// A single content object (a prompt variable, for example) sanitizes
// like a one-part list without gaining the list shape.
func sanitizeInputContent(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var parts []json.RawMessage
	if json.Unmarshal(raw, &parts) != nil {
		return sanitizeInputContentPart(raw)
	}
	sanitized := make([]any, 0, len(parts))
	for _, part := range parts {
		sanitized = append(sanitized, sanitizeInputContentPart(part))
	}
	return sanitized
}

func sanitizeInputContentPart(raw json.RawMessage) map[string]any {
	var part struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &part) != nil {
		return map[string]any{"type": "unknown", "omitted": true}
	}
	switch part.Type {
	case "input_text":
		return map[string]any{"type": "input_text", "text": part.Text}
	case "input_image", "input_file":
		return map[string]any{"type": part.Type, "omitted": true}
	default:
		return map[string]any{"type": "unknown", "omitted": true}
	}
}

// sanitizePromptReference retains exactly {id, version, variables} with
// every variable value passed through the input-content sanitizer; a
// scalar string variable stays a scalar (the smallest projection).
func sanitizePromptReference(raw json.RawMessage) map[string]any {
	var prompt struct {
		ID        string                     `json:"id"`
		Version   string                     `json:"version"`
		Variables map[string]json.RawMessage `json:"variables"`
	}
	if err := json.Unmarshal(raw, &prompt); err != nil {
		return nil
	}
	result := map[string]any{"id": prompt.ID}
	if prompt.Version != "" {
		result["version"] = prompt.Version
	}
	if len(prompt.Variables) != 0 {
		variables := make(map[string]any, len(prompt.Variables))
		for key, value := range prompt.Variables {
			var scalar string
			if json.Unmarshal(value, &scalar) == nil {
				variables[key] = scalar
				continue
			}
			variables[key] = sanitizeInputContent(value)
		}
		result["variables"] = variables
	}
	return result
}

// --- streaming ---

func (c *responsesCall) FeedEvent(data []byte) wiretap.EventVerdict {
	if data == nil {
		// EOF probe: this route has typed terminal events; clean EOF
		// before one is always incomplete.
		return wiretap.EventVerdict{}
	}
	c.sawEvents = true
	if bytes.Equal(bytes.TrimSpace(data), doneSentinel) {
		// [DONE] is not a Responses terminal; treat it as an unknown
		// event so a stream ending with only it stays incomplete.
		c.partial = true
		return wiretap.EventVerdict{}
	}
	// The scanner is the single grammar authority for BOTH the under-
	// and over-cap paths: object root, full number grammar, escape
	// handling, and duplicate selected control keys. A payload it
	// rejects yields no hard verdict, even when early fields spelled a
	// terminal type.
	grammar := newControlScanner(scanSSEEnvelope)
	grammar.feed(data)
	if !grammar.documentUsable() {
		c.partial = true
		return wiretap.EventVerdict{}
	}

	var event responsesStreamEvent
	if err := json.Unmarshal(data, &event); err != nil {
		c.partial = true
		return wiretap.EventVerdict{}
	}
	return c.consumeEvent(&event)
}

type responsesStreamEvent struct {
	Type            string          `json:"type"`
	OutputIndex     *int            `json:"output_index"`
	ContentIndex    *int            `json:"content_index"`
	ItemID          string          `json:"item_id"`
	Delta           string          `json:"delta"`
	Text            string          `json:"text"`
	Refusal         string          `json:"refusal"`
	Arguments       string          `json:"arguments"`
	PartialImageB64 string          `json:"partial_image_b64"`
	Item            json.RawMessage `json:"item"`
	Part            json.RawMessage `json:"part"`
	Response        json.RawMessage `json:"response"`
	Code            string          `json:"code"`
	Message         string          `json:"message"`
	Param           string          `json:"param"`
}

func (c *responsesCall) consumeEvent(event *responsesStreamEvent) wiretap.EventVerdict {
	switch event.Type {
	case "response.completed", "response.failed", "response.incomplete":
		return c.consumeTerminal(event)
	case "error":
		c.errorCategory = "provider error"
		c.errorOutput = map[string]any{
			"type": "error", "code": event.Code,
			"message": event.Message, "param": event.Param,
		}
		return wiretap.EventVerdict{Terminal: wiretap.TerminalError}
	case "response.output_text.delta":
		bearing := event.Delta != ""
		c.appendText(event, event.Delta, false)
		return wiretap.EventVerdict{Output: bearing}
	case "response.output_text.done":
		bearing := event.Text != ""
		c.replaceText(event, event.Text, false)
		return wiretap.EventVerdict{Output: bearing}
	case "response.refusal.delta":
		bearing := event.Delta != ""
		c.appendText(event, event.Delta, true)
		return wiretap.EventVerdict{Output: bearing}
	case "response.refusal.done":
		bearing := event.Refusal != ""
		c.replaceText(event, event.Refusal, true)
		return wiretap.EventVerdict{Output: bearing}
	case "response.function_call_arguments.delta":
		bearing := event.Delta != ""
		if item := c.itemFor(event); item != nil && !item.tombstone && c.charge(len(event.Delta)) {
			item.kind = "function_call"
			item.args.WriteString(event.Delta)
		}
		return wiretap.EventVerdict{Output: bearing}
	case "response.function_call_arguments.done":
		bearing := event.Arguments != ""
		if item := c.itemFor(event); item != nil && !item.tombstone && c.charge(len(event.Arguments)) {
			item.kind = "function_call"
			item.args.Reset()
			item.args.WriteString(event.Arguments)
		}
		return wiretap.EventVerdict{Output: bearing}
	case "response.reasoning_summary_text.delta":
		if item := c.itemFor(event); item != nil && !item.tombstone && c.charge(len(event.Delta)) {
			item.kind = "reasoning"
			item.summary.WriteString(event.Delta)
		}
		return wiretap.EventVerdict{} // reasoning never stamps TTFT
	case "response.audio.delta":
		c.audioPresent = true
		return wiretap.EventVerdict{Output: event.Delta != ""}
	case "response.audio.transcript.delta":
		// Transcript text is media-adjacent and not retained in v0.2;
		// dropping non-empty content is declared.
		c.audioPresent = true
		if event.Delta != "" {
			c.partial = true
		}
		return wiretap.EventVerdict{Output: event.Delta != ""}
	case "response.image_generation_call.partial_image":
		if item := c.itemFor(event); item != nil {
			item.kind = "image_generation_call"
			item.imaged = true
		}
		return wiretap.EventVerdict{Output: event.PartialImageB64 != ""}
	case "response.output_item.added":
		c.bindItem(event)
		return wiretap.EventVerdict{}
	case "response.output_item.done":
		return wiretap.EventVerdict{Output: c.finalizeItem(event)}
	case "response.content_part.added", "response.content_part.done":
		return wiretap.EventVerdict{Output: c.replacePart(event)}
	default:
		// Lifecycle and future event types: continue.
		return wiretap.EventVerdict{}
	}
}

// consumeTerminal parses the embedded final Response BEFORE returning
// the hard verdict (the wrapper freezes the parser afterwards). The
// event type is primary; a class mismatch against the embedded status
// takes the MORE SEVERE outcome and declares partial telemetry.
func (c *responsesCall) consumeTerminal(event *responsesStreamEvent) wiretap.EventVerdict {
	var body responsesBody
	embeddedStatus := ""
	if len(event.Response) != 0 && json.Unmarshal(event.Response, &body) == nil {
		embeddedStatus = body.Status
		c.applyBody(&body, true)
	} else {
		c.partial = true
	}
	eventClass := strings.TrimPrefix(event.Type, "response.")
	verdictClass, mismatch := moreSevereClass(eventClass, embeddedStatus)
	if mismatch {
		c.partial = true
	}
	switch verdictClass {
	case "failed":
		if c.errorCategory == "" {
			c.errorCategory = "provider error"
		}
		return wiretap.EventVerdict{Terminal: wiretap.TerminalError}
	case "incomplete":
		return wiretap.EventVerdict{Terminal: wiretap.TerminalIncomplete}
	default:
		return wiretap.EventVerdict{Terminal: wiretap.TerminalSuccess}
	}
}

// moreSevereClass ranks failed > incomplete > completed. The embedded
// SDK statuses in_progress, queued, cancelled, and unknown non-empty
// values rank as incomplete; an absent embedded status never
// conflicts.
func moreSevereClass(eventClass, embeddedStatus string) (string, bool) {
	rank := func(class string) int {
		switch class {
		case "failed":
			return 2
		case "incomplete":
			return 1
		default:
			return 0
		}
	}
	var embeddedClass string
	switch embeddedStatus {
	case "":
		return eventClass, false
	case "completed":
		embeddedClass = "completed"
	case "failed":
		embeddedClass = "failed"
	default:
		embeddedClass = "incomplete"
	}
	if rank(embeddedClass) > rank(eventClass) {
		return embeddedClass, true
	}
	return eventClass, embeddedClass != eventClass
}

// itemFor returns the bounded fallback state for the event's
// output_index, enforcing the index domain and the index-to-item_id
// binding: a conflicting binding rejects the event without mutating
// prior state.
func (c *responsesCall) itemFor(event *responsesStreamEvent) *responseItemState {
	if event.OutputIndex == nil {
		c.partial = true
		return nil
	}
	index := *event.OutputIndex
	if index < 0 || index >= maxResponseItems {
		c.partial = true
		return nil
	}
	if c.items == nil {
		c.items = make(map[int]*responseItemState)
		c.idToIndex = make(map[string]int)
	}
	item, exists := c.items[index]
	if !exists {
		if len(c.items) >= maxResponseItems {
			c.partial = true
			return nil
		}
		item = &responseItemState{kind: "unknown"}
		c.items[index] = item
		c.itemOrder = append(c.itemOrder, index)
	}
	if event.ItemID != "" {
		if item.id == "" {
			if bound, taken := c.idToIndex[event.ItemID]; taken && bound != index {
				c.partial = true
				return nil
			}
			item.id = event.ItemID
			c.idToIndex[event.ItemID] = index
		} else if item.id != event.ItemID {
			c.partial = true
			return nil
		}
	}
	return item
}

func (c *responsesCall) appendText(event *responsesStreamEvent, text string, refusal bool) {
	item := c.itemFor(event)
	if item == nil || item.tombstone || event.ContentIndex == nil {
		if event.ContentIndex == nil && item != nil {
			c.partial = true
		}
		return
	}
	builder := item.part(*event.ContentIndex, refusal, c)
	if builder == nil || !c.charge(len(text)) {
		return
	}
	item.kind = "message"
	builder.WriteString(text)
}

func (c *responsesCall) replaceText(event *responsesStreamEvent, text string, refusal bool) {
	item := c.itemFor(event)
	if item == nil || item.tombstone || event.ContentIndex == nil {
		if event.ContentIndex == nil && item != nil {
			c.partial = true
		}
		return
	}
	builder := item.part(*event.ContentIndex, refusal, c)
	if builder == nil || !c.charge(len(text)) {
		return
	}
	item.kind = "message"
	builder.Reset()
	builder.WriteString(text)
}

// part returns the bounded per-content_index builder.
func (i *responseItemState) part(index int, refusal bool, c *responsesCall) *strings.Builder {
	if index < 0 || index >= maxContentParts {
		c.partial = true
		return nil
	}
	target := &i.texts
	if refusal {
		target = &i.refusals
	}
	if *target == nil {
		*target = make(map[int]*strings.Builder)
	}
	builder, exists := (*target)[index]
	if !exists {
		builder = &strings.Builder{}
		(*target)[index] = builder
		if !containsInt(i.partOrder, index) {
			i.partOrder = append(i.partOrder, index)
		}
	}
	return builder
}

func containsInt(values []int, value int) bool {
	return slices.Contains(values, value)
}

// bindItem records type and identity from output_item.added without
// retaining content (added items are initial versions).
func (c *responsesCall) bindItem(event *responsesStreamEvent) {
	item := c.itemFor(event)
	if item == nil || len(event.Item) == 0 {
		return
	}
	var head struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}
	if json.Unmarshal(event.Item, &head) != nil {
		return
	}
	if head.Type != "" {
		if knownResponsesOutputTypes[head.Type] {
			item.kind = head.Type
		} else {
			item.kind = "unknown"
		}
	}
	if head.ID != "" && item.id == "" {
		if bound, taken := c.idToIndex[head.ID]; !taken || bound == *event.OutputIndex {
			item.id = head.ID
			c.idToIndex[head.ID] = *event.OutputIndex
		}
	}
}

// finalizeItem applies output_item.done: the finalized item atomically
// replaces all accumulated state at its index; an item over the byte
// cap becomes a fixed tombstone that later stale deltas cannot
// resurrect. Returns whether the sanitized item carries visible
// output.
func (c *responsesCall) finalizeItem(event *responsesStreamEvent) bool {
	item := c.itemFor(event)
	if item == nil || len(event.Item) == 0 {
		return false
	}
	if len(event.Item) > maxItemBytes {
		var head struct {
			Type string `json:"type"`
		}
		kind := "unknown"
		if json.Unmarshal(event.Item[:min(len(event.Item), 4096)], &head) == nil && knownResponsesOutputTypes[head.Type] {
			kind = head.Type
		}
		_ = kind
		item.tombstone = true
		item.done = map[string]any{"type": item.kindOr(kind), "omitted": true}
		item.texts, item.refusals, item.partOrder = nil, nil, nil
		item.args.Reset()
		item.summary.Reset()
		c.partial = true
		return false
	}
	sanitized, bearing, partial := sanitizeResponsesOutputItem(event.Item)
	if partial {
		c.partial = true
	}
	item.tombstone = false
	item.done = sanitized
	item.texts, item.refusals, item.partOrder = nil, nil, nil
	item.args.Reset()
	item.summary.Reset()
	if kind, ok := sanitized.(map[string]any)["type"].(string); ok {
		item.kind = kind
	}
	return bearing
}

func (i *responseItemState) kindOr(kind string) string {
	if i.kind != "" && i.kind != "unknown" {
		return i.kind
	}
	return kind
}

// replacePart applies content_part.added/done: the part replaces the
// accumulated value at its (output_index, content_index).
func (c *responsesCall) replacePart(event *responsesStreamEvent) bool {
	if len(event.Part) == 0 {
		return false
	}
	var part struct {
		Type    string `json:"type"`
		Text    string `json:"text"`
		Refusal string `json:"refusal"`
	}
	if json.Unmarshal(event.Part, &part) != nil {
		c.partial = true
		return false
	}
	switch part.Type {
	case "output_text":
		c.replaceText(event, part.Text, false)
		return part.Text != ""
	case "refusal":
		c.replaceText(event, part.Refusal, true)
		return part.Refusal != ""
	default:
		return false
	}
}

// charge counts retained fallback bytes against the capture cap; over
// the cap, fallback output is dropped entirely (never truncated).
func (c *responsesCall) charge(n int) bool {
	if c.outputBytes+n > c.captureCap {
		c.partial = true
		c.items = nil
		c.itemOrder = nil
		c.idToIndex = nil
		return false
	}
	c.outputBytes += n
	return true
}

// --- unary ---

func (c *responsesCall) FinishUnary(body []byte, httpStatus int) {
	if len(body) == 0 {
		return
	}
	if httpStatus >= 400 {
		var errorBody any
		if json.Unmarshal(body, &errorBody) == nil {
			c.unaryOutput = sanitizeValue(errorBody)
			c.haveUnary = true
		}
		return
	}
	grammar := newControlScanner(scanUnaryRoot)
	grammar.feed(body)
	if !grammar.documentUsable() {
		// Malformed or schema-invalid body: partial telemetry, no
		// status extracted, lifecycle from the wire only.
		c.partial = true
		return
	}
	var response responsesBody
	if err := json.Unmarshal(body, &response); err != nil {
		c.partial = true
		return
	}
	c.applyBody(&response, false)
	c.applyUnaryStatus(response.Status)
}

type responsesBody struct {
	Status string            `json:"status"`
	Model  string            `json:"model"`
	Usage  *responsesUsage   `json:"usage"`
	Output []json.RawMessage `json:"output"`
	Error  *responsesError   `json:"error"`
}

type responsesError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type responsesUsage struct {
	InputTokens        *int64 `json:"input_tokens"`
	OutputTokens       *int64 `json:"output_tokens"`
	TotalTokens        *int64 `json:"total_tokens"`
	InputTokensDetails struct {
		CachedTokens *int64 `json:"cached_tokens"`
	} `json:"input_tokens_details"`
	OutputTokensDetails struct {
		ReasoningTokens *int64 `json:"reasoning_tokens"`
	} `json:"output_tokens_details"`
}

// applyBody folds an embedded or unary final Response into the call:
// model, presence-preserving usage, sanitized error, and, when
// authoritative, the sanitized output array replacing all incremental
// state.
func (c *responsesCall) applyBody(body *responsesBody, authoritativeOutput bool) {
	if body.Model != "" {
		c.responseModel = body.Model
	}
	if body.Usage != nil {
		if usage, ok := mapResponsesUsage(body.Usage); ok {
			c.usage = usage
		} else {
			c.partial = true
		}
	}
	if body.Error != nil && (body.Error.Code != "" || body.Error.Message != "") {
		c.errorCategory = "provider error"
		c.errorOutput = map[string]any{
			"type": "error", "code": body.Error.Code, "message": body.Error.Message,
		}
	}
	if body.Output != nil {
		outputs := make([]any, 0, min(len(body.Output), maxResponseItems))
		for index, raw := range body.Output {
			if index >= maxResponseItems {
				c.partial = true
				break
			}
			if len(raw) > maxItemBytes {
				outputs = append(outputs, map[string]any{"type": "unknown", "omitted": true})
				c.partial = true
				continue
			}
			sanitized, _, partial := sanitizeResponsesOutputItem(raw)
			if partial {
				c.partial = true
			}
			outputs = append(outputs, sanitized)
		}
		if authoritativeOutput {
			c.finalOutput = outputs
			c.haveFinal = true
		} else {
			c.unaryOutput = outputs
			c.haveUnary = true
		}
	}
}

// applyUnaryStatus maps the body status field per the terminal
// contract; parser flags refine only an otherwise-complete lifecycle.
func (c *responsesCall) applyUnaryStatus(status string) {
	switch status {
	case "", "completed":
	case "failed":
		if c.errorCategory == "" {
			c.errorCategory = "provider error"
		}
	case "incomplete":
		c.incomplete = true
	default:
		// in_progress, queued, cancelled, or an unknown status is
		// unexpected in a synchronous unary response.
		c.incomplete = true
		c.partial = true
	}
}

// mapResponsesUsage maps the presence-preserving wire usage onto the
// core inclusive semantics: input and output are inclusive totals, the
// two details are subset breakdowns, and nothing is ever added.
// Missing or negative totals drop the usage entirely.
func mapResponsesUsage(usage *responsesUsage) (*langfuse.Usage, bool) {
	if usage.InputTokens == nil || usage.OutputTokens == nil {
		return nil, false
	}
	if *usage.InputTokens < 0 || *usage.OutputTokens < 0 {
		return nil, false
	}
	result := &langfuse.Usage{
		InputTokens:  *usage.InputTokens,
		OutputTokens: *usage.OutputTokens,
	}
	if cached := usage.InputTokensDetails.CachedTokens; cached != nil && *cached >= 0 {
		result.CacheReadInputTokens = *cached
	}
	if reasoning := usage.OutputTokensDetails.ReasoningTokens; reasoning != nil && *reasoning >= 0 {
		result.ReasoningOutputTokens = *reasoning
	}
	return result, true
}

// sanitizeResponsesOutputItem applies the closed v0.2 output schema to
// one item. It reports whether the item carries visible output and
// whether the policy degraded telemetry (placeholder emitted).
func sanitizeResponsesOutputItem(raw json.RawMessage) (any, bool, bool) {
	var item struct {
		Type    string `json:"type"`
		Role    string `json:"role"`
		Content []struct {
			Type    string `json:"type"`
			Text    string `json:"text"`
			Refusal string `json:"refusal"`
		} `json:"content"`
		Name      string          `json:"name"`
		Arguments string          `json:"arguments"`
		CallID    string          `json:"call_id"`
		Summary   json.RawMessage `json:"summary"`
	}
	if err := json.Unmarshal(raw, &item); err != nil {
		return map[string]any{"type": "unknown", "omitted": true}, false, true
	}
	switch item.Type {
	case "message":
		content := make([]any, 0, len(item.Content))
		bearing := false
		for _, part := range item.Content {
			switch part.Type {
			case "output_text":
				content = append(content, map[string]any{"type": "output_text", "text": part.Text})
				bearing = bearing || part.Text != ""
			case "refusal":
				content = append(content, map[string]any{"type": "refusal", "refusal": part.Refusal})
				bearing = bearing || part.Refusal != ""
			}
		}
		return map[string]any{"type": "message", "role": item.Role, "content": content}, bearing, false
	case "function_call":
		return map[string]any{
			"type": "function_call", "name": item.Name,
			"arguments": item.Arguments, "call_id": item.CallID,
		}, item.Arguments != "" || item.Name != "", false
	case "reasoning":
		return map[string]any{
			"type": "reasoning", "thought": true,
			"summary": sanitizeReasoningSummary(item.Summary),
		}, false, false
	default:
		kind := "unknown"
		if knownResponsesOutputTypes[item.Type] {
			kind = item.Type
		}
		return map[string]any{"type": kind, "omitted": true}, false, true
	}
}

// sanitizeReasoningSummary retains visible summary text only;
// encrypted content and every other field never appear.
func sanitizeReasoningSummary(raw json.RawMessage) []any {
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &parts) != nil {
		return nil
	}
	summary := make([]any, 0, len(parts))
	for _, part := range parts {
		if part.Text != "" {
			summary = append(summary, part.Text)
		}
	}
	return summary
}

// --- over-cap salvage (wiretap.ChunkedCall) ---

func (c *responsesCall) BeginOversizedUnary() {
	c.scanner = newControlScanner(scanUnaryRoot)
}

func (c *responsesCall) FeedOversized(p []byte) {
	if c.scanner == nil {
		c.scanner = newControlScanner(scanSSEEnvelope)
	}
	c.scanner.feed(p)
}

// FinishOversizedEvent derives the verdict for one over-cap SSE event
// from the bounded scan. Every oversized data-bearing event degrades
// telemetry by construction (its semantic content was not retained).
func (c *responsesCall) FinishOversizedEvent() wiretap.EventVerdict {
	scanner := c.scanner
	c.scanner = nil
	c.sawEvents = true
	c.partial = true
	if scanner == nil || !scanner.documentUsable() {
		return wiretap.EventVerdict{}
	}
	eventType, ok := decodeScannedString(scanner.fields["type"])
	if !ok {
		return wiretap.EventVerdict{}
	}
	if scanner.fieldOver {
		c.partial = true
	}
	verdict := wiretap.EventVerdict{Output: oversizedOutputPresence(eventType, scanner)}
	switch eventType {
	case "response.completed", "response.failed", "response.incomplete":
		c.applyScannedControl(scanner)
		eventClass := strings.TrimPrefix(eventType, "response.")
		status, _ := decodeScannedString(scanner.fields["status"])
		verdictClass, mismatch := moreSevereClass(eventClass, status)
		if mismatch {
			c.partial = true
		}
		switch verdictClass {
		case "failed":
			if c.errorCategory == "" {
				c.errorCategory = "provider error"
			}
			verdict.Terminal = wiretap.TerminalError
		case "incomplete":
			verdict.Terminal = wiretap.TerminalIncomplete
		default:
			verdict.Terminal = wiretap.TerminalSuccess
		}
	case "error":
		c.errorCategory = "provider error"
		if raw, ok := scannedRaw(scanner.fields["error"]); ok {
			var detail any
			if json.Unmarshal(raw, &detail) == nil {
				c.errorOutput = sanitizeValue(detail)
			}
		}
		verdict.Terminal = wiretap.TerminalError
	}
	return verdict
}

// oversizedOutputPresence publishes Output only for recognized
// (event type, JSON path) pairs whose non-empty presence the scan
// observed; unknown event types never stamp TTFT.
func oversizedOutputPresence(eventType string, scanner *controlScanner) bool {
	switch eventType {
	case "response.output_text.delta", "response.refusal.delta",
		"response.function_call_arguments.delta",
		"response.audio.delta", "response.audio.transcript.delta":
		return scanner.presentTop["delta"]
	case "response.output_text.done":
		return scanner.presentTop["text"]
	case "response.refusal.done":
		return scanner.presentTop["refusal"]
	case "response.function_call_arguments.done":
		return scanner.presentTop["arguments"]
	case "response.image_generation_call.partial_image":
		return scanner.presentTop["partial_image_b64"]
	case "response.content_part.added", "response.content_part.done",
		"response.output_item.done",
		"response.completed", "response.failed", "response.incomplete":
		return scanner.presentNested
	default:
		return false
	}
}

// applyScannedControl folds salvaged control fields into the call:
// status/model/usage/error survive; over-cap output stays with the
// incremental fallback state.
func (c *responsesCall) applyScannedControl(scanner *controlScanner) {
	if model, ok := decodeScannedString(scanner.fields["model"]); ok && model != "" {
		c.responseModel = model
	}
	if raw, ok := scannedRaw(scanner.fields["usage"]); ok {
		var usage responsesUsage
		if json.Unmarshal(raw, &usage) == nil {
			if mapped, valid := mapResponsesUsage(&usage); valid {
				c.usage = mapped
			} else {
				c.partial = true
			}
		}
	}
	if raw, ok := scannedRaw(scanner.fields["error"]); ok {
		var wireError responsesError
		if json.Unmarshal(raw, &wireError) == nil &&
			(wireError.Code != "" || wireError.Message != "") {
			c.errorCategory = "provider error"
			c.errorOutput = map[string]any{
				"type": "error", "code": wireError.Code, "message": wireError.Message,
			}
		}
	}
}

func (c *responsesCall) FinishOversizedUnary(httpStatus int) {
	scanner := c.scanner
	c.scanner = nil
	c.partial = true
	c.unaryOutput = map[string]any{"omitted": true}
	c.haveUnary = true
	if scanner == nil || !scanner.documentUsable() {
		return
	}
	if httpStatus >= 400 {
		return
	}
	c.applyScannedControl(scanner)
	if status, ok := decodeScannedString(scanner.fields["status"]); ok {
		c.applyUnaryStatus(status)
	}
}

func (c *responsesCall) UnaryComplete() bool {
	return c.scanner != nil && c.scanner.complete && !c.scanner.invalid
}

// decodeScannedString decodes a captured raw field into a string with
// encoding/json semantics (escapes and Unicode included).
func decodeScannedString(raw []byte) (string, bool) {
	trimmed, ok := scannedRaw(raw)
	if !ok {
		return "", false
	}
	var value string
	if json.Unmarshal(trimmed, &value) != nil {
		return "", false
	}
	return value, true
}

// scannedRaw strips the structural colon and whitespace that raw
// capture necessarily includes before the value's first byte.
func scannedRaw(raw []byte) ([]byte, bool) {
	trimmed := bytes.TrimLeft(raw, ": \t\r\n")
	if len(trimmed) == 0 {
		return nil, false
	}
	return trimmed, true
}

// --- result ---

func (c *responsesCall) Result() wiretap.Result {
	result := wiretap.Result{
		Input:            c.input,
		Model:            c.responseModel,
		RequestModel:     c.requestModel,
		ModelParameters:  c.modelParameters,
		Usage:            c.usage,
		ErrorCategory:    c.errorCategory,
		Incomplete:       c.incomplete,
		TelemetryPartial: c.partial,
	}
	switch {
	case c.errorOutput != nil && !c.haveFinal && !c.haveUnary:
		result.Output = c.errorOutput
	case c.haveFinal:
		result.Output = c.renderWithError(c.finalOutput)
	case c.haveUnary:
		if outputs, ok := c.unaryOutput.([]any); ok {
			result.Output = c.renderWithError(outputs)
		} else {
			result.Output = c.unaryOutput
		}
	case c.sawEvents:
		if rendered := c.renderFallback(); len(rendered) != 0 {
			result.Output = c.renderWithError(rendered)
		} else if c.errorOutput != nil {
			result.Output = c.errorOutput
		}
	}
	return result
}

// renderWithError appends the sanitized provider error object to the
// output array so it stays inside the Mask/content-governed channel.
func (c *responsesCall) renderWithError(outputs []any) any {
	if c.errorOutput != nil {
		outputs = append(outputs, c.errorOutput)
	}
	if len(outputs) == 1 {
		return outputs[0]
	}
	if len(outputs) == 0 {
		return nil
	}
	return outputs
}

// renderFallback produces the bounded incremental output when no
// authoritative terminal output was retained: finalized items win at
// their index, then accumulated state, with the route-level audio
// placeholder at the deterministic end position.
func (c *responsesCall) renderFallback() []any {
	indices := append([]int(nil), c.itemOrder...)
	sort.Ints(indices)
	outputs := make([]any, 0, len(indices)+1)
	for _, index := range indices {
		item := c.items[index]
		if item == nil {
			continue
		}
		if item.done != nil {
			outputs = append(outputs, item.done)
			continue
		}
		if rendered := item.render(); rendered != nil {
			outputs = append(outputs, rendered)
		}
	}
	if c.audioPresent {
		outputs = append(outputs, map[string]any{"type": "audio", "omitted": true})
	}
	return outputs
}

// render produces one accumulated (non-finalized) item.
func (i *responseItemState) render() any {
	switch {
	case i.imaged:
		return map[string]any{"type": "image_generation_call", "omitted": true}
	case i.args.Len() > 0 || i.kind == "function_call":
		return map[string]any{
			"type": "function_call", "name": i.name,
			"arguments": i.args.String(), "call_id": i.callID,
		}
	case i.summary.Len() > 0:
		return map[string]any{
			"type": "reasoning", "thought": true,
			"summary": []any{i.summary.String()},
		}
	case len(i.partOrder) > 0:
		order := append([]int(nil), i.partOrder...)
		sort.Ints(order)
		content := make([]any, 0, len(order))
		for _, index := range order {
			if builder, ok := i.texts[index]; ok && builder.Len() > 0 {
				content = append(content, map[string]any{"type": "output_text", "text": builder.String()})
			}
			if builder, ok := i.refusals[index]; ok && builder.Len() > 0 {
				content = append(content, map[string]any{"type": "refusal", "refusal": builder.String()})
			}
		}
		if len(content) == 0 {
			return nil
		}
		return map[string]any{"type": "message", "content": content}
	default:
		return nil
	}
}

// allowedParametersFrom filters a request through an allowlist, keeping
// only JSON numbers and booleans (shared with the chat parser's rule).
func allowedParametersFrom(request map[string]json.RawMessage, allowlist map[string]bool) map[string]any {
	var parameters map[string]any
	for key, raw := range request {
		if !allowlist[key] {
			continue
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			continue
		}
		switch value.(type) {
		case float64, bool:
			if parameters == nil {
				parameters = map[string]any{}
			}
			parameters[key] = value
		}
	}
	return parameters
}
