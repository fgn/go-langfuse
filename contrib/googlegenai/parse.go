package langfusegenai

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/contrib/googlegenai/internal/wiretap"
)

// generationConfigAllowlist is the strict numeric/boolean subset of
// generationConfig exported as ModelParameters. Content-bearing fields
// (stop sequences, response schemas, system instructions, tools) are
// never model parameters.
var generationConfigAllowlist = map[string]bool{
	"temperature":      true,
	"topP":             true,
	"topK":             true,
	"maxOutputTokens":  true,
	"candidateCount":   true,
	"seed":             true,
	"presencePenalty":  true,
	"frequencyPenalty": true,
}

// call accumulates one attempt's parsed request and response for the
// Gemini wire format. Gemini streams have no terminal sentinel: clean
// EOF is protocol success, and finish reasons are data, never
// terminals.
type call struct {
	route      wiretap.Route
	captureCap int

	input           any
	modelParameters map[string]any

	modelVersion  string
	usage         *langfuse.Usage
	finishReasons []string
	unaryOutput   any
	errorCategory string
	partial       bool

	deltas     []strings.Builder
	deltaBytes int
	deltasOver bool
	extraParts []any // sanitized non-text stream parts, bounded
	sawStream  bool
	embeddings int
}

// maxCandidates bounds provider-controlled fan-out; wire fields are
// untrusted and never size allocations.
const maxCandidates = 128

func (c *call) ParseRequest(body []byte) {
	var request map[string]json.RawMessage
	if err := json.Unmarshal(body, &request); err != nil {
		c.partial = true
		return
	}
	if raw, ok := request["generationConfig"]; ok {
		var config map[string]json.RawMessage
		if json.Unmarshal(raw, &config) == nil {
			c.modelParameters = allowedParameters(config)
		}
	}
	input := map[string]any{}
	if raw, ok := request["contents"]; ok {
		var contents any
		if json.Unmarshal(raw, &contents) == nil {
			input["contents"] = sanitizeValue(contents)
		}
	}
	if raw, ok := request["systemInstruction"]; ok {
		var system any
		if json.Unmarshal(raw, &system) == nil {
			input["system_instruction"] = sanitizeValue(system)
		}
	}
	if raw, ok := request["instances"]; ok {
		// Vertex :predict embedding requests carry instances.
		var instances any
		if json.Unmarshal(raw, &instances) == nil {
			input["instances"] = sanitizeValue(instances)
		}
	}
	if raw, ok := request["content"]; ok {
		// embedContent carries a single content.
		var content any
		if json.Unmarshal(raw, &content) == nil {
			input["content"] = sanitizeValue(content)
		}
	}
	if raw, ok := request["requests"]; ok {
		// batchEmbedContents carries per-item requests.
		var requests any
		if json.Unmarshal(raw, &requests) == nil {
			input["requests"] = sanitizeValue(requests)
		}
	}
	switch len(input) {
	case 0:
	case 1:
		if contents, ok := input["contents"]; ok {
			c.input = contents
			return
		}
		c.input = input
	default:
		c.input = input
	}
}

func (c *call) FeedEvent(data []byte) wiretap.EventVerdict {
	if data == nil {
		// Clean-EOF probe: Gemini streams end at transport EOF; a
		// stream that delivered any events completed successfully.
		// An SSE stream with zero events is protocol-incomplete.
		if c.sawStream {
			return wiretap.EventVerdict{Terminal: wiretap.TerminalSuccess}
		}
		return wiretap.EventVerdict{}
	}
	c.sawStream = true
	var chunk wireResponse
	if err := json.Unmarshal(data, &chunk); err != nil {
		c.partial = true
		return wiretap.EventVerdict{}
	}
	if isRealError(chunk.Error) {
		c.errorCategory = "provider error"
		return wiretap.EventVerdict{Terminal: wiretap.TerminalError}
	}
	return wiretap.EventVerdict{Output: c.consumeResponse(&chunk, true)}
}

func (c *call) FinishUnary(body []byte, httpStatus int) {
	if len(body) == 0 {
		return
	}
	if httpStatus >= 400 {
		var errorBody any
		if json.Unmarshal(body, &errorBody) == nil {
			c.unaryOutput = errorBody
		}
		return
	}
	var response wireResponse
	if err := json.Unmarshal(body, &response); err != nil {
		c.partial = true
		return
	}
	c.consumeResponse(&response, false)
	if outputs := c.candidateOutputs(&response); outputs != nil {
		c.unaryOutput = outputs
	}
}

func (c *call) Result() wiretap.Result {
	result := wiretap.Result{
		Input:            c.input,
		Model:            c.modelVersion,
		RequestModel:     c.route.Model,
		ModelParameters:  c.modelParameters,
		Usage:            c.usage,
		ErrorCategory:    c.errorCategory,
		TelemetryPartial: c.partial || c.deltasOver,
	}
	metadata := map[string]any{}
	if len(c.finishReasons) == 1 {
		metadata["finish_reason"] = c.finishReasons[0]
	} else if len(c.finishReasons) > 1 {
		metadata["finish_reason"] = c.finishReasons
	}
	if c.embeddings > 0 {
		metadata["embeddings"] = c.embeddings
	}
	if len(metadata) > 0 {
		result.Metadata = metadata
	}
	if c.unaryOutput != nil {
		result.Output = c.unaryOutput
	} else if c.sawStream && !c.deltasOver {
		var text any
		switch len(c.deltas) {
		case 0:
		case 1:
			text = c.deltas[0].String()
		default:
			texts := make([]any, len(c.deltas))
			for index := range c.deltas {
				texts[index] = c.deltas[index].String()
			}
			text = texts
		}
		switch {
		case text != nil && len(c.extraParts) > 0:
			result.Output = map[string]any{"text": text, "parts": c.extraParts}
		case len(c.extraParts) > 0:
			result.Output = c.extraParts
		case text != nil:
			result.Output = text
		}
	}
	return result
}

// consumeResponse folds one response object (unary body or stream
// chunk) into the accumulated state and reports whether it carried
// output-bearing parts: text, function calls, executable code, or
// media, per the semantic-delta definition.
func (c *call) consumeResponse(response *wireResponse, streaming bool) bool {
	if response.ModelVersion != "" {
		c.modelVersion = response.ModelVersion
	}
	if response.UsageMetadata != nil {
		c.usage = mapUsage(response.UsageMetadata)
	}
	if count := len(response.Embeddings); count > 0 {
		c.embeddings = count
	}
	if response.Embedding != nil {
		c.embeddings = 1
	}
	if count := len(response.Predictions); count > 0 {
		c.embeddings = count
	}
	output := false
	for index, candidate := range response.Candidates {
		// Output charges before finish metadata within one candidate
		// (review round 4 finding 22).
		for _, part := range candidate.Content.Parts {
			if streaming && part.Thought && part.Text != "" {
				// Thought parts are reasoning: retained marked in
				// exported content (the documented contract) but never
				// output-bearing, so they do not stamp
				// time-to-first-token.
				c.retainExtra(part.sanitized())
				continue
			}
			if !part.outputBearing() {
				continue
			}
			output = true
			if !streaming {
				continue
			}
			if part.Text != "" {
				c.appendDelta(index, part.Text)
			} else {
				// Non-text output (function calls, executable code,
				// media placeholders) is preserved sanitized so a
				// tool-only stream still exports Output.
				c.retainExtra(part.sanitized())
			}
		}
		if candidate.FinishReason != "" {
			c.retainFinishReason(candidate.FinishReason)
		}
	}
	return output
}

func (c *call) candidateOutputs(response *wireResponse) any {
	var outputs []any
	for _, candidate := range response.Candidates {
		if len(candidate.Content.Parts) == 0 {
			continue
		}
		parts := make([]any, 0, len(candidate.Content.Parts))
		for _, part := range candidate.Content.Parts {
			parts = append(parts, part.sanitized())
		}
		outputs = append(outputs, map[string]any{
			"role": candidate.Content.Role, "parts": parts,
		})
	}
	switch len(outputs) {
	case 0:
		return nil
	case 1:
		return outputs[0]
	default:
		return outputs
	}
}

func (c *call) appendDelta(index int, text string) {
	if c.deltasOver {
		return
	}
	if index < 0 || index >= maxCandidates {
		c.partial = true
		return
	}
	if !c.charge(len(text)) {
		return
	}
	for len(c.deltas) <= index {
		c.deltas = append(c.deltas, strings.Builder{})
	}
	c.deltas[index].WriteString(text)
}

// retainExtra keeps a sanitized structured part, byte-accounted
// against the same capture cap as text output: decoded provider
// objects must not bypass the advertised response ceiling.
func (c *call) retainExtra(part any) {
	if c.deltasOver || len(c.extraParts) >= maxCandidates {
		return
	}
	rendered, err := json.Marshal(part)
	if err != nil {
		c.partial = true
		return
	}
	if !c.charge(len(rendered)) {
		return
	}
	c.extraParts = append(c.extraParts, part)
}

// retainFinishReason retains a finish reason under the shared byte
// ceiling with a field-aware failure: an over-budget reason drops only
// the reason (marking partial telemetry) and never clears valid
// accumulated output.
func (c *call) retainFinishReason(reason string) {
	if len(c.finishReasons) >= maxCandidates {
		return
	}
	if c.deltaBytes+len(reason) > c.captureCap {
		c.partial = true
		return
	}
	c.deltaBytes += len(reason)
	c.finishReasons = append(c.finishReasons, reason)
}

// charge counts retained bytes against the capture cap; over the cap,
// streamed output capture is dropped entirely (never truncated).
func (c *call) charge(n int) bool {
	if c.deltaBytes+n > c.captureCap {
		c.deltasOver = true
		c.deltas = nil
		c.extraParts = nil
		return false
	}
	c.deltaBytes += n
	return true
}

type wireResponse struct {
	Error         json.RawMessage   `json:"error"`
	ModelVersion  string            `json:"modelVersion"`
	UsageMetadata *wireUsage        `json:"usageMetadata"`
	Candidates    []wireCandidate   `json:"candidates"`
	Embedding     json.RawMessage   `json:"embedding"`
	Embeddings    []json.RawMessage `json:"embeddings"`
	Predictions   []json.RawMessage `json:"predictions"`
}

type wireCandidate struct {
	FinishReason string `json:"finishReason"`
	Content      struct {
		Role  string     `json:"role"`
		Parts []wirePart `json:"parts"`
	} `json:"content"`
}

type wirePart struct {
	Text            string          `json:"text"`
	Thought         bool            `json:"thought"`
	FunctionCall    json.RawMessage `json:"functionCall"`
	ExecutableCode  json.RawMessage `json:"executableCode"`
	CodeExecution   json.RawMessage `json:"codeExecutionResult"`
	InlineData      json.RawMessage `json:"inlineData"`
	FileData        json.RawMessage `json:"fileData"`
	FunctionResults json.RawMessage `json:"functionResponse"`
}

// outputBearing implements the semantic-delta definition for Gemini:
// any output Part variant counts; thought-only parts are reasoning,
// not output.
func (p wirePart) outputBearing() bool {
	if p.Thought {
		return false
	}
	return p.Text != "" || len(p.FunctionCall) > 0 || len(p.ExecutableCode) > 0 ||
		len(p.CodeExecution) > 0 || len(p.InlineData) > 0 || len(p.FileData) > 0
}

// sanitized renders a part for export with media placeholders: inline
// bytes and file references never leave the adapter.
func (p wirePart) sanitized() any {
	switch {
	case len(p.InlineData) > 0 || len(p.FileData) > 0:
		return map[string]any{"media": "omitted"}
	case p.Text != "":
		if p.Thought {
			return map[string]any{"thought": true, "text": p.Text}
		}
		return map[string]any{"text": p.Text}
	case len(p.FunctionCall) > 0:
		return map[string]any{"functionCall": rawValue(p.FunctionCall)}
	case len(p.ExecutableCode) > 0:
		return map[string]any{"executableCode": rawValue(p.ExecutableCode)}
	case len(p.CodeExecution) > 0:
		return map[string]any{"codeExecutionResult": rawValue(p.CodeExecution)}
	case len(p.FunctionResults) > 0:
		return map[string]any{"functionResponse": rawValue(p.FunctionResults)}
	default:
		return map[string]any{}
	}
}

// isRealError distinguishes an actual error object from an explicit
// JSON null placeholder.
func isRealError(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

func rawValue(raw json.RawMessage) any {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return nil
	}
	return value
}

type wireUsage struct {
	PromptTokenCount        int64 `json:"promptTokenCount"`
	CandidatesTokenCount    int64 `json:"candidatesTokenCount"`
	CachedContentTokenCount int64 `json:"cachedContentTokenCount"`
	ThoughtsTokenCount      int64 `json:"thoughtsTokenCount"`
	ToolUsePromptTokenCount int64 `json:"toolUsePromptTokenCount"`
}

// mapUsage converts Gemini usage to the core inclusive semantics.
// Gemini's candidatesTokenCount excludes thought tokens, while the
// core OutputTokens is inclusive of reasoning, so the two are summed;
// promptTokenCount already includes cached content but excludes
// tool-use prompt tokens, which the SDK's own total adds separately,
// so they join the inclusive input side with their own detail bucket.
func mapUsage(usage *wireUsage) *langfuse.Usage {
	mapped := &langfuse.Usage{
		InputTokens:           usage.PromptTokenCount + usage.ToolUsePromptTokenCount,
		OutputTokens:          usage.CandidatesTokenCount + usage.ThoughtsTokenCount,
		CacheReadInputTokens:  usage.CachedContentTokenCount,
		ReasoningOutputTokens: usage.ThoughtsTokenCount,
	}
	if usage.ToolUsePromptTokenCount > 0 {
		mapped.Details = map[string]int64{
			"input_tool_use_tokens": usage.ToolUsePromptTokenCount,
		}
	}
	return mapped
}

func allowedParameters(config map[string]json.RawMessage) map[string]any {
	var parameters map[string]any
	for key, raw := range config {
		if !generationConfigAllowlist[key] {
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

// sanitizeValue replaces media-bearing request structures (inlineData,
// fileData) with fixed placeholders during parsing, before export.
func sanitizeValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		if _, ok := value["inlineData"]; ok {
			return map[string]any{"media": "omitted"}
		}
		if _, ok := value["fileData"]; ok {
			return map[string]any{"media": "omitted"}
		}
		sanitized := make(map[string]any, len(value))
		for key, item := range value {
			sanitized[key] = sanitizeValue(item)
		}
		return sanitized
	case []any:
		sanitized := make([]any, len(value))
		for index, item := range value {
			sanitized[index] = sanitizeValue(item)
		}
		return sanitized
	default:
		return value
	}
}
