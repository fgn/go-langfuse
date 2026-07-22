package langfuseopenai

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/fgn/go-langfuse"
	"github.com/fgn/go-langfuse/contrib/openai/internal/wiretap"
)

// doneSentinel is the OpenAI streaming success terminal. It is the only
// event that proves a chat/completions stream finished; clean EOF
// without it is protocol-incomplete.
var doneSentinel = []byte("[DONE]")

// modelParameterAllowlist is the strict numeric/boolean set exported as
// ModelParameters. Content-bearing request fields (stop sequences, tool
// definitions, schemas, user identifiers) are never model parameters.
var modelParameterAllowlist = map[string]bool{
	"temperature":           true,
	"top_p":                 true,
	"max_tokens":            true,
	"max_completion_tokens": true,
	"n":                     true,
	"presence_penalty":      true,
	"frequency_penalty":     true,
	"seed":                  true,
	"logprobs":              true,
	"top_logprobs":          true,
}

// call accumulates one attempt's parsed request and response.
type call struct {
	route      wiretap.Route
	captureCap int

	input           any
	requestModel    string
	modelParameters map[string]any

	responseModel string
	usage         *langfuse.Usage
	finishReasons []string
	unaryOutput   any
	errorCategory string
	partial       bool

	// Streaming accumulation, bounded by captureCap in total and by
	// maxChoices in fan-out.
	choices      []choiceAccumulator
	deltaBytes   int
	deltasOver   bool
	sawDone      bool
	streamEvents bool
	embeddings   int
}

// choiceAccumulator gathers one choice's streamed output: text and
// per-tool-index call accumulation, so parallel tool calls export as
// distinct structured calls rather than one joined string.
type choiceAccumulator struct {
	text      strings.Builder
	toolOrder []int
	tools     map[int]*toolAccumulator
}

type toolAccumulator struct {
	name string
	args strings.Builder
}

func (c *call) ParseRequest(body []byte) {
	var request map[string]json.RawMessage
	if err := json.Unmarshal(body, &request); err != nil {
		c.partial = true
		return
	}
	if raw, ok := request["model"]; ok {
		_ = json.Unmarshal(raw, &c.requestModel)
	}
	c.modelParameters = allowedParameters(request)
	switch c.route.Name {
	case "openai.embeddings":
		if raw, ok := request["input"]; ok {
			var input any
			if json.Unmarshal(raw, &input) == nil {
				c.input = input
			}
		}
	default:
		if raw, ok := request["messages"]; ok {
			var messages []any
			if json.Unmarshal(raw, &messages) == nil {
				c.input = sanitizeMessages(messages)
			}
		} else if raw, ok := request["prompt"]; ok {
			var prompt any
			if json.Unmarshal(raw, &prompt) == nil {
				c.input = prompt
			}
		}
	}
}

func (c *call) FeedEvent(data []byte) wiretap.EventVerdict {
	if data == nil {
		// Clean-EOF probe: OpenAI streams require the [DONE] sentinel.
		if c.sawDone {
			return wiretap.EventVerdict{Terminal: wiretap.TerminalSuccess}
		}
		return wiretap.EventVerdict{}
	}
	c.streamEvents = true
	if bytes.Equal(bytes.TrimSpace(data), doneSentinel) {
		c.sawDone = true
		return wiretap.EventVerdict{Terminal: wiretap.TerminalSuccess}
	}
	var chunk streamChunk
	if err := json.Unmarshal(data, &chunk); err != nil {
		c.partial = true
		return wiretap.EventVerdict{}
	}
	if isRealError(chunk.Error) {
		c.errorCategory = "provider error"
		return wiretap.EventVerdict{Terminal: wiretap.TerminalError}
	}
	if chunk.Model != "" {
		c.responseModel = chunk.Model
	}
	if chunk.Usage != nil {
		c.usage = mapUsage(chunk.Usage)
	}
	output := false
	for _, choice := range chunk.Choices {
		if choice.FinishReason != "" {
			c.appendFinishReason(choice.FinishReason)
		}
		if choice.consumeInto(c) {
			output = true
		}
	}
	return wiretap.EventVerdict{Output: output}
}

func (c *call) FinishUnary(body []byte, httpStatus int) {
	if len(body) == 0 {
		return
	}
	if httpStatus >= 400 {
		// Provider error bodies are exported only as Output, where the
		// capture switch and Mask govern them like any content.
		var errorBody any
		if json.Unmarshal(body, &errorBody) == nil {
			c.unaryOutput = errorBody
		}
		return
	}
	var response unaryResponse
	if err := json.Unmarshal(body, &response); err != nil {
		c.partial = true
		return
	}
	if response.Model != "" {
		c.responseModel = response.Model
	}
	if response.Usage != nil {
		c.usage = mapUsage(response.Usage)
	}
	if len(response.Data) > 0 {
		c.embeddings = len(response.Data)
		return
	}
	var outputs []any
	for _, choice := range response.Choices {
		if choice.FinishReason != "" {
			c.appendFinishReason(choice.FinishReason)
		}
		if choice.Message != nil {
			outputs = append(outputs, sanitizeValue(choice.Message))
		} else if choice.Text != "" {
			outputs = append(outputs, choice.Text)
		}
	}
	switch len(outputs) {
	case 0:
	case 1:
		c.unaryOutput = outputs[0]
	default:
		c.unaryOutput = outputs
	}
}

func (c *call) Result() wiretap.Result {
	result := wiretap.Result{
		Input: c.input,
		// Only the response model is eligible for the Model field; a
		// request-body model must stay Mask-governed (metadata).
		Model:           c.responseModel,
		RequestModel:    c.requestModel,
		ModelParameters: c.modelParameters,
		Usage:           c.usage,
		ErrorCategory:   c.errorCategory,
		TelemetryPartial: c.partial ||
			c.deltasOver,
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
	} else if c.streamEvents && !c.deltasOver {
		outputs := make([]any, 0, len(c.choices))
		structured := false
		for index := range c.choices {
			rendered := c.choices[index].render()
			if _, plain := rendered.(string); !plain {
				structured = true
			}
			outputs = append(outputs, rendered)
		}
		switch {
		case len(outputs) == 0:
		case len(outputs) == 1:
			result.Output = outputs[0]
		default:
			result.Output = outputs
		}
		_ = structured
	}
	return result
}

// maxChoices bounds provider-controlled fan-out: choice indices,
// candidate slots, and finish-reason accumulation. Wire fields are
// untrusted input and must never size allocations.
const maxChoices = 128

// choiceAt returns the bounded accumulator for a provider-supplied
// index, or nil when the index is hostile: wire fields never size
// allocations.
func (c *call) choiceAt(index int) *choiceAccumulator {
	if c.deltasOver {
		return nil
	}
	if index < 0 || index >= maxChoices {
		c.partial = true
		return nil
	}
	for len(c.choices) <= index {
		c.choices = append(c.choices, choiceAccumulator{})
	}
	return &c.choices[index]
}

// charge counts retained output bytes against the capture cap; over
// the cap, streamed output is dropped entirely (never truncated).
func (c *call) charge(n int) bool {
	if c.deltaBytes+n > c.captureCap {
		c.deltasOver = true
		c.choices = nil
		return false
	}
	c.deltaBytes += n
	return true
}

func (c *call) appendFinishReason(reason string) {
	if len(c.finishReasons) < maxChoices && c.charge(len(reason)) {
		c.finishReasons = append(c.finishReasons, reason)
	}
}

// consumeInto folds one stream choice into the call state and reports
// whether it carried output-bearing content.
func (choice streamChoice) consumeInto(c *call) bool {
	if choice.Text != "" {
		if acc := c.choiceAt(choice.Index); acc != nil && c.charge(len(choice.Text)) {
			acc.text.WriteString(choice.Text)
		}
		return true
	}
	delta := choice.Delta
	if delta == nil {
		return false
	}
	bearing := false
	switch {
	case delta.Content != "":
		bearing = true
		if acc := c.choiceAt(choice.Index); acc != nil && c.charge(len(delta.Content)) {
			acc.text.WriteString(delta.Content)
		}
	case delta.Refusal != "":
		bearing = true
		if acc := c.choiceAt(choice.Index); acc != nil && c.charge(len(delta.Refusal)) {
			acc.text.WriteString(delta.Refusal)
		}
	case len(delta.Audio) > 0 && !bytes.Equal(bytes.TrimSpace(delta.Audio), []byte("null")):
		bearing = true // audio bytes are media and never accumulate
	}
	if delta.FunctionCall != nil {
		bearing = true
		if acc := c.choiceAt(choice.Index); acc != nil {
			acc.tool(0, delta.FunctionCall.Name, delta.FunctionCall.Arguments, c)
		}
	}
	for _, tool := range delta.ToolCalls {
		if tool.Function.Name == "" && tool.Function.Arguments == "" {
			continue
		}
		bearing = true
		if acc := c.choiceAt(choice.Index); acc != nil {
			acc.tool(tool.Index, tool.Function.Name, tool.Function.Arguments, c)
		}
	}
	return bearing
}

// tool accumulates one tool-call element by its wire index, keeping
// parallel calls distinct. Hostile indices degrade to partial.
func (a *choiceAccumulator) tool(index int, name, arguments string, c *call) {
	if index < 0 || index >= maxChoices {
		c.partial = true
		return
	}
	if !c.charge(len(name) + len(arguments)) {
		return
	}
	if a.tools == nil {
		a.tools = map[int]*toolAccumulator{}
	}
	accumulator, ok := a.tools[index]
	if !ok {
		accumulator = &toolAccumulator{}
		a.tools[index] = accumulator
		a.toolOrder = append(a.toolOrder, index)
	}
	if name != "" {
		accumulator.name = name
	}
	accumulator.args.WriteString(arguments)
}

// render produces one choice's exported output: a plain string for
// text-only choices, a structured object when tool calls are present.
func (a *choiceAccumulator) render() any {
	if len(a.toolOrder) == 0 {
		return a.text.String()
	}
	calls := make([]any, 0, len(a.toolOrder))
	for _, index := range a.toolOrder {
		accumulator := a.tools[index]
		calls = append(calls, map[string]any{
			"name": accumulator.name, "arguments": accumulator.args.String(),
		})
	}
	rendered := map[string]any{"tool_calls": calls}
	if text := a.text.String(); text != "" {
		rendered["content"] = text
	}
	return rendered
}

type streamChunk struct {
	Model   string          `json:"model"`
	Error   json.RawMessage `json:"error"`
	Usage   *wireUsage      `json:"usage"`
	Choices []streamChoice  `json:"choices"`
}

type streamChoice struct {
	Index        int          `json:"index"`
	FinishReason string       `json:"finish_reason"`
	Delta        *streamDelta `json:"delta"`
	Text         string       `json:"text"`
}

type streamDelta struct {
	Role         string          `json:"role"`
	Content      string          `json:"content"`
	Refusal      string          `json:"refusal"`
	Audio        json.RawMessage `json:"audio"`
	FunctionCall *struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function_call"`
	ToolCalls []struct {
		Index    int `json:"index"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	} `json:"tool_calls"`
}

// isRealError distinguishes an actual error object from an explicit
// JSON null, which providers may emit as a field placeholder.
func isRealError(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

type unaryResponse struct {
	Model   string            `json:"model"`
	Usage   *wireUsage        `json:"usage"`
	Choices []unaryChoice     `json:"choices"`
	Data    []json.RawMessage `json:"data"`
}

type unaryChoice struct {
	FinishReason string         `json:"finish_reason"`
	Message      map[string]any `json:"message"`
	Text         string         `json:"text"`
}

type wireUsage struct {
	PromptTokens        int64 `json:"prompt_tokens"`
	CompletionTokens    int64 `json:"completion_tokens"`
	PromptTokensDetails struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	CompletionTokensDetails struct {
		ReasoningTokens int64 `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
}

// mapUsage converts wire usage to the core inclusive semantics: input
// includes cached tokens and output includes reasoning tokens, matching
// what OpenAI reports. Unknown buckets are deliberately not forwarded.
func mapUsage(usage *wireUsage) *langfuse.Usage {
	return &langfuse.Usage{
		InputTokens:           usage.PromptTokens,
		OutputTokens:          usage.CompletionTokens,
		CacheReadInputTokens:  usage.PromptTokensDetails.CachedTokens,
		ReasoningOutputTokens: usage.CompletionTokensDetails.ReasoningTokens,
	}
}

// allowedParameters filters the request body through the strict model
// parameter allowlist, keeping only JSON numbers and booleans.
func allowedParameters(request map[string]json.RawMessage) map[string]any {
	var parameters map[string]any
	for key, raw := range request {
		if !modelParameterAllowlist[key] {
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

// sanitizeMessages replaces multimodal message parts with fixed
// placeholders during parsing, before anything is exported: media
// bytes and signed URLs never leave the adapter.
func sanitizeMessages(messages []any) []any {
	sanitized := make([]any, len(messages))
	for index, message := range messages {
		sanitized[index] = sanitizeValue(message)
	}
	return sanitized
}

func sanitizeValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		if partType, ok := value["type"].(string); ok && partType != "" && partType != "text" {
			if _, media := value["image_url"]; media {
				return map[string]any{"type": partType, "media": "omitted"}
			}
			if _, media := value["input_audio"]; media {
				return map[string]any{"type": partType, "media": "omitted"}
			}
			if _, media := value["file"]; media {
				return map[string]any{"type": partType, "media": "omitted"}
			}
		}
		sanitized := make(map[string]any, len(value))
		for key, item := range value {
			// Response messages carry audio without a part type
			// (message.audio.data holds base64); replace the whole
			// object so media bytes never reach export.
			if key == "audio" {
				if _, isMap := item.(map[string]any); isMap {
					sanitized[key] = map[string]any{"media": "omitted"}
					continue
				}
			}
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
