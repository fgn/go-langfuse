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

	// Streaming accumulation, bounded by captureCap in total.
	deltas       []strings.Builder
	deltaBytes   int
	deltasOver   bool
	sawDone      bool
	streamEvents bool
	embeddings   int
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
	if chunk.Error != nil {
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
			c.finishReasons = append(c.finishReasons, choice.FinishReason)
		}
		text, bearing := choice.outputDelta()
		if !bearing {
			continue
		}
		output = true
		c.appendDelta(choice.Index, text)
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
			c.finishReasons = append(c.finishReasons, choice.FinishReason)
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
		Input:           c.input,
		Model:           c.responseModel,
		ModelParameters: c.modelParameters,
		Usage:           c.usage,
		ErrorCategory:   c.errorCategory,
		TelemetryPartial: c.partial ||
			c.deltasOver,
	}
	if c.responseModel == "" {
		result.Model = c.requestModel
	}
	metadata := map[string]any{}
	if c.requestModel != "" && c.requestModel != result.Model {
		metadata["request_model"] = c.requestModel
	}
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
		switch len(c.deltas) {
		case 0:
		case 1:
			result.Output = c.deltas[0].String()
		default:
			texts := make([]any, len(c.deltas))
			for index := range c.deltas {
				texts[index] = c.deltas[index].String()
			}
			result.Output = texts
		}
	}
	return result
}

// appendDelta grows per-choice accumulated output, bounded in aggregate
// by the capture cap: over the cap, output is dropped entirely (never
// truncated) while usage and terminal scanning continue.
func (c *call) appendDelta(index int, text string) {
	if c.deltasOver {
		return
	}
	if c.deltaBytes+len(text) > c.captureCap {
		c.deltasOver = true
		c.deltas = nil
		return
	}
	for len(c.deltas) <= index {
		c.deltas = append(c.deltas, strings.Builder{})
	}
	c.deltas[index].WriteString(text)
	c.deltaBytes += len(text)
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
	Role      string          `json:"role"`
	Content   string          `json:"content"`
	Refusal   string          `json:"refusal"`
	Audio     json.RawMessage `json:"audio"`
	ToolCalls []struct {
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	} `json:"tool_calls"`
}

// outputDelta reports the first output-bearing content of a stream
// choice: text, refusal, audio, or tool-call argument deltas. Role-only
// and empty control chunks are not output.
func (choice streamChoice) outputDelta() (text string, bearing bool) {
	if choice.Text != "" {
		return choice.Text, true
	}
	delta := choice.Delta
	if delta == nil {
		return "", false
	}
	switch {
	case delta.Content != "":
		return delta.Content, true
	case delta.Refusal != "":
		return delta.Refusal, true
	case len(delta.Audio) > 0:
		return "", true
	}
	for _, tool := range delta.ToolCalls {
		if tool.Function.Name != "" || tool.Function.Arguments != "" {
			return tool.Function.Arguments, true
		}
	}
	return "", false
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
