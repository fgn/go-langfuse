package langfuseopenai

import (
	"bytes"
	"encoding/json"
	"regexp"
)

const maxToolDefinitions = 128

var toolTypeShape = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)

// sanitizeToolDefinitions keeps each function tool's name, description,
// and JSON-schema parameters in its route's own shape: nested under
// "function" for chat completions, flat for the Responses API. A
// well-formed tool of another type becomes a fixed placeholder by policy
// and does not make the capture partial. Malformed entries, oversized or
// malformed function definitions, and entries beyond the cap (dropped)
// do, so the observation reports telemetry_partial.
func sanitizeToolDefinitions(raw json.RawMessage, nested bool) (tools []any, partial bool) {
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		return nil, true
	}
	if len(items) > maxToolDefinitions {
		items, partial = items[:maxToolDefinitions], true
	}
	tools = make([]any, 0, len(items))
	for _, item := range items {
		tool, ok := sanitizeToolDefinition(item, nested)
		partial = partial || !ok
		tools = append(tools, tool)
	}
	return tools, partial
}

func sanitizeToolDefinition(item json.RawMessage, nested bool) (any, bool) {
	var tool struct {
		Type     string          `json:"type"`
		Function json.RawMessage `json:"function"`
	}
	trimmed := bytes.TrimSpace(item)
	if len(trimmed) == 0 || trimmed[0] != '{' || json.Unmarshal(trimmed, &tool) != nil ||
		!toolTypeShape.MatchString(tool.Type) {
		return map[string]any{"type": "unknown", "omitted": true}, false
	}
	if tool.Type != "function" {
		return map[string]any{"type": tool.Type, "omitted": true}, true
	}
	if len(item) > maxItemBytes {
		return map[string]any{"type": "function", "omitted": true}, false
	}
	source := trimmed
	if nested {
		source = bytes.TrimSpace(tool.Function)
	}
	var function struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Parameters  any    `json:"parameters"`
	}
	if len(source) == 0 || source[0] != '{' || json.Unmarshal(source, &function) != nil || function.Name == "" {
		return map[string]any{"type": "function", "omitted": true}, false
	}
	definition := map[string]any{"name": function.Name}
	if function.Description != "" {
		definition["description"] = function.Description
	}
	if function.Parameters != nil {
		definition["parameters"] = function.Parameters
	}
	if nested {
		return map[string]any{"type": "function", "function": definition}, true
	}
	definition["type"] = "function"
	return definition, true
}
