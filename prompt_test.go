package langfuse_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/fgn/go-langfuse"
)

func TestPromptCompileText(t *testing.T) {
	t.Parallel()
	prompt := langfuse.Prompt{
		Name: "n", Version: 1, Type: langfuse.PromptTypeText,
		Text: "Review {{ movie }} as {{critic}}; missing {{other}}, empty {{}}, open {{brace",
	}
	compiled := prompt.Compile(map[string]any{
		"movie":  "Dune",
		"critic": map[string]string{"style": "harsh"},
	})
	want := `Review Dune as {"style":"harsh"}; missing {{other}}, empty {{}}, open {{brace`
	if compiled.Text != want {
		t.Fatalf("Compile() text = %q, want %q", compiled.Text, want)
	}
	if prompt.Text == compiled.Text {
		t.Fatal("Compile() mutated its receiver")
	}
	if compiled.Name != "n" || compiled.Version != 1 {
		t.Fatalf("Compile() = %+v, want metadata carried through", compiled)
	}
}

func TestPromptCompileStringValuesAreVerbatim(t *testing.T) {
	t.Parallel()
	prompt := langfuse.Prompt{Type: langfuse.PromptTypeText, Text: `{{v}}`}
	compiled := prompt.Compile(map[string]any{"v": `quote " stays`})
	if compiled.Text != `quote " stays` {
		t.Fatalf("Compile() text = %q, want the string verbatim without JSON quoting", compiled.Text)
	}
}

func TestPromptCompileChatPlaceholders(t *testing.T) {
	t.Parallel()
	prompt := langfuse.Prompt{
		Name: "n", Version: 2, Type: langfuse.PromptTypeChat,
		Messages: []langfuse.PromptMessage{
			{Role: "system", Content: "Be {{tone}}."},
			{PlaceholderName: "history"},
			{PlaceholderName: "empty"},
			{PlaceholderName: "missing"},
			{PlaceholderName: "invalid"},
			{Role: "user", Content: "{{question}}"},
		},
	}
	compiled := prompt.Compile(map[string]any{
		"tone":     "kind",
		"question": "why?",
		"history": []langfuse.PromptMessage{
			{Role: "user", Content: "earlier"},
			{Role: "assistant", Content: "reply"},
		},
		"empty":   []langfuse.PromptMessage{},
		"invalid": []langfuse.PromptMessage{{Content: "no role"}},
	})
	want := []langfuse.PromptMessage{
		{Role: "system", Content: "Be kind."},
		{Role: "user", Content: "earlier"},
		{Role: "assistant", Content: "reply"},
		{PlaceholderName: "missing"},
		{PlaceholderName: "invalid"},
		{Role: "user", Content: "why?"},
	}
	if len(compiled.Messages) != len(want) {
		t.Fatalf("Compile() produced %d messages, want %d: %+v", len(compiled.Messages), len(want), compiled.Messages)
	}
	for i, message := range want {
		if compiled.Messages[i].Role != message.Role ||
			compiled.Messages[i].Content != message.Content ||
			compiled.Messages[i].PlaceholderName != message.PlaceholderName {
			t.Fatalf("Compile() message %d = %+v, want %+v", i, compiled.Messages[i], message)
		}
	}
	if prompt.Messages[0].Content != "Be {{tone}}." || prompt.Messages[1].PlaceholderName != "history" {
		t.Fatal("Compile() mutated its receiver")
	}
}

func TestPromptCompileDoesNotSubstituteInsideExtra(t *testing.T) {
	t.Parallel()
	prompt := langfuse.Prompt{
		Type: langfuse.PromptTypeChat,
		Messages: []langfuse.PromptMessage{
			{Role: "assistant", Extra: json.RawMessage(`{"note":"{{v}}"}`)},
		},
	}
	compiled := prompt.Compile(map[string]any{"v": "x"})
	if string(compiled.Messages[0].Extra) != `{"note":"{{v}}"}` {
		t.Fatalf("Extra = %s, want it untouched by substitution", compiled.Messages[0].Extra)
	}
}

func TestPromptCompileTextIgnoresMessageVariables(t *testing.T) {
	t.Parallel()
	prompt := langfuse.Prompt{Type: langfuse.PromptTypeText, Text: "{{history}}"}
	compiled := prompt.Compile(map[string]any{
		"history": []langfuse.PromptMessage{{Role: "user", Content: "hi"}},
	})
	if compiled.Text != "{{history}}" {
		t.Fatalf("Compile() text = %q, want message-slice variables left verbatim in text", compiled.Text)
	}
}

type panickyMarshaler struct{}

func (panickyMarshaler) MarshalJSON() ([]byte, error) { panic("hostile marshaler") }

type failingMarshaler struct{}

func (failingMarshaler) MarshalJSON() ([]byte, error) { return nil, errors.New("refused") }

func TestPromptCompileSurvivesHostileMarshalers(t *testing.T) {
	t.Parallel()
	prompt := langfuse.Prompt{Type: langfuse.PromptTypeText, Text: "a {{p}} b {{f}} c {{ok}}"}
	compiled := prompt.Compile(map[string]any{
		"p":  panickyMarshaler{},
		"f":  failingMarshaler{},
		"ok": 42,
	})
	if compiled.Text != "a {{p}} b {{f}} c 42" {
		t.Fatalf("Compile() text = %q, want hostile values left verbatim and the rest substituted", compiled.Text)
	}
}

func TestPromptCompileCopyIsolation(t *testing.T) {
	t.Parallel()
	prompt := langfuse.Prompt{
		Type:     langfuse.PromptTypeChat,
		Messages: []langfuse.PromptMessage{{PlaceholderName: "h"}},
		Labels:   []string{"production"},
		Config:   json.RawMessage(`{}`),
	}
	fill := []langfuse.PromptMessage{{Role: "user", Content: "x", Extra: json.RawMessage(`{"a":1}`)}}
	compiled := prompt.Compile(map[string]any{"h": fill})
	fill[0].Content = "mutated"
	fill[0].Extra[1] = 'X'
	if compiled.Messages[0].Content != "x" || string(compiled.Messages[0].Extra) != `{"a":1}` {
		t.Fatalf("compiled messages alias the caller's fill slice: %+v", compiled.Messages[0])
	}
	compiled.Labels[0] = "mutated"
	if prompt.Labels[0] != "production" {
		t.Fatal("Compile() result aliases the receiver's slices")
	}
}

func TestPromptCompileStrictReportsUnresolvedVariables(t *testing.T) {
	t.Parallel()
	prompt := langfuse.Prompt{
		Type: langfuse.PromptTypeText,
		Text: "Use {{ second }} and {{first}}; repeat {{second}}.",
	}
	compiled, err := prompt.CompileStrict(nil)
	if err == nil || !strings.Contains(err.Error(), "first") || !strings.Contains(err.Error(), "second") {
		t.Fatalf("CompileStrict() error = %v, want both unresolved variable names", err)
	}
	if compiled.Text != prompt.Text {
		t.Fatalf("CompileStrict() text = %q, want unresolved variables left verbatim", compiled.Text)
	}
	if strings.Index(err.Error(), "first") > strings.Index(err.Error(), "second") {
		t.Fatalf("CompileStrict() error = %v, want sorted identifiers", err)
	}
}

func TestPromptCompileStrictReportsUnstringifiableValue(t *testing.T) {
	t.Parallel()
	prompt := langfuse.Prompt{Type: langfuse.PromptTypeText, Text: "{{unsupported}} and {{ok}}"}
	compiled, err := prompt.CompileStrict(map[string]any{
		"unsupported": panickyMarshaler{},
		"ok":          42,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("CompileStrict() error = %v, want the unstringifiable variable name", err)
	}
	if compiled.Text != "{{unsupported}} and 42" {
		t.Fatalf("CompileStrict() text = %q, want the same partial result as Compile", compiled.Text)
	}
}

func TestPromptCompileStrictReportsUnfilledPlaceholder(t *testing.T) {
	t.Parallel()
	prompt := langfuse.Prompt{
		Type: langfuse.PromptTypeChat,
		Messages: []langfuse.PromptMessage{
			{PlaceholderName: "context"},
			{Role: "user", Content: "{{input}}"},
		},
	}
	compiled, err := prompt.CompileStrict(map[string]any{"input": "value"})
	if err == nil || !strings.Contains(err.Error(), "context") {
		t.Fatalf("CompileStrict() error = %v, want the unfilled placeholder name", err)
	}
	if compiled.Messages[0].PlaceholderName != "context" || compiled.Messages[1].Content != "value" {
		t.Fatalf("CompileStrict() messages = %+v, want the lenient compiled result", compiled.Messages)
	}
}

func TestPromptCompileStrictAllResolved(t *testing.T) {
	t.Parallel()
	prompt := langfuse.Prompt{
		Type: langfuse.PromptTypeChat,
		Messages: []langfuse.PromptMessage{
			{Role: "system", Content: "Process {{kind}}."},
			{PlaceholderName: "context"},
			{Role: "user", Content: "{{input}}"},
		},
	}
	compiled, err := prompt.CompileStrict(map[string]any{
		"kind":  "carefully",
		"input": "value",
		"context": []langfuse.PromptMessage{
			{Role: "assistant", Content: "ready"},
		},
	})
	if err != nil {
		t.Fatalf("CompileStrict() error = %v", err)
	}
	want := []langfuse.PromptMessage{
		{Role: "system", Content: "Process carefully."},
		{Role: "assistant", Content: "ready"},
		{Role: "user", Content: "value"},
	}
	if len(compiled.Messages) != len(want) {
		t.Fatalf("CompileStrict() produced %d messages, want %d", len(compiled.Messages), len(want))
	}
	for i := range want {
		if compiled.Messages[i].Role != want[i].Role || compiled.Messages[i].Content != want[i].Content {
			t.Fatalf("CompileStrict() message %d = %+v, want %+v", i, compiled.Messages[i], want[i])
		}
	}
}

func TestPromptDecodeConfig(t *testing.T) {
	t.Parallel()
	type promptConfig struct {
		Model string `json:"model"`
		Limit int    `json:"limit"`
	}

	config := promptConfig{Model: "default-model", Limit: 25}
	prompt := langfuse.Prompt{Config: json.RawMessage(`{"model":"configured-model"}`)}
	if err := prompt.DecodeConfig(&config); err != nil {
		t.Fatalf("DecodeConfig() error = %v", err)
	}
	if config.Model != "configured-model" || config.Limit != 25 {
		t.Fatalf("DecodeConfig() = %+v, want decoded values with caller defaults preserved", config)
	}

	defaults := promptConfig{Model: "default-model", Limit: 25}
	if err := (langfuse.Prompt{}).DecodeConfig(&defaults); err != nil {
		t.Fatalf("DecodeConfig(empty) error = %v", err)
	}
	if defaults.Model != "default-model" || defaults.Limit != 25 {
		t.Fatalf("DecodeConfig(empty) = %+v, want caller defaults unchanged", defaults)
	}

	if err := (langfuse.Prompt{Config: json.RawMessage(`{`)}).DecodeConfig(&promptConfig{}); err == nil ||
		!strings.Contains(err.Error(), "decode prompt config") {
		t.Fatalf("DecodeConfig(malformed) error = %v, want a clear decode error", err)
	}
	if err := prompt.DecodeConfig(promptConfig{}); err == nil || !strings.Contains(err.Error(), "decode prompt config") {
		t.Fatalf("DecodeConfig(non-pointer) error = %v, want a clear target error", err)
	}
}

func TestPromptRef(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		prompt langfuse.Prompt
		want   *langfuse.PromptRef
	}{
		"fetched":  {langfuse.Prompt{Name: "n", Version: 3}, &langfuse.PromptRef{Name: "n", Version: 3}},
		"fallback": {langfuse.Prompt{Name: "n", Version: 3, Source: langfuse.PromptSourceFallback}, nil},
		"zero":     {langfuse.Prompt{}, nil},
		"no name":  {langfuse.Prompt{Version: 3}, nil},
		"version0": {langfuse.Prompt{Name: "n"}, nil},
	}
	for label, test := range cases {
		got := test.prompt.Ref()
		if (got == nil) != (test.want == nil) {
			t.Fatalf("Ref(%s) = %+v, want %+v", label, got, test.want)
		}
		if got != nil && (got.Name != test.want.Name || got.Version != test.want.Version) {
			t.Fatalf("Ref(%s) = %+v, want %+v", label, got, test.want)
		}
	}
}

func FuzzPromptCompile(f *testing.F) {
	f.Add("Hello {{name}}, {{ a }} {{", "name", "world")
	f.Add("{{}} {{x}}{{y}}", "x", "")
	f.Add("plain", "k", "v")
	f.Fuzz(func(t *testing.T, text, key, value string) {
		prompt := langfuse.Prompt{
			Type: langfuse.PromptTypeChat,
			Text: text,
			Messages: []langfuse.PromptMessage{
				{Role: "user", Content: text},
				{PlaceholderName: key},
			},
		}
		compiled := prompt.Compile(map[string]any{key: value})
		if utf8.ValidString(text) && utf8.ValidString(value) && !utf8.ValidString(compiled.Text) {
			t.Fatalf("Compile() produced invalid UTF-8 from valid inputs: %q", compiled.Text)
		}
		if !strings.Contains(text, "{{") && compiled.Text != text {
			t.Fatalf("Compile() changed text without any variable syntax: %q -> %q", text, compiled.Text)
		}
	})
}
