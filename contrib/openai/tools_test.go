package langfuseopenai_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

const weatherParameters = `{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`

func TestToolDefinitionsAreExportedOnlyWhenEnabled(t *testing.T) {
	weather := map[string]any{
		"name": "get_weather", "description": "Current weather",
		"parameters": map[string]any{
			"type": "object", "properties": map[string]any{"city": map[string]any{"type": "string"}},
			"required": []any{"city"},
		},
	}
	cases := map[string]struct {
		path, response, request string
		want                    any
	}{
		"responses": {
			"/v1/responses", responsesUnaryBody,
			`{"model":"m","input":"q","tools":[` +
				`{"type":"function","name":"get_weather","description":"Current weather","parameters":` + weatherParameters + `,"strict":true},` +
				`{"type":"web_search"},{"type":"SECRET custom tool"}]}`,
			[]any{
				map[string]any{
					"type": "function", "name": "get_weather", "description": "Current weather",
					"parameters": weather["parameters"],
				},
				map[string]any{"type": "web_search", "omitted": true},
				map[string]any{"type": "unknown", "omitted": true},
			},
		},
		"chat": {
			"/v1/chat/completions", chatResponse,
			`{"model":"m","messages":[{"role":"user","content":"q"}],"tools":[` +
				`{"type":"function","function":{"name":"get_weather","description":"Current weather","parameters":` +
				weatherParameters + `}}]}`,
			[]any{map[string]any{"type": "function", "function": weather}},
		},
	}
	for name, tc := range cases {
		for _, enabled := range []bool{false, true} {
			t.Run(name+map[bool]string{false: " default", true: " enabled"}[enabled], func(t *testing.T) {
				receiver := newOTLPReceiver(t)
				lf := newTestClient(t, receiver, nil)
				provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, tc.response)
				})
				var options []langfuseopenai.Option
				if enabled {
					options = append(options, langfuseopenai.WithToolDefinitions())
				}
				httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil, options...)}
				req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
					provider.URL+tc.path, strings.NewReader(tc.request))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/json")
				resp, err := httpClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				drainAndClose(t, resp)
				flush(t, lf)

				raw := attrString(t, receiver.nextSpan(t), "langfuse.observation.input")
				if strings.Contains(raw, "SECRET") {
					t.Fatalf("unknown tool type leaked into input: %s", raw)
				}
				var input any
				if err := json.Unmarshal([]byte(raw), &input); err != nil {
					t.Fatalf("input is not JSON: %v", err)
				}
				object, isObject := input.(map[string]any)
				tools, exported := object["tools"]
				if exported != enabled {
					t.Fatalf("tools exported = %v, want %v; input %s", exported, enabled, raw)
				}
				if enabled && !reflect.DeepEqual(tools, tc.want) {
					t.Fatalf("tools = %#v, want %#v", tools, tc.want)
				}
				if name == "chat" && enabled != isObject {
					t.Fatalf("chat input object = %v, want only with tool definitions; input %s", isObject, raw)
				}
			})
		}
	}
}

func TestToolDefinitionsReportPartialCapture(t *testing.T) {
	function := func(description string) string {
		return `{"type":"function","name":"get_weather","description":"` + description + `","parameters":` + weatherParameters + `}`
	}
	atCap := function(strings.Repeat("d", 64<<10-len(function(""))))
	many := make([]string, 129)
	for i := range many {
		many[i] = `{"type":"web_search"}`
	}
	cases := map[string]struct {
		path, tools string
		count       int
		partial     bool
	}{
		"function and other type":   {"/v1/responses", function("ok") + `,{"type":"web_search"}`, 2, false},
		"function at the item cap":  {"/v1/responses", atCap, 1, false},
		"oversized function":        {"/v1/responses", function(strings.Repeat("d", 64<<10)), 1, true},
		"null entry":                {"/v1/responses", `null`, 1, true},
		"non-object entry":          {"/v1/responses", `7`, 1, true},
		"missing type":              {"/v1/responses", `{"name":"x"}`, 1, true},
		"nameless function":         {"/v1/responses", `{"type":"function","description":"x"}`, 1, true},
		"malformed nested function": {"/v1/chat/completions", `{"type":"function","function":"x"}`, 1, true},
		"more than 128 tools":       {"/v1/responses", strings.Join(many, ","), 128, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			receiver := newOTLPReceiver(t)
			lf := newTestClient(t, receiver, nil)
			response := `{"id":"resp-2","status":"completed","model":"m","output":[{"type":"message","role":"assistant",` +
				`"content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`
			request := `{"model":"m","input":"q","tools":[` + tc.tools + `]}`
			if tc.path == "/v1/chat/completions" {
				response, request = chatResponse, `{"model":"m","messages":[{"role":"user","content":"q"}],"tools":[`+tc.tools+`]}`
			}
			provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, response)
			})
			httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil, langfuseopenai.WithToolDefinitions())}
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
				provider.URL+tc.path, strings.NewReader(request))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			drainAndClose(t, resp)
			flush(t, lf)

			span := receiver.nextSpan(t)
			var input struct {
				Tools []any `json:"tools"`
			}
			if err := json.Unmarshal([]byte(attrString(t, span, "langfuse.observation.input")), &input); err != nil {
				t.Fatalf("input is not JSON: %v", err)
			}
			if len(input.Tools) != tc.count {
				t.Fatalf("exported tools = %d, want %d", len(input.Tools), tc.count)
			}
			if partial := attrString(t, span, "langfuse.observation.status_message") == "telemetry_partial"; partial != tc.partial {
				t.Fatalf("telemetry_partial = %v, want %v", partial, tc.partial)
			}
		})
	}
}
