package langfuseopenai_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

func TestResponseIDIsRecordedAsMetadata(t *testing.T) {
	cases := map[string]struct {
		path, contentType, response, request, want string
	}{
		"chat": {
			"/v1/chat/completions", "application/json", chatResponse,
			`{"model":"m","messages":[{"role":"user","content":"q"}]}`, "chatcmpl-1",
		},
		"responses": {
			"/v1/responses", "application/json", responsesUnaryBody, `{"model":"m","input":"q"}`, "resp-1",
		},
		"responses stream": {
			"/v1/responses", "text/event-stream", sseBody(responsesTerminal),
			`{"model":"m","input":"q","stream":true}`, "r",
		},
		"chat stream": {
			"/v1/chat/completions", "text/event-stream",
			sseBody(`{"id":"chatcmpl-9","model":"m","choices":[{"index":0,"delta":{"content":"hi"}}]}`,
				`{"id":"chatcmpl-9","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`, "[DONE]"),
			`{"model":"m","messages":[{"role":"user","content":"q"}],"stream":true}`, "chatcmpl-9",
		},
		"responses created then disconnected": {
			"/v1/responses", "text/event-stream",
			sseBody(`{"type":"response.created","response":{"id":"resp_probe","status":"in_progress","model":"m","output":[]}}`),
			`{"model":"m","input":"q","stream":true}`, "resp_probe",
		},
		"responses terminal overrides lifecycle": {
			"/v1/responses", "text/event-stream",
			sseBody(`{"type":"response.created","response":{"id":"resp_early","status":"in_progress","model":"m","output":[]}}`,
				strings.Replace(responsesTerminal, `"id":"r"`, `"id":"resp_final"`, 1)),
			`{"model":"m","input":"q","stream":true}`, "resp_final",
		},
		"oversized responses terminal": {
			"/v1/responses", "text/event-stream",
			sseBody(`{"type":"response.completed","response":{"id":"resp_big","status":"completed","model":"m",` +
				`"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"` +
				strings.Repeat("p", 300<<10) + `"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`),
			`{"model":"m","input":"q","stream":true}`, "resp_big",
		},
		"oversized responses unary": {
			"/v1/responses", "application/json",
			`{"id":"resp_unary_big","status":"completed","model":"m","output":[{"type":"message","role":"assistant",` +
				`"content":[{"type":"output_text","text":"` + strings.Repeat("p", 600<<10) + `"}]}]}`,
			`{"model":"m","input":"q"}`, "resp_unary_big",
		},
		"escaped identifier": {
			"/v1/responses", "application/json", strings.Replace(responsesUnaryBody, `"resp-1"`, `"resp\u005fescaped"`, 1),
			`{"model":"m","input":"q"}`, "resp_escaped",
		},
		"oversized identifier": {
			"/v1/responses", "application/json",
			strings.Replace(responsesUnaryBody, `"resp-1"`, `"`+strings.Repeat("r", 129)+`"`, 1),
			`{"model":"m","input":"q"}`, "",
		},
		"malformed identifier": {
			"/v1/responses", "application/json", strings.Replace(responsesUnaryBody, `"resp-1"`, `42`, 1),
			`{"model":"m","input":"q"}`, "",
		},
		"text is not an identifier": {
			"/v1/chat/completions", "application/json",
			`{"id":"SECRET response text","model":"m","choices":[]}`,
			`{"model":"m","messages":[{"role":"user","content":"q"}]}`, "",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			receiver := newOTLPReceiver(t)
			lf := newTestClient(t, receiver, nil)
			provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				_, _ = io.WriteString(w, tc.response)
			})
			httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
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

			span := receiver.nextSpan(t)
			if got := attrString(t, span, "langfuse.observation.metadata.response_id"); got != tc.want {
				t.Fatalf("response_id = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOversizedLifecycleEventsKeepOnlyTheResponseID(t *testing.T) {
	for _, eventType := range []string{"response.created", "response.queued", "response.in_progress"} {
		t.Run(eventType, func(t *testing.T) {
			receiver, flushFn := runResponsesStream(t, sseBody(`{"type":"`+eventType+`","response":{"id":"resp_lifecycle",`+
				`"status":"in_progress","model":"m","output":[],"instructions":"`+strings.Repeat("i", 300<<10)+`"}}`))
			flushFn()
			span := receiver.nextSpan(t)
			if got := attrString(t, span, "langfuse.observation.metadata.response_id"); got != "resp_lifecycle" {
				t.Fatalf("response_id = %q, want resp_lifecycle", got)
			}
			if got := attrString(t, span, "langfuse.observation.status_message"); got != "incomplete" {
				t.Fatalf("status = %q, want incomplete: a lifecycle event is never terminal", got)
			}
			for _, key := range []string{"langfuse.observation.usage_details", "langfuse.observation.completion_start_time"} {
				if hasAttr(span, key) {
					t.Fatalf("%s set from a lifecycle event", key)
				}
			}
		})
	}
}

func TestMalformedChatIDKeepsTheRestOfTheResponse(t *testing.T) {
	cases := map[string]struct{ contentType, body string }{
		"unary": {"application/json", `{"id":42,"model":"example-model-002","choices":[{"index":0,"finish_reason":"stop",` +
			`"message":{"role":"assistant","content":"answer"}}],"usage":{"prompt_tokens":9,"completion_tokens":12}}`},
		"stream": {"text/event-stream", sseBody(
			`{"id":{"x":1},"model":"example-model-002","choices":[{"index":0,"delta":{"content":"answer"}}]}`,
			`{"id":{"x":1},"model":"example-model-002","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],`+
				`"usage":{"prompt_tokens":9,"completion_tokens":12}}`, "[DONE]")},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			receiver := newOTLPReceiver(t)
			lf := newTestClient(t, receiver, nil)
			provider := chatServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				_, _ = io.WriteString(w, tc.body)
			})
			httpClient := &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)}
			resp := postChat(t, httpClient, provider.URL, context.Background())
			drainAndClose(t, resp)
			flush(t, lf)

			span := receiver.nextSpan(t)
			if hasAttr(span, "langfuse.observation.metadata.response_id") {
				t.Fatal("a malformed id was exported")
			}
			if !hasAttr(span, "langfuse.observation.usage_details") || !strings.Contains(attrString(t, span, "langfuse.observation.output"), "answer") {
				t.Fatal("a malformed id discarded usage or output")
			}
		})
	}
}
