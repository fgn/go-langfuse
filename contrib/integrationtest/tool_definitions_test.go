package integrationtest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const toolSentinel = "SYNTHETIC-TOOL-SCHEMA-SENTINEL"

func spanText(span *tracepb.Span) string {
	var text strings.Builder
	for _, attribute := range span.GetAttributes() {
		text.WriteString(attribute.GetValue().GetStringValue())
	}
	for _, event := range span.GetEvents() {
		for _, attribute := range event.GetAttributes() {
			text.WriteString(attribute.GetValue().GetStringValue())
		}
	}
	return text.String()
}

// TestToolDefinitionsFollowContentCaptureAndMask lives here because the
// adapter module is also tested against a core without MaskField.
func TestToolDefinitionsFollowContentCaptureAndMask(t *testing.T) {
	routes := map[string]struct{ path, request, response string }{
		"chat": {
			"/v1/chat/completions",
			`{"model":"m","messages":[{"role":"user","content":"q"}],"tools":[{"type":"function","function":{"name":"lookup",` +
				`"description":"` + toolSentinel + `","parameters":{"type":"object"}}}]}`,
			`{"id":"chatcmpl-1","model":"m","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}]}`,
		},
		"responses": {
			"/v1/responses",
			`{"model":"m","input":"q","tools":[{"type":"function","name":"lookup","description":"` + toolSentinel +
				`","parameters":{"type":"object"}}]}`,
			`{"id":"resp_1","status":"completed","model":"m","output":[{"type":"message","role":"assistant",` +
				`"content":[{"type":"output_text","text":"ok"}]}]}`,
		},
	}
	cases := map[string]struct {
		disableCapture, optIn, mask, noContentExport bool
		wantSentinel, wantInputMasked                bool
	}{
		"capture disabled":       {disableCapture: true},
		"per-request opt-in":     {disableCapture: true, optIn: true, wantSentinel: true},
		"input masked":           {mask: true, wantInputMasked: true},
		"without content export": {noContentExport: true},
	}
	for route, r := range routes {
		for name, tc := range cases {
			t.Run(route+"/"+name, func(t *testing.T) {
				receiver := newOTLPReceiver(t)
				var inputMasks atomic.Int64
				config := langfuse.Config{
					BaseURL: receiver.server.URL, PublicKey: "pk-lf-test", SecretKey: "sk-lf-test",
					DisableContentCapture: tc.disableCapture,
					Mask: func(field langfuse.MaskField, value any) any {
						if field != langfuse.MaskObservationInput {
							return value
						}
						inputMasks.Add(1)
						if tc.mask {
							return "[redacted]"
						}
						return value
					},
				}
				lf, err := langfuse.New(context.Background(), config)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_ = lf.Shutdown(ctx)
				})
				provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, r.response)
				}))
				t.Cleanup(provider.Close)
				options := []langfuseopenai.Option{langfuseopenai.WithToolDefinitions()}
				if tc.noContentExport {
					options = append(options, langfuseopenai.WithoutContentExport())
				}
				ctx := context.Background()
				if tc.optIn {
					ctx = lf.WithContentCapture(ctx, true)
				}
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, provider.URL+r.path, strings.NewReader(r.request))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/json")
				resp, err := (&http.Client{Transport: langfuseopenai.NewTransport(lf, nil, options...)}).Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				flush(t, lf)

				span := receiver.nextSpan(t)
				if got := strings.Contains(spanText(span), toolSentinel); got != tc.wantSentinel {
					t.Fatalf("tool sentinel exported = %v, want %v", got, tc.wantSentinel)
				}
				if tc.disableCapture && !tc.optIn && inputMasks.Load() != 0 {
					t.Fatalf("input masker called %d times with capture disabled", inputMasks.Load())
				}
				if tc.wantInputMasked && attrString(span, "langfuse.observation.input") != "[redacted]" {
					t.Fatalf("input = %q, want the masked value", attrString(span, "langfuse.observation.input"))
				}
			})
		}
	}
}
