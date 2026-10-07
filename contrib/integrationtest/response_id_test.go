package integrationtest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fgn/go-langfuse"
	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
	openaigo "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

// TestResponseIDIsGovernedByTheMetadataMasker lives here because the
// adapter module is also tested against a core without MaskField.
func TestResponseIDIsGovernedByTheMetadataMasker(t *testing.T) {
	receiver := newOTLPReceiver(t)
	lf, err := langfuse.New(context.Background(), langfuse.Config{
		BaseURL: receiver.server.URL, PublicKey: "pk-lf-test", SecretKey: "sk-lf-test", DisableContentCapture: true,
		Mask: func(field langfuse.MaskField, value any) any {
			if metadata, ok := value.(map[string]any); ok && field == langfuse.MaskObservationMetadata {
				delete(metadata, "response_id")
			}
			return value
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = lf.Shutdown(ctx)
	})
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chatcmpl-1","object":"chat.completion","model":"example-model-002",`+
			`"choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"answer"}}]}`)
	}))
	t.Cleanup(provider.Close)
	client := officialClient(provider.URL+"/v1", &http.Client{Transport: langfuseopenai.NewTransport(lf, nil)},
		option.WithMaxRetries(0))
	if _, err := client.Chat.Completions.New(context.Background(), openaigo.ChatCompletionNewParams{
		Model:    openaigo.ChatModel("example-model"),
		Messages: []openaigo.ChatCompletionMessageParamUnion{openaigo.UserMessage("synthetic question")},
	}); err != nil {
		t.Fatal(err)
	}
	flush(t, lf)

	span := receiver.nextSpan(t)
	if attrString(span, "langfuse.observation.metadata.response_id") != "" {
		t.Fatal("response_id exported although the metadata masker removed it")
	}
	if attrString(span, "langfuse.observation.metadata.provider") == "" {
		t.Fatal("the masker removed more than response_id")
	}
}
