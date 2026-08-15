package langfuseopenai_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"

	langfuseopenai "github.com/fgn/go-langfuse/contrib/openai"
)

type panickingErrorHandler struct{}

func (panickingErrorHandler) Handle(error) { panic("application error handler panic") }

func TestDiagnosticsContainApplicationErrorHandlerPanic(t *testing.T) {
	previous := otel.GetErrorHandler()
	otel.SetErrorHandler(panickingErrorHandler{})
	t.Cleanup(func() { otel.SetErrorHandler(previous) })

	_ = langfuseopenai.NewTransport(nil, nil, langfuseopenai.WithProvider("INVALID PROVIDER"))
	var nilContext context.Context
	_ = langfuseopenai.ContextWithCall(nilContext, langfuseopenai.CallAttributes{})
	first := langfuseopenai.NewTransport(nil, nil)
	_ = langfuseopenai.NewTransport(nil, first)
}
