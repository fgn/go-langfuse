package langfusegenai_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"

	langfusegenai "github.com/fgn/go-langfuse/contrib/googlegenai"
)

type panickingErrorHandler struct{}

func (panickingErrorHandler) Handle(error) { panic("application error handler panic") }

func TestDiagnosticsContainApplicationErrorHandlerPanic(t *testing.T) {
	previous := otel.GetErrorHandler()
	otel.SetErrorHandler(panickingErrorHandler{})
	t.Cleanup(func() { otel.SetErrorHandler(previous) })

	_ = langfusegenai.NewTransport(nil, nil, langfusegenai.WithProvider("INVALID PROVIDER"))
	var nilContext context.Context
	_ = langfusegenai.ContextWithCall(nilContext, langfusegenai.CallAttributes{})
	first := langfusegenai.NewTransport(nil, nil)
	_ = langfusegenai.NewTransport(nil, first)
}
