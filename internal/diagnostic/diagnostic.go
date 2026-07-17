// Package diagnostic reports payload-free SDK diagnostics through OpenTelemetry.
package diagnostic

import (
	"errors"

	"go.opentelemetry.io/otel"
)

// Report sends a payload-free message to the process OpenTelemetry error
// handler. Callers must never include credentials or telemetry payloads.
func Report(message string) {
	defer func() {
		// The OTel error handler is application-pluggable. A hostile or buggy
		// handler must not turn a telemetry diagnostic into an application panic.
		_ = recover()
	}()
	otel.Handle(errors.New("lunte: " + message))
}
