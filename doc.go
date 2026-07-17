// Package langfuse provides observation-centric Langfuse tracing on top of
// OpenTelemetry.
//
// It exports OTLP/HTTP protobuf traces to Langfuse and can either own an
// isolated tracer provider or attach a Langfuse processor to an existing
// OpenTelemetry SDK tracer provider through [Config]. The package never
// changes global OpenTelemetry state.
//
// Construct a [Client] with [New], typically from [ConfigFromEnv], stamp
// request-scoped identity with [Client.WithTraceAttributes], and record work
// with [Client.Observe]:
//
//	lf, err := langfuse.New(ctx, langfuse.ConfigFromEnv())
//	if err != nil {
//		return err
//	}
//	defer lf.Shutdown(shutdownCtx)
//
//	err = lf.Observe(ctx, "answer-question", langfuse.TypeGeneration,
//		langfuse.ObservationAttributes{Input: question},
//		func(ctx context.Context, o *langfuse.Observation) error {
//			answer, err := callModel(ctx, question)
//			o.Update(langfuse.ObservationAttributes{Output: answer})
//			return err
//		})
//
// [Client.StartObservation] is the lower-level pair for lifetimes that span
// functions; the context it returns carries the parent-child relationship,
// so observations started from it nest under their parent.
// [Client.RecordScore] submits evaluations and user feedback through the
// Langfuse REST scores endpoint, and [Client.Flush] and [Client.Shutdown]
// control the export lifecycle. A nil or disabled [Client] and the zero
// [Observation] are safe no-ops.
package langfuse
