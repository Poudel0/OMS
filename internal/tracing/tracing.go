// Package tracing sets up OpenTelemetry tracing and exports to an OTLP
// collector (Jaeger, in this project's compose file).
//
// # What is and is not a span
//
// Spans cover the gRPC handler, the Submit round trip, and the settlement write.
// The **group commit is deliberately not a span**, and that is worth stating
// because its absence looks like an oversight.
//
// One group commit serves many concurrent requests (ADR-003), so it belongs to
// many traces at once. A span has one parent. Modelling a batch as a span means
// either picking one arbitrary trace to attach it to — misleading, since it looks
// like that request caused the whole cost — or inventing links from N traces to
// one span, which is the correct OTel construct and produces something no trace
// UI usefully renders.
//
// So the batch is measured where a fan-in fits naturally: as metrics.
// `rate(oms_group_commit_requests_total) / rate(oms_group_commits_total)` is the
// mean orders-per-fsync, live. The trace shows a request waiting inside Submit;
// the metric explains why. Tracing and batching are in genuine tension, and this
// is the seam.
package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// ServiceName is what this node calls itself in traces.
const ServiceName = "dhukuti-oms"

// Tracer is the package's tracer, usable whether or not tracing was set up: with
// no provider installed, OTel's default is a no-op and every span costs
// essentially nothing.
func Tracer() trace.Tracer { return otel.Tracer("github.com/Poudel0/OMS") }

// Setup installs a tracer provider exporting to the OTLP endpoint over gRPC and
// returns a shutdown function.
//
// endpoint is a host:port ("localhost:4317"), not a URL. An empty endpoint
// installs nothing and returns a no-op shutdown, which is the normal
// configuration: tracing should be something you turn on, not something a node
// refuses to start without.
//
// sampleRatio is the head-sampling fraction. Tracing every order at a few
// thousand orders/sec would generate more span traffic than trade traffic, so the
// default is a sample rather than everything — and it is parent-based, so a
// sampled trace stays whole across services rather than being decided again at
// each hop.
func Setup(ctx context.Context, endpoint, environment string, sampleRatio float64) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if endpoint == "" {
		return noop, nil
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		// Plaintext because the collector is expected to be a sidecar or a
		// same-host process. A collector across a network needs TLS, and that is a
		// deployment decision rather than a default to bake in.
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return noop, fmt.Errorf("tracing: create otlp exporter: %w", err)
	}

	// Merge, not replace: resource.Default() carries the SDK/telemetry attributes
	// a collector expects. Both halves must declare the SAME schema URL or Merge
	// refuses — which is why semconv here is pinned to the version the installed
	// SDK's default resource uses, not whichever one an example used.
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(ServiceName),
		attribute.String("deployment.environment", environment),
	))
	if err != nil {
		return noop, fmt.Errorf("tracing: build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))),
	)
	otel.SetTracerProvider(tp)
	// W3C tracecontext so a trace started by a client carries through, rather
	// than this node starting a fresh one and losing the caller's half.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	return tp.Shutdown, nil
}
