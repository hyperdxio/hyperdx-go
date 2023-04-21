package hyperdx

import (
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func InitTracer() {
	ctx := context.Background()

	client := otlptracehttp.NewClient()

	otlpTraceExporter, err := otlptrace.New(ctx, client)
	if err != nil {
		log.Fatal(err)
	}

	batchSpanProcessor := sdktrace.NewBatchSpanProcessor(otlpTraceExporter)

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(batchSpanProcessor),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
}
