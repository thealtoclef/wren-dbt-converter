from __future__ import annotations


def configure_telemetry(service_name: str = "dbt-graphql") -> None:
    """Bootstrap the OTel SDK from standard environment variables.

    Reads:
      OTEL_EXPORTER_OTLP_ENDPOINT  — OTLP collector endpoint
      OTEL_SERVICE_NAME            — overrides the service_name argument
      OTEL_TRACES_EXPORTER         — "otlp" (default) or "console"

    No-op if opentelemetry-sdk is not installed or OTLP exporter is unavailable.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        return

    import os

    service = os.environ.get("OTEL_SERVICE_NAME", service_name)
    resource = Resource({SERVICE_NAME: service})
    provider = TracerProvider(resource=resource)

    exporter_name = os.environ.get("OTEL_TRACES_EXPORTER", "otlp")

    if exporter_name == "console":
        from opentelemetry.sdk.trace.export import ConsoleSpanExporter

        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    else:
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )

            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        except ImportError:
            return

    trace.set_tracer_provider(provider)

    try:
        from opentelemetry.instrumentation.logging import LoggingInstrumentor

        LoggingInstrumentor().instrument()
    except ImportError:
        pass
