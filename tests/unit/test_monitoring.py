"""Unit tests for dbt_graphql.monitoring.configure_monitoring."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_otel_mocks():
    trace_mod = MagicMock()
    sdk_resources = MagicMock()
    sdk_resources.SERVICE_NAME = "service.name"
    sdk_trace = MagicMock()
    sdk_export = MagicMock()
    return {
        "opentelemetry": trace_mod,
        "opentelemetry.trace": trace_mod,
        "opentelemetry.sdk": MagicMock(),
        "opentelemetry.sdk.resources": sdk_resources,
        "opentelemetry.sdk.trace": sdk_trace,
        "opentelemetry.sdk.trace.export": sdk_export,
        "opentelemetry.instrumentation": MagicMock(),
        "opentelemetry.instrumentation.logging": MagicMock(),
    }


class TestConfigureMonitoring:
    def test_console_exporter_used_when_specified(self):
        mocks = _make_otel_mocks()
        console_exporter_cls = MagicMock()
        mocks[
            "opentelemetry.sdk.trace.export"
        ].ConsoleSpanExporter = console_exporter_cls

        with patch.dict("sys.modules", mocks):
            from importlib import reload
            import dbt_graphql.monitoring as tel

            reload(tel)
            tel.configure_monitoring(exporter="console")

        console_exporter_cls.assert_called_once()

    def test_otlp_grpc_exporter_used_by_default(self):
        mocks = _make_otel_mocks()
        otlp_exporter_cls = MagicMock()
        otlp_mod = MagicMock()
        otlp_mod.OTLPSpanExporter = otlp_exporter_cls
        mocks["opentelemetry.exporter"] = MagicMock()
        mocks["opentelemetry.exporter.otlp"] = MagicMock()
        mocks["opentelemetry.exporter.otlp.proto"] = MagicMock()
        mocks["opentelemetry.exporter.otlp.proto.grpc"] = MagicMock()
        mocks["opentelemetry.exporter.otlp.proto.grpc.trace_exporter"] = otlp_mod

        with patch.dict("sys.modules", mocks):
            from importlib import reload
            import dbt_graphql.monitoring as tel

            reload(tel)
            tel.configure_monitoring()

        otlp_exporter_cls.assert_called_once_with()

    def test_otlp_http_exporter_used_when_configured(self):
        mocks = _make_otel_mocks()
        otlp_exporter_cls = MagicMock()
        otlp_mod = MagicMock()
        otlp_mod.OTLPSpanExporter = otlp_exporter_cls
        mocks["opentelemetry.exporter"] = MagicMock()
        mocks["opentelemetry.exporter.otlp"] = MagicMock()
        mocks["opentelemetry.exporter.otlp.proto"] = MagicMock()
        mocks["opentelemetry.exporter.otlp.proto.http"] = MagicMock()
        mocks["opentelemetry.exporter.otlp.proto.http.trace_exporter"] = otlp_mod

        with patch.dict("sys.modules", mocks):
            from importlib import reload
            import dbt_graphql.monitoring as tel

            reload(tel)
            tel.configure_monitoring(protocol="http", endpoint="http://collector:4318")

        otlp_exporter_cls.assert_called_once_with(endpoint="http://collector:4318")

    def test_otlp_exporter_receives_endpoint(self):
        mocks = _make_otel_mocks()
        otlp_exporter_cls = MagicMock()
        otlp_mod = MagicMock()
        otlp_mod.OTLPSpanExporter = otlp_exporter_cls
        mocks["opentelemetry.exporter"] = MagicMock()
        mocks["opentelemetry.exporter.otlp"] = MagicMock()
        mocks["opentelemetry.exporter.otlp.proto"] = MagicMock()
        mocks["opentelemetry.exporter.otlp.proto.grpc"] = MagicMock()
        mocks["opentelemetry.exporter.otlp.proto.grpc.trace_exporter"] = otlp_mod

        with patch.dict("sys.modules", mocks):
            from importlib import reload
            import dbt_graphql.monitoring as tel

            reload(tel)
            tel.configure_monitoring(endpoint="http://collector:4317")

        otlp_exporter_cls.assert_called_once_with(endpoint="http://collector:4317")

    def test_service_name_passed_to_resource(self):
        mocks = _make_otel_mocks()
        resource_cls = MagicMock()
        mocks["opentelemetry.sdk.resources"].Resource = resource_cls
        otlp_mod = MagicMock()
        mocks["opentelemetry.exporter"] = MagicMock()
        mocks["opentelemetry.exporter.otlp"] = MagicMock()
        mocks["opentelemetry.exporter.otlp.proto"] = MagicMock()
        mocks["opentelemetry.exporter.otlp.proto.grpc"] = MagicMock()
        mocks["opentelemetry.exporter.otlp.proto.grpc.trace_exporter"] = otlp_mod

        with patch.dict("sys.modules", mocks):
            from importlib import reload
            import dbt_graphql.monitoring as tel

            reload(tel)
            tel.configure_monitoring(service_name="my-service")

        resource_cls.assert_called_once_with({"service.name": "my-service"})
