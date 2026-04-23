from __future__ import annotations


def instrument_sqlalchemy(engine) -> None:
    """Attach SQLAlchemy OTel instrumentation to an engine."""
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)


def instrument_starlette(app) -> None:
    """Attach Starlette OTel instrumentation to the app."""
    from opentelemetry.instrumentation.starlette import StarletteInstrumentor

    StarletteInstrumentor().instrument_app(app)


def build_graphql_http_handler():
    """Return GraphQLHTTPHandler with OpenTelemetryExtension."""
    from ariadne.asgi.handlers import GraphQLHTTPHandler
    from ariadne.contrib.tracing.opentelemetry import OpenTelemetryExtension

    return GraphQLHTTPHandler(extensions=[OpenTelemetryExtension])
