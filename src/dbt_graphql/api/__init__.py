"""GraphQL API server (Starlette + Ariadne + Granian).

Requires the ``api`` extra: ``pip install dbt-graphql[api]``
"""


def __getattr__(name):
    if name in ("create_app", "serve"):
        from . import app

        return getattr(app, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
