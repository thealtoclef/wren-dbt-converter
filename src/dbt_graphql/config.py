from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class DbConfig(BaseModel):
    type: str
    host: str = ""
    port: int | None = None
    dbname: str = ""
    user: str = ""
    password: str = ""


class ServeConfig(BaseModel):
    host: str
    port: int


class MonitoringConfig(BaseModel):
    service_name: str = "dbt-graphql"
    exporter: str = "otlp"
    protocol: str = "grpc"  # "grpc" or "http"
    endpoint: str | None = None
    log_level: str = "INFO"


class EnrichmentConfig(BaseModel):
    budget: int = 20
    distinct_values_limit: int = 50
    distinct_values_max_cardinality: int = 500


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DBT_GRAPHQL__",
        env_nested_delimiter="__",
    )

    db: DbConfig
    serve: ServeConfig | None = None
    monitoring: MonitoringConfig = MonitoringConfig()
    enrichment: EnrichmentConfig = EnrichmentConfig()

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,  # noqa: ARG003 — required for override signature
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        # env vars take precedence over config file (init_settings)
        return env_settings, init_settings, dotenv_settings, file_secret_settings


def load_config(path: str | Path) -> AppConfig:
    """Load config.yml and merge with DBT_GRAPHQL__* environment variables.

    Env vars override file values. Example: DBT_GRAPHQL__ENRICHMENT__BUDGET=5
    """
    data = yaml.safe_load(Path(path).read_text())
    if not isinstance(data, dict):
        raise ValueError("config.yml must be a YAML mapping")
    return AppConfig(**data)
