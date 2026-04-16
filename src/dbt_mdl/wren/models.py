"""Wren MDL models: codegen wrappers and DataSource enum.

The generated models live in ``codegen/mdl.py`` and are produced by
``datamodel-codegen`` from the upstream Wren MDL JSON schema.  This module
re-exports them under the names used throughout the converter so that downstream
code only imports from here — never directly from the generated module.
"""

from __future__ import annotations

import enum
from enum import auto

from .codegen.mdl import (  # noqa: F401
    Column,
    EnumDefinition,
    JoinType,
    Models2,
    Relationship,
    TableReference,
    Value,
    WrenmdlManifestSchema,
)

# Domain aliases — the rest of the codebase uses these names
WrenColumn = Column
WrenModel = Models2
WrenMDLManifest = WrenmdlManifestSchema
EnumValue = Value


# ---------------------------------------------------------------------------
# DataSource enum
# ---------------------------------------------------------------------------


@enum.unique
class DataSource(enum.StrEnum):
    """Supported database and data-source types for Wren MDL.

    ``DataSource("postgresql")`` resolves to ``DataSource.postgres`` and
    ``DataSource("sqlserver")`` resolves to ``DataSource.mssql`` — the two
    dbt adapter names that differ from our internal identifiers.
    """

    athena = auto()
    bigquery = auto()
    canner = auto()
    clickhouse = auto()
    mssql = auto()
    mysql = auto()
    doris = auto()
    oracle = auto()
    postgres = auto()
    redshift = auto()
    snowflake = auto()
    trino = auto()
    local_file = auto()
    s3_file = auto()
    minio_file = auto()
    gcs_file = auto()
    duckdb = auto()
    spark = auto()
    databricks = auto()

    @classmethod
    def _missing_(cls, value: object) -> DataSource | None:
        """Handle dbt adapter name aliases."""
        aliases: dict[str, str] = {
            "postgresql": "postgres",
            "sqlserver": "mssql",
        }
        if isinstance(value, str) and value.lower() in aliases:
            return cls(aliases[value.lower()])
        return None
