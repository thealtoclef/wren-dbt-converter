"""Intermediate representation: format-agnostic domain models.

These Pydantic models decouple dbt artifact parsing from any specific output
format (MDL, GraphJin, etc.). Processors populate these types, and formatters
consume them to produce format-specific output.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, constr


# ---------------------------------------------------------------------------
# Project / model / column / relationship
# ---------------------------------------------------------------------------


class ColumnInfo(BaseModel):
    name: str
    type: str = ""  # raw DB type from catalog.json (e.g. "INTEGER", "VARCHAR(255)")
    not_null: bool = False
    unique: bool = False
    description: str = ""
    enum_values: list[str] | None = None
    is_primary_key: bool = False
    is_hidden: bool = False


class RelationshipInfo(BaseModel):
    """A foreign-key relationship between two models."""

    name: str
    from_model: str
    from_column: str
    to_model: str
    to_column: str
    join_type: str = (
        "many_to_one"  # "many_to_one" | "one_to_many" | "one_to_one" | "many_to_many"
    )


class ModelInfo(BaseModel):
    """A dbt model (maps to a physical table in the database)."""

    name: str  # dbt model name
    table_name: str  # actual DB table name (usually same as model name)
    catalog: str | None = None
    schema_: str | None = Field(None, alias="schema")
    columns: list[ColumnInfo] = Field(default_factory=list)
    primary_key: str | None = None
    description: str = ""
    relationships: list[RelationshipInfo] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class DbtProjectInfo(BaseModel):
    """Complete extracted information from a dbt project.

    This is the intermediate representation that formatters consume.
    """

    models: list[ModelInfo] = Field(default_factory=list)
    relationships: list[RelationshipInfo] = Field(default_factory=list)
    enums: dict[str, list[str]] = Field(default_factory=dict)
    table_lineage: dict[str, list[str]] = Field(default_factory=dict)
    column_lineage: dict[str, dict[str, list[dict[str, str]]]] = Field(
        default_factory=dict
    )
    data_source: str = ""  # e.g. "postgres", "duckdb", "bigquery"
    connection_info: dict[str, Any] = Field(default_factory=dict)

    def build_lineage_schema(self) -> LineageSchema:
        """Build a LineageSchema from the raw lineage data in this project."""
        table_lineage_items = [
            TableLineageItem(source=source, target=target)
            for target, sources in self.table_lineage.items()
            for source in sources
        ]

        # Group column lineage by (source, target)
        grouped: dict[tuple[str, str], list[Column]] = {}
        for target, col_map in self.column_lineage.items():
            for col_name, edges in col_map.items():
                for edge in edges:
                    key = (edge["source_model"], target)
                    try:
                        lt = LineageType(edge["lineage_type"])
                    except ValueError:
                        lt = LineageType.unknown
                    grouped.setdefault(key, []).append(
                        Column(
                            source_column=edge["source_column"],
                            target_column=edge["target_column"],
                            lineage_type=lt,
                        )
                    )

        column_lineage_items = [
            ColumnLineageItem(source=s, target=t, columns=c)
            for (s, t), c in grouped.items()
        ]

        catalog_name = ""
        schema_name = ""
        if self.models:
            catalog_name = self.models[0].catalog or ""
            schema_name = self.models[0].schema_ or ""

        return LineageSchema(
            catalog=catalog_name,
            schema=schema_name,
            data_source=self.data_source,
            table_lineage=table_lineage_items if table_lineage_items else [],
            column_lineage=column_lineage_items if column_lineage_items else [],
        )


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


class TableLineageItem(BaseModel):
    """A single table-level lineage edge (source feeds into target)."""

    model_config = ConfigDict(extra="forbid", validate_by_name=True)

    source: constr(min_length=1) = Field(
        ..., description="The upstream (feeding) model name."
    )
    target: constr(min_length=1) = Field(
        ..., description="The downstream (consuming) model name."
    )


class LineageType(StrEnum):
    """Classification of how a column value is propagated."""

    pass_through = "pass-through"
    rename = "rename"
    transformation = "transformation"
    filter = "filter"
    join = "join"
    unknown = "unknown"


class Column(BaseModel):
    """A single column-level lineage mapping within an edge."""

    model_config = ConfigDict(extra="forbid", validate_by_name=True)

    source_column: constr(min_length=1) = Field(
        ..., alias="sourceColumn", description="Column name in the source model."
    )
    target_column: constr(min_length=0) = Field(
        ...,
        alias="targetColumn",
        description="Column name in the target model. Empty for structural edges (filter/join/unknown).",
    )
    lineage_type: LineageType = Field(
        ...,
        alias="lineageType",
        description="Values: pass-through, rename, transformation, filter, join, unknown.",
    )


class ColumnLineageItem(BaseModel):
    """Column-level lineage edges grouped by a single table-level relationship."""

    model_config = ConfigDict(extra="forbid", validate_by_name=True)

    source: constr(min_length=1) = Field(
        ..., description="The upstream (feeding) model name."
    )
    target: constr(min_length=1) = Field(
        ..., description="The downstream (consuming) model name."
    )
    columns: list[Column] = Field(..., description="Column-level lineage mappings.")


class LineageSchema(BaseModel):
    """Root schema for dbt model lineage (table + column level)."""

    model_config = ConfigDict(extra="forbid", validate_by_name=True)

    catalog: str = Field(..., description="Catalog name.")
    schema_: str = Field(..., alias="schema", description="Schema name.")
    data_source: str = Field(
        ...,
        alias="dataSource",
        description="Data source type (e.g., BIGQUERY, SNOWFLAKE, POSTGRES).",
    )
    table_lineage: list[TableLineageItem] = Field(
        ...,
        alias="tableLineage",
        description="Table-level lineage edges. Each edge represents a data flow from a source model to a target model.",
    )
    column_lineage: list[ColumnLineageItem] = Field(
        ..., alias="columnLineage", description="Column-level lineage edges."
    )
