"""Tests for GraphJin config generation."""

import json
from pathlib import Path

import yaml

from dbt_mdl import extract_project, format_graphjin


SQLITE_DIR = Path(__file__).parent.parent / "fixtures" / "sqlite"
DUCKDB_DIR = Path(__file__).parent.parent / "fixtures" / "duckdb"


def _make_project(fixture_dir: Path = SQLITE_DIR):
    """Return explicit paths for extract_project."""
    return {
        "profiles_path": fixture_dir / "profiles.yml",
        "catalog_path": fixture_dir / "catalog.json",
        "manifest_path": fixture_dir / "manifest.json",
    }


class TestDevYml:
    def test_is_valid_yaml(self):
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        assert isinstance(yaml.safe_load(gj.dev_yml), dict)

    def test_database_type_matches_adapter(self):
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.dev_yml)
        assert data["database"]["type"] == "sqlite"

    def test_has_enable_schema(self):
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.dev_yml)
        assert data.get("enable_schema") is True

    def test_has_auth_block(self):
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.dev_yml)
        assert data.get("auth", {}).get("type") == "none"

    def test_default_block_is_false(self):
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.dev_yml)
        assert data.get("default_block") is False

    def test_sqlite_db_path_in_host(self):
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.dev_yml)
        # GraphJin sqlite uses host for the file path
        assert data["database"]["type"] == "sqlite"
        assert "host" in data["database"]
        assert data["database"]["host"].endswith(".db")

    def test_tables_only_when_meaningful(self):
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.dev_yml)
        tables = data.get("tables") or []
        for entry in tables:
            assert "columns" in entry or entry.get("table") != entry.get("name")

    def test_relationships_in_tables_columns(self):
        """Relationships are specified via @relation directive in db.graphql schema."""
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        # Verify tables block is empty - all relationship info is in schema
        data = yaml.safe_load(gj.dev_yml)
        tables = data.get("tables") or []
        assert tables == [], "tables should be empty - relations in schema"
        # Verify @relation directive is in schema
        assert "@relation(type: customers, field: customer_id)" in gj.db_graphql

    def test_unsupported_adapter_raises(self):
        """DuckDB is unsupported — should raise ValueError."""
        import pytest

        project = extract_project(**_make_project(DUCKDB_DIR))
        with pytest.raises(ValueError, match="does not support.*duckdb"):
            format_graphjin(project)


class TestDbGraphQL:
    def test_has_dbinfo_header(self):
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        first_line = gj.db_graphql.splitlines()[0]
        assert first_line.startswith("# dbinfo:")
        assert "sqlite" in first_line

    def test_has_types(self):
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        assert "type customers" in gj.db_graphql
        assert "type orders" in gj.db_graphql

    def test_has_relation_directive(self):
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        assert "@relation(type: customers, field: customer_id)" in gj.db_graphql

    def test_required_fields_have_bang(self):
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        assert "Integer!" in gj.db_graphql or "BigInt!" in gj.db_graphql

    def test_all_models_present(self):
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        for model in project.models:
            assert f"type {model.name}" in gj.db_graphql

    def test_non_public_schema_directive(self):
        """SQLite fixture uses 'main' schema — should emit @schema(name: main)."""
        project = extract_project(**_make_project())
        gj = format_graphjin(project)
        if any(m.schema_ and m.schema_ != "public" for m in project.models):
            assert "@schema(name:" in gj.db_graphql

    def test_exclude_patterns(self):
        project = extract_project(**_make_project(), exclude_patterns=[r"^stg_"])
        gj = format_graphjin(project)
        assert "type stg_orders" not in gj.db_graphql
        assert "type customers" in gj.db_graphql


class TestTypeMapping:
    def test_sql_to_gql_known_types(self):
        from dbt_mdl.graphjin.formatter import _sql_to_gql_type

        assert _sql_to_gql_type("INTEGER") == ("Integer", "")
        assert _sql_to_gql_type("BIGINT") == ("BigInt", "")
        assert _sql_to_gql_type("SMALLINT") == ("SmallInt", "")
        assert _sql_to_gql_type("VARCHAR") == ("Varchar", "")
        assert _sql_to_gql_type("TEXT") == ("Text", "")
        assert _sql_to_gql_type("BOOLEAN") == ("Boolean", "")
        assert _sql_to_gql_type("DATE") == ("Date", "")
        assert _sql_to_gql_type("TIMESTAMP") == ("Timestamp", "")
        assert _sql_to_gql_type("JSONB") == ("Jsonb", "")
        assert _sql_to_gql_type("UUID") == ("Uuid", "")

    def test_size_is_extracted(self):
        from dbt_mdl.graphjin.formatter import _sql_to_gql_type

        assert _sql_to_gql_type("VARCHAR(255)") == ("Varchar", "255")
        assert _sql_to_gql_type("NUMERIC(10,2)") == ("Numeric", "10,2")

    def test_multiword_types(self):
        from dbt_mdl.graphjin.formatter import _sql_to_gql_type

        assert _sql_to_gql_type("TIMESTAMP WITH TIME ZONE") == (
            "TimestampWithTimeZone",
            "",
        )
        assert _sql_to_gql_type("CHARACTER VARYING") == ("CharacterVarying", "")
        assert _sql_to_gql_type("DOUBLE PRECISION") == ("DoublePrecision", "")

    def test_bigquery_aliases(self):
        from dbt_mdl.graphjin.formatter import _sql_to_gql_type

        assert _sql_to_gql_type("INT64") == ("BigInt", "")
        assert _sql_to_gql_type("FLOAT64") == ("DoublePrecision", "")

    def test_array_detection(self):
        from dbt_mdl.graphjin.formatter import _parse_sql_type

        assert _parse_sql_type("INTEGER[]") == ("integer", "", True)
        assert _parse_sql_type("TEXT[]") == ("text", "", True)
        assert _parse_sql_type("ARRAY<STRING>") == ("string", "", True)

    def test_array_renders_as_list(self):
        from dbt_mdl.graphjin.formatter import _column_line
        from dbt_mdl.domain.models import ColumnInfo, ModelInfo

        m = ModelInfo(name="t", database="db", schema_="public", columns=[])
        c = ColumnInfo(name="tags", type="TEXT[]", not_null=False)
        line = _column_line(m, c, rel_map={})
        assert "[Text]" in line


class TestNoRelationships:
    def test_no_relationships_still_works(self, tmp_path):
        data = json.loads((SQLITE_DIR / "manifest.json").read_text())
        keys_to_remove = [
            k for k in data["nodes"] if k.startswith("test.") and "relationships" in k
        ]
        for k in keys_to_remove:
            del data["nodes"][k]

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(data))

        project = extract_project(
            profiles_path=SQLITE_DIR / "profiles.yml",
            catalog_path=SQLITE_DIR / "catalog.json",
            manifest_path=manifest_path,
        )
        assert len(project.relationships) == 0

        gj = format_graphjin(project)
        assert "@relation" not in gj.db_graphql
        assert isinstance(yaml.safe_load(gj.dev_yml), dict)
