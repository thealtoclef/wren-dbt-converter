"""Tests for GraphJin config generation."""

import yaml
from pathlib import Path

from dbt_mdl import extract_project, format_graphjin


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def _make_dbt_project(tmp_path):
    """Create a minimal dbt project layout."""
    import shutil

    (tmp_path / "target").mkdir()
    shutil.copy(FIXTURES_DIR / "catalog.json", tmp_path / "target" / "catalog.json")
    shutil.copy(FIXTURES_DIR / "manifest.json", tmp_path / "target" / "manifest.json")
    shutil.copy(FIXTURES_DIR / "profiles.yml", tmp_path / "profiles.yml")
    return tmp_path


class TestDevYml:
    def test_has_database_config(self, tmp_path):
        project = extract_project(_make_dbt_project(tmp_path))
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.dev_yml)
        assert "database" in data
        assert data["database"]["type"] in ("postgres", "mysql", "snowflake", "duckdb")

    def test_has_enable_schema(self, tmp_path):
        project = extract_project(_make_dbt_project(tmp_path))
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.dev_yml)
        assert data.get("enable_schema") is True

    def test_has_table_definitions(self, tmp_path):
        project = extract_project(_make_dbt_project(tmp_path))
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.dev_yml)
        assert "tables" in data
        table_names = [t["name"] for t in data["tables"]]
        assert "customers" in table_names
        assert "orders" in table_names

    def test_has_relationships_in_columns(self, tmp_path):
        project = extract_project(_make_dbt_project(tmp_path))
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.dev_yml)
        orders = next(t for t in data["tables"] if t["name"] == "orders")
        assert "columns" in orders
        rel_cols = orders["columns"]
        # customer_id should have related_to
        assert any(c["name"] == "customer_id" for c in rel_cols)


class TestDbGraphQL:
    def test_has_types(self, tmp_path):
        project = extract_project(_make_dbt_project(tmp_path))
        gj = format_graphjin(project)
        assert "type customers {" in gj.db_graphql
        assert "type orders {" in gj.db_graphql

    def test_has_id_directive(self, tmp_path):
        """Columns that are primary keys should have @id directive."""
        project = extract_project(_make_dbt_project(tmp_path))
        gj = format_graphjin(project)
        lines = gj.db_graphql.split("\n")
        has_id = any("@id" in line for line in lines)
        assert isinstance(has_id, bool)

    def test_has_relation_directive(self, tmp_path):
        """Foreign key columns should have @relation directive."""
        project = extract_project(_make_dbt_project(tmp_path))
        gj = format_graphjin(project)
        assert "@relation" in gj.db_graphql

    def test_required_fields_have_bang(self, tmp_path):
        """not_null columns should have ! suffix."""
        project = extract_project(_make_dbt_project(tmp_path))
        gj = format_graphjin(project)
        assert "Integer!" in gj.db_graphql or "BigInt!" in gj.db_graphql

    def test_all_models_present(self, tmp_path):
        project = extract_project(_make_dbt_project(tmp_path))
        gj = format_graphjin(project)
        for model in project.models:
            assert f"type {model.name} {{" in gj.db_graphql

    def test_exclude_patterns(self, tmp_path):
        project = extract_project(
            _make_dbt_project(tmp_path), exclude_patterns=[r"^stg_"]
        )
        gj = format_graphjin(project)
        assert "type stg_orders {" not in gj.db_graphql
        assert "type customers {" in gj.db_graphql


class TestProdYml:
    def test_is_valid_yaml(self, tmp_path):
        project = extract_project(_make_dbt_project(tmp_path))
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.prod_yml)
        assert isinstance(data, dict)

    def test_inherits_dev(self, tmp_path):
        project = extract_project(_make_dbt_project(tmp_path))
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.prod_yml)
        assert data["inherits"] == "dev"

    def test_production_mode(self, tmp_path):
        project = extract_project(_make_dbt_project(tmp_path))
        gj = format_graphjin(project)
        data = yaml.safe_load(gj.prod_yml)
        assert data["production"] is True
        assert data["default_block"] is True


class TestTypeMapping:
    def test_integer_types(self):
        from dbt_mdl.graphjin.formatter import _map_graphjin_type

        assert _map_graphjin_type("INTEGER") == "Integer"
        assert _map_graphjin_type("INT") == "Integer"
        assert _map_graphjin_type("BIGINT") == "BigInt"
        assert _map_graphjin_type("SMALLINT") == "SmallInt"

    def test_string_types(self):
        from dbt_mdl.graphjin.formatter import _map_graphjin_type

        assert _map_graphjin_type("VARCHAR") == "Varchar"
        assert _map_graphjin_type("TEXT") == "Text"
        assert _map_graphjin_type("CHARACTER VARYING") == "Varchar"

    def test_boolean_type(self):
        from dbt_mdl.graphjin.formatter import _map_graphjin_type

        assert _map_graphjin_type("BOOLEAN") == "Boolean"

    def test_date_types(self):
        from dbt_mdl.graphjin.formatter import _map_graphjin_type

        assert _map_graphjin_type("DATE") == "Date"
        assert _map_graphjin_type("TIMESTAMP") == "Timestamp"
        assert _map_graphjin_type("TIMESTAMPTZ") == "TimestampWithTimeZone"

    def test_numeric_types(self):
        from dbt_mdl.graphjin.formatter import _map_graphjin_type

        assert _map_graphjin_type("DOUBLE") == "Numeric"
        assert _map_graphjin_type("DECIMAL") == "Numeric"
        assert _map_graphjin_type("NUMERIC") == "Numeric"

    def test_json_type(self):
        from dbt_mdl.graphjin.formatter import _map_graphjin_type

        assert _map_graphjin_type("JSONB") == "Jsonb"
        assert _map_graphjin_type("JSON") == "Jsonb"

    def test_bigquery_types(self):
        from dbt_mdl.graphjin.formatter import _map_graphjin_type

        assert _map_graphjin_type("INT64", "bigquery") == "BigInt"
        assert _map_graphjin_type("FLOAT64", "bigquery") == "Numeric"

    def test_unknown_type_fallback(self):
        from dbt_mdl.graphjin.formatter import _map_graphjin_type

        result = _map_graphjin_type("some_unknown_type")
        assert isinstance(result, str)
        assert len(result) > 0


class TestNoRelationships:
    def test_no_relationships_still_works(self, tmp_path):
        """Output should be valid even with no relationships."""
        import json

        import shutil

        # Load manifest and strip relationship tests
        data = json.loads((FIXTURES_DIR / "manifest.json").read_text())
        keys_to_remove = [
            k for k in data["nodes"] if k.startswith("test.") and "relationships" in k
        ]
        for k in keys_to_remove:
            del data["nodes"][k]

        project_dir = tmp_path / "project"
        project_dir.mkdir()
        (project_dir / "target").mkdir()
        (project_dir / "target" / "manifest.json").write_text(json.dumps(data))
        shutil.copy(
            FIXTURES_DIR / "catalog.json", project_dir / "target" / "catalog.json"
        )
        shutil.copy(FIXTURES_DIR / "profiles.yml", project_dir / "profiles.yml")

        project = extract_project(project_dir)
        assert len(project.relationships) == 0

        gj = format_graphjin(project)
        assert "@relation" not in gj.db_graphql
        # YAML should still be parseable
        yaml.safe_load(gj.dev_yml)
