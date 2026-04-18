"""Tests for GraphQL db.graphql generation."""

import json
from pathlib import Path

from dbt_mdl import extract_project, format_graphql


DUCKDB_DIR = Path(__file__).parent.parent / "fixtures" / "dbt-artifacts"
CATALOG = DUCKDB_DIR / "catalog.json"
MANIFEST = DUCKDB_DIR / "manifest.json"


def _make_project(**kwargs):
    return extract_project(CATALOG, MANIFEST, **kwargs)


class TestDbGraphQL:
    def test_has_types(self):
        project = _make_project()
        gj = format_graphql(project)
        assert "type customers" in gj.db_graphql
        assert "type orders" in gj.db_graphql

    def test_has_relation_directive(self):
        project = _make_project()
        gj = format_graphql(project)
        assert "@relation(type: customers, field: customer_id)" in gj.db_graphql

    def test_required_fields_have_bang(self):
        project = _make_project()
        gj = format_graphql(project)
        assert "Integer!" in gj.db_graphql or "Bigint!" in gj.db_graphql

    def test_all_models_present(self):
        project = _make_project()
        gj = format_graphql(project)
        for model in project.models:
            assert f"type {model.name}" in gj.db_graphql

    def test_non_public_schema_directive(self):
        project = _make_project()
        gj = format_graphql(project)
        if any(m.schema_ and m.schema_ != "public" for m in project.models):
            assert "@schema(name:" in gj.db_graphql

    def test_exclude_patterns(self):
        project = _make_project(exclude_patterns=[r"^stg_"])
        gj = format_graphql(project)
        assert "type stg_orders" not in gj.db_graphql
        assert "type customers" in gj.db_graphql

    def test_starts_with_type(self):
        project = _make_project()
        gj = format_graphql(project)
        first_line = gj.db_graphql.splitlines()[0]
        assert first_line.startswith("type ")


class TestTypeMapping:
    def test_pascal_case_type_names(self):
        from dbt_mdl.graphql.formatter import _column_line
        from dbt_mdl.ir.models import ColumnInfo, ModelInfo

        m = ModelInfo(name="t", database="db", schema_="public", columns=[])

        c = ColumnInfo(name="id", type="INTEGER", not_null=True)
        line = _column_line(m, c, rel_map={})
        assert "id: Integer!" in line
        assert '@sql(type: "INTEGER")' in line

        c = ColumnInfo(name="name", type="VARCHAR(255)", not_null=False)
        line = _column_line(m, c, rel_map={})
        assert "name: Varchar" in line
        assert '@sql(type: "VARCHAR", size: "255")' in line

    def test_multiword_types(self):
        from dbt_mdl.graphql.formatter import _column_line
        from dbt_mdl.ir.models import ColumnInfo, ModelInfo

        m = ModelInfo(name="t", database="db", schema_="public", columns=[])

        c = ColumnInfo(name="ts", type="TIMESTAMP WITH TIME ZONE", not_null=False)
        line = _column_line(m, c, rel_map={})
        assert "ts: TimestampWithTimeZone" in line
        assert '@sql(type: "TIMESTAMP WITH TIME ZONE")' in line

    def test_array_type(self):
        from dbt_mdl.graphql.formatter import _column_line
        from dbt_mdl.ir.models import ColumnInfo, ModelInfo

        m = ModelInfo(name="t", database="db", schema_="public", columns=[])

        c = ColumnInfo(name="tags", type="TEXT[]", not_null=False)
        line = _column_line(m, c, rel_map={})
        assert "tags: [Text]" in line
        assert '@sql(type: "TEXT")' in line

    def test_bigquery_array(self):
        from dbt_mdl.graphql.formatter import _column_line
        from dbt_mdl.ir.models import ColumnInfo, ModelInfo

        m = ModelInfo(name="t", database="db", schema_="public", columns=[])

        c = ColumnInfo(name="items", type="ARRAY<STRING>", not_null=False)
        line = _column_line(m, c, rel_map={})
        assert "items: [String]" in line
        assert '@sql(type: "STRING")' in line

    def test_empty_type_no_fallback(self):
        from dbt_mdl.graphql.formatter import _column_line
        from dbt_mdl.ir.models import ColumnInfo, ModelInfo

        m = ModelInfo(name="t", database="db", schema_="public", columns=[])
        c = ColumnInfo(name="x", type="", not_null=False)
        line = _column_line(m, c, rel_map={})
        # Empty type → empty pascal, but @sql still emitted with empty value
        assert "x: " in line
        assert '@sql(type: "")' in line


class TestNoRelationships:
    def test_no_relationships_still_works(self, tmp_path):
        data = json.loads(MANIFEST.read_text())
        keys_to_remove = [
            k for k in data["nodes"] if k.startswith("test.") and "relationships" in k
        ]
        for k in keys_to_remove:
            del data["nodes"][k]

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(data))

        project = extract_project(CATALOG, manifest_path)
        assert len(project.relationships) == 0

        gj = format_graphql(project)
        assert "@relation" not in gj.db_graphql
