from pathlib import Path

from wren_dbt_converter.processors.lineage import (
    ColumnLineageEdge,
    extract_table_lineage,
    extract_column_lineage,
)


FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestTableLineage:
    def test_customers_depends_on_three_stg_models(self, manifest):
        result = extract_table_lineage(manifest)
        assert set(result["customers"]) == {
            "stg_customers",
            "stg_orders",
            "stg_payments",
        }

    def test_orders_depends_on_two_stg_models(self, manifest):
        result = extract_table_lineage(manifest)
        assert set(result["orders"]) == {"stg_orders", "stg_payments"}

    def test_stg_models_depend_on_seeds(self, manifest):
        result = extract_table_lineage(manifest)
        assert result["stg_customers"] == ["raw_customers"]
        assert result["stg_orders"] == ["raw_orders"]
        assert result["stg_payments"] == ["raw_payments"]

    def test_only_model_nodes_in_keys(self, manifest):
        result = extract_table_lineage(manifest)
        for key in result:
            assert key in {
                "customers",
                "orders",
                "stg_customers",
                "stg_orders",
                "stg_payments",
            }

    def test_returns_dict(self, manifest):
        result = extract_table_lineage(manifest)
        assert isinstance(result, dict)


class TestColumnLineage:
    def test_returns_dict_of_dicts(self, manifest_path, catalog_path):
        result = extract_column_lineage(manifest_path, catalog_path)
        assert isinstance(result, dict)
        for model_name, col_map in result.items():
            assert isinstance(model_name, str)
            assert isinstance(col_map, dict)

    def test_edges_have_required_fields(self, manifest_path, catalog_path):
        result = extract_column_lineage(manifest_path, catalog_path)
        for model_name, col_map in result.items():
            for col_name, edges in col_map.items():
                for edge in edges:
                    assert isinstance(edge, ColumnLineageEdge)
                    assert edge.source_model
                    assert edge.source_column
                    assert edge.target_column == col_name
                    assert edge.lineage_type in (
                        "pass-through",
                        "rename",
                        "transformation",
                    )

    def test_customers_has_column_lineage(self, manifest_path, catalog_path):
        result = extract_column_lineage(manifest_path, catalog_path)
        assert "customers" in result
        assert isinstance(result["customers"], dict)
        assert len(result["customers"]) > 0

    def test_stg_models_have_column_lineage(self, manifest_path, catalog_path):
        result = extract_column_lineage(manifest_path, catalog_path)
        assert "stg_customers" in result
        assert "stg_orders" in result
        assert "stg_payments" in result


class TestBuildLineage:
    """Tests for build_lineage which returns LineageSchema."""

    def test_returns_lineage_schema(self, manifest, manifest_path, catalog_path):
        from wren_dbt_converter.processors.lineage import build_lineage
        from wren_dbt_converter.models.lineage import LineageSchema
        from wren_dbt_converter.parsers.artifacts import load_catalog

        catalog = load_catalog(catalog_path)
        result = build_lineage(manifest, catalog, "DUCKDB", manifest_path, catalog_path)
        assert isinstance(result, LineageSchema)

    def test_lineage_schema_has_required_fields(
        self, manifest, manifest_path, catalog_path
    ):
        from wren_dbt_converter.processors.lineage import build_lineage
        from wren_dbt_converter.parsers.artifacts import load_catalog

        catalog = load_catalog(catalog_path)
        result = build_lineage(manifest, catalog, "DUCKDB", manifest_path, catalog_path)
        assert hasattr(result, "catalog")
        assert hasattr(result, "schema")
        assert hasattr(result, "data_source")
        assert hasattr(result, "table_lineage")
        assert hasattr(result, "column_lineage")


class TestConvertResultLineage:
    """Tests for lineage in ConvertResult."""

    def test_convert_result_has_lineage(self, dbt_project):
        from wren_dbt_converter import build_manifest

        result = build_manifest(dbt_project)
        assert hasattr(result, "lineage")
        assert result.lineage is not None

    def test_lineage_is_lineage_schema(self, dbt_project):
        from wren_dbt_converter import build_manifest
        from wren_dbt_converter.models.lineage import LineageSchema

        result = build_manifest(dbt_project)
        assert isinstance(result.lineage, LineageSchema)

    def test_lineage_schema_serialization(self, dbt_project):
        from wren_dbt_converter import build_manifest

        result = build_manifest(dbt_project)
        json_str = result.lineage.model_dump_json(by_alias=True, indent=2)
        assert "tableLineage" in json_str
        assert "columnLineage" in json_str
