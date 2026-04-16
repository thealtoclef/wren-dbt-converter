import json
import shutil
import pytest
from pathlib import Path

from dbt_mdl import extract_project, format_mdl, ConvertResult
from dbt_mdl.wren.models import WrenMDLManifest


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "duckdb"


def _build(dbt_project, **kwargs):
    """Helper: extract + format as MDL."""
    project = extract_project(**dbt_project, **kwargs)
    return format_mdl(project)


def test_build_manifest_returns_result(dbt_project):
    result = _build(dbt_project)
    assert isinstance(result, ConvertResult)
    assert isinstance(result.manifest, WrenMDLManifest)


def test_build_manifest_has_models(dbt_project):
    result = _build(dbt_project)
    model_names = {m.name for m in result.manifest.models}
    assert "customers" in model_names
    assert "orders" in model_names
    # all models included by default
    assert "stg_orders" in model_names


def test_build_manifest_exclude_patterns(dbt_project):
    result = _build(dbt_project, exclude_patterns=[r"^stg_", r"^staging_"])
    model_names = {m.name for m in result.manifest.models}
    assert "customers" in model_names
    assert "orders" in model_names
    assert "stg_orders" not in model_names


def test_build_manifest_exclude_multiple_independent_patterns(dbt_project):
    # customers matches ^cust, orders matches ^ord — both excluded
    result = _build(dbt_project, exclude_patterns=[r"^cust", r"^ord"])
    model_names = {m.name for m in result.manifest.models}
    assert "customers" not in model_names
    assert "orders" not in model_names
    assert "stg_orders" in model_names


def test_build_manifest_has_relationship(dbt_project):
    result = _build(dbt_project)
    assert len(result.manifest.relationships) == 1
    rel = result.manifest.relationships[0]
    model_names = set(rel.models)
    assert model_names == {"orders", "customers"}


def test_build_manifest_has_enum(dbt_project):
    result = _build(dbt_project)
    # orders.status / stg_orders.status → 1 deduped enum; stg_payments.payment_method → 1 more
    assert len(result.manifest.enum_definitions) == 2
    all_value_sets = {
        tuple(sorted(v.name for v in e.values))
        for e in result.manifest.enum_definitions
    }
    assert (
        "completed",
        "placed",
        "return_pending",
        "returned",
        "shipped",
    ) in all_value_sets
    assert ("bank_transfer", "coupon", "credit_card", "gift_card") in all_value_sets


def test_manifest_str_is_base64_json(dbt_project):
    result = _build(dbt_project)
    # manifest_str should be base64 encoded JSON
    import base64

    raw = base64.b64decode(result.manifest_str)
    data = json.loads(raw)
    assert "models" in data
    assert "relationships" in data


def test_missing_catalog(tmp_path):
    profiles = tmp_path / "profiles.yml"
    manifest = tmp_path / "manifest.json"
    shutil.copy(FIXTURES_DIR / "profiles.yml", profiles)
    shutil.copy(FIXTURES_DIR / "manifest.json", manifest)
    with pytest.raises(FileNotFoundError, match="catalog.json"):
        extract_project(
            profiles_path=profiles,
            catalog_path=tmp_path / "catalog.json",
            manifest_path=manifest,
        )


def test_missing_profiles(tmp_path):
    catalog = tmp_path / "catalog.json"
    manifest = tmp_path / "manifest.json"
    shutil.copy(FIXTURES_DIR / "catalog.json", catalog)
    shutil.copy(FIXTURES_DIR / "manifest.json", manifest)
    with pytest.raises(FileNotFoundError, match="profiles.yml"):
        extract_project(
            profiles_path=tmp_path / "profiles.yml",
            catalog_path=catalog,
            manifest_path=manifest,
        )


def test_not_null_propagated(dbt_project):
    result = _build(dbt_project)
    customers = next(m for m in result.manifest.models if m.name == "customers")
    by_name = {c.name: c for c in customers.columns}
    assert by_name["customer_id"].not_null is True


def test_model_description_in_properties(dbt_project):
    result = _build(dbt_project)
    customers = next(m for m in result.manifest.models if m.name == "customers")
    assert customers.properties is not None
    assert (
        customers.properties.get("description")
        == "This table has basic information about a customer, as well as some derived facts based on a customer's orders"
    )
