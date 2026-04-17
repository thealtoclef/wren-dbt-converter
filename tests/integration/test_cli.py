import json
from pathlib import Path

import pytest

from dbt_mdl.cli import main

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "duckdb"


def _cli_base_args(dbt_project):
    return [
        "wren",
        "--profiles",
        str(dbt_project["profiles_path"]),
        "--catalog",
        str(dbt_project["catalog_path"]),
        "--manifest",
        str(dbt_project["manifest_path"]),
    ]


def test_cli_produces_output_files(dbt_project, tmp_path):
    output_dir = tmp_path / "out"
    main(_cli_base_args(dbt_project) + ["--output", str(output_dir)])
    assert (output_dir / "mdl.json").exists()
    assert (output_dir / "connection.json").exists()


def test_cli_mdl_json_valid(dbt_project, tmp_path):
    output_dir = tmp_path / "out"
    main(_cli_base_args(dbt_project) + ["--output", str(output_dir)])
    data = json.loads((output_dir / "mdl.json").read_text())
    assert "models" in data
    assert "relationships" in data
    assert any(m["name"] == "customers" for m in data["models"])


def test_cli_connection_json_valid(dbt_project, tmp_path):
    output_dir = tmp_path / "out"
    main(_cli_base_args(dbt_project) + ["--output", str(output_dir)])
    data = json.loads((output_dir / "connection.json").read_text())
    assert "dataSource" in data
    assert "connection" in data
    assert data["dataSource"] == "duckdb"


def test_cli_all_models_included_by_default(dbt_project, tmp_path):
    output_dir = tmp_path / "out"
    main(_cli_base_args(dbt_project) + ["--output", str(output_dir)])
    data = json.loads((output_dir / "mdl.json").read_text())
    model_names = [m["name"] for m in data["models"]]
    assert "stg_orders" in model_names


def test_cli_exclude_single_pattern(dbt_project, tmp_path):
    output_dir = tmp_path / "out"
    main(
        _cli_base_args(dbt_project)
        + ["--exclude", "^stg_", "--output", str(output_dir)]
    )
    data = json.loads((output_dir / "mdl.json").read_text())
    model_names = [m["name"] for m in data["models"]]
    assert "stg_orders" not in model_names
    assert "customers" in model_names


def test_cli_exclude_multiple_patterns(dbt_project, tmp_path):
    output_dir = tmp_path / "out"
    main(
        _cli_base_args(dbt_project)
        + [
            "--exclude",
            "^stg_",
            "--exclude",
            "^ord",
            "--output",
            str(output_dir),
        ]
    )
    data = json.loads((output_dir / "mdl.json").read_text())
    model_names = [m["name"] for m in data["models"]]
    assert "stg_orders" not in model_names
    assert "orders" not in model_names
    assert "customers" in model_names


def test_cli_missing_profiles_exits(tmp_path):
    with pytest.raises(SystemExit):
        main(
            [
                "wren",
                "--catalog",
                str(tmp_path / "catalog.json"),
                "--manifest",
                str(tmp_path / "manifest.json"),
                "--output",
                str(tmp_path / "out"),
            ]
        )


def test_cli_graphjin_unsupported_adapter(dbt_project, tmp_path):
    """DuckDB is unsupported by GraphJin — should error."""
    output_dir = tmp_path / "out"
    with pytest.raises(SystemExit):
        main(
            [
                "graphjin",
                "--profiles",
                str(dbt_project["profiles_path"]),
                "--catalog",
                str(dbt_project["catalog_path"]),
                "--manifest",
                str(dbt_project["manifest_path"]),
                "--output",
                str(output_dir),
            ]
        )


def test_cli_default_output_is_cwd(dbt_project, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    main(_cli_base_args(dbt_project))
    assert (tmp_path / "mdl.json").exists()
    assert (tmp_path / "connection.json").exists()
