import pytest
from pathlib import Path

from dbt_graphql.cli import main

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "dbt-artifacts"
CATALOG = FIXTURES_DIR / "catalog.json"
MANIFEST = FIXTURES_DIR / "manifest.json"


def _cli_graphql_args():
    return [
        "generate",
        "--catalog",
        str(CATALOG),
        "--manifest",
        str(MANIFEST),
    ]


def test_cli_produces_db_graphql(tmp_path):
    output_dir = tmp_path / "out"
    main(_cli_graphql_args() + ["--output", str(output_dir)])
    assert (output_dir / "db.graphql").exists()


def test_cli_all_models_included_by_default(tmp_path):
    output_dir = tmp_path / "out"
    main(_cli_graphql_args() + ["--output", str(output_dir)])
    content = (output_dir / "db.graphql").read_text()
    assert "type stg_orders" in content


def test_cli_exclude_single_pattern(tmp_path):
    output_dir = tmp_path / "out"
    main(_cli_graphql_args() + ["--exclude", "^stg_", "--output", str(output_dir)])
    content = (output_dir / "db.graphql").read_text()
    assert "type stg_orders" not in content
    assert "type customers" in content


def test_cli_exclude_multiple_patterns(tmp_path):
    output_dir = tmp_path / "out"
    main(
        _cli_graphql_args()
        + [
            "--exclude",
            "^stg_",
            "--exclude",
            "^ord",
            "--output",
            str(output_dir),
        ]
    )
    content = (output_dir / "db.graphql").read_text()
    assert "type stg_orders" not in content
    assert "type orders" not in content
    assert "type customers" in content


def test_cli_missing_artifacts_exits():
    with pytest.raises(SystemExit):
        main(["generate"])


def test_cli_default_output_is_cwd(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    main(_cli_graphql_args())
    assert (tmp_path / "db.graphql").exists()


def test_cli_produces_lineage_json(tmp_path):
    output_dir = tmp_path / "out"
    main(_cli_graphql_args() + ["--output", str(output_dir)])
    lineage_path = output_dir / "lineage.json"
    assert lineage_path.exists()


def test_cli_lineage_json_has_table_lineage(tmp_path):
    import json

    output_dir = tmp_path / "out"
    main(_cli_graphql_args() + ["--output", str(output_dir)])
    data = json.loads((output_dir / "lineage.json").read_text())
    assert "tableLineage" in data or "table_lineage" in data


def test_cli_missing_catalog_exits_nonzero(tmp_path):
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "generate",
                "--catalog",
                str(tmp_path / "no_catalog.json"),
                "--manifest",
                str(MANIFEST),
                "--output",
                str(tmp_path),
            ]
        )
    assert exc_info.value.code != 0


def test_cli_missing_manifest_exits_nonzero(tmp_path):
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "generate",
                "--catalog",
                str(CATALOG),
                "--manifest",
                str(tmp_path / "no_manifest.json"),
                "--output",
                str(tmp_path),
            ]
        )
    assert exc_info.value.code != 0


def test_cli_no_command_exits_zero():
    with pytest.raises(SystemExit) as exc_info:
        main([])
    assert exc_info.value.code == 0


def test_env_var_overrides_enrichment_budget(monkeypatch, tmp_path):
    """DBT_GRAPHQL__ENRICHMENT__BUDGET env var must override config.yml enrichment.budget."""
    import dbt_graphql.compiler.connection as conn_mod
    import dbt_graphql.mcp.server as mcp_server_mod

    captured = {}

    def _fake_serve(_project, *, enrichment=None, **_kwargs):
        captured["enrichment"] = enrichment
        raise SystemExit(0)

    monkeypatch.setattr(mcp_server_mod, "serve_mcp", _fake_serve)
    monkeypatch.setattr(conn_mod, "DatabaseManager", lambda **_kw: None)
    monkeypatch.setenv("DBT_GRAPHQL__ENRICHMENT__BUDGET", "7")

    config_file = tmp_path / "config.yml"
    config_file.write_text(
        "db:\n  type: postgres\n  host: localhost\n  dbname: test\n"
        "serve:\n  host: 0.0.0.0\n  port: 8080\n"
        "enrichment:\n  budget: 100\n"
    )

    with pytest.raises(SystemExit):
        main(
            [
                "serve",
                "--target",
                "mcp",
                "--catalog",
                str(CATALOG),
                "--manifest",
                str(MANIFEST),
                "--config",
                str(config_file),
            ]
        )

    assert captured["enrichment"].budget == 7
