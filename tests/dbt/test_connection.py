import base64
import json

import pytest

from dbt_mdl.dbt.connection import get_active_connection
from dbt_mdl.wren.connection import build_connection_info
from dbt_mdl.dbt.models import DbtConnection, DbtProfile, DbtProfiles


# ---------------------------------------------------------------------------
# build_connection_info — postgres
# ---------------------------------------------------------------------------


def test_postgres_connection_info():
    conn = DbtConnection(
        type="postgres",
        host="localhost",
        port=5432,
        database="mydb",
        user="admin",
        password="secret",
    )
    info = build_connection_info(conn, dbt_home=None)
    assert info["host"] == "localhost"
    assert info["port"] == 5432
    assert info["database"] == "mydb"
    assert info["user"] == "admin"
    assert info["password"] == "secret"


def test_postgres_uses_dbname_over_database():
    conn = DbtConnection(
        type="postgres",
        host="localhost",
        port=5432,
        dbname="jaffle_shop",
        database="other",
        user="u",
        password="p",
    )
    info = build_connection_info(conn, dbt_home=None)
    assert info["database"] == "jaffle_shop"


def test_postgres_default_port():
    conn = DbtConnection(type="postgres", host="h", database="db", user="u")
    info = build_connection_info(conn, dbt_home=None)
    assert info["port"] == 5432


# ---------------------------------------------------------------------------
# build_connection_info — mssql
# ---------------------------------------------------------------------------


def test_mssql_connection_info():
    conn = DbtConnection(
        type="sqlserver",
        server="sql-host",
        port=1433,
        database="mydb",
        user="sa",
        password="pw",
    )
    info = build_connection_info(conn, dbt_home=None)
    assert info["host"] == "sql-host"
    assert info["port"] == 1433
    assert info["database"] == "mydb"


def test_mssql_default_port():
    conn = DbtConnection(type="sqlserver", server="h", database="db", user="u")
    info = build_connection_info(conn, dbt_home=None)
    assert info["port"] == 1433


# ---------------------------------------------------------------------------
# build_connection_info — mysql
# ---------------------------------------------------------------------------


def test_mysql_connection_info():
    conn = DbtConnection(
        type="mysql",
        host="mysql-host",
        port=3306,
        database="mydb",
        user="root",
        password="pw",
    )
    info = build_connection_info(conn, dbt_home=None)
    assert info["host"] == "mysql-host"
    assert info["port"] == 3306


def test_mysql_ssl_disabled():
    conn = DbtConnection(
        type="mysql", host="h", port=3306, database="db", user="u", ssl_disable=True
    )
    info = build_connection_info(conn, dbt_home=None)
    assert info["sslMode"] == "DISABLED"


# ---------------------------------------------------------------------------
# build_connection_info — duckdb
# ---------------------------------------------------------------------------


def test_duckdb_absolute_path(tmp_path):
    db_file = tmp_path / "data" / "shop.duckdb"
    conn = DbtConnection(type="duckdb", path=str(db_file))
    info = build_connection_info(conn, dbt_home=tmp_path)
    assert info["url"] == str(tmp_path / "data")
    assert info["format"] == "duckdb"


def test_duckdb_relative_path(tmp_path):
    conn = DbtConnection(type="duckdb", path="subdir/shop.duckdb")
    info = build_connection_info(conn, dbt_home=tmp_path)
    assert info["url"] == str(tmp_path / "subdir")


def test_duckdb_missing_path():
    conn = DbtConnection(type="duckdb")
    with pytest.raises(ValueError, match="missing 'path'"):
        build_connection_info(conn, dbt_home=None)


# ---------------------------------------------------------------------------
# build_connection_info — bigquery
# ---------------------------------------------------------------------------


SAMPLE_SA = {"type": "service_account", "project_id": "proj"}


def test_bigquery_service_account_json():
    raw = json.dumps(SAMPLE_SA)
    conn = DbtConnection(
        type="bigquery",
        method="service-account-json",
        project="proj",
        dataset="ds",
        **{"keyfile_json": raw},
    )
    info = build_connection_info(conn, dbt_home=None)
    assert info["project_id"] == "proj"
    assert info["dataset_id"] == "ds"
    decoded = base64.b64decode(info["credentials"])
    assert json.loads(decoded) == SAMPLE_SA


def test_bigquery_service_account_absolute_keyfile(tmp_path):
    kf = tmp_path / "sa.json"
    kf.write_text(json.dumps(SAMPLE_SA))
    conn = DbtConnection(
        type="bigquery",
        method="service-account",
        project="proj",
        dataset="ds",
        keyfile=str(kf),
    )
    info = build_connection_info(conn, dbt_home=tmp_path)
    decoded = base64.b64decode(info["credentials"])
    assert json.loads(decoded) == SAMPLE_SA


def test_bigquery_service_account_relative_keyfile(tmp_path):
    (tmp_path / "keys").mkdir()
    kf = tmp_path / "keys" / "sa.json"
    kf.write_text(json.dumps(SAMPLE_SA))
    conn = DbtConnection(
        type="bigquery",
        method="service-account",
        project="proj",
        dataset="ds",
        keyfile="keys/sa.json",
    )
    info = build_connection_info(conn, dbt_home=tmp_path)
    decoded = base64.b64decode(info["credentials"])
    assert json.loads(decoded) == SAMPLE_SA


def test_bigquery_oauth_raises():
    """OAuth is not supported — raises ValueError."""
    conn = DbtConnection(type="bigquery", method="oauth", project="p", dataset="d")
    with pytest.raises(ValueError, match="oauth"):
        build_connection_info(conn, dbt_home=None)


def test_bigquery_missing_keyfile_raises():
    """Missing keyfile raises ValueError."""
    conn = DbtConnection(
        type="bigquery", method="service-account", project="p", dataset="d"
    )
    with pytest.raises(ValueError, match="requires"):
        build_connection_info(conn, dbt_home=None)


def test_bigquery_invalid_json_raises():
    conn = DbtConnection(
        type="bigquery",
        method="service-account-json",
        project="p",
        dataset="d",
        **{"keyfile_json": "not-json"},
    )
    with pytest.raises(ValueError, match="invalid"):
        build_connection_info(conn, dbt_home=None)


# ---------------------------------------------------------------------------
# get_active_connection
# ---------------------------------------------------------------------------


def _make_profiles(**kwargs) -> DbtProfiles:
    return DbtProfiles(
        profiles={
            "myproject": DbtProfile(
                target="dev",
                outputs={
                    "dev": DbtConnection(
                        type="postgres",
                        host="localhost",
                        port=5432,
                        database="dev_db",
                        user="dev_user",
                    ),
                    "prod": DbtConnection(
                        type="postgres",
                        host="prod-host",
                        port=5432,
                        database="prod_db",
                        user="prod_user",
                    ),
                },
            )
        }
    )


def test_get_active_uses_default_target():
    profiles = _make_profiles()
    conn = get_active_connection(profiles, "myproject", None)
    assert conn.type == "postgres"


def test_get_active_explicit_target():
    profiles = _make_profiles()
    conn = get_active_connection(profiles, "myproject", "prod")
    assert conn.type == "postgres"


def test_get_active_first_profile_if_none():
    profiles = _make_profiles()
    conn = get_active_connection(profiles, None, None)
    assert conn.type == "postgres"


def test_get_active_missing_profile():
    profiles = _make_profiles()
    with pytest.raises(KeyError, match="nonexistent"):
        get_active_connection(profiles, "nonexistent", None)


def test_get_active_missing_target():
    profiles = _make_profiles()
    with pytest.raises(KeyError, match="staging"):
        get_active_connection(profiles, "myproject", "staging")
