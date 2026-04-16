import pytest

from dbt_mdl.wren.models import DataSource


# ---------------------------------------------------------------------------
# DataSource enum + aliases
# ---------------------------------------------------------------------------


def test_datasource_postgres():
    assert DataSource("postgres") == DataSource.postgres


def test_datasource_postgresql_alias():
    assert DataSource("postgresql") == DataSource.postgres


def test_datasource_duckdb():
    assert DataSource("duckdb") == DataSource.duckdb


def test_datasource_sqlserver_alias():
    assert DataSource("sqlserver") == DataSource.mssql


def test_datasource_mysql():
    assert DataSource("mysql") == DataSource.mysql


def test_datasource_bigquery():
    assert DataSource("bigquery") == DataSource.bigquery


def test_datasource_snowflake():
    assert DataSource("snowflake") == DataSource.snowflake


def test_datasource_unsupported_raises():
    with pytest.raises(ValueError):
        DataSource("supabase")
