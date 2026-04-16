"""Build connection-info dicts from dbt profile configuration.

Normalizes adapter-specific connection parameters (host, port, user, etc.)
into flat dicts consumed by output formatters.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

from .models import DbtConnection, DbtProfiles


# ---------------------------------------------------------------------------
# connection_info builders
# ---------------------------------------------------------------------------


def _build_postgres_info(conn: DbtConnection) -> dict[str, Any]:
    db = getattr(conn, "dbname", None) or getattr(conn, "database", None) or ""
    port = str(conn.port) if getattr(conn, "port", None) else "5432"
    return {
        "host": getattr(conn, "host", None) or "",
        "port": port,
        "database": db,
        "user": getattr(conn, "user", None) or "",
        "password": getattr(conn, "password", None),
    }


def _build_mssql_info(conn: DbtConnection) -> dict[str, Any]:
    port = str(conn.port) if getattr(conn, "port", None) else "1433"
    return {
        "host": getattr(conn, "server", None) or getattr(conn, "host", None) or "",
        "port": port,
        "database": getattr(conn, "database", None) or "",
        "user": getattr(conn, "user", None) or "",
        "password": getattr(conn, "password", None),
        "driver": "ODBC Driver 18 for SQL Server",
        "TDS_Version": "8.0",
        "kwargs": {"TrustServerCertificate": "YES"},
    }


def _build_mysql_info(conn: DbtConnection) -> dict[str, Any]:
    port = str(conn.port) if getattr(conn, "port", None) else "3306"
    ssl_mode = "DISABLED" if getattr(conn, "ssl_disable", None) else "ENABLED"
    return {
        "host": getattr(conn, "host", None) or "",
        "port": port,
        "database": getattr(conn, "database", None) or "",
        "user": getattr(conn, "user", None) or "",
        "password": getattr(conn, "password", None),
        "sslMode": ssl_mode,
    }


def _build_duckdb_info(conn: DbtConnection, dbt_home: Path) -> dict[str, Any]:
    raw_path = getattr(conn, "path", None) or getattr(conn, "file", None) or ""
    if not raw_path:
        raise ValueError("duckdb connection missing 'path' field")
    p = Path(raw_path)
    if p.is_absolute():
        url = str(p.parent)
    else:
        base = dbt_home if dbt_home else Path.cwd()
        url = str((base / p).parent)
    return {"url": url, "format": "duckdb"}


def _encode_json_bytes(data: bytes) -> str:
    """Validate JSON and return base64-encoded string."""
    try:
        json.loads(data)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Service account JSON is invalid: {exc}") from exc
    return base64.b64encode(data).decode()


def _build_bigquery_info(conn: DbtConnection, dbt_home: Path) -> dict[str, Any]:
    method = (getattr(conn, "method", None) or "").strip().lower()

    credentials: str
    if method == "service-account-json":
        raw = getattr(conn, "keyfile_json", None) or ""
        if not raw:
            raise ValueError(
                "bigquery: method 'service-account-json' requires 'keyfile_json'"
            )
        credentials = _encode_json_bytes(raw.encode())

    elif method in ("service-account", ""):
        keyfile_path = (getattr(conn, "keyfile", None) or "").strip()
        if not keyfile_path:
            raw = getattr(conn, "keyfile_json", None) or ""
            if raw:
                credentials = _encode_json_bytes(raw.encode())
            else:
                raise ValueError(
                    "bigquery: method 'service-account' requires 'keyfile' path"
                )
        else:
            p = Path(keyfile_path)
            if not p.is_absolute() and dbt_home:
                p = dbt_home / p
            p = p.resolve()
            try:
                data = p.read_bytes()
            except OSError as exc:
                raise ValueError(f"Failed to read keyfile '{p}': {exc}") from exc
            credentials = _encode_json_bytes(data)

    elif method == "oauth":
        raise ValueError("bigquery: oauth auth method is not supported")
    else:
        raise ValueError(
            f"bigquery: unsupported auth method {method!r}; "
            "supported: service-account, service-account-json"
        )

    return {
        "credentials": credentials,
        "project_id": getattr(conn, "project", None) or "",
        "dataset_id": getattr(conn, "dataset", None) or "",
        "bigquery_type": "dataset",
    }


def _build_snowflake_info(conn: DbtConnection) -> dict[str, Any]:
    return {
        "user": getattr(conn, "user", None) or "",
        "password": getattr(conn, "password", None),
        "account": getattr(conn, "account", None) or "",
        "database": getattr(conn, "database", None) or "",
        "schema": getattr(conn, "schema", None) or "",
        "warehouse": getattr(conn, "warehouse", None),
    }


def build_connection_info(
    conn: DbtConnection,
    dbt_home: Path | None = None,
) -> dict[str, Any]:
    """Build a connection-info dict appropriate for *conn*'s database type."""
    dbt_home = dbt_home or Path.cwd()
    dbt_type = conn.type.strip().lower()

    if dbt_type in ("postgres", "postgresql"):
        return _build_postgres_info(conn)
    elif dbt_type == "duckdb":
        return _build_duckdb_info(conn, dbt_home)
    elif dbt_type == "sqlserver":
        return _build_mssql_info(conn)
    elif dbt_type == "mysql":
        return _build_mysql_info(conn)
    elif dbt_type == "bigquery":
        return _build_bigquery_info(conn, dbt_home)
    elif dbt_type == "snowflake":
        return _build_snowflake_info(conn)
    else:
        raise ValueError(f"Unsupported dbt adapter type: {conn.type!r}")


def get_active_connection(
    profiles: DbtProfiles,
    profile_name: str | None,
    target: str | None,
    dbt_home: Path | None = None,
) -> tuple[str, dict[str, Any]]:
    """Return ``(data_source_str, connection_info_dict)`` for the active target."""
    if not profiles.profiles:
        raise ValueError("profiles is empty")

    name = profile_name or next(iter(profiles.profiles))
    profile = profiles.profiles.get(name)
    if profile is None:
        raise KeyError(f"Profile {name!r} not found")

    tgt = target or profile.target
    conn = profile.outputs.get(tgt)
    if conn is None:
        raise KeyError(f"Target {tgt!r} not found in profile {name!r}")

    data_source = conn.type.strip().lower()
    # Normalize aliases
    if data_source == "postgresql":
        data_source = "postgres"
    elif data_source == "sqlserver":
        data_source = "mssql"

    connection_info = build_connection_info(conn, dbt_home)
    return data_source, connection_info
