"""Integration tests for the GraphQL HTTP server (Starlette + Ariadne).

Starts the real Starlette app via TestClient against PostgreSQL and MySQL
databases populated by the jaffle-shop dbt project, then makes real HTTP
GraphQL requests to verify the full request path.
"""

from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from dbt_graphql.api.app import create_app

pytest.importorskip("ariadne", reason="ariadne required for serve tests")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def client(serve_adapter_env):
    app = create_app(
        db_graphql_path=serve_adapter_env["db_graphql_path"],
        db_url=serve_adapter_env["db_url"],
    )
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGraphQLHTTP:
    def test_query_all_customers(self, client):
        resp = client.post(
            "/graphql",
            json={"query": "{ customers { customer_id first_name last_name } }"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "errors" not in data, data.get("errors")
        rows = data["data"]["customers"]
        assert len(rows) > 0
        assert "customer_id" in rows[0]
        assert "first_name" in rows[0]

    def test_query_all_orders(self, client):
        resp = client.post(
            "/graphql",
            json={"query": "{ orders { order_id status } }"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "errors" not in data, data.get("errors")
        assert len(data["data"]["orders"]) > 0

    def test_query_with_limit(self, client):
        resp = client.post(
            "/graphql",
            json={"query": "{ customers(limit: 1) { customer_id } }"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "errors" not in data, data.get("errors")
        assert len(data["data"]["customers"]) == 1

    def test_query_selected_fields_only(self, client):
        resp = client.post(
            "/graphql",
            json={"query": "{ customers { first_name } }"},
        )
        assert resp.status_code == 200
        data = resp.json()
        rows = data["data"]["customers"]
        assert all("first_name" in r for r in rows)
        assert all("customer_id" not in r for r in rows)

    def test_invalid_graphql_syntax_returns_error(self, client):
        resp = client.post("/graphql", json={"query": "{ not valid graphql {{{"})
        assert resp.status_code in (400, 200)
        data = resp.json()
        if resp.status_code == 200:
            assert "errors" in data

    def test_introspection_type_names(self, client):
        resp = client.post(
            "/graphql",
            json={"query": "{ __schema { types { name } } }"},
        )
        assert resp.status_code == 200
        type_names = {t["name"] for t in resp.json()["data"]["__schema"]["types"]}
        assert "customers" in type_names
        assert "orders" in type_names

    def test_schema_exposes_where_input_types(self, client):
        resp = client.post(
            "/graphql",
            json={"query": "{ __schema { types { name } } }"},
        )
        assert resp.status_code == 200
        type_names = {t["name"] for t in resp.json()["data"]["__schema"]["types"]}
        assert "customersWhereInput" in type_names
        assert "ordersWhereInput" in type_names

    def test_where_filter_end_to_end(self, client):
        resp = client.post(
            "/graphql",
            json={
                "query": "{ customers(where: { customer_id: 1 }) { customer_id first_name } }"
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "errors" not in data, data.get("errors")
        rows = data["data"]["customers"]
        assert len(rows) == 1
        assert rows[0]["customer_id"] == 1

    def test_where_filter_no_match_returns_empty(self, client):
        resp = client.post(
            "/graphql",
            json={
                "query": "{ customers(where: { customer_id: 9999 }) { customer_id } }"
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "errors" not in data, data.get("errors")
        assert data["data"]["customers"] == []
