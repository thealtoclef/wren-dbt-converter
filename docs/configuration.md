# Configuration Reference

All configuration is loaded from a single YAML file passed via `--config`. A commented template is in [`config.example.yml`](../config.example.yml).

---

## `db` (required)

Database connection. The `type` field selects the adapter; remaining fields vary by adapter.

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | string | — | Adapter: `postgres`, `mysql`, `mariadb`, `sqlite`, `duckdb`, `doris` |
| `host` | string | `""` | Database host |
| `port` | int | `null` | Database port (adapter default if omitted) |
| `dbname` | string | `""` | Database / catalog name |
| `user` | string | `""` | Login user |
| `password` | string | `""` | Login password |

---

## `serve` (required for `--target api`)

HTTP server bind config.

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | string | — | Bind address (e.g. `0.0.0.0`) |
| `port` | int | — | TCP port |

---

## `enrichment` (optional)

Controls live DB queries issued by `describe_table` in the MCP server. Omit to use defaults.

| Field | Type | Default | Description |
|---|---|---|---|
| `budget` | int | `20` | Max live DB queries fired per `describe_table` call. Row count and sample rows are excluded from this count; budget applies to per-column value enrichment only. |
| `distinct_values_limit` | int | `50` | Max values returned in a `distinct` value summary. |
| `distinct_values_max_cardinality` | int | `500` | If a column's distinct count exceeds this, skip the distinct summary entirely. |

Any field can be overridden at runtime via env var without editing the config file. See the **Environment variables** section below.

---

## `monitoring` (optional)

OpenTelemetry tracing and log level. Omit if you don't use OTel.

| Field | Type | Default | Description |
|---|---|---|---|
| `service_name` | string | `"dbt-graphql"` | OTel `service.name` resource attribute |
| `exporter` | string | `"otlp"` | Span exporter: `otlp` or `console` |
| `protocol` | string | `"grpc"` | OTLP transport: `grpc` or `http` |
| `endpoint` | string | `null` | OTLP collector URL. Uses SDK default if omitted. |
| `log_level` | string | `"INFO"` | Python log level for the `dbt_graphql` logger |

The entire monitoring block is a no-op if `opentelemetry-sdk` is not installed.

---

## Environment variables

All config fields can be overridden via `DBT_GRAPHQL__` prefixed env vars. Nested fields use `__` as delimiter.

```
DBT_GRAPHQL__DB__HOST=myhost
DBT_GRAPHQL__DB__PASSWORD=secret
DBT_GRAPHQL__ENRICHMENT__BUDGET=5
DBT_GRAPHQL__MONITORING__LOG_LEVEL=DEBUG
```

Env vars take precedence over values in `config.yml`.

---

## CLI flags (MCP serve)

These flags are passed to `dbt-graphql serve --target mcp`:

| Flag | Description |
|---|---|
| `--config PATH` | Path to `config.yml`. Optional for MCP; required for API. |
| `--catalog PATH` | Path to `catalog.json` (required). |
| `--manifest PATH` | Path to `manifest.json` (required). |
| `--exclude PATTERN` | Regex to exclude models by name. Repeatable. |
