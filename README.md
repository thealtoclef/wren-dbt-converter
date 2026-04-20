# dbt-graphql

Turn a dbt project into a typed GraphQL schema, a SQL-backed GraphQL API, and an MCP surface for LLM agents.

dbt-graphql reads `catalog.json` and `manifest.json`, projects them into a GraphQL SDL enriched with custom directives (database/schema/table, SQL types, primary keys, unique constraints, foreign-key relationships), and provides a compiler that turns GraphQL queries into warehouse SQL. It also exposes an MCP server so AI agents can discover the schema, find join paths, build queries, and execute them — grounded in the same dbt artifacts your analytics team already maintains.

## Features

- **Generate** `db.graphql` + `lineage.json` from dbt artifacts
- **Serve** a read-only GraphQL API over your warehouse (FastAPI + Ariadne + SQLAlchemy)
- **MCP server** for LLM agents with schema discovery, join-path search, query build, and execution tools
- **Multi-warehouse**: DuckDB, PostgreSQL, MySQL/MariaDB, SQLite (anything with an async SQLAlchemy driver)
- **Lineage-aware**: table and column lineage surfaced alongside the schema

## Installation

```bash
pip install dbt-graphql                 # generate only
pip install dbt-graphql[api]            # + GraphQL API server (includes OpenTelemetry)
pip install dbt-graphql[mcp]            # + MCP server
pip install dbt-graphql[duckdb]         # warehouse drivers
pip install dbt-graphql[postgres]
pip install dbt-graphql[mysql]
pip install dbt-graphql[sqlite]
```

## Quick start

### 1. Generate schema

```bash
dbt-graphql generate \
  --catalog target/catalog.json \
  --manifest target/manifest.json \
  --output output/
```

Produces `output/db.graphql` and `output/lineage.json`.

### 2. Serve GraphQL API

```bash
dbt-graphql serve \
  --target api \
  --db-graphql output/db.graphql \
  --config config.yml
```

Playground at `http://localhost:8080/graphql`.

### 3. Start the MCP server

```bash
dbt-graphql serve \
  --target mcp \
  --catalog target/catalog.json \
  --manifest target/manifest.json \
  --config config.yml
```

Starts an MCP stdio server for Claude Desktop, Cline, and other MCP clients.

### 4. Serve both at once

```bash
dbt-graphql serve \
  --target api,mcp \
  --db-graphql output/db.graphql \
  --catalog target/catalog.json \
  --manifest target/manifest.json \
  --config config.yml
```

## Commands

### `generate`

```
dbt-graphql generate --catalog PATH --manifest PATH [--output DIR] [--exclude PATTERN]
```

| Flag         | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| `--catalog`  | Path to `catalog.json` (from `dbt docs generate`)                           |
| `--manifest` | Path to `manifest.json` (from `dbt compile` or `dbt run`)                   |
| `--output`   | Output directory (default: current directory)                               |
| `--exclude`  | Regex pattern to exclude models; may be repeated                            |

### `serve`

```
dbt-graphql serve --target TARGET [--db-graphql PATH] [--config PATH] [--catalog PATH] [--manifest PATH] [--exclude PATTERN]
```

| Flag           | Description                                                                  |
|----------------|------------------------------------------------------------------------------|
| `--target`     | Interfaces to serve: `api`, `mcp`, or `api,mcp` (default: `api`)             |
| `--db-graphql` | Path to `db.graphql` SDL file (required for `api`)                           |
| `--config`     | Path to `config.yml` (required for `api`; optional for `mcp`)                |
| `--catalog`    | Path to `catalog.json` (required for `mcp`)                                  |
| `--manifest`   | Path to `manifest.json` (required for `mcp`)                                 |
| `--exclude`    | Regex pattern to exclude models (mcp only); may be repeated                  |

`config.yml` format:

```yaml
db:
  type: postgres   # postgres | mysql | mariadb | sqlite | duckdb | doris
  host: localhost
  port: 5432
  dbname: mydb
  user: alice
  password: secret

serve:
  host: 0.0.0.0
  port: 8080
```

## A taste of the generated schema

```graphql
type orders @table(database: "mydb", schema: "public", name: "orders") {
  order_id: Int! @column(type: "INTEGER") @id
  customer_id: Int! @column(type: "INTEGER") @relation(type: customers, field: customer_id)
  status: String @column(type: "VARCHAR")
  amount: Float @column(type: "NUMERIC", size: "10,2")
}
```

SQL types map to standard GraphQL scalars (`Int`, `Float`, `Boolean`, `String`). The exact SQL type and precision are preserved in an `@column` directive so the compiler can emit warehouse-correct SQL without parsing the GraphQL type name.

## Documentation

- [**Architecture & Design**](docs/architecture.md) — pipeline flow, component-by-component deep dive, and the design rationale behind them.
- [**Landscape & Comparison**](docs/comparison.md) — how dbt-graphql relates to Cube, Wren, Malloy, Hasura, PostGraphile, pg_graphql, and agent-side text-to-SQL; an honest strengths/gaps assessment.
- [**Roadmap**](ROADMAP.md) — planned features (dbt selector support, source node inclusion, …).

## Development

```bash
uv sync --all-extras --all-groups           # install
uv run pytest tests/ -v                     # tests
uv run ruff check --fix && uv run ruff format
```

## License

MIT.
