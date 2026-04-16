"""Format dbt project info as GraphJin configuration.

Produces:
- dev.yml: database connection + table definitions + enable_schema
- db.graphql: full schema with @id and @relation directives
- prod.yml: production template with access control placeholders
"""

from __future__ import annotations

from pydantic import BaseModel

from ..domain.models import DbtProjectInfo

# ---------------------------------------------------------------------------
# Type mapping: raw DB type → GraphJin GraphQL type
# ---------------------------------------------------------------------------

_GRAPHJIN_TYPE_MAP: dict[str, str] = {
    # Integer types
    "INTEGER": "Integer",
    "INT": "Integer",
    "INT4": "Integer",
    "SMALLINT": "SmallInt",
    "INT2": "SmallInt",
    "BIGINT": "BigInt",
    "INT8": "BigInt",
    "TINYINT": "SmallInt",
    # String types
    "VARCHAR": "Varchar",
    "TEXT": "Text",
    "CHAR": "Character",
    "CHARACTER": "Character",
    "CHARACTER VARYING": "Varchar",
    "NVARCHAR": "Varchar",
    "NCHAR": "Character",
    "NTEXT": "Text",
    "STRING": "Text",
    # Boolean
    "BOOLEAN": "Boolean",
    "BOOL": "Boolean",
    "BIT": "Boolean",
    # Date/Time
    "DATE": "Date",
    "TIMESTAMP": "Timestamp",
    "DATETIME": "Timestamp",
    "TIMESTAMPTZ": "TimestampWithTimeZone",
    "TIMESTAMP WITH TIME ZONE": "TimestampWithTimeZone",
    "TIMESTAMP WITHOUT TIME ZONE": "Timestamp",
    "TIME": "Time",
    "INTERVAL": "Interval",
    # Numeric
    "DOUBLE": "Numeric",
    "FLOAT": "Numeric",
    "FLOAT8": "Numeric",
    "FLOAT4": "Numeric",
    "REAL": "Numeric",
    "DECIMAL": "Numeric",
    "NUMERIC": "Numeric",
    "MONEY": "Numeric",
    "SMALLMONEY": "Numeric",
    # JSON
    "JSON": "Jsonb",
    "JSONB": "Jsonb",
    # Binary
    "BYTEA": "Bytes",
    "BLOB": "Bytes",
    "BYTES": "Bytes",
    # Other
    "UUID": "Varchar",
    "INET": "Varchar",
    "TSVECTOR": "Tsvector",
}

# BigQuery-specific type overrides
_BIGQUERY_TYPE_MAP: dict[str, str] = {
    "INT64": "BigInt",
    "FLOAT64": "Numeric",
    "NUMERIC": "Numeric",
    "BIGNUMERIC": "Numeric",
}


def _map_graphjin_type(raw_type: str, data_source: str = "") -> str:
    """Map a raw DB column type to a GraphJin GraphQL type."""
    upper = raw_type.upper().strip()

    # Strip type parameters like VARCHAR(255) → VARCHAR
    base = upper.split("(")[0].split("[")[0].strip()

    # Try BigQuery-specific first
    if data_source == "bigquery":
        mapped = _BIGQUERY_TYPE_MAP.get(base)
        if mapped:
            return mapped

    # Try generic mapping
    mapped = _GRAPHJIN_TYPE_MAP.get(base)
    if mapped:
        return mapped

    # Array types (e.g., "INTEGER[]", "TEXT[]")
    if upper.endswith("[]"):
        inner = upper[:-2].strip()
        inner_mapped = _GRAPHJIN_TYPE_MAP.get(inner, "Varchar")
        return f"[{inner_mapped}]"

    # BigQuery array notation "ARRAY<...>"
    if upper.startswith("ARRAY<") and upper.endswith(">"):
        inner = upper[6:-1].strip()
        inner_mapped = _GRAPHJIN_TYPE_MAP.get(inner, "Varchar")
        return f"[{inner_mapped}]"

    # Fallback: capitalize first letter
    return base.capitalize() if base else "Varchar"


# ---------------------------------------------------------------------------
# dbt data source → GraphJin database type
# ---------------------------------------------------------------------------

_GRAPHJIN_DB_TYPE_MAP: dict[str, str] = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "duckdb": "postgres",  # GraphJin doesn't support DuckDB natively; use postgres protocol
    "mysql": "mysql",
    "sqlserver": "mssql",
    "mssql": "mssql",
    "snowflake": "snowflake",
    "bigquery": "snowflake",  # GraphJin doesn't support BigQuery; placeholder
}


def _map_db_type(data_source: str) -> str:
    """Map dbt adapter type to GraphJin database type."""
    return _GRAPHJIN_DB_TYPE_MAP.get(data_source.lower(), "postgres")


# ---------------------------------------------------------------------------
# Output types
# ---------------------------------------------------------------------------


class GraphJinResult(BaseModel):
    """GraphJin configuration output."""

    dev_yml: str
    db_graphql: str
    prod_yml: str


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def format_graphjin(project: DbtProjectInfo) -> GraphJinResult:
    """Convert domain-neutral DbtProjectInfo into GraphJin config files."""
    return GraphJinResult(
        dev_yml=_build_dev_yml(project),
        db_graphql=_build_db_graphql(project),
        prod_yml=_build_prod_yml(project),
    )


# ---------------------------------------------------------------------------
# dev.yml builder
# ---------------------------------------------------------------------------


def _build_dev_yml(project: DbtProjectInfo) -> str:
    """Build the development config YAML."""
    lines: list[str] = []

    # Database section
    db_type = _map_db_type(project.data_source)
    conn = project.connection_info

    lines.append("database:")
    lines.append(f"  type: {db_type}")

    # Connection fields vary by adapter
    if "host" in conn:
        lines.append(f"  host: {_yaml_str(conn['host'])}")
    if "port" in conn:
        lines.append(f"  port: {conn['port']}")
    if "database" in conn:
        lines.append(f"  dbname: {_yaml_str(conn['database'])}")
    if "user" in conn:
        lines.append(f"  user: {_yaml_str(conn['user'])}")
    if "password" in conn:
        password = conn["password"]
        if password:
            lines.append(f"  password: {_yaml_str(password)}")
    if "url" in conn:
        # DuckDB-style
        lines.append(f"  connection_string: {_yaml_str(conn['url'])}")

    lines.append("")

    # Enable schema-driven mode (no DB introspection needed)
    lines.append("enable_schema: true")
    lines.append("")

    # Table aliases (dbt model name → physical table)
    if project.models:
        lines.append("tables:")
        for model in project.models:
            # Only add table entry if model name differs from table name,
            # or if there are custom column relationships
            lines.append(f"  - name: {model.name}")
            lines.append(f"    table: {model.table_name}")

            # Add custom relationships (columns with related_to)
            # These are for relationships not discoverable via DB foreign keys
            has_related_columns = False
            for rel in model.relationships:
                if model.name == rel.from_model:
                    if not has_related_columns:
                        lines.append("    columns:")
                        has_related_columns = True
                    lines.append(f"      - name: {rel.from_column}")
                    lines.append(f"        related_to: {rel.to_model}.{rel.to_column}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# db.graphql builder
# ---------------------------------------------------------------------------


def _build_db_graphql(project: DbtProjectInfo) -> str:
    """Build the GraphQL SDL schema file."""
    lines: list[str] = []

    # Build a map of which columns have relationships
    # from_model.from_column → (to_model, to_column)
    rel_map: dict[str, dict[str, tuple[str, str]]] = {}
    for rel in project.relationships:
        rel_map.setdefault(rel.from_model, {})[rel.from_column] = (
            rel.to_model,
            rel.to_column,
        )

    for model in project.models:
        lines.append(f"type {model.name} {{")
        for col in model.columns:
            gql_type = _map_graphjin_type(col.type, project.data_source)
            directives: list[str] = []

            # Primary key
            if col.is_primary_key or col.name == model.primary_key:
                directives.append("@id")

            # Relationship
            if model.name in rel_map and col.name in rel_map[model.name]:
                target_model, target_col = rel_map[model.name][col.name]
                directives.append(
                    f"@relation(type: {target_model}, field: {target_col})"
                )

            # Nullable vs required
            type_suffix = "!" if col.not_null else ""

            dir_str = " " + " ".join(directives) if directives else ""
            lines.append(f"  {col.name}: {gql_type}{type_suffix}{dir_str}")

        lines.append("}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# prod.yml builder
# ---------------------------------------------------------------------------

_PROD_YML_TEMPLATE = """\
inherits: dev
production: true
default_block: true

# ---
# Access Control Configuration
# Uncomment and customize for your auth setup.
# See: https://graphjin.com/pages/auth.html
# ---

# auth:
#   type: jwt
#   jwt:
#     provider: auth0
#     secret: "$JWT_SECRET"

# roles_query: "SELECT role FROM user_roles WHERE user_id = $user_id"

# roles:
#   - name: anon
#     tables:
#       - name: products
#         query:
#           limit: 10
#           columns: [id, name, description]

#   - name: user
#     match: role = 'user'
#     tables:
#       - name: users
#         query:
#           filters: ["{ id: { _eq: $user_id } }"]
#       - name: orders
#         query:
#           filters: ["{ user_id: { _eq: $user_id } }"]
#         insert:
#           presets:
#             - user_id: "$user_id"
#             - created_at: "now"

#   - name: admin
#     match: role = 'admin'
#     tables:
#       - name: users
#         query:
#           limit: 100
"""


def _build_prod_yml(project: DbtProjectInfo) -> str:
    """Build the production config template with role placeholders."""
    # Add model-specific comments for each table
    model_comments = "\n".join(f"#       - name: {m.name}" for m in project.models)
    template = _PROD_YML_TEMPLATE
    if model_comments:
        # Insert model list as comment in the anon role section
        template = template.replace(
            "#       - name: products\n#         query:\n#           limit: 10\n#           columns: [id, name, description]",
            f"# Available tables:\n{model_comments}\n#         query:\n#           limit: 10",
        )
    return template


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _yaml_str(value: str) -> str:
    """Quote a YAML string value if it contains special characters."""
    if not value:
        return '""'
    # Quote if contains special chars
    if any(c in value for c in ":{}[]&*?|>!%@`#,'\""):
        return f'"{value}"'
    return value
