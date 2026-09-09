"""
Databricks SQL Warehouse connection helper.

Uses the official Databricks SDK auth chain (databricks.sdk.core.Config),
NOT a manual token. This is the documented pattern for Databricks Apps:
- On Databricks Apps: Config() auto-detects the app's own service principal
  identity, no token needed at all.
- Running locally: Config() falls back to your `databricks configure` CLI
  profile, or DATABRICKS_HOST + DATABRICKS_TOKEN env vars if set — same
  code works both places.

Only thing you must set yourself either way: DATABRICKS_HTTP_PATH
(the SQL Warehouse's HTTP path, e.g. /sql/1.0/warehouses/abc123def456).
On Databricks Apps this comes from app.yaml. Locally, export it as an env var.
"""

import os
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

cfg = Config()


def _get_http_path() -> str:
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    if not http_path:
        st.error(
            "DATABRICKS_HTTP_PATH not set. On Databricks Apps, check app.yaml's "
            "env section. Running locally, export DATABRICKS_HTTP_PATH."
        )
        st.stop()
    return http_path


@st.cache_resource
def get_connection():
    """Cached connection — reused across reruns within a session."""
    server_hostname = cfg.host.replace("https://", "").replace("http://", "").rstrip("/")
    http_path = _get_http_path()
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )


def run_query(query: str, params: tuple = None):
    """Run a SQL query, return list of dict rows."""
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query, params) if params else cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]