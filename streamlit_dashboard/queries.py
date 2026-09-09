"""
Query layer. Every function is cached (ttl=300s) — dashboard doesn't hammer
the warehouse on every Streamlit rerun (tab switch, widget interaction, etc).

CATALOG constant assumes project01_databricks01 — change if renamed.

NOTE: Two queries (job_run_status, cost_per_run) use Unity Catalog "system" schema
table names that are Databricks' standard names as of recent releases
(system.lakeflow.job_run_timeline, system.billing.usage). These are UNVERIFIED
against your specific workspace — system schemas must be enabled by an admin,
and exact table/column names can vary by workspace region/version. Run each
query manually in a SQL editor first; if it errors, check:
  Catalog Explorer -> system -> lakeflow / billing (confirm schema is enabled,
  confirm exact column names) and adjust below.
"""

import pandas as pd
import streamlit as st
from db_utils import run_query

CATALOG = "project01_databricks01"


def _df(query: str, params: tuple = None) -> pd.DataFrame:
    rows = run_query(query, params)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Page 1: Data Engineers / On-call — AI Remediation + Alerts
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_quarantine_diagnosis(run_date: str = None) -> pd.DataFrame:
    where = f"WHERE run_date = '{run_date}'" if run_date else ""
    q = f"""
        SELECT table_name, check_name, column_name, failure_count,
               root_cause, category, suggested_fix, confidence,
               llm_call_status, run_date, diagnosed_at
        FROM {CATALOG}.silver.quarantine_diagnosis
        {where}
        ORDER BY failure_count DESC
    """
    return _df(q)


@st.cache_data(ttl=300)
def get_diagnosis_errors() -> pd.DataFrame:
    q = f"""
        SELECT table_name, check_name, column_name, llm_call_status, run_date
        FROM {CATALOG}.silver.quarantine_diagnosis
        WHERE llm_call_status LIKE 'error%'
        ORDER BY run_date DESC
    """
    return _df(q)


@st.cache_data(ttl=300)
def get_mean_time_to_diagnosis() -> pd.DataFrame:
    """
    MTTD proxy: per (table_name, check_name, column_name, run_date) group,
    delta between earliest failure record (quarantine.processed_date) and
    diagnosis completion (quarantine_diagnosis.diagnosed_at).
    """
    q = f"""
        WITH first_failure AS (
            SELECT table_name, check_name, column_name,
                   DATE(processed_date) AS run_date,
                   MIN(processed_date) AS first_failed_at
            FROM {CATALOG}.silver.quarantine
            GROUP BY table_name, check_name, column_name, DATE(processed_date)
        )
        SELECT d.table_name, d.check_name, d.column_name, d.run_date,
               f.first_failed_at, d.diagnosed_at,
               (unix_timestamp(d.diagnosed_at) - unix_timestamp(f.first_failed_at)) AS mttd_seconds
        FROM {CATALOG}.silver.quarantine_diagnosis d
        JOIN first_failure f
          ON d.table_name = f.table_name
         AND d.check_name = f.check_name
         AND d.column_name = f.column_name
         AND d.run_date = f.run_date
        WHERE d.diagnosed_at IS NOT NULL
        ORDER BY d.run_date DESC
    """
    return _df(q)


@st.cache_data(ttl=300)
def get_quarantine_trend() -> pd.DataFrame:
    q = f"""
        SELECT DATE(processed_date) AS run_date, table_name, COUNT(*) AS failure_count
        FROM {CATALOG}.silver.quarantine
        GROUP BY DATE(processed_date), table_name
        ORDER BY run_date
    """
    return _df(q)


@st.cache_data(ttl=300)
def get_job_run_status(limit_days: int = 14) -> pd.DataFrame:
    """UNVERIFIED table name — confirm system.lakeflow.job_run_timeline exists
    in your workspace (Catalog Explorer -> system -> lakeflow) before trusting."""
    q = f"""
        SELECT job_id, run_id, period_start_time, period_end_time,
               result_state, trigger_type
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= date_sub(current_date(), {limit_days})
        ORDER BY period_start_time DESC
    """
    return _df(q)


# ---------------------------------------------------------------------------
# Page 2: Operations / Fulfillment — SLA + Table Health
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_sla_compliance() -> pd.DataFrame:
    q = f"""
        SELECT delivery_sla_status, COUNT(*) AS order_count
        FROM {CATALOG}.gold.fact_orders
        GROUP BY delivery_sla_status
    """
    return _df(q)


@st.cache_data(ttl=300)
def get_sla_compliance_trend() -> pd.DataFrame:
    q = f"""
        SELECT DATE(order_purchase_timestamp) AS order_date,
               delivery_sla_status,
               COUNT(*) AS order_count
        FROM {CATALOG}.gold.fact_orders
        WHERE order_purchase_timestamp IS NOT NULL
        GROUP BY DATE(order_purchase_timestamp), delivery_sla_status
        ORDER BY order_date
    """
    return _df(q)


@st.cache_data(ttl=300)
def get_table_health() -> pd.DataFrame:
    """Pass rate proxy per table: quarantine failures vs total processed
    records for the same run_date, pulled from quarantine_diagnosis
    (which already has failure_count aggregated)."""
    q = f"""
        SELECT table_name, run_date, SUM(failure_count) AS total_failures
        FROM {CATALOG}.silver.quarantine_diagnosis
        GROUP BY table_name, run_date
        ORDER BY run_date DESC
    """
    return _df(q)


# ---------------------------------------------------------------------------
# Page 3: Finance / Marketplace leadership — Payout, Revenue Trust
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_seller_payout_summary() -> pd.DataFrame:
    q = f"""
        SELECT seller_id, seller_state, total_orders, total_payout, avg_review_score
        FROM {CATALOG}.gold.dim_sellers
        ORDER BY total_payout DESC
    """
    return _df(q)


@st.cache_data(ttl=300)
def get_revenue_summary() -> pd.DataFrame:
    q = f"""
        SELECT DATE(order_purchase_timestamp) AS order_date,
               SUM(order_total_value) AS revenue,
               SUM(seller_payout_amount) AS payout,
               COUNT(DISTINCT order_id) AS order_count
        FROM {CATALOG}.gold.fact_orders
        WHERE order_purchase_timestamp IS NOT NULL
        GROUP BY DATE(order_purchase_timestamp)
        ORDER BY order_date
    """
    return _df(q)


@st.cache_data(ttl=300)
def get_todays_trust_score(run_date: str = None) -> dict:
    """Simple trust badge: total failures today vs a threshold. Threshold is
    arbitrary (50) — pick a real number based on your typical daily volume."""
    where = f"WHERE run_date = '{run_date}'" if run_date else "WHERE run_date = current_date()"
    q = f"""
        SELECT SUM(failure_count) AS total_failures
        FROM {CATALOG}.silver.quarantine_diagnosis
        {where}
    """
    rows = run_query(q)
    total = rows[0]["total_failures"] if rows and rows[0]["total_failures"] is not None else 0
    return {"total_failures": total, "trustworthy": total < 50}


@st.cache_data(ttl=300)
def get_cost_per_run(limit_days: int = 14) -> pd.DataFrame:
    """UNVERIFIED table name — confirm system.billing.usage is enabled
    (Catalog Explorer -> system -> billing) and check column names match
    your workspace before trusting this query."""
    q = f"""
        SELECT usage_date, sku_name, SUM(usage_quantity) AS dbu_quantity
        FROM system.billing.usage
        WHERE usage_date >= date_sub(current_date(), {limit_days})
        GROUP BY usage_date, sku_name
        ORDER BY usage_date DESC
    """
    return _df(q)
