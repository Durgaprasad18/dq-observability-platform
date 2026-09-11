import streamlit as st

st.set_page_config(page_title="DQ Observability Platform", layout="wide")

st.title("AI-Powered Data Quality & Observability Platform")
st.caption("Olist E-Commerce Lakehouse — Databricks / Delta Lake / Unity Catalog")

st.markdown(
    """
    ### Core problem
    If any link in the order chain is unreliable — a seller record missing a zip code,
    a duplicate payment, an order with no matching line items — the business cannot
    answer basic operational questions. This platform verifies every link is trustworthy
    before it's reported on, and explains *why* it broke when it does.

    ### Navigate
    Use the sidebar to jump to a stakeholder view:
    - **Data Engineers / On-call** — AI root-cause diagnosis, quarantine trends, alerts
    - **Operations / Fulfillment** — SLA compliance, table health, pipeline success rate
    - **Finance / Leadership** — seller payouts, revenue trust score, cost per run
    """
)

st.info(
    "Connection uses env vars DATABRICKS_SERVER_HOSTNAME / DATABRICKS_HTTP_PATH / "
    "DATABRICKS_TOKEN. See db_utils.py for setup."
)
