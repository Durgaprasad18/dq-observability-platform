import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
from queries import get_sla_compliance, get_sla_compliance_trend, get_table_health

st.set_page_config(page_title="Operations / Fulfillment", layout="wide")
st.title("Operations / Fulfillment — SLA & Table Health")

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("SLA Compliance (overall)")
    sla = get_sla_compliance()
    if not sla.empty:
        total = sla["order_count"].sum()
        sla["pct"] = (sla["order_count"] / total * 100).round(1)
        st.dataframe(sla, use_container_width=True, hide_index=True)
        on_time = sla[sla["delivery_sla_status"].str.contains("on.?time", case=False, na=False)]
        if not on_time.empty:
            st.metric("On-time %", f"{on_time['pct'].iloc[0]}%")
    else:
        st.write("No SLA data.")

with col2:
    st.subheader("SLA compliance trend over time")
    sla_trend = get_sla_compliance_trend()
    if not sla_trend.empty:
        pivot = sla_trend.pivot_table(
            index="order_date", columns="delivery_sla_status", values="order_count", fill_value=0
        )
        st.line_chart(pivot)
    else:
        st.write("No trend data.")

st.divider()

st.subheader("Table Health — failures by table over time")
health = get_table_health()
if not health.empty:
    pivot = health.pivot_table(
        index="run_date", columns="table_name", values="total_failures", fill_value=0
    )
    st.bar_chart(pivot)
    st.dataframe(health, use_container_width=True, hide_index=True)
else:
    st.write("No table health data.")
