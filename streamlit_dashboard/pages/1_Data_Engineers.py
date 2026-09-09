import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
from queries import (
    get_quarantine_diagnosis,
    get_diagnosis_errors,
    get_mean_time_to_diagnosis,
    get_quarantine_trend,

)

st.set_page_config(page_title="Data Engineers / On-call", layout="wide")
st.title("Data Engineers / On-call — AI Remediation")

# --- Alert banner: quarantine spike check ---
trend = get_quarantine_trend()
if not trend.empty:
    daily_totals = trend.groupby("run_date")["failure_count"].sum().sort_index()
    if len(daily_totals) >= 2:
        latest, prior = daily_totals.iloc[-1], daily_totals.iloc[-2]
        if prior > 0 and latest > prior * 1.5:
            st.error(
                f"Quarantine spike: {latest} failures on latest run vs {prior} prior run "
                f"({(latest / prior - 1) * 100:.0f}% increase)"
            )
        else:
            st.success(f"No spike detected. Latest run: {latest} failures (prior: {prior})")
else:
    st.warning("No quarantine data returned — check table has rows.")

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Quarantine trend by table")
    if not trend.empty:
        pivot = trend.pivot_table(
            index="run_date", columns="table_name", values="failure_count", fill_value=0
        )
        st.line_chart(pivot)
    else:
        st.write("No data.")

with col2:
    st.subheader("Mean Time to Diagnosis")
    mttd = get_mean_time_to_diagnosis()
    if not mttd.empty:
        avg_seconds = mttd["mttd_seconds"].mean()
        st.metric("Avg MTTD", f"{avg_seconds / 60:.1f} min")
        st.dataframe(
            mttd[["table_name", "check_name", "run_date", "mttd_seconds"]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.write("No diagnosis timing data yet.")

st.divider()

st.subheader("AI Root-Cause Diagnosis")
diag = get_quarantine_diagnosis()
if not diag.empty:
    st.dataframe(diag, use_container_width=True, hide_index=True)
else:
    st.write("No diagnosis records.")

st.divider()

st.subheader("Failed / Errored Diagnosis Calls")
errors = get_diagnosis_errors()
if not errors.empty:
    st.dataframe(errors, use_container_width=True, hide_index=True)
else:
    st.write("No errored diagnosis calls — clean run.")

