import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
from queries import get_seller_payout_summary, get_revenue_summary, get_todays_trust_score

st.set_page_config(page_title="Finance / Leadership", layout="wide")
st.title("Finance / Marketplace Leadership")

trust = get_todays_trust_score()
badge_col, _ = st.columns([1, 3])
with badge_col:
    if trust["trustworthy"]:
        st.success(f"Today's numbers: TRUSTWORTHY ({trust['total_failures']} failures, under threshold)")
    else:
        st.error(f"Today's numbers: FLAGGED ({trust['total_failures']} failures, over threshold)")
st.caption("Threshold is a placeholder (50 failures/day) — set to your real expected volume in queries.py")

st.divider()

st.subheader("Revenue & Payout Trend")
rev = get_revenue_summary()
if not rev.empty:
    st.line_chart(rev.set_index("order_date")[["revenue", "payout"]])
    st.dataframe(rev, use_container_width=True, hide_index=True)
else:
    st.write("No revenue data.")

st.divider()

st.subheader("Seller Payout Summary")
sellers = get_seller_payout_summary()
if not sellers.empty:
    st.dataframe(sellers, use_container_width=True, hide_index=True)
    top10 = sellers.nlargest(10, "total_payout")
    st.bar_chart(top10.set_index("seller_id")["total_payout"])
else:
    st.write("No seller data.")
