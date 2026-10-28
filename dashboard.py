#!/usr/bin/env python3
"""
Live dashboard for the Telecom Churn Big Data pipeline.
  Live (MongoDB)   : customers, events, simulated day, event mix   -> refreshes every 5 s
  Spark results    : churn_results / churn_runs in MongoDB          -> after each pipeline run
Run: python3 -m streamlit run dashboard.py --server.headless true --server.port 8502
"""
import time
from datetime import timedelta

import pandas as pd
import streamlit as st
from pymongo import MongoClient

REFRESH = 5
IST = timedelta(hours=5, minutes=30)

st.set_page_config(page_title="Telecom Churn Dashboard", layout="wide")


@st.cache_resource
def get_db():
    return MongoClient("mongodb://localhost:27017")["telecom"]


db = get_db()


def result(name):
    doc = db.churn_results.find_one({"name": name})
    return pd.DataFrame(doc["rows"]) if doc and doc["rows"] else pd.DataFrame()


st.title("Telecom Customer Churn - Live Big Data Dashboard")
st.caption("Live events -> MongoDB -> HDFS -> Spark + MLlib on YARN -> results back to MongoDB")

# ---------------- KPIs
day = (db.meta.find_one({"_id": "producer"}) or {}).get("day", 0)
last = db.churn_runs.find_one(sort=[("run_at", -1)])
ov = result("overview")
k = st.columns(5)
k[0].metric("Customers", f"{db.customers.estimated_document_count():,}")
k[1].metric("Live events in MongoDB", f"{db.events.estimated_document_count():,}")
k[2].metric("Simulated day", f"{day} / 30")
k[3].metric("Churn rate", f"{ov['churn_rate_pct'][0]}%" if not ov.empty else "-")
k[4].metric("Last Spark run", (last["run_at"] + IST).strftime("%H:%M:%S") if last else "not yet")

# ---------------- Live activity
st.subheader("Live activity (MongoDB)")
left, right = st.columns(2)
per_day = list(db.events.aggregate([{"$group": {"_id": "$sim_day", "events": {"$sum": 1}}},
                                    {"$sort": {"_id": 1}}]))
if per_day:
    d = pd.DataFrame(per_day).rename(columns={"_id": "day"}).set_index("day")
    left.markdown("**Events received per simulated day**")
    left.bar_chart(d["events"])
mix = list(db.events.aggregate([{"$group": {"_id": "$event_type", "events": {"$sum": 1}}}]))
if mix:
    m = pd.DataFrame(mix).rename(columns={"_id": "event_type"}).set_index("event_type")
    right.markdown("**Event types**")
    right.bar_chart(m["events"])

# ---------------- Spark results
if ov.empty:
    st.info("Run ./run_pipeline.sh to see the Spark churn analysis.")
else:
    st.subheader(f"Spark churn analysis ({ov['days_of_data'][0]} days, "
                 f"{int(ov['events_processed'][0]):,} events)")

    a, b = st.columns(2)
    ind = result("indicators")
    if not ind.empty:
        a.markdown("**Top churn indicators (Random Forest feature importance)**")
        a.dataframe(ind, hide_index=True, column_config={
            "importance_pct": st.column_config.ProgressColumn(
                "Importance %", format="%.1f", min_value=0, max_value=float(ind["importance_pct"].max()))})
    comp = result("by_complaints")
    if not comp.empty:
        b.markdown("**Churn rate (%) by complaints per month**")
        b.bar_chart(comp.set_index("complaints")["churn_rate_pct"])

    a, b = st.columns(2)
    plan = result("by_plan")
    if not plan.empty:
        a.markdown("**Churn rate by service plan**")
        a.dataframe(plan, hide_index=True)
    model = result("model")
    if not model.empty:
        b.markdown("**Model quality (20% test data)**")
        b.dataframe(model, hide_index=True)

    cmp_df = result("comparison")
    if not cmp_df.empty:
        st.markdown("**Churned vs active customers (monthly averages, diff % = how different churners are)**")
        st.dataframe(cmp_df, hide_index=True)

    rs, hr = result("risk_summary"), result("high_risk")
    if not rs.empty and not hr.empty:
        st.markdown(f"**High-risk active customers** - {rs['risk_50pct_or_more'][0]} of "
                    f"{rs['active_customers'][0]} active customers have 50%+ churn risk")
        st.dataframe(hr, hide_index=True)

    runs = pd.DataFrame(list(db.churn_runs.find({}, {"_id": 0}).sort("run_at", 1)))
    if not runs.empty:
        runs["run_at"] = (runs["run_at"] + IST).dt.strftime("%H:%M:%S")
        st.markdown("**Analysis runs - data growing over time**")
        st.dataframe(runs, hide_index=True)

st.caption(f"Live section refreshes every {REFRESH} seconds.")
time.sleep(REFRESH)
st.rerun()
