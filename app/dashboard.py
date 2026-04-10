"""
CLM Data Quality & Glossary Dashboard
"""

import streamlit as st
import pandas as pd
import sqlite3
import json
import os
import sys

# Allow imports from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "output", "clm.db")

st.set_page_config(
    page_title="CLM Data Quality Dashboard",
    page_icon="🏦",
    layout="wide"
)

# ── Load data ────────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    try:
        # If DB doesn't exist → run pipeline
        if not os.path.exists(DB_PATH):
            from run_pipeline import run
            run()

        conn = sqlite3.connect(DB_PATH)

        scored   = pd.read_sql("SELECT * FROM clm_scored", conn)
        issues   = pd.read_sql("SELECT * FROM issue_log", conn)
        glossary = pd.read_sql("SELECT * FROM data_glossary", conn)
        lineage  = pd.read_sql("SELECT * FROM data_lineage", conn)
        profile  = pd.read_sql("SELECT * FROM profile_report", conn)

        conn.close()
        return scored, issues, glossary, lineage, profile

    except Exception as e:
        st.error(f"❌ Data loading failed: {e}")
        return None, None, None, None, None


# LOAD DATA SAFELY
scored, issues, glossary, lineage, profile = load_data()

if scored is None:
    st.stop()


# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("CLM-T Data Quality")
st.sidebar.markdown("Client Lifecycle Management\nData Quality Framework")

page = st.sidebar.radio("Navigate", [
    "DQ Overview",
    "Issue Explorer",
    "KYC Expiry Monitor",
    "Data Glossary",
    "Data Lineage",
    "Attribute Profiling"
])

# ── Page: DQ Overview ─────────────────────────────────────────────────────────
if page == "DQ Overview":
    st.title("CLM Data Quality Overview")

    total   = len(scored)
    clean   = (scored["remediation_status"] == "Clean").sum()
    flagged = (scored["remediation_status"] == "Flagged").sum()
    quar    = (scored["remediation_status"] == "Quarantined").sum()
    avg_dq  = scored["dq_score"].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Records", f"{total:,}")
    c2.metric("Avg DQ Score", f"{avg_dq:.1f}")
    c3.metric("Clean", f"{clean:,}")
    c4.metric("Flagged", f"{flagged:,}")

    st.subheader("DQ Score Distribution")
    st.bar_chart(scored["dq_score"])


# ── Issue Explorer ────────────────────────────────────────────────────────────
elif page == "Issue Explorer":
    st.title("Issue Explorer")

    st.dataframe(issues.head(100))


# ── KYC Monitor ───────────────────────────────────────────────────────────────
elif page == "KYC Expiry Monitor":
    st.title("KYC Expiry Monitor")

    expired = issues[issues["reason"].str.contains("EXPIRED", na=False)]
    st.metric("Expired KYC", len(expired))


# ── Data Glossary ─────────────────────────────────────────────────────────────
elif page == "Data Glossary":
    st.title("Data Glossary")

    st.dataframe(glossary)


# ── Data Lineage ──────────────────────────────────────────────────────────────
elif page == "Data Lineage":
    st.title("Data Lineage")

    st.dataframe(lineage)


# ── Attribute Profiling ───────────────────────────────────────────────────────
elif page == "Attribute Profiling":
    st.title("Attribute Profiling")

    st.dataframe(profile)