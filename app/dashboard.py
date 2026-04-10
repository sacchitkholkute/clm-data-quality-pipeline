"""
CLM Data Quality & Glossary Dashboard
Streamlit application — run with: streamlit run app/dashboard.py
"""

import streamlit as st
import pandas as pd
import sqlite3
import json
import os
import sys

# Allow imports from sibling directories
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
    import os

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

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/0/0e/Standard_Chartered.svg/320px-Standard_Chartered.svg.png", width=180)
st.sidebar.title("CLM-T Data Quality")
st.sidebar.markdown("**Client Lifecycle Management**\nData Quality & Glossary Framework")
page = st.sidebar.radio("Navigate", [
    "📊 DQ Overview",
    "🔍 Issue Explorer",
    "⚠️ KYC Expiry Monitor",
    "📋 Data Glossary",
    "🔗 Data Lineage",
    "📈 Attribute Profiling"
])

# ── Page: DQ Overview ─────────────────────────────────────────────────────────
if page == "📊 DQ Overview":
    st.title("📊 CLM Data Quality Overview")
    st.caption("Pipeline run across all client records in the CLM system")

    total   = len(scored)
    clean   = (scored["remediation_status"] == "Clean").sum()
    flagged = (scored["remediation_status"] == "Flagged").sum()
    quar    = (scored["remediation_status"] == "Quarantined").sum()
    avg_dq  = scored["dq_score"].mean()
    critical_clients = (scored["critical_issues"] > 0).sum()
    kyc_expired = (scored["kyc_expiry_date"] < "2024-04-10").sum() if "kyc_expiry_date" in scored.columns else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Records",       f"{total:,}")
    c2.metric("Avg DQ Score",        f"{avg_dq:.1f}/100")
    c3.metric("✅ Clean",            f"{clean:,}",   f"{clean/total*100:.1f}%")
    c4.metric("⚠️ Flagged",         f"{flagged:,}", f"{flagged/total*100:.1f}%")
    c5.metric("🚨 Quarantined",      f"{quar:,}",   f"{quar/total*100:.1f}%")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("DQ Score Distribution")
        bins = pd.cut(scored["dq_score"], bins=[0,50,70,85,95,100],
                      labels=["0-50","50-70","70-85","85-95","95-100"])
        dist = bins.value_counts().sort_index().reset_index()
        dist.columns = ["Score Range", "Count"]
        st.bar_chart(dist.set_index("Score Range"))

    with col2:
        st.subheader("Issues by Severity")
        sev = issues["severity"].value_counts().reset_index()
        sev.columns = ["Severity", "Count"]
        sev["Severity"] = pd.Categorical(sev["Severity"],
            categories=["critical","high","medium","low"], ordered=True)
        sev = sev.sort_values("Severity")
        st.bar_chart(sev.set_index("Severity"))

    st.markdown("---")
    st.subheader("Top 10 Attributes by Issue Frequency")
    top_attrs = issues["column"].value_counts().head(10).reset_index()
    top_attrs.columns = ["Attribute", "Issue Count"]
    st.bar_chart(top_attrs.set_index("Attribute"))

    st.markdown("---")
    st.subheader("Remediation Status Breakdown")
    status_counts = scored["remediation_status"].value_counts().reset_index()
    status_counts.columns = ["Status", "Count"]
    st.dataframe(status_counts, use_container_width=True)


# ── Page: Issue Explorer ──────────────────────────────────────────────────────
elif page == "🔍 Issue Explorer":
    st.title("🔍 Issue Explorer")

    col1, col2 = st.columns(2)
    with col1:
        sev_filter = st.multiselect("Filter by Severity",
            ["critical","high","medium","low"],
            default=["critical","high"])
    with col2:
        attr_filter = st.multiselect("Filter by Attribute",
            sorted(issues["column"].unique().tolist()),
            default=[])

    filtered = issues.copy()
    if sev_filter:
        filtered = filtered[filtered["severity"].isin(sev_filter)]
    if attr_filter:
        filtered = filtered[filtered["column"].isin(attr_filter)]

    st.markdown(f"**{len(filtered):,} issues** matching filters")
    st.dataframe(filtered[["client_id","column","reason","severity","dq_score","status"]],
                 use_container_width=True, height=400)

    st.markdown("---")
    st.subheader("Record-Level Drill-Down")
    cid = st.text_input("Enter Client ID (e.g. CLM000001)")
    if cid:
        rec = scored[scored["client_id"] == cid]
        if len(rec) == 0:
            st.warning("Client ID not found.")
        else:
            r = rec.iloc[0]
            st.markdown(f"**DQ Score:** {r['dq_score']} | **Status:** {r['remediation_status']} | **Critical Issues:** {r['critical_issues']}")
            rec_issues = pd.DataFrame(json.loads(r["issues_json"]))
            if len(rec_issues):
                st.dataframe(rec_issues, use_container_width=True)
            else:
                st.success("No issues found for this record.")


# ── Page: KYC Expiry Monitor ─────────────────────────────────────────────────
elif page == "⚠️ KYC Expiry Monitor":
    st.title("⚠️ KYC Expiry Monitor")
    st.caption("Identifies clients with expired or soon-to-expire KYC — triggers mandatory review workflow")

    from datetime import date, timedelta
    today_str = str(date.today())
    soon_str  = str(date.today() + timedelta(days=90))

    kyc_issues = issues[issues["column"] == "kyc_expiry_date"].copy()

    expired = kyc_issues[kyc_issues["reason"].str.contains("EXPIRED")]
    expiring = kyc_issues[kyc_issues["reason"].str.contains("expiring soon")]

    c1, c2 = st.columns(2)
    c1.metric("🚨 KYC Expired",        len(expired))
    c2.metric("⏳ Expiring within 90d", len(expiring))

    st.markdown("---")
    st.subheader("Expired KYC Clients")
    if len(expired):
        exp_clients = scored[scored["client_id"].isin(expired["client_id"])][[
            "client_id","full_name","client_type","risk_rating",
            "kyc_expiry_date","segment","relationship_mgr","remediation_status"
        ]]
        st.dataframe(exp_clients, use_container_width=True, height=350)
    else:
        st.success("No expired KYC records.")

    st.subheader("Expiring Soon (Next 90 Days)")
    if len(expiring):
        exp_soon = scored[scored["client_id"].isin(expiring["client_id"])][[
            "client_id","full_name","risk_rating","kyc_expiry_date",
            "relationship_mgr","remediation_status"
        ]]
        st.dataframe(exp_soon, use_container_width=True)
    else:
        st.success("No KYC expiring in next 90 days.")


# ── Page: Data Glossary ───────────────────────────────────────────────────────
elif page == "📋 Data Glossary":
    st.title("📋 CLM Data Glossary")
    st.caption("Business term definitions for all CLM system-level attributes — aligned with Group Data Management Standards")

    search = st.text_input("Search attribute or business term")
    show_pii = st.checkbox("Show PII attributes only", value=False)

    filtered_g = glossary.copy()
    if search:
        mask = (
            filtered_g["system_attribute"].str.contains(search, case=False, na=False) |
            filtered_g["business_term"].str.contains(search, case=False, na=False) |
            filtered_g["business_definition"].str.contains(search, case=False, na=False)
        )
        filtered_g = filtered_g[mask]
    if show_pii:
        filtered_g = filtered_g[filtered_g["pii"] == True]

    for _, row in filtered_g.iterrows():
        with st.expander(f"**{row['system_attribute']}** → {row['business_term']}"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Business Definition:**\n{row['business_definition']}")
                st.markdown(f"**DQ Rules:** {row['dq_rules']}")
                st.markdown(f"**Transformation:** {row['transformation']}")
                st.markdown(f"**Regulatory Basis:** {row['regulatory_basis']}")
            with col2:
                st.markdown(f"**Data Type:** `{row['data_type']}`")
                st.markdown(f"**Source System:** {row['source_system']}")
                st.markdown(f"**Data Owner:** {row['data_owner']}")
                st.markdown(f"**Mandatory:** {'✅ Yes' if row['mandatory'] else '❌ No'}")
                st.markdown(f"**PII:** {'🔒 Yes' if row['pii'] else 'No'}")
                st.markdown(f"**Related Terms:** {row['related_terms']}")


# ── Page: Data Lineage ────────────────────────────────────────────────────────
elif page == "🔗 Data Lineage":
    st.title("🔗 CLM Data Lineage")
    st.caption("End-to-end data flow from source systems through to reporting — aligned with Group Data Management Standards §4")

    for _, row in lineage.iterrows():
        with st.expander(f"**{row['stage']}** — {row['system']}"):
            st.markdown(f"**Description:** {row['description']}")
            st.markdown(f"**Output:** `{row['output']}`")
            st.markdown(f"**DQ Checks Applied:** {row['dq_checks']}")

    st.markdown("---")
    st.subheader("Lineage Flow")
    st.markdown("""
```
Source Systems                     CLM DQ Pipeline                          Consumers
──────────────                     ───────────────                          ─────────
CORE_BANKING  ──┐
CRM           ──┼──► Raw Ingest ──► Profile ──► Score ──► Remediate ──►   DQ Dashboard
ONBOARDING    ──┤                                                      ──►  KYC Workflow
PORTAL           │                                                     ──►  Risk Reports
MANUAL        ──┘                                                      ──►  Compliance Alerts
                                                                       ──►  Coverage Dashboard
```
    """)

    st.subheader("Attribute-Level Lineage")
    attr = st.selectbox("Select attribute", glossary["system_attribute"].tolist())
    row = glossary[glossary["system_attribute"] == attr].iloc[0]
    st.markdown(f"**{attr}** → **{row['business_term']}**")
    st.markdown(f"**Source:** {row['source_system']}")
    st.code(row["lineage_stage"], language=None)
    st.markdown(f"**Transformation applied:** {row['transformation']}")


# ── Page: Attribute Profiling ─────────────────────────────────────────────────
elif page == "📈 Attribute Profiling":
    st.title("📈 Attribute Profiling Report")
    st.caption("Data completeness and uniqueness statistics across all CLM attributes")

    profile_show = profile[["attribute","total","null","missing_pct","unique"]].copy()
    profile_show["missing_pct"] = profile_show["missing_pct"].apply(lambda x: f"{x}%")
    profile_show = profile_show.sort_values("null", ascending=False)

    st.dataframe(profile_show, use_container_width=True, height=450)

    st.markdown("---")
    st.subheader("Missing % by Attribute")
    miss = profile[["attribute","missing_pct"]].sort_values("missing_pct", ascending=False)
    st.bar_chart(miss.set_index("attribute"))
