"""
CLM Data Quality Pipeline
--------------------------
Stage 1 : Ingestion      — load raw data, snapshot stats
Stage 2 : Profiling      — per-attribute completeness, uniqueness, pattern checks
Stage 3 : Quality Scoring — weighted DQ score per record and per attribute
Stage 4 : Remediation    — standardise, flag, quarantine
Stage 5 : Output         — clean dataset + DQ report + issue log
"""

import pandas as pd
import numpy as np
import sqlite3
import json
import re
import os
from datetime import datetime, date

TODAY = date.today()

# ---------------------------------------------------------------------------
# Quality rule weights (higher = more critical for CLM/KYC)
# ---------------------------------------------------------------------------
ATTRIBUTE_WEIGHTS = {
    "client_id":        10,
    "full_name":        9,
    "client_type":      8,
    "nationality":      7,
    "country":          7,
    "dob":              8,
    "phone":            6,
    "email":            6,
    "risk_rating":      9,
    "segment":          7,
    "onboard_date":     8,
    "kyc_expiry_date":  9,
    "relationship_mgr": 6,
    "aum_usd":          8,
    "pep_flag":         9,
    "source_system":    5,
}

VALID_CLIENT_TYPES  = {"Individual", "Corporate", "SME"}
VALID_RISK_RATINGS  = {"Low", "Medium", "High"}
VALID_SEGMENTS      = {"Retail", "Private", "Institutional"}
VALID_PEP_FLAGS     = {"Y", "N"}
COUNTRY_MAP = {
    "IN": "India", "SG": "Singapore", "AE": "UAE",
    "GB": "UK", "US": "USA", "HK": "Hong Kong", "DE": "Germany"
}
EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_REGEX = re.compile(r"^\+?[0-9]{10,15}$")


# ---------------------------------------------------------------------------
# Stage 1: Ingestion
# ---------------------------------------------------------------------------
def ingest(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    print(f"[Ingest] Loaded {len(df)} records, {len(df.columns)} columns")
    return df


# ---------------------------------------------------------------------------
# Stage 2: Profiling
# ---------------------------------------------------------------------------
def profile(df: pd.DataFrame) -> dict:
    report = {}
    for col in df.columns:
        s = df[col]
        null_count   = int(s.isna().sum())
        blank_count  = int((s == "").sum()) if s.dtype == object else 0
        unique_count = int(s.nunique(dropna=True))
        report[col] = {
            "total":        len(s),
            "null":         null_count,
            "blank":        blank_count,
            "missing_pct":  round((null_count + blank_count) / len(s) * 100, 2),
            "unique":       unique_count,
            "duplicates":   int(len(s) - unique_count) if col == "client_id" else None,
            "sample_values": s.dropna().unique()[:5].tolist()
        }
    print(f"[Profile] Profiled {len(df.columns)} attributes")
    return report


# ---------------------------------------------------------------------------
# Stage 3: Quality Scoring
# ---------------------------------------------------------------------------
def score_record(row: pd.Series) -> dict:
    issues = []
    scores = {}

    def flag(col, reason, severity="medium"):
        issues.append({"column": col, "reason": reason, "severity": severity})
        scores[col] = 0

    def ok(col):
        scores[col] = ATTRIBUTE_WEIGHTS.get(col, 5)

    # client_id
    if pd.isna(row["client_id"]) or str(row["client_id"]).strip() == "":
        flag("client_id", "Missing client ID", "critical")
    else:
        ok("client_id")

    # full_name
    if pd.isna(row["full_name"]) or len(str(row["full_name"]).strip()) < 2:
        flag("full_name", "Missing or invalid name", "high")
    else:
        ok("full_name")

    # client_type
    val = str(row.get("client_type", "")).strip().title() if pd.notna(row.get("client_type")) else ""
    if val not in VALID_CLIENT_TYPES:
        flag("client_type", f"Invalid/missing client type: '{row.get('client_type')}'", "high")
    else:
        ok("client_type")

    # nationality
    if pd.isna(row.get("nationality")) or str(row["nationality"]).strip() == "":
        flag("nationality", "Missing nationality", "medium")
    else:
        ok("nationality")

    # country
    if pd.isna(row.get("country")) or str(row["country"]).strip() == "":
        flag("country", "Missing country", "medium")
    else:
        ok("country")

    # dob
    dob_val = row.get("dob")
    if pd.isna(dob_val):
        flag("dob", "Missing date of birth", "high")
    else:
        try:
            dob = datetime.strptime(str(dob_val), "%Y-%m-%d").date()
            age = (TODAY - dob).days / 365.25
            if dob > TODAY:
                flag("dob", f"Future DOB: {dob_val}", "critical")
            elif age < 18:
                flag("dob", f"Client under 18: age {age:.1f}", "high")
            elif age > 100:
                flag("dob", f"Implausible age: {age:.1f}", "medium")
            else:
                ok("dob")
        except ValueError:
            flag("dob", f"Unparseable DOB format: {dob_val}", "high")

    # phone
    phone_val = str(row.get("phone", "") or "").strip()
    if not phone_val or phone_val in ("None", "N/A", "nan"):
        flag("phone", "Missing phone number", "medium")
    elif not PHONE_REGEX.match(phone_val.replace(" ", "").replace("-", "")):
        flag("phone", f"Invalid phone format: {phone_val}", "low")
    else:
        ok("phone")

    # email
    email_val = str(row.get("email", "") or "").strip()
    if not email_val or email_val in ("None", "nan"):
        flag("email", "Missing email", "medium")
    elif not EMAIL_REGEX.match(email_val):
        flag("email", f"Invalid email format: {email_val}", "low")
    elif "placeholder" in email_val:
        flag("email", "Placeholder email detected", "medium")
    else:
        ok("email")

    # risk_rating
    rr = str(row.get("risk_rating", "")).strip().title() if pd.notna(row.get("risk_rating")) else ""
    if rr not in VALID_RISK_RATINGS:
        flag("risk_rating", f"Invalid/missing risk rating: '{row.get('risk_rating')}'", "critical")
    else:
        ok("risk_rating")

    # segment
    seg = str(row.get("segment", "")).strip().title() if pd.notna(row.get("segment")) else ""
    if seg not in VALID_SEGMENTS:
        flag("segment", f"Invalid/missing segment: '{row.get('segment')}'", "high")
    else:
        ok("segment")

    # onboard_date
    od = row.get("onboard_date")
    if pd.isna(od):
        flag("onboard_date", "Missing onboard date", "high")
    else:
        try:
            odate = datetime.strptime(str(od), "%Y-%m-%d").date()
            if odate > TODAY:
                flag("onboard_date", f"Future onboard date: {od}", "high")
            else:
                ok("onboard_date")
        except ValueError:
            flag("onboard_date", f"Unparseable onboard date: {od}", "medium")

    # kyc_expiry_date
    kd = row.get("kyc_expiry_date")
    if pd.isna(kd):
        flag("kyc_expiry_date", "Missing KYC expiry date", "critical")
    else:
        try:
            kdate = datetime.strptime(str(kd), "%Y-%m-%d").date()
            if kdate < TODAY:
                flag("kyc_expiry_date", f"KYC EXPIRED on {kd}", "critical")
            elif (kdate - TODAY).days <= 90:
                issues.append({"column": "kyc_expiry_date",
                                "reason": f"KYC expiring soon: {kd}", "severity": "medium"})
                scores["kyc_expiry_date"] = ATTRIBUTE_WEIGHTS["kyc_expiry_date"] * 0.5
            else:
                ok("kyc_expiry_date")
        except ValueError:
            flag("kyc_expiry_date", f"Unparseable KYC expiry: {kd}", "high")

    # aum_usd
    aum = row.get("aum_usd")
    if pd.isna(aum):
        flag("aum_usd", "Missing AUM", "medium")
    elif float(aum) < 0:
        flag("aum_usd", f"Negative AUM: {aum}", "critical")
    elif float(aum) == 0:
        flag("aum_usd", "Zero AUM — possible placeholder", "low")
    else:
        ok("aum_usd")

    # pep_flag
    pep = str(row.get("pep_flag", "")).strip().upper() if pd.notna(row.get("pep_flag")) else ""
    if pep not in VALID_PEP_FLAGS:
        flag("pep_flag", f"Invalid/missing PEP flag: '{row.get('pep_flag')}'", "critical")
    else:
        ok("pep_flag")

    # relationship_mgr
    rm = row.get("relationship_mgr")
    if pd.isna(rm) or str(rm).strip() == "":
        flag("relationship_mgr", "No RM assigned", "medium")
    else:
        ok("relationship_mgr")

    # source_system
    ss = row.get("source_system")
    if pd.isna(ss) or str(ss).strip() == "":
        flag("source_system", "Missing source system", "low")
    else:
        ok("source_system")

    # Compute weighted DQ score
    total_weight  = sum(ATTRIBUTE_WEIGHTS.values())
    earned_weight = sum(scores.get(col, 0) for col in ATTRIBUTE_WEIGHTS)
    dq_score      = round(earned_weight / total_weight * 100, 2)

    return {
        "dq_score":      dq_score,
        "issue_count":   len(issues),
        "critical_count": sum(1 for i in issues if i["severity"] == "critical"),
        "issues":        issues
    }


def apply_scoring(df: pd.DataFrame) -> pd.DataFrame:
    results = df.apply(score_record, axis=1)
    df["dq_score"]       = results.apply(lambda x: x["dq_score"])
    df["issue_count"]    = results.apply(lambda x: x["issue_count"])
    df["critical_issues"]= results.apply(lambda x: x["critical_count"])
    df["issues_json"]    = results.apply(lambda x: json.dumps(x["issues"]))
    print(f"[Score] Avg DQ score: {df['dq_score'].mean():.1f} | "
          f"Records with critical issues: {(df['critical_issues'] > 0).sum()}")
    return df


# ---------------------------------------------------------------------------
# Stage 4: Remediation
# ---------------------------------------------------------------------------
def remediate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Standardise casing
    df["client_type"]  = df["client_type"].str.strip().str.title()
    df["risk_rating"]  = df["risk_rating"].str.strip().str.title()
    df["segment"]      = df["segment"].str.strip().str.title()
    df["nationality"]  = df["nationality"].str.strip().str.title()
    df["pep_flag"]     = df["pep_flag"].str.strip().str.upper()

    # Replace pep n/N variants
    df["pep_flag"] = df["pep_flag"].replace({"Y": "Y", "N": "N", "y": "Y", "n": "N"})

    # Normalise country codes → full names
    df["country"] = df["country"].replace(COUNTRY_MAP)

    # Replace placeholder emails
    df.loc[df["email"].str.contains("placeholder", na=False), "email"] = None

    # Replace N/A phones
    df.loc[df["phone"].isin(["N/A", "n/a", "NA"]), "phone"] = None

    # Quarantine flag: critical issues or duplicate
    df["is_duplicate"] = df.duplicated(subset=["client_id"], keep="first")
    df["quarantine"]   = (df["critical_issues"] > 0) | df["is_duplicate"]
    df["remediation_status"] = "Clean"
    df.loc[df["quarantine"], "remediation_status"] = "Quarantined"
    df.loc[(df["issue_count"] > 0) & (~df["quarantine"]), "remediation_status"] = "Flagged"

    print(f"[Remediate] Clean: {(df['remediation_status']=='Clean').sum()} | "
          f"Flagged: {(df['remediation_status']=='Flagged').sum()} | "
          f"Quarantined: {(df['remediation_status']=='Quarantined').sum()}")
    return df


# ---------------------------------------------------------------------------
# Stage 5: Output
# ---------------------------------------------------------------------------
def save_outputs(raw_df: pd.DataFrame, clean_df: pd.DataFrame,
                 profile_report: dict, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    # Cleaned dataset
    clean_df.to_csv(f"{output_dir}/clm_cleaned.csv", index=False)

    # SQLite
    conn = sqlite3.connect(f"{output_dir}/clm.db")
    raw_df.to_sql("clm_raw",     conn, if_exists="replace", index=False)
    clean_df.to_sql("clm_scored", conn, if_exists="replace", index=False)

    # Issue log
    issues = []
    for _, row in clean_df.iterrows():
        for issue in json.loads(row["issues_json"]):
            issues.append({
                "client_id":  row["client_id"],
                "column":     issue["column"],
                "reason":     issue["reason"],
                "severity":   issue["severity"],
                "dq_score":   row["dq_score"],
                "status":     row["remediation_status"]
            })
    issue_df = pd.DataFrame(issues)
    issue_df.to_csv(f"{output_dir}/issue_log.csv", index=False)
    issue_df.to_sql("issue_log", conn, if_exists="replace", index=False)

    # Profile report
    profile_df = pd.DataFrame([
        {"attribute": col, **{k: v for k, v in stats.items() if k != "sample_values"}}
        for col, stats in profile_report.items()
    ])
    profile_df.to_csv(f"{output_dir}/profile_report.csv", index=False)
    profile_df.to_sql("profile_report", conn, if_exists="replace", index=False)

    conn.close()
    print(f"[Output] All files saved to {output_dir}/")


# ---------------------------------------------------------------------------
# Run pipeline
# ---------------------------------------------------------------------------
def run(input_path: str = "output/clm_raw.csv", output_dir: str = "output"):
    print("=" * 60)
    print("CLM DATA QUALITY PIPELINE")
    print("=" * 60)
    raw_df         = ingest(input_path)
    profile_report = profile(raw_df)
    scored_df      = apply_scoring(raw_df)
    clean_df       = remediate(scored_df)
    save_outputs(raw_df, clean_df, profile_report, output_dir)
    print("=" * 60)
    print("Pipeline complete.")
    return clean_df, profile_report


if __name__ == "__main__":
    run()
