"""
CLM Data Glossary & Lineage Mapping
-------------------------------------
Maps system-level attribute names → business terms with:
- Business definition
- Data owner
- Source system
- Transformation rules applied
- Data quality rules
- Lineage: Raw → Staged → Cleaned → Reported
"""

import pandas as pd
import json
import os

GLOSSARY = [
    {
        "system_attribute":   "client_id",
        "business_term":      "Client Identifier",
        "business_definition": "Unique alphanumeric identifier assigned to each client at onboarding. Used as the primary key across all CLM systems.",
        "data_type":          "VARCHAR(10)",
        "data_owner":         "CLM Operations",
        "source_system":      "CORE_BANKING",
        "mandatory":          True,
        "pii":                False,
        "dq_rules":           "Must be non-null; must be unique; format CLM######",
        "transformation":     "No transformation applied. Validated for uniqueness and format.",
        "lineage_stage":      "Raw → Validated (uniqueness check) → Cleaned → Reporting",
        "related_terms":      "Customer ID, Account Holder ID",
        "regulatory_basis":   "KYC Policy §3.1 — Client Identification"
    },
    {
        "system_attribute":   "full_name",
        "business_term":      "Client Full Legal Name",
        "business_definition": "The complete legal name of the client as stated in their primary identification document. Used for KYC verification and sanctions screening.",
        "data_type":          "VARCHAR(200)",
        "data_owner":         "KYC & Compliance",
        "source_system":      "ONBOARDING_PORTAL / CRM",
        "mandatory":          True,
        "pii":                True,
        "dq_rules":           "Must be non-null; minimum 2 characters; no numeric characters",
        "transformation":     "Whitespace trimmed. No casing normalisation (legal name preserved as-is).",
        "lineage_stage":      "Raw → Trimmed → Validated → Sanctions Screening Feed",
        "related_terms":      "Legal Name, Account Holder Name",
        "regulatory_basis":   "FATF Recommendation 10 — Customer Due Diligence"
    },
    {
        "system_attribute":   "client_type",
        "business_term":      "Client Classification",
        "business_definition": "Categorisation of the client entity type. Determines onboarding workflow, documentation requirements, and regulatory treatment.",
        "data_type":          "VARCHAR(20)",
        "data_owner":         "CLM Data Domain",
        "source_system":      "ONBOARDING_PORTAL",
        "mandatory":          True,
        "pii":                False,
        "dq_rules":           "Must be one of: Individual, Corporate, SME. Case-insensitive input normalised to Title Case.",
        "transformation":     "Standardised to Title Case. Invalid values flagged and quarantined.",
        "lineage_stage":      "Raw → Standardised (Title Case) → Validated → Client 360 Profile",
        "related_terms":      "Entity Type, Customer Category",
        "regulatory_basis":   "CIB Onboarding Policy §2 — Client Classification Framework"
    },
    {
        "system_attribute":   "nationality",
        "business_term":      "Client Nationality",
        "business_definition": "The nationality of the client as per their passport or primary identification document. Used in sanctions screening and risk assessment.",
        "data_type":          "VARCHAR(50)",
        "data_owner":         "KYC & Compliance",
        "source_system":      "ONBOARDING_PORTAL",
        "mandatory":          True,
        "pii":                True,
        "dq_rules":           "Must be non-null. Must be a recognised nationality. Inconsistent casing normalised.",
        "transformation":     "Standardised to Title Case. ISO country code variants mapped to full nationality name.",
        "lineage_stage":      "Raw → Normalised → Sanctions Screening → Risk Scoring",
        "related_terms":      "Country of Citizenship, Passport Nationality",
        "regulatory_basis":   "FATF Recommendation 10; MAS Notice 626"
    },
    {
        "system_attribute":   "country",
        "business_term":      "Country of Residence",
        "business_definition": "The country in which the client is currently resident. Used for tax reporting (CRS/FATCA), regulatory jurisdiction determination, and risk rating.",
        "data_type":          "VARCHAR(50)",
        "data_owner":         "CLM Data Domain",
        "source_system":      "CORE_BANKING / CRM",
        "mandatory":          True,
        "pii":                False,
        "dq_rules":           "Must be non-null. ISO 2-letter codes remapped to full country names for consistency.",
        "transformation":     "ISO code → Full country name (e.g. IN → India). Null values flagged.",
        "lineage_stage":      "Raw → ISO Mapped → Validated → Tax Reporting / CRS Feed",
        "related_terms":      "Domicile Country, Tax Residency Country",
        "regulatory_basis":   "OECD CRS; US FATCA; CIB Tax Policy §4"
    },
    {
        "system_attribute":   "dob",
        "business_term":      "Date of Birth",
        "business_definition": "The client's date of birth as per their primary identification document. Used for age verification, KYC, and sanctions screening.",
        "data_type":          "DATE (YYYY-MM-DD)",
        "data_owner":         "KYC & Compliance",
        "source_system":      "ONBOARDING_PORTAL",
        "mandatory":          True,
        "pii":                True,
        "dq_rules":           "Must be non-null; must be in the past; client must be ≥18 years old; age must be <100.",
        "transformation":     "Parsed to ISO date format. Future dates and implausible ages flagged as critical issues.",
        "lineage_stage":      "Raw → Parsed → Age Validated → KYC Records",
        "related_terms":      "Birth Date, Date of Birth (DOB)",
        "regulatory_basis":   "KYC Policy §3.2 — Individual Client Verification"
    },
    {
        "system_attribute":   "phone",
        "business_term":      "Primary Contact Phone Number",
        "business_definition": "The client's primary telephone contact number. Used for authentication, alerts, and regulatory communications.",
        "data_type":          "VARCHAR(20)",
        "data_owner":         "CRM Team",
        "source_system":      "CRM / ONBOARDING_PORTAL",
        "mandatory":          False,
        "pii":                True,
        "dq_rules":           "If provided, must be 10–15 digits. Country code prefix (+) accepted. Placeholder values (N/A) treated as null.",
        "transformation":     "N/A and placeholder strings nullified. Format validated against regex.",
        "lineage_stage":      "Raw → Placeholder Removed → Format Validated → CRM",
        "related_terms":      "Mobile Number, Contact Number",
        "regulatory_basis":   "CIB Data Privacy Policy §5 — PII Handling"
    },
    {
        "system_attribute":   "email",
        "business_term":      "Primary Email Address",
        "business_definition": "The client's primary email address for digital communications and e-statements.",
        "data_type":          "VARCHAR(100)",
        "data_owner":         "CRM Team",
        "source_system":      "CRM / ONBOARDING_PORTAL",
        "mandatory":          False,
        "pii":                True,
        "dq_rules":           "If provided, must match standard email format. Placeholder emails (e.g. missing@placeholder.com) treated as null.",
        "transformation":     "Placeholder emails nullified. Format validated against regex pattern.",
        "lineage_stage":      "Raw → Placeholder Removed → Format Validated → CRM / Notifications",
        "related_terms":      "Email Address, Digital Contact",
        "regulatory_basis":   "CIB Data Privacy Policy §5"
    },
    {
        "system_attribute":   "risk_rating",
        "business_term":      "Client Risk Rating",
        "business_definition": "The risk classification assigned to the client based on KYC due diligence, transaction behaviour, and country risk. Determines review frequency and enhanced due diligence requirements.",
        "data_type":          "VARCHAR(10)",
        "data_owner":         "Compliance & Risk",
        "source_system":      "CORE_BANKING / RISK_ENGINE",
        "mandatory":          True,
        "pii":                False,
        "dq_rules":           "Must be one of: Low, Medium, High. Null or invalid values are critical failures.",
        "transformation":     "Standardised to Title Case. Invalid values quarantined pending remediation.",
        "lineage_stage":      "Raw → Standardised → Validated → Risk Reporting / EDD Trigger",
        "related_terms":      "AML Risk Score, Customer Risk Classification",
        "regulatory_basis":   "FATF Recommendation 10; MAS Notice 626 §6 — Risk-Based Approach"
    },
    {
        "system_attribute":   "segment",
        "business_term":      "Client Segment",
        "business_definition": "The business segment to which the client belongs, determining the product set, service model, and coverage team responsible.",
        "data_type":          "VARCHAR(20)",
        "data_owner":         "Coverage Operations",
        "source_system":      "CRM",
        "mandatory":          True,
        "pii":                False,
        "dq_rules":           "Must be one of: Retail, Private, Institutional.",
        "transformation":     "Standardised to Title Case. Invalid values flagged.",
        "lineage_stage":      "Raw → Standardised → Validated → Coverage Assignment",
        "related_terms":      "Business Segment, Customer Tier",
        "regulatory_basis":   "CIB Coverage Model Policy"
    },
    {
        "system_attribute":   "onboard_date",
        "business_term":      "Client Onboarding Date",
        "business_definition": "The date on which the client relationship was formally established and KYC onboarding was completed.",
        "data_type":          "DATE (YYYY-MM-DD)",
        "data_owner":         "CLM Operations",
        "source_system":      "ONBOARDING_PORTAL",
        "mandatory":          True,
        "pii":                False,
        "dq_rules":           "Must be non-null; must be a past date; must precede or equal KYC expiry date.",
        "transformation":     "Parsed to ISO date format. Future dates flagged as high severity.",
        "lineage_stage":      "Raw → Parsed → Validated → Relationship Timeline",
        "related_terms":      "Account Opening Date, Relationship Start Date",
        "regulatory_basis":   "KYC Policy §3.1"
    },
    {
        "system_attribute":   "kyc_expiry_date",
        "business_term":      "KYC Review Expiry Date",
        "business_definition": "The date by which the client's KYC review must be renewed. Expired KYC triggers client activity restrictions and mandatory remediation.",
        "data_type":          "DATE (YYYY-MM-DD)",
        "data_owner":         "KYC & Compliance",
        "source_system":      "CORE_BANKING",
        "mandatory":          True,
        "pii":                False,
        "dq_rules":           "Must be non-null. Expired dates are critical. Dates within 90 days trigger early warning flag.",
        "transformation":     "Parsed to ISO date format. Expiry status computed relative to system date.",
        "lineage_stage":      "Raw → Parsed → Expiry Status Computed → KYC Workflow / Compliance Alerts",
        "related_terms":      "CDD Expiry Date, KYC Renewal Date",
        "regulatory_basis":   "FATF Recommendation 10; MAS Notice 626 §8 — Ongoing Monitoring"
    },
    {
        "system_attribute":   "relationship_mgr",
        "business_term":      "Assigned Relationship Manager",
        "business_definition": "The RM code identifying the bank employee responsible for managing the client relationship.",
        "data_type":          "VARCHAR(10)",
        "data_owner":         "Coverage Operations",
        "source_system":      "CRM",
        "mandatory":          False,
        "pii":                False,
        "dq_rules":           "Should be non-null for active clients. Format: RM###.",
        "transformation":     "No transformation. Null values flagged as medium severity.",
        "lineage_stage":      "Raw → Validated → Coverage Dashboard",
        "related_terms":      "RM Code, Account Manager, Coverage Officer",
        "regulatory_basis":   "CIB Coverage Model Policy"
    },
    {
        "system_attribute":   "aum_usd",
        "business_term":      "Assets Under Management (USD)",
        "business_definition": "Total value of assets held by the client with the bank, denominated in USD. Used for segment classification, revenue attribution, and risk exposure calculations.",
        "data_type":          "DECIMAL(18,2)",
        "data_owner":         "Finance & Analytics",
        "source_system":      "CORE_BANKING",
        "mandatory":          True,
        "pii":                False,
        "dq_rules":           "Must be non-null and non-negative. Zero values flagged as potential placeholders.",
        "transformation":     "Negative values flagged as critical. Zero values flagged as low severity.",
        "lineage_stage":      "Raw → Sign Validated → Cleaned → Revenue Reporting / Segment Review",
        "related_terms":      "Portfolio Value, Client AUM, Book Size",
        "regulatory_basis":   "CIB Finance Policy — Revenue Attribution"
    },
    {
        "system_attribute":   "pep_flag",
        "business_term":      "Politically Exposed Person (PEP) Flag",
        "business_definition": "Indicates whether the client has been identified as a Politically Exposed Person. PEP clients require Enhanced Due Diligence (EDD) and senior management approval.",
        "data_type":          "CHAR(1)",
        "data_owner":         "KYC & Compliance",
        "source_system":      "ONBOARDING_PORTAL / RISK_ENGINE",
        "mandatory":          True,
        "pii":                False,
        "dq_rules":           "Must be Y or N. Null or invalid values are critical failures. PEP=Y triggers EDD workflow.",
        "transformation":     "Normalised to uppercase Y/N. Invalid variants (y, n) standardised.",
        "lineage_stage":      "Raw → Normalised → Validated → EDD Workflow / Sanctions Screening",
        "related_terms":      "PEP Indicator, Politically Exposed Flag",
        "regulatory_basis":   "FATF Recommendation 12 — Politically Exposed Persons"
    },
    {
        "system_attribute":   "source_system",
        "business_term":      "Data Source System",
        "business_definition": "Identifies the originating system from which the client record was ingested. Used for data lineage tracking and issue root cause analysis.",
        "data_type":          "VARCHAR(30)",
        "data_owner":         "CLM Data Domain",
        "source_system":      "Multiple",
        "mandatory":          False,
        "pii":                False,
        "dq_rules":           "Should be one of: CORE_BANKING, CRM, ONBOARDING_PORTAL, MANUAL.",
        "transformation":     "No transformation. Used as lineage metadata.",
        "lineage_stage":      "Raw → Lineage Metadata → Data Quality Dashboard",
        "related_terms":      "Origin System, Feeder System",
        "regulatory_basis":   "Group Data Management Standards — Data Lineage §4"
    },
]

LINEAGE_STAGES = [
    {
        "stage":       "1. Raw Ingestion",
        "system":      "Source Systems (CORE_BANKING, CRM, ONBOARDING_PORTAL, MANUAL)",
        "description": "Client data ingested from multiple source systems in raw form. No transformations applied. Snapshot preserved for audit trail.",
        "output":      "clm_raw.csv / clm_raw (SQLite)",
        "dq_checks":   "Record count validation, column presence check"
    },
    {
        "stage":       "2. Profiling",
        "system":      "CLM DQ Pipeline — Profile Stage",
        "description": "Each attribute profiled for completeness (null %, blank %), uniqueness, and value distribution. Profile report generated for data steward review.",
        "output":      "profile_report.csv",
        "dq_checks":   "Null %, unique count, duplicate detection, sample value review"
    },
    {
        "stage":       "3. Quality Scoring",
        "system":      "CLM DQ Pipeline — Score Stage",
        "description": "Each record scored against attribute-level DQ rules. Weighted DQ score computed per record (0–100). Issues logged with severity (critical / high / medium / low).",
        "output":      "issue_log.csv / dq_score column in clm_scored",
        "dq_checks":   "Format validation, referential integrity, business rule checks, date logic, PEP/risk rating validation"
    },
    {
        "stage":       "4. Remediation & Standardisation",
        "system":      "CLM DQ Pipeline — Remediate Stage",
        "description": "Automated remediations applied: casing standardised, placeholder values nullified, ISO country codes mapped. Records with critical issues quarantined. Others flagged for manual review.",
        "output":      "clm_cleaned.csv / clm_scored (SQLite)",
        "dq_checks":   "Remediation status assigned: Clean / Flagged / Quarantined"
    },
    {
        "stage":       "5. Reporting & Monitoring",
        "system":      "CLM DQ Dashboard (Streamlit)",
        "description": "Interactive dashboard surfacing DQ metrics, issue breakdowns, KYC expiry alerts, and record-level drill-down. Supports data steward review and escalation.",
        "output":      "Streamlit Dashboard",
        "dq_checks":   "Ongoing — run pipeline on new data loads"
    },
]


def save_glossary(output_dir: str = "output"):
    os.makedirs(output_dir, exist_ok=True)

    glossary_df = pd.DataFrame(GLOSSARY)
    glossary_df.to_csv(f"{output_dir}/clm_data_glossary.csv", index=False)

    lineage_df = pd.DataFrame(LINEAGE_STAGES)
    lineage_df.to_csv(f"{output_dir}/data_lineage.csv", index=False)

    import sqlite3
    conn = sqlite3.connect(f"{output_dir}/clm.db")
    glossary_df.to_sql("data_glossary", conn, if_exists="replace", index=False)
    lineage_df.to_sql("data_lineage",   conn, if_exists="replace", index=False)
    conn.close()

    print(f"[Glossary] Saved {len(GLOSSARY)} attribute definitions + lineage stages → {output_dir}/")


if __name__ == "__main__":
    save_glossary()
