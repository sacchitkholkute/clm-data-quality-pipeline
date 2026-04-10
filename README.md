# CLM Client Data Quality & Glossary Framework

**A production-style data quality pipeline and business glossary for Client Lifecycle Management (CLM) data in a banking context.**

Built to simulate the data stewardship work done by a Data Business Analyst in a bank's CLM-T team — covering data discovery, profiling, quality scoring, remediation, lineage documentation, and an interactive monitoring dashboard.

---

## Project Structure

```
clm_project/
├── data/
│   └── generate_data.py        # Synthetic CLM client dataset generator
├── pipeline/
│   ├── dq_pipeline.py          # 5-stage data quality pipeline
│   └── glossary.py             # Data glossary & lineage definitions (16 attributes)
├── app/
│   └── dashboard.py            # Streamlit monitoring dashboard
├── output/                     # Generated on first run
│   ├── clm_raw.csv             # Raw synthetic data (2,000+ records)
│   ├── clm_cleaned.csv         # Scored & remediated data
│   ├── issue_log.csv           # Per-record issue log with severity
│   ├── profile_report.csv      # Attribute-level profiling stats
│   ├── clm_data_glossary.csv   # Business term definitions
│   ├── data_lineage.csv        # Lineage stage documentation
│   └── clm.db                  # SQLite database (all tables)
├── run_pipeline.py             # Master script — run this first
└── README.md
```

---

## What This Project Demonstrates

### 1. Data Discovery & Profiling
- Ingests raw CLM client records from multiple simulated source systems (CORE_BANKING, CRM, ONBOARDING_PORTAL, MANUAL)
- Profiles each attribute for completeness, uniqueness, null %, and value distribution
- Identifies data anomalies introduced at source (inconsistent casing, ISO code variants, placeholder values)

### 2. Data Quality Scoring & Root Cause Analysis
- 16 business rules applied per record (format, referential integrity, date logic, regulatory constraints)
- Weighted DQ score (0–100) computed per record based on attribute criticality
- Issues logged with severity: **Critical / High / Medium / Low**
- Root cause tagged at attribute level (e.g. "KYC EXPIRED", "Future DOB", "Invalid PEP flag")

### 3. Remediation
- Automated standardisation: casing normalisation, ISO country code mapping, placeholder removal
- Records classified as: **Clean / Flagged / Quarantined**
- Quarantine criteria: critical issues (e.g. expired KYC, invalid PEP) or duplicate client IDs

### 4. Data Glossary (16 Attributes)
Each attribute mapped from system name → business term with:
- Business definition
- Data owner & source system
- Mandatory / PII flags
- DQ rules and transformation applied
- Regulatory basis (FATF, MAS Notice 626, IFRS, CRS/FATCA)
- Lineage: Raw → Staged → Cleaned → Reported

### 5. Data Lineage (5 Stages)
End-to-end lineage from source systems through pipeline stages to consuming systems (KYC Workflow, Risk Reports, Coverage Dashboard, Compliance Alerts).

### 6. Interactive Dashboard (Streamlit)
- **DQ Overview** — score distribution, issue severity, top problem attributes
- **Issue Explorer** — filter by severity/attribute, drill into individual client records
- **KYC Expiry Monitor** — expired and soon-to-expire KYC clients with RM assignment
- **Data Glossary** — searchable attribute definitions with regulatory context
- **Data Lineage** — stage-by-stage lineage with attribute-level detail
- **Attribute Profiling** — completeness stats across all attributes

---

## Setup & Run

### Requirements
```bash
pip install pandas numpy streamlit plotly openpyxl
```

### Run the pipeline
```bash
cd clm_project
python run_pipeline.py
```

### Launch the dashboard
```bash
streamlit run app/dashboard.py
```

---

## Key Data Quality Issues Simulated

| Issue Type | Attributes Affected | Severity |
|---|---|---|
| Expired KYC | kyc_expiry_date | Critical |
| Invalid/missing PEP flag | pep_flag | Critical |
| Future date of birth | dob | Critical |
| Missing risk rating | risk_rating | Critical |
| Negative AUM | aum_usd | Critical |
| Duplicate client IDs | client_id | Critical |
| Inconsistent casing | client_type, risk_rating, segment | High |
| ISO country code variants | country | Medium |
| Placeholder emails/phones | email, phone | Medium |
| Missing nationality | nationality | Medium |

---

## Alignment with Banking Standards

- **FATF Recommendations 10, 12** — Customer Due Diligence, PEP handling
- **MAS Notice 626** — AML/CFT requirements (Singapore)
- **IFRS 9** — Financial instrument classification (referenced in ECL project)
- **OECD CRS / US FATCA** — Tax residency reporting
- **Group Data Management Standards** — Data lineage, metadata management

---

## Skills Demonstrated

`Data Discovery` `Data Profiling` `Root Cause Analysis` `Data Quality Management`
`Data Lineage` `Metadata Management` `Data Glossary` `SQL` `Python` `Streamlit`
`Banking / CLM Domain Knowledge` `Regulatory Data Standards` `Stakeholder Reporting`
