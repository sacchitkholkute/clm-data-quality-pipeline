"""
CLM Project — Master Run Script
Run this once to generate data and execute the full pipeline.
Then launch the dashboard separately.
"""

import os
import sys

# Set working directory to project root
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(PROJECT_ROOT)
sys.path.insert(0, PROJECT_ROOT)

print("\n" + "="*60)
print("  CLM Data Quality & Glossary Framework")
print("  Standard Chartered Bank — CLM-T Data Team")
print("="*60 + "\n")

# Step 1: Generate synthetic data
print("STEP 1: Generating synthetic CLM client data...")
from data.generate_data import generate
import sqlite3

os.makedirs("output", exist_ok=True)
df = generate()
df.to_csv("output/clm_raw.csv", index=False)
conn = sqlite3.connect("output/clm.db")
df.to_sql("clm_raw", conn, if_exists="replace", index=False)
conn.close()
print(f"  ✓ {len(df)} records generated\n")

# Step 2: Run DQ pipeline
print("STEP 2: Running Data Quality Pipeline...")
from pipeline.dq_pipeline import run
clean_df, profile_report = run(
    input_path="output/clm_raw.csv",
    output_dir="output"
)
print("  ✓ Pipeline complete\n")

# Step 3: Save glossary
print("STEP 3: Saving Data Glossary & Lineage...")
from pipeline.glossary import save_glossary
save_glossary(output_dir="output")
print("  ✓ Glossary saved\n")

# Summary
total     = len(clean_df)
clean     = (clean_df["remediation_status"] == "Clean").sum()
flagged   = (clean_df["remediation_status"] == "Flagged").sum()
quarant   = (clean_df["remediation_status"] == "Quarantined").sum()
avg_score = clean_df["dq_score"].mean()

print("="*60)
print("  PIPELINE SUMMARY")
print("="*60)
print(f"  Total records    : {total:,}")
print(f"  Avg DQ Score     : {avg_score:.1f}/100")
print(f"  Clean            : {clean:,}  ({clean/total*100:.1f}%)")
print(f"  Flagged          : {flagged:,}  ({flagged/total*100:.1f}%)")
print(f"  Quarantined      : {quarant:,}  ({quarant/total*100:.1f}%)")
print("="*60)

print("\n  OUTPUT FILES:")
for f in sorted(os.listdir("output")):
    size = os.path.getsize(f"output/{f}")
    print(f"  output/{f}  ({size:,} bytes)")

print("\n  TO LAUNCH DASHBOARD:")
print("  pip install streamlit pandas plotly")
print("  streamlit run app/dashboard.py")
print()
