"""
CLM Synthetic Dataset Generator
Generates realistic but intentionally dirty client onboarding data
to simulate what a bank's CLM system would ingest.
"""

import pandas as pd
import numpy as np
import sqlite3
import random
import os

random.seed(42)
np.random.seed(42)

N = 2000  # number of client records

COUNTRIES = ["India", "Singapore", "UAE", "UK", "USA", "Hong Kong", "Germany", "IN", "SG", None]
CLIENT_TYPES = ["Individual", "Corporate", "SME", "individual", "CORPORATE", None]
RISK_RATINGS = ["Low", "Medium", "High", "MEDIUM", "low", None, "Unknown"]
SEGMENTS = ["Retail", "Private", "Institutional", "RETAIL", None]
RELATIONSHIP_MGRS = [f"RM{str(i).zfill(3)}" for i in range(1, 51)]
NATIONALITIES = ["Indian", "Singaporean", "British", "American", "Emirati", None, "INDIAN"]

def random_date(start_year=1950, end_year=2003):
    y = random.randint(start_year, end_year)
    m = random.randint(1, 12)
    d = random.randint(1, 28)
    return f"{y}-{str(m).zfill(2)}-{str(d).zfill(2)}"

def random_onboard_date():
    y = random.randint(2015, 2024)
    m = random.randint(1, 12)
    d = random.randint(1, 28)
    return f"{y}-{str(m).zfill(2)}-{str(d).zfill(2)}"

def random_phone():
    patterns = [
        f"+91{random.randint(7000000000, 9999999999)}",
        f"91{random.randint(7000000000, 9999999999)}",
        f"0{random.randint(7000000000, 9999999999)}",
        str(random.randint(7000000000, 9999999999)),
        None,
        "N/A",
        "9999999999"  # placeholder
    ]
    return random.choices(patterns, weights=[40, 10, 10, 20, 10, 5, 5])[0]

def random_email(name):
    domains = ["gmail.com", "yahoo.com", "outlook.com", "bank.com", None]
    domain = random.choices(domains, weights=[40, 20, 20, 15, 5])[0]
    if domain is None:
        return None
    variants = [
        f"{name.lower().replace(' ', '.')}{random.randint(1,999)}@{domain}",
        f"{name.lower().replace(' ', '_')}@{domain}",
        "missing@placeholder.com" if random.random() < 0.03 else f"{name.lower().replace(' ', '')}@{domain}"
    ]
    return random.choice(variants)

def introduce_issues(df):
    """Deliberately introduce data quality issues for the pipeline to catch."""
    idx = df.index.tolist()

    # Duplicate records (~2%)
    dup_idx = random.sample(idx, int(N * 0.02))
    dups = df.loc[dup_idx].copy()
    df = pd.concat([df, dups], ignore_index=True)

    # Missing critical fields (low rates — realistic for a bank)
    for col, pct in [("dob", 0.03), ("nationality", 0.04), ("risk_rating", 0.04),
                     ("kyc_expiry_date", 0.03), ("phone", 0.05), ("email", 0.04)]:
        mask = df.sample(frac=pct).index
        df.loc[mask, col] = None

    # Inconsistent casing
    mask = df.sample(frac=0.04).index
    df.loc[mask, "client_type"] = df.loc[mask, "client_type"].str.upper()

    # Future dates in DOB (rare data entry errors ~1%)
    mask = df.sample(frac=0.01).index
    df.loc[mask, "dob"] = "2030-01-01"

    # KYC expired (~8% — realistic backlog)
    mask = df.sample(frac=0.08).index
    df.loc[mask, "kyc_expiry_date"] = "2022-12-31"

    # Negative AUM (~1% system errors)
    mask = df.sample(frac=0.01).index
    df.loc[mask, "aum_usd"] = df.loc[mask, "aum_usd"] * -1

    # Country code inconsistency (~3%)
    mask = df.sample(frac=0.03).index
    df.loc[mask, "country"] = df.loc[mask, "country"].map(
        lambda x: "IN" if x == "India" else ("SG" if x == "Singapore" else x)
    )

    return df.reset_index(drop=True)


def generate():
    first_names = ["Arjun","Priya","Rahul","Sneha","Vikram","Ananya","Rohan","Kavya",
                   "Amit","Divya","Kiran","Neha","Suresh","Pooja","Ravi","Meera",
                   "James","Sarah","Michael","Emma","David","Olivia","Robert","Sophia",
                   "Wei","Li","Mei","Zhang","Kumar","Lakshmi","Raj","Sunita"]
    last_names = ["Sharma","Patel","Singh","Kumar","Reddy","Nair","Iyer","Gupta",
                  "Smith","Johnson","Brown","Williams","Jones","Davis","Wilson","Taylor",
                  "Chen","Wang","Liu","Yang","Lee","Kim","Park","Ng","Tan","Lim"]

    records = []
    for i in range(N):
        cid = f"CLM{str(i+1).zfill(6)}"
        fname = random.choice(first_names)
        lname = random.choice(last_names)
        full_name = f"{fname} {lname}"

        records.append({
            "client_id":        cid,
            "full_name":        full_name,
            "client_type":      random.choices(CLIENT_TYPES, weights=[40,30,20,5,3,2])[0],
            "nationality":      random.choices(NATIONALITIES, weights=[35,15,10,10,10,5,15])[0],
            "country":          random.choices(COUNTRIES, weights=[35,15,10,10,10,5,5,3,2,5])[0],
            "dob":              random_date(),
            "phone":            random_phone(),
            "email":            random_email(full_name),
            "risk_rating":      random.choices(RISK_RATINGS, weights=[40,35,15,3,2,3,2])[0],
            "segment":          random.choices(SEGMENTS, weights=[40,25,25,8,2])[0],
            "onboard_date":     random_onboard_date(),
            "kyc_expiry_date":  f"202{random.randint(3,6)}-{str(random.randint(1,12)).zfill(2)}-28",
            "relationship_mgr": random.choice(RELATIONSHIP_MGRS),
            "aum_usd":          round(random.lognormvariate(10, 2), 2),
            "pep_flag":         random.choices(["Y","N","y","n",None], weights=[5,80,3,10,2])[0],
            "source_system":    random.choices(["CORE_BANKING","CRM","ONBOARDING_PORTAL","MANUAL"],
                                               weights=[50,25,20,5])[0],
        })

    df = pd.DataFrame(records)
    df = introduce_issues(df)
    return df


if __name__ == "__main__":
    os.makedirs("output", exist_ok=True)

    df = generate()
    df.to_csv("output/clm_raw.csv", index=False)

    # Also load into SQLite
    conn = sqlite3.connect("output/clm.db")
    df.to_sql("clm_raw", conn, if_exists="replace", index=False)
    conn.close()

    print(f"Generated {len(df)} records → output/clm_raw.csv + output/clm.db")
