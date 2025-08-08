import pandas as pd
import requests
from supabase import create_client
import os
import math

# Config
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
TABLE_NAME = "sqm_readings"
GAN_2024_CSV_URL = "https://globeatnight.org/documents/926/GaN2024.csv"

# Init Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def build_iso_utc(date_str, time_str):
    try:
        return pd.to_datetime(f"{date_str} {time_str}", utc=True).isoformat()
    except Exception:
        return None

def fetch_and_clean():
    print("Downloading GaN 2024…")
    r = requests.get(GAN_2024_CSV_URL)
    r.raise_for_status()

    df = pd.read_csv(pd.compat.StringIO(r.text))
    print(f"Downloaded rows: {len(df)}")

    # Clean numeric fields
    if "SQMReading" in df.columns:
        df["SQMReading"] = pd.to_numeric(df["SQMReading"], errors="coerce")

    # Add timestamp
    if "UTDate" in df.columns and "UTTime" in df.columns:
        df["timestamp_utc"] = df.apply(lambda r: build_iso_utc(r.get("UTDate"), r.get("UTTime")), axis=1)

    # Tag source
    df["source_tag"] = "globe_at_night"

    # Remove empty rows
    df = df.dropna(subset=["SQMReading", "timestamp_utc"])
    print(f"Prepared rows: {len(df)}")
    return df

def upsert_bulk(df):
    # Supabase bulk insert in chunks
    batch_size = 500
    for start in range(0, len(df), batch_size):
        end = start + batch_size
        chunk = df.iloc[start:end].to_dict(orient="records")
        if chunk:
            r = supabase.table(TABLE_NAME).upsert(chunk).execute()
            if r.data is None:
                raise RuntimeError(f"Upsert error: {r}")
    print("Upsert complete.")

def main():
    df = fetch_and_clean()
    upsert_bulk(df)

if __name__ == "__main__":
    main()
