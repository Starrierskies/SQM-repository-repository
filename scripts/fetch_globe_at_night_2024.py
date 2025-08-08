import pandas as pd
import requests
import io
import os
from datetime import datetime
from dateutil import parser
from supabase import create_client, Client

# Fetch environment variables from GitHub secrets
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE"]

# Connect to Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Download Globe at Night CSV
url = "https://globeatnight.org/documents/926/GaN2024.csv"
r = requests.get(url, timeout=120)
r.raise_for_status()

df = pd.read_csv(io.StringIO(r.text))

# Map columns to our schema
rows = []
for _, row in df.iterrows():
    try:
        ts = parser.parse(str(row["ObsDateTime"]))
    except Exception:
        continue
    try:
        lat = float(row["Latitude"])
        lon = float(row["Longitude"])
    except Exception:
        continue
    sqm = row.get("SQMReading", None)
    notes_parts = []
    for field in ["LimitingMag", "CloudCover", "Constellation", "Comments"]:
        if pd.notna(row.get(field, None)):
            notes_parts.append(f"{field}: {row[field]}")
    notes = " | ".join(notes_parts)

    rows.append({
        "timestamp_utc": ts.isoformat(),
        "latitude": lat,
        "longitude": lon,
        "sky_brightness_mag_arcsec2": float(sqm) if pd.notna(sqm) else None,
        "device_type": row.get("Device", None),
        "observer_name": row.get("User", None),
        "notes": notes,
        "source_tag": "globe_at_night",
        "upload_method": "historical_import"
    })

# Upload in batches
batch_size = 500
for i in range(0, len(rows), batch_size):
    batch = rows[i:i+batch_size]
    data, count = supabase.table("sqm_readings").upsert(batch).execute()
    print(f"Upserted batch {i//batch_size + 1}: {len(batch)} records")

print(f"Done. Total rows processed: {len(rows)}")
