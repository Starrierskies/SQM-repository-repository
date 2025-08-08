import os, io, json, requests
import pandas as pd
from dateutil import parser as dtp

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_ROLE"]
TABLE        = "sqm_readings"

GAN_CSV_URL  = "https://globeatnight.org/documents/926/GaN2024.csv"  # 2024 CSV (UTC fields available)

# Expected columns (from your sample):
# ID, ObsType, Latitude, Longitude, Elevation(m),
# LocalDate, LocalTime, UTDate, UTTime,
# LimitingMag, SQMReading, SQMSerial, CloudCover,
# Constellation, SkyComment, LocationComment, Country

REQUIRED_SOURCE_COLS = ["UTDate","UTTime","Latitude","Longitude","SQMReading"]

def download_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

def build_iso_utc(date_str: str, time_str: str) -> str | None:
    """
    Combine UTDate + UTTime into ISO-8601 Z. Handles various formats robustly.
    """
    if pd.isna(date_str) or pd.isna(time_str):
        return None
    s = f"{str(date_str).strip()} {str(time_str).strip()} UTC"
    try:
        dt = dtp.parse(s)
        iso = dt.isoformat()
        # normalize to Z
        return iso.replace("+00:00","Z") if "+00:00" in iso else (iso if iso.endswith("Z") else iso + "Z")
    except Exception:
        return None

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # make sure required columns exist
    for c in REQUIRED_SOURCE_COLS:
        if c not in df.columns:
            raise RuntimeError(f"Missing expected column: {c}")

    # drop rows without numeric SQMReading
    df["SQMReading"] = pd.to_numeric(df["SQMReading"], errors="coerce")
    df = df.dropna(subset=["SQMReading", "Latitude", "Longitude"])

    # build timestamp_utc
    df["timestamp_utc"] = df.apply(lambda r: build_iso_utc(r.get("UTDate"), r.get("UTTime")), axis=1)
    df = df.dropna(subset=["timestamp_utc"])

    # coerce coords
    df["latitude"]  = pd.to_numeric(df["Latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df = df.dropna(subset=["latitude","longitude"])

    # brightness
    df["sky_brightness_mag_arcsec2"] = df["SQMReading"]

    # device_type: use SQM serial if present, else generic
    if "SQMSerial" in df.columns:
        df["device_type"] = df["SQMSerial"].fillna("SQM-unknown")
    else:
        df["device_type"] = "SQM-unknown"

    # notes from optional fields
    note_fields = ["LimitingMag","CloudCover","Constellation","SkyComment","LocationComment","Country"]
    have = [c for c in note_fields if c in df.columns]
    if have:
        def mk_notes(row):
            parts = []
            for c in have:
                val = row.get(c, None)
                if pd.notna(val) and str(val).strip() != "":
                    parts.append(f"{c}={val}")
            return "; ".join(parts) or None
        df["notes"] = df.apply(mk_notes, axis=1)
    else:
        df["notes"] = None

    # standard fields
    df["source_type"]   = "imported"
    df["upload_method"] = "script"
    df["source_tag"]    = "globe_at_night"

    keep = [
        "timestamp_utc","latitude","longitude","sky_brightness_mag_arcsec2",
        "device_type","source_type","upload_method","source_tag","notes"
    ]
    return df[keep]

def upsert_bulk(df: pd.DataFrame):
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=timestamp_utc,latitude,longitude,device_type,source_tag"
    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    rows = df.to_dict(orient="records")
    CHUNK = 500
    total = 0
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i+CHUNK]
        r = requests.post(url, headers=headers, data=json.dumps(chunk), timeout=300)
        if not r.ok:
            raise RuntimeError(f"Upsert error {r.status_code}: {r.text[:400]}")
        total += len(chunk)
    print(f"✅ Upserted {total} rows from GaN 2024.")

def main():
    print("Downloading GaN 2024…")
    raw = download_csv(GAN_CSV_URL)
    print(f"Downloaded {len(raw)} rows.")
    df = normalize(raw)
    print(f"Prepared {len(df)} rows for upsert.")
    if len(df) == 0:
        print("Nothing to upsert (no SQM readings or all invalid).")
        return
    upsert_bulk(df)

if __name__ == "__main__":
    main()
