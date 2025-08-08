import os, io, json, requests
import pandas as pd
from dateutil import parser as dtp

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_ROLE"]
TABLE        = "sqm_readings"

GAN_CSV_URL  = "https://globeatnight.org/documents/926/GaN2024.csv"

REQ_COLS = ["UTDate","UTTime","Latitude","Longitude"]

def download_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

def build_iso_utc(date_str: str, time_str: str) -> str | None:
    if pd.isna(date_str) or pd.isna(time_str):
        return None
    s = f"{str(date_str).strip()} {str(time_str).strip()} UTC"
    try:
        dt = dtp.parse(s)
        iso = dt.isoformat()
        if iso.endswith("Z"):
            return iso
        return iso.replace("+00:00","Z") if "+00:00" in iso else iso + "Z"
    except Exception:
        return None

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure required columns
    for c in REQ_COLS:
        if c not in df.columns:
            raise RuntimeError(f"Missing expected column: {c}")

    # Work on a copy
    df = df.copy()

    # Coerce numerics where present
    if "SQMReading" in df.columns:
        df["SQMReading"] = pd.to_numeric(df["SQMReading"], errors="coerce")
    else:
        df["SQMReading"] = pd.NA

    if "LimitingMag" in df.columns:
        df["LimitingMag"] = pd.to_numeric(df["LimitingMag"], errors="coerce")
    else:
        df["LimitingMag"] = pd.NA

    # Build timestamp + coords
    df["timestamp_utc"] = df.apply(lambda r: build_iso_utc(r.get("UTDate"), r.get("UTTime")), axis=1)
    df["latitude"]  = pd.to_numeric(df["Latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")

    # Drop rows missing essentials
    df = df.dropna(subset=["timestamp_utc","latitude","longitude"])

    # Map observables
    df["sky_brightness_mag_arcsec2"] = df["SQMRe]()_]()
