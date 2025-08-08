import os, io, json, requests
import pandas as pd
from dateutil import parser as dtp

# --- Config from GitHub Action secrets ---
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_ROLE"]
TABLE        = "sqm_readings"

# GaN 2024 CSV (UTC date & time columns present)
GAN_CSV_URL  = "https://globeatnight.org/documents/926/GaN2024.csv"

# Required base columns present in GaN CSV (both visual & SQM rows)
REQ_COLS = ["UTDate","UTTime","Latitude","Longitude"]

def download_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

def build_iso_utc(date_str: str, time_str: str) -> str | None:
    """Combine UTDate + UTTime into ISO-8601 Z."""
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

    df = df.copy()

    # Coerce numeric fields if present
    df["SQMReading"]  = pd.to_numeric(df.get("SQMReading"), errors="coerce")
    df["LimitingMag"] = pd.to_numeric(df.get("LimitingMag"), errors="coerce")

    # Build timestamp & coords
    df["timestamp_utc"] = df.apply(lambda r: build_iso_utc(r.get("UTDate"), r.get("UTTime")), axis=1)
    df["latitude"]      = pd.to_numeric(df["Latitude"], errors="coerce")
    df["longitude"]     = pd.to_numeric(df["Longitude"], errors="coerce")

    # Drop rows missing essentials
    df = df.dropna(subset=["timestamp_utc","latitude","longitude"])

    # Map observables
    df["sky_brightness_mag_arcsec2"] = df["SQMReading"]       # may be NaN for visual
    df["limiting_magnitude"]         = df["LimitingMag"]      # may be NaN for SQM

    # Keep values within sane bounds to avoid numeric overflow in DB
    # (brightness ~8–25, limiting mag ~0–9.9)
    df = df[
        (
            df["sky_brightness_mag_arcsec2"].isna() |
            ((df["sky_brightness_mag_arcsec2"] >= 8.0) & (df["sky_brightness_mag_arcsec2"] <= 25.0))
        ) &
        (
            df["limiting_magnitude"].isna() |
