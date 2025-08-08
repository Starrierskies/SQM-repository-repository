import pandas as pd
import requests
import io
from datetime import datetime
from supabase import create_client
import os

# Supabase credentials
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Data source URL
GAN_URL = "https://globeatnight.org/documents/926/GaN2024.csv"

def build_iso_utc(date_str, time_str):
    """Combine UTDate + UTTime into ISO8601 UTC timestamp."""
    try:
        return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S").isoformat() + "Z"
    except Exception:
        return None

def main():
    print("Downloading GaN 2024…")
    r = requests.get(GAN_URL)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))

    # Clean column names
    df.columns = [c.strip() for c in df.columns]

    # Build timestamp
    df["timestamp_utc"] = df.apply(lambda r: build_iso_utc(r.get("UTDate"), r.get("UTTime")), axis=1)

    # Numeric cleaning for SQMReading
    df["SQMReading"] = pd.to_numeric(df["SQMReading"], errors="coerce")
    df["sky_brightness_mag_arcsec2"] = df["SQMReading"]

    # Rename + add metadata
    df["latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df["device_type"] = df["ObsType"]
    df["source_tag"] = "globe_at_night"
    df["country"] = df["Country"]
    df["cloud_cover"] = df["CloudCover"]

    # Keep only columns your table can accept
    df_out = df[[
        "timestamp_utc",
        "latitude",
        "longitude",
        "device_type",
        "sky_brightness_mag_arcsec2",
        "source_tag",
        "country",
        "cloud_cover"
    ]]

    # Drop rows with no coords or no timestamp
    df_out = df_out.dropna(subset=["timestamp_utc", "latitude", "longitude"])

    print(f"Downloaded {len(df)} rows.")
    print(f"Prepared {len(df_out)} rows for upsert.")

    # Push to Supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    batch_size = 500
    for start in range(0, len
