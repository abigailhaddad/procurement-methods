"""
enrich_sam.py — Enrich contracts_raw.csv with SAM.gov entity data.

Uses the SAM.gov monthly bulk extract — one API call downloads a full
snapshot of all registered entities (~500MB zip). We filter to just the
UEIs in our contracts dataset and save data/sam_lookup.csv.

This is much more efficient than per-entity API calls (would be 10k+ calls).

Output: data/sam_lookup.csv
  Columns: uei, legal_business_name, sam_registration_date, entity_start_date,
           city, state

Setup:
  1. Get a free API key at sam.gov (Account Details → API Keys)
  2. Add to .env: SAM_API_KEY=your_key_here

Run:
    python enrich_sam.py
"""

import csv
import io
import os
import sys
import time
import zipfile
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

CONTRACTS_CSV  = Path("data/contracts_raw.csv")
OUTPUT_CSV     = Path("data/sam_lookup.csv")
EXTRACT_CACHE  = Path("data/sam_extract_cache.zip")
EXTRACT_URL    = "https://api.sam.gov/data-services/v1/extracts"

# SAM Public V2 .dat file — pipe-delimited, no header row.
# Field positions confirmed against known UEIs.
SAM_FIELDS = {
    0:  "uei",
    7:  "sam_registration_date",  # YYYYMMDD
    11: "legal_business_name",
    17: "city",
    18: "state",
    24: "entity_start_date",      # YYYYMMDD
}


def collect_ueis() -> set[str]:
    if not CONTRACTS_CSV.exists():
        raise FileNotFoundError(f"{CONTRACTS_CSV} not found — run fetch.py first.")
    ueis = set()
    with open(CONTRACTS_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            uei = (row.get("recipient_uei") or "").strip()
            if uei:
                ueis.add(uei)
    print(f"Unique UEIs in contracts_raw.csv: {len(ueis):,}")
    return ueis


def download_extract(api_key: str) -> Path:
    if EXTRACT_CACHE.exists():
        print(f"Using cached extract: {EXTRACT_CACHE} ({EXTRACT_CACHE.stat().st_size // 1_000_000}MB)")
        return EXTRACT_CACHE

    print("Downloading SAM.gov monthly entity extract (~500MB)...")
    params = {"api_key": api_key, "fileType": "ENTITY", "sensitivity": "PUBLIC"}
    resp = requests.get(EXTRACT_URL, params=params, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"SAM extract download failed: HTTP {resp.status_code} — {resp.text[:200]}")

    # Response is a JSON with a download URL
    data = resp.json()
    download_url = data.get("fileExtracts", [{}])[0].get("fileUri")
    if not download_url:
        raise RuntimeError(f"No download URL in response: {data}")

    print(f"Downloading from: {download_url}")
    with requests.get(download_url, stream=True, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(EXTRACT_CACHE, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    print(f"\r  {downloaded // 1_000_000}MB / {total // 1_000_000}MB", end="", flush=True)
    print(f"\nDownloaded → {EXTRACT_CACHE}")
    return EXTRACT_CACHE


def parse_extract(zip_path: Path, target_ueis: set[str]) -> list[dict]:
    print("Parsing extract (scanning for matching UEIs)...")
    matches = []
    field_indices = sorted(SAM_FIELDS.keys())

    with zipfile.ZipFile(zip_path) as zf:
        dat_files = [n for n in zf.namelist() if n.endswith(".dat")]
        if not dat_files:
            raise RuntimeError(f"No .dat file found in zip. Contents: {zf.namelist()}")
        dat_file = dat_files[0]
        print(f"  Reading {dat_file}...")

        with zf.open(dat_file) as f:
            for i, raw_line in enumerate(f):
                line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
                parts = line.split("|")
                if not parts or len(parts) < max(field_indices) + 1:
                    continue
                uei = parts[0].strip()
                if uei not in target_ueis:
                    continue
                row = {col: parts[idx].strip() for idx, col in SAM_FIELDS.items()}
                matches.append(row)

                if i % 500_000 == 0 and i > 0:
                    print(f"  ...scanned {i:,} lines, {len(matches):,} matches so far")

    print(f"Found {len(matches):,} matching entities")
    return matches


def main():
    api_key = os.environ.get("SAM_API_KEY")
    if not api_key:
        print("SAM_API_KEY not set. Get a free key at sam.gov → Account Details → API Keys.")
        print("Add it to .env: SAM_API_KEY=your_key_here")
        return

    ueis = collect_ueis()
    zip_path = download_extract(api_key)
    matches = parse_extract(zip_path, ueis)

    if not matches:
        print("No matches found — check that contracts_raw.csv has valid UEIs.")
        return

    import pandas as pd
    df = pd.DataFrame(matches).drop_duplicates(subset="uei")

    # Normalize dates YYYYMMDD → YYYY-MM-DD
    for col in ["sam_registration_date", "entity_start_date"]:
        df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors="coerce").dt.date.astype(str)
        df[col] = df[col].replace("NaT", "")

    df.to_csv(OUTPUT_CSV, index=False)
    coverage = len(df) / len(ueis) * 100
    print(f"\nSaved {len(df):,} entities → {OUTPUT_CSV}")
    print(f"Coverage: {coverage:.1f}% of UEIs in contracts_raw.csv")
    print(f"(Unmatched UEIs are likely deregistered or foreign entities)")


if __name__ == "__main__":
    main()
