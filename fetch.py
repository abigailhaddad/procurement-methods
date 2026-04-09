"""
fetch.py — Pull all federal IT services contracts from Tango API.

Covers NAICS 541511 (custom programming) and 541512 (systems design),
FY2020 through the current fiscal year. Writes data/contracts_raw.csv.

CHECKPOINT/RESUME: Batches by fiscal year. Each completed (naics, fy) batch
is saved to data/checkpoints/. Re-running skips already-completed batches,
so daily rate-limit interruptions are safe — just re-run tomorrow.

Fields captured per contract:
  - tradeoff_code:  LPTA / TO (trade-off) / O (other) / null
  - contract_type:  J (FFP/deliverable) / Y (T&M) / Z (Labor Hours) / etc.
  - set_aside:      SBA, 8A, SDVOSBC, NONE, etc.
  - obligated, award_date, naics_code, recipient UEI + name, agency

Rate limits:
  Free tier:  100 calls/day — completes in ~12 days
  Micro tier: 250 calls/day — completes in ~5 days

Run:
    pip install tango-python python-dotenv pandas
    python fetch.py

Re-running is safe and resumes from the last completed batch.
"""

import os
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tango import TangoClient
from tango.exceptions import TangoRateLimitError, TangoAPIError

load_dotenv()

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

NAICS_CODES = [541511, 541512]   # Custom programming + systems design
FY_START    = 2020               # First fiscal year to fetch
FY_END      = 2025               # Last fiscal year (current)
OUTPUT_CSV  = Path("data/contracts_raw.csv")
CHECKPOINT_DIR = Path("data/checkpoints")
PAGE_LIMIT  = 100                # Tango max per page
SLEEP_BETWEEN_CALLS = 3          # Seconds between pages (free: 25/min limit)

SHAPE = (
    "key,obligated,award_date,naics_code,set_aside,"
    "tradeoff_process(*),"
    "competition(*),"
    "recipient(uei,display_name),"
    "awarding_office(*)"
)


# ---------------------------------------------------------------------------
# Record extraction
# ---------------------------------------------------------------------------

def extract_record(c: dict) -> dict:
    comp   = c.get("competition") or {}
    ct     = comp.get("contract_type") or {}
    tp     = c.get("tradeoff_process") or {}
    rec    = c.get("recipient") or {}
    office = c.get("awarding_office") or {}
    return {
        "key":            c.get("key"),
        "naics_code":     c.get("naics_code"),
        "obligated":      c.get("obligated"),
        "award_date":     c.get("award_date"),
        "set_aside":      c.get("set_aside"),
        "tradeoff_code":  tp.get("code"),
        "tradeoff_desc":  tp.get("description"),
        "contract_type":  ct.get("code") if isinstance(ct, dict) else ct,
        "recipient_uei":  rec.get("uei"),
        "recipient_name": rec.get("display_name"),
        "department":     office.get("agency"),
        "agency":         office.get("name"),
    }


def obj_to_dict(obj) -> dict:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "__dict__"):
        return {k: obj_to_dict(v) for k, v in vars(obj).items() if not k.startswith("_")}
    return obj


# ---------------------------------------------------------------------------
# API call with retry
# ---------------------------------------------------------------------------

class DailyLimitReached(Exception):
    pass


def api_call(client: TangoClient, naics: int, fy: int, cursor, limit: int) -> dict:
    """One paginated API call with retry. Raises DailyLimitReached if limit persists."""
    for attempt in range(4):
        try:
            resp = client.list_contracts(
                naics_code=str(naics),
                fiscal_year=fy,
                cursor=cursor,
                limit=limit,
                shape=SHAPE,
            )
            return {
                "total_count": resp.count,
                "results":     [extract_record(obj_to_dict(r)) for r in (resp.results or [])],
                "next":        resp.next,
                "cursor":      resp.cursor,
            }
        except TangoRateLimitError:
            if attempt == 3:
                raise DailyLimitReached("Daily rate limit reached — re-run tomorrow to resume.")
            wait = 30 * (attempt + 1)
            print(f"\n  Rate limited — waiting {wait}s (attempt {attempt+1}/4)")
            time.sleep(wait)
        except TangoAPIError as e:
            wait = 10 * (attempt + 1)
            print(f"\n  API error: {e} — retrying in {wait}s")
            time.sleep(wait)
            if attempt == 3:
                raise RuntimeError(f"API error persisted on NAICS {naics} FY{fy}")


# ---------------------------------------------------------------------------
# Fetch one (naics, fy) batch
# ---------------------------------------------------------------------------

def fetch_batch(client: TangoClient, naics: int, fy: int) -> list[dict]:
    """Fetch all contracts for one NAICS + fiscal year combination."""
    records = []
    cursor  = None
    page    = 0

    first = api_call(client, naics, fy, cursor=None, limit=1)
    total = first["total_count"]
    total_pages = (total + PAGE_LIMIT - 1) // PAGE_LIMIT
    print(f"  FY{fy}: {total:,} contracts (~{total_pages} pages)", end="  ", flush=True)

    if total == 0:
        print()
        return []

    while True:
        resp    = api_call(client, naics, fy, cursor=cursor, limit=PAGE_LIMIT)
        batch   = resp["results"]
        records.extend(batch)
        page   += 1

        print(f"\r  FY{fy}: page {page}/{total_pages} — {len(records):,} records", end="", flush=True)

        cursor = resp.get("cursor")
        if not resp.get("next") or not cursor:
            break
        time.sleep(SLEEP_BETWEEN_CALLS)

    print(f"\r  FY{fy}: done — {len(records):,} records          ")
    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    client = TangoClient(api_key=os.environ["TANGO_API_KEY"])
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    total_calls_used = 0

    print(f"Fetching IT services contracts")
    print(f"NAICS: {NAICS_CODES}  |  FY{FY_START}–FY{FY_END}")
    print(f"Checkpoints: {CHECKPOINT_DIR}/\n")

    try:
        for naics in NAICS_CODES:
            print(f"NAICS {naics}:")
            for fy in range(FY_START, FY_END + 1):
                cp = CHECKPOINT_DIR / f"{naics}_fy{fy}.csv"
                if cp.exists():
                    n = sum(1 for _ in open(cp)) - 1
                    print(f"  FY{fy}: already done ({n:,} records) — skipping")
                    continue

                records = fetch_batch(client, naics, fy)

                pd.DataFrame(records).to_csv(cp, index=False)
            print()

    except DailyLimitReached as e:
        print(f"\n⚠  {e}")
        print("Progress saved. Re-run tomorrow to continue.\n")

    # Merge all checkpoints into final CSV
    frames = []
    for cp in sorted(CHECKPOINT_DIR.glob("*.csv")):
        df = pd.read_csv(cp)
        if not df.empty:
            frames.append(df)

    if not frames:
        print("No data yet.")
        return

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset="key")
    df = df.sort_values(["naics_code", "award_date"]).reset_index(drop=True)
    df.to_csv(OUTPUT_CSV, index=False)

    tp_pct = df["tradeoff_code"].notna().mean()
    ct_pct = df["contract_type"].notna().mean()

    print(f"contracts_raw.csv: {len(df):,} contracts")
    print(f"  tradeoff_process populated: {df['tradeoff_code'].notna().sum():,} ({tp_pct:.1%})")
    print(f"  contract_type populated:    {df['contract_type'].notna().sum():,} ({ct_pct:.1%})")
    print(f"\nSaved → {OUTPUT_CSV}")

    # Show how many batches remain
    done    = sum(1 for _ in CHECKPOINT_DIR.glob("*.csv"))
    total_b = len(NAICS_CODES) * (FY_END - FY_START + 1)
    if done < total_b:
        print(f"\n{total_b - done} batches remaining — re-run to continue fetching.")


if __name__ == "__main__":
    main()
