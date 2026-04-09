"""
fetch.py — Pull all federal IT services contracts from Tango API.

Covers NAICS 541511 (custom programming) and 541512 (systems design),
FY2020 through the current fiscal year. Writes data/contracts_raw.csv.

CHECKPOINT/RESUME: Batches by calendar month. Each completed (naics, year, month)
batch saves to data/checkpoints/. Re-running skips completed batches. Safe to
interrupt at any time — just re-run to continue.

Free tier  (100 calls/day): ~12 months/day → ~12 days to complete
Micro tier (250 calls/day): ~30 months/day → ~5 days to complete

Run:
    pip install tango-python python-dotenv pandas
    python fetch.py

Re-running is safe — resumes from last saved month.
"""

import os
import time
from datetime import date, timedelta
from pathlib import Path
from calendar import monthrange

import pandas as pd
from dotenv import load_dotenv
from tango import TangoClient
from tango.exceptions import TangoRateLimitError, TangoAPIError

load_dotenv()

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

NAICS_CODES    = [541511, 541512]   # Custom programming + systems design
START_DATE     = date(2019, 10, 1)  # FY2020 starts Oct 1 2019
END_DATE       = date.today()
OUTPUT_CSV     = Path("data/contracts_raw.csv")
CHECKPOINT_DIR = Path("data/checkpoints")
PAGE_LIMIT     = 100
SLEEP_BETWEEN_CALLS = 3             # Seconds between pages (free: 25/min)

SHAPE = (
    "key,obligated,award_date,naics_code,set_aside,"
    "tradeoff_process(*),"
    "competition(*),"
    "recipient(uei,display_name),"
    "awarding_office(*)"
)


# ---------------------------------------------------------------------------
# Month window generation
# ---------------------------------------------------------------------------

def month_windows(start: date, end: date):
    """Yield (year, month, window_start, window_end) for each calendar month."""
    cur = date(start.year, start.month, 1)
    while cur <= end:
        yr, mo = cur.year, cur.month
        last_day = monthrange(yr, mo)[1]
        ws = cur
        we = min(date(yr, mo, last_day), end)
        yield yr, mo, ws, we
        # Advance to first of next month
        if mo == 12:
            cur = date(yr + 1, 1, 1)
        else:
            cur = date(yr, mo + 1, 1)


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


def api_call(client, naics, date_gte, date_lte, cursor, limit):
    for attempt in range(4):
        try:
            resp = client.list_contracts(
                naics_code=str(naics),
                award_date_gte=date_gte,
                award_date_lte=date_lte,
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
                raise RuntimeError(f"API error on NAICS {naics} {date_gte}–{date_lte}")


# ---------------------------------------------------------------------------
# Fetch one monthly batch
# ---------------------------------------------------------------------------

def fetch_month(client, naics, yr, mo, ws, we):
    records = []
    cursor  = None
    page    = 0
    date_gte = ws.isoformat()
    date_lte = we.isoformat()

    first = api_call(client, naics, date_gte, date_lte, cursor=None, limit=1)
    total = first["total_count"]
    if total == 0:
        return []

    total_pages = (total + PAGE_LIMIT - 1) // PAGE_LIMIT

    while True:
        resp   = api_call(client, naics, date_gte, date_lte, cursor=cursor, limit=PAGE_LIMIT)
        batch  = resp["results"]
        records.extend(batch)
        page  += 1

        print(f"\r    page {page}/{total_pages} — {len(records)}/{total} records", end="", flush=True)

        cursor = resp.get("cursor")
        if not resp.get("next") or not cursor:
            break
        time.sleep(SLEEP_BETWEEN_CALLS)

    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    client = TangoClient(api_key=os.environ["TANGO_API_KEY"])
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    windows = list(month_windows(START_DATE, END_DATE))
    total_batches = len(NAICS_CODES) * len(windows)
    done_batches  = sum(
        1 for n in NAICS_CODES for yr, mo, ws, we in windows
        if (CHECKPOINT_DIR / f"{n}_{yr}{mo:02d}.csv").exists()
    )

    print(f"IT Services Contract Fetch")
    print(f"NAICS {NAICS_CODES} | {START_DATE} → {END_DATE}")
    print(f"Progress: {done_batches}/{total_batches} monthly batches done\n")

    try:
        for naics in NAICS_CODES:
            for yr, mo, ws, we in windows:
                cp = CHECKPOINT_DIR / f"{naics}_{yr}{mo:02d}.csv"
                if cp.exists():
                    continue

                label = f"NAICS {naics}  {yr}-{mo:02d}"
                print(f"  {label}", end="  ", flush=True)
                records = fetch_month(client, naics, yr, mo, ws, we)
                pd.DataFrame(records).to_csv(cp, index=False)
                print(f"\r  {label}  →  {len(records):,} records          ")

    except DailyLimitReached as e:
        print(f"\n⚠  {e}")
        print("Progress saved. Re-run tomorrow to continue.\n")

    # Merge all checkpoints
    frames = [pd.read_csv(cp) for cp in sorted(CHECKPOINT_DIR.glob("*.csv"))]
    if not frames:
        print("No data yet — rate limit hit before first batch completed.")
        return

    df = (pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset="key")
            .sort_values(["naics_code", "award_date"])
            .reset_index(drop=True))
    df.to_csv(OUTPUT_CSV, index=False)

    done = sum(1 for _ in CHECKPOINT_DIR.glob("*.csv"))
    remaining = total_batches - done

    print(f"contracts_raw.csv: {len(df):,} contracts from {done} batches")
    print(f"  tradeoff_process: {df['tradeoff_code'].notna().mean():.1%} populated")
    print(f"  contract_type:    {df['contract_type'].notna().mean():.1%} populated")
    if remaining:
        print(f"\n{remaining} batches remaining — re-run tomorrow to continue.")
    else:
        print("\nFetch complete!")


if __name__ == "__main__":
    main()
