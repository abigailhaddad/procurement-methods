"""
fetch_tradeoff.py — Pull source_selection_process (LPTA/TO/O) from Tango API.

Only fetches key + tradeoff_process — minimal shape to conserve API calls.
Joins with USASpending bulk data via contract_award_unique_key.

Checkpoints per NAICS+month. Resume-safe. Re-run daily until complete.

    python3 fetch_tradeoff.py
"""

import csv
import os
import time
from datetime import date
from calendar import monthrange
from pathlib import Path

from dotenv import load_dotenv
from tango import TangoClient
from tango.exceptions import TangoRateLimitError, TangoAPIError

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NAICS_CODES    = ["541511", "541512"]
START_DATE     = date(2021, 10, 1)   # FY2022
END_DATE       = date.today()
OUTPUT_CSV     = Path("data/tradeoff_lookup.csv")
CHECKPOINT_DIR = Path("data/tradeoff_checkpoints")
PAGE_LIMIT     = 100
SLEEP_BETWEEN  = 3  # seconds between calls (25/min limit)

SHAPE = "key,tradeoff_process(*)"


# ---------------------------------------------------------------------------
# Month windows
# ---------------------------------------------------------------------------

def month_windows(start: date, end: date):
    cur = date(start.year, start.month, 1)
    while cur <= end:
        yr, mo = cur.year, cur.month
        last_day = monthrange(yr, mo)[1]
        we = min(date(yr, mo, last_day), end)
        yield yr, mo, cur, we
        if mo == 12:
            cur = date(yr + 1, 1, 1)
        else:
            cur = date(yr, mo + 1, 1)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

class DailyLimitReached(Exception):
    pass


def obj_to_dict(obj):
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "__dict__"):
        return {k: obj_to_dict(v) for k, v in vars(obj).items() if not k.startswith("_")}
    return obj


def api_call(client, naics, date_gte, date_lte, cursor):
    for attempt in range(4):
        try:
            resp = client.list_contracts(
                naics_code=naics,
                award_date_gte=date_gte,
                award_date_lte=date_lte,
                cursor=cursor,
                limit=PAGE_LIMIT,
                shape=SHAPE,
            )
            results = []
            for r in (resp.results or []):
                d = obj_to_dict(r)
                tp = d.get("tradeoff_process") or {}
                if isinstance(tp, str):
                    tp = {}
                results.append({
                    "contract_award_unique_key": d.get("key", ""),
                    "tradeoff_code": tp.get("code", ""),
                    "tradeoff_desc": tp.get("description", ""),
                })
            return {
                "total_count": resp.count,
                "results": results,
                "next": resp.next,
                "cursor": resp.cursor,
            }
        except TangoRateLimitError:
            if attempt == 3:
                raise DailyLimitReached("Daily rate limit — re-run tomorrow.")
            wait = 60 * (attempt + 1)
            print(f"\n  Rate limited — waiting {wait}s (attempt {attempt+1}/4)")
            time.sleep(wait)
        except TangoAPIError as e:
            wait = 10 * (attempt + 1)
            print(f"\n  API error: {e} — retrying in {wait}s")
            time.sleep(wait)
            if attempt == 3:
                raise


# ---------------------------------------------------------------------------
# Fetch one month, saving every page
# ---------------------------------------------------------------------------

def fetch_month(client, naics, yr, mo, ws, we):
    cp = CHECKPOINT_DIR / f"{naics}_{yr}{mo:02d}.csv"
    cursor_file = CHECKPOINT_DIR / f"{naics}_{yr}{mo:02d}.cursor"
    date_gte = ws.isoformat()
    date_lte = we.isoformat()

    cursor = None
    page = 0
    total_saved = 0

    if cursor_file.exists():
        cursor = cursor_file.read_text().strip()
        if cp.exists():
            total_saved = max(0, sum(1 for _ in open(cp)) - 1)
            page = (total_saved + PAGE_LIMIT - 1) // PAGE_LIMIT

    # get count
    first = api_call(client, naics, date_gte, date_lte, cursor=None)
    total = first["total_count"]
    if total == 0:
        # write empty checkpoint
        with open(cp, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["contract_award_unique_key", "tradeoff_code", "tradeoff_desc"])
            w.writeheader()
        return 0

    total_pages = (total + PAGE_LIMIT - 1) // PAGE_LIMIT

    while True:
        resp = api_call(client, naics, date_gte, date_lte, cursor=cursor)
        batch = resp["results"]
        page += 1

        write_header = not cp.exists() or cp.stat().st_size == 0
        with open(cp, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["contract_award_unique_key", "tradeoff_code", "tradeoff_desc"])
            if write_header:
                w.writeheader()
            w.writerows(batch)
        total_saved += len(batch)

        print(f"\r    page {page}/{total_pages} — {total_saved}/{total}", end="", flush=True)

        cursor = resp.get("cursor")
        if not resp.get("next") or not cursor:
            if cursor_file.exists():
                cursor_file.unlink()
            break

        cursor_file.write_text(cursor)
        time.sleep(SLEEP_BETWEEN)

    return total_saved


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    client = TangoClient(api_key=os.environ["TANGO_API_KEY"])
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    windows = list(month_windows(START_DATE, END_DATE))
    total_batches = len(NAICS_CODES) * len(windows)

    done_batches = 0
    for naics in NAICS_CODES:
        for yr, mo, ws, we in windows:
            cp = CHECKPOINT_DIR / f"{naics}_{yr}{mo:02d}.csv"
            cursor_file = CHECKPOINT_DIR / f"{naics}_{yr}{mo:02d}.cursor"
            if cp.exists() and not cursor_file.exists():
                done_batches += 1

    print(f"Tradeoff Lookup Fetch (Tango API)")
    print(f"NAICS {NAICS_CODES} | {START_DATE} → {END_DATE}")
    print(f"Progress: {done_batches}/{total_batches} monthly batches done\n")

    try:
        for naics in NAICS_CODES:
            for yr, mo, ws, we in windows:
                cp = CHECKPOINT_DIR / f"{naics}_{yr}{mo:02d}.csv"
                cursor_file = CHECKPOINT_DIR / f"{naics}_{yr}{mo:02d}.cursor"
                if cp.exists() and not cursor_file.exists():
                    continue

                resuming = cursor_file.exists()
                label = f"NAICS {naics}  {yr}-{mo:02d}"
                print(f"  {label}{'  (resuming)' if resuming else ''}", end="  ", flush=True)
                count = fetch_month(client, naics, yr, mo, ws, we)
                print(f"\r  {label}  →  {count:,} records          ")

    except DailyLimitReached as e:
        print(f"\n\n⚠  {e}")
        print("Progress saved. Re-run tomorrow to continue.\n")

    # Merge checkpoints
    all_rows = []
    for cp in sorted(CHECKPOINT_DIR.glob("*.csv")):
        if cp.stat().st_size == 0:
            continue
        with open(cp, newline="") as f:
            reader = csv.DictReader(f)
            all_rows.extend(list(reader))

    if not all_rows:
        print("No data yet.")
        return

    # Deduplicate by key
    seen = set()
    unique = []
    for row in all_rows:
        k = row["contract_award_unique_key"]
        if k not in seen:
            seen.add(k)
            unique.append(row)

    with open(OUTPUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["contract_award_unique_key", "tradeoff_code", "tradeoff_desc"])
        w.writeheader()
        w.writerows(unique)

    done = sum(1 for cp in CHECKPOINT_DIR.glob("*.csv"))
    remaining = total_batches - done
    print(f"\n{OUTPUT_CSV}: {len(unique):,} unique contracts from {done} batches")
    if remaining:
        print(f"{remaining} batches remaining — re-run tomorrow to continue.")
    else:
        print("Fetch complete!")


if __name__ == "__main__":
    main()
