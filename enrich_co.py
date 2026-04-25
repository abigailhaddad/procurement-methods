"""
enrich_co.py — Pull contracting officer audit-trail data from SAM.gov.

USASpending strips contracting officer PII from its bulk downloads. SAM.gov's
Contract Awards API (the FPDS replacement) exposes it publicly:
  - createdBy / createdDate
  - lastModifiedBy / lastModifiedDate
  - approvedBy / approvedDate
  - closedStatus

For each contract in contracts_raw.csv, this script queries SAM, collects the
audit trail across ALL modifications, and aggregates to one row per contract:
  - num_mods             how many modifications the contract has
  - original_co          createdBy of the base award (mod 0)
  - current_co           lastModifiedBy of the latest mod
  - all_cos              pipe-separated list of every distinct email
  - num_distinct_cos     count of distinct people
  - co_changed           bool: did the CO change at any point?
  - is_system_account    bool: base award was auto-created by an eProc system

API: https://api.sam.gov/contract-awards/v1/search
Docs: https://open.gsa.gov/api/contract-awards/

Rate limit: ~1000/day on the free non-federal tier. Use --limit for testing.

Run:
    python3 enrich_co.py --limit 10          # test on 10 contracts
    python3 enrich_co.py                     # run everything (resumable)
"""

import argparse
import csv
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

CONTRACTS_CSV = Path("data/contracts_raw.csv")
OUTPUT_CSV    = Path("data/co_lookup.csv")
API_URL       = "https://api.sam.gov/contract-awards/v1/search"
API_KEY       = os.environ.get("SAM_API_KEY")

OUTPUT_COLS = [
    "key", "piid", "ref_idv_piid", "num_mods",
    "original_co", "current_co", "all_cos", "num_distinct_cos",
    "co_changed", "is_system_account",
    "first_action_date", "last_action_date",
]


def parse_key(key: str) -> tuple[str, str] | None:
    """
    USASpending key format:
      CONT_AWD_<piid>_<agency>_<ref_idv_piid>_<ref_idv_agency>
      CONT_AWD_<piid>_<agency>_-NONE-_-NONE-        (standalone award, no IDV)
    Returns (piid, ref_idv_piid) or None if unparseable.
    """
    if not key or not key.startswith("CONT_AWD_"):
        return None
    parts = key.split("_")
    if len(parts) < 6:
        return None
    piid = parts[2]
    ref = parts[4] if parts[4] != "-NONE-" else ""
    return piid, ref


def fetch_contract(piid: str, ref_idv_piid: str, session: requests.Session) -> list[dict]:
    """Fetch all modifications for a contract from SAM. Returns list of awardSummary entries."""
    params = {
        "api_key": API_KEY,
        "piid": piid,
        "limit": 100,
    }
    if ref_idv_piid:
        params["referencedIDVPiid"] = ref_idv_piid

    try:
        r = session.get(API_URL, params=params, timeout=30)
    except requests.RequestException as e:
        print(f"    ERROR: {e}", file=sys.stderr)
        return []

    if r.status_code == 429:
        print("    RATE LIMIT — stopping", file=sys.stderr)
        raise SystemExit("Hit SAM rate limit — re-run tomorrow to continue.")
    if r.status_code != 200:
        print(f"    HTTP {r.status_code}: {r.text[:200]}", file=sys.stderr)
        return []

    return r.json().get("awardSummary", []) or []


def aggregate(mods: list[dict]) -> dict:
    """Collapse a list of SAM modification records into one row of CO stats."""
    # Sort by modification number so mod 0 is first, latest mod is last
    def mod_key(m):
        mn = m["contractId"].get("modificationNumber") or "0"
        # numeric sort where possible, alpha fallback (e.g. "P00001")
        return (len(mn), mn)

    mods = sorted(mods, key=mod_key)

    def txn(m, field):
        return (m.get("awardDetails", {}).get("transactionData", {}) or {}).get(field)

    original_co = txn(mods[0], "createdBy") if mods else None
    current_co  = txn(mods[-1], "lastModifiedBy") if mods else None

    all_emails: list[str] = []
    seen: set[str] = set()
    for m in mods:
        for f in ("createdBy", "lastModifiedBy", "approvedBy"):
            v = txn(m, f)
            if v and v not in seen:
                seen.add(v)
                all_emails.append(v)

    def date(m, field):
        return txn(m, field)

    return {
        "num_mods":          len(mods),
        "original_co":       original_co or "",
        "current_co":        current_co or "",
        "all_cos":           "|".join(all_emails),
        "num_distinct_cos":  len(all_emails),
        "co_changed":        original_co != current_co and bool(original_co) and bool(current_co),
        "is_system_account": (original_co or "").upper().startswith("EPROCUREMENT."),
        "first_action_date": date(mods[0],  "createdDate") or "" if mods else "",
        "last_action_date":  date(mods[-1], "lastModifiedDate") or "" if mods else "",
    }


def load_done_keys() -> set[str]:
    if not OUTPUT_CSV.exists():
        return set()
    done: set[str] = set()
    with open(OUTPUT_CSV, newline="") as f:
        for row in csv.DictReader(f):
            done.add(row["key"])
    return done


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="Only process this many contracts")
    ap.add_argument("--sleep", type=float, default=0.1, help="Seconds between API calls")
    args = ap.parse_args()

    if not API_KEY:
        sys.exit("SAM_API_KEY not set in .env")
    if not CONTRACTS_CSV.exists():
        sys.exit(f"{CONTRACTS_CSV} not found — run build_contracts.py first")

    done = load_done_keys()
    print(f"Already processed: {len(done):,}")

    with open(CONTRACTS_CSV, newline="") as f:
        all_rows = list(csv.DictReader(f))
    todo = [r for r in all_rows if r["key"] not in done]
    if args.limit:
        todo = todo[: args.limit]

    print(f"To process:        {len(todo):,}")
    if not todo:
        return

    # Open output append-safely (write header only if new file)
    new_file = not OUTPUT_CSV.exists()
    out_f = open(OUTPUT_CSV, "a", newline="")
    writer = csv.DictWriter(out_f, fieldnames=OUTPUT_COLS)
    if new_file:
        writer.writeheader()

    session = requests.Session()
    hits = misses = 0

    try:
        for i, row in enumerate(todo, 1):
            key = row["key"]
            parsed = parse_key(key)
            if not parsed:
                writer.writerow({"key": key, **{c: "" for c in OUTPUT_COLS if c != "key"}})
                out_f.flush()
                misses += 1
                continue

            piid, ref_idv = parsed
            mods = fetch_contract(piid, ref_idv, session)

            if not mods:
                writer.writerow({
                    "key": key, "piid": piid, "ref_idv_piid": ref_idv,
                    **{c: "" for c in OUTPUT_COLS if c not in {"key","piid","ref_idv_piid"}},
                })
                misses += 1
            else:
                agg = aggregate(mods)
                writer.writerow({"key": key, "piid": piid, "ref_idv_piid": ref_idv, **agg})
                hits += 1

            out_f.flush()

            if i % 25 == 0 or i == len(todo):
                print(f"  [{i:>5,}/{len(todo):,}]  hits={hits:,}  misses={misses:,}  "
                      f"last: {key[:50]}")

            time.sleep(args.sleep)
    finally:
        out_f.close()

    print(f"\nDone. hits={hits:,}  misses={misses:,}")
    print(f"Wrote to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
