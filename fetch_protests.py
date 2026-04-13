"""
fetch_protests.py — Pull GAO bid protest data from the Tango API.

Fetches all protests from FY2022 onward, then matches them to our IT contracts
by solicitation_identifier. This tells us which contract awards got challenged
and what happened.

Outputs:
  data/protests_raw.csv     — all fetched protests
  data/protests_matched.csv — protests matched to our IT contracts

The Tango API is rate-limited (same pool as fetch_tradeoff.py), so this
may need multiple runs. However, protest volume is much lower than contracts
(~5K-10K per year vs. hundreds of thousands), so it typically completes in
one session.

Run:
    python3 fetch_protests.py
"""

import csv
import os
import time
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from tango import TangoClient
from tango.exceptions import TangoRateLimitError, TangoAPIError

load_dotenv()

RAW_CSV     = Path("data/protests_raw.csv")
MATCHED_CSV = Path("data/protests_matched.csv")
CONTRACTS   = Path("data/contracts_raw.csv")

RAW_FIELDS = [
    "case_number", "title", "solicitation_number",
    "agency", "protester", "filed_date", "decision_date",
    "outcome", "docket_url", "decision_url",
]

MATCHED_FIELDS = [
    "solicitation_identifier",
    "protest_count", "sustained_count", "denied_count",
    "withdrawn_count", "dismissed_count", "pending_count",
    "agencies", "outcomes", "docket_urls",
]


def obj_to_dict(obj):
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "__dict__"):
        return {k: obj_to_dict(v) for k, v in vars(obj).items() if not k.startswith("_")}
    return obj


def fetch_all_protests(client: TangoClient, start_date: str) -> list[dict]:
    """Fetch all protests filed after start_date. Pages through results."""
    all_protests = []
    page = 1
    limit = 100

    while True:
        for attempt in range(4):
            try:
                resp = client.list_protests(
                    page=page,
                    limit=limit,
                    filed_date_after=start_date,
                )
                break
            except TangoRateLimitError:
                if attempt == 3:
                    print(f"\n  Daily rate limit hit at page {page}.")
                    return all_protests
                wait = 60 * (attempt + 1)
                print(f"\n  Rate limited — waiting {wait}s (attempt {attempt+1}/4)")
                time.sleep(wait)
            except TangoAPIError as e:
                if attempt == 3:
                    raise
                print(f"\n  API error: {e} — retrying...")
                time.sleep(10)

        results = resp.results or []
        for r in results:
            d = obj_to_dict(r)
            row = {
                "case_number":          d.get("case_number", ""),
                "title":                d.get("title", ""),
                "solicitation_number":  d.get("solicitation_number", ""),
                "agency":               d.get("agency", ""),
                "protester":            d.get("protester", ""),
                "filed_date":           d.get("filed_date", ""),
                "decision_date":        d.get("decision_date", ""),
                "outcome":              d.get("outcome", ""),
                "docket_url":           d.get("docket_url", ""),
                "decision_url":         d.get("decision_url", ""),
            }
            all_protests.append(row)

        total = resp.count if hasattr(resp, "count") else 0
        print(f"\r  Page {page} — {len(all_protests)}/{total} protests", end="", flush=True)

        if len(results) < limit or len(all_protests) >= total:
            break
        page += 1
        time.sleep(3)

    print()
    return all_protests


def load_solicitation_index() -> set[str]:
    """Load all solicitation_identifiers from our IT contracts."""
    if not CONTRACTS.exists():
        return set()
    sol_set = set()
    with open(CONTRACTS, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            # solicitation_identifier may be in contracts_raw if we carried it through
            # Fall back to checking contracts_bulk.csv
            sol = (row.get("solicitation_identifier") or "").strip().upper()
            if sol:
                sol_set.add(sol)
    return sol_set


def load_solicitations_from_bulk() -> set[str]:
    """Load solicitation_identifiers from bulk data (has this column)."""
    bulk = Path("data/contracts_bulk.csv")
    if not bulk.exists():
        return set()
    sol_set = set()
    with open(bulk, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            naics = (row.get("naics_code") or "").strip()
            if naics not in ("541511", "541512"):
                continue
            sol = (row.get("solicitation_identifier") or "").strip().upper()
            if sol:
                sol_set.add(sol)
    print(f"  {len(sol_set):,} unique solicitation IDs from IT contracts")
    return sol_set


def match_and_aggregate(protests: list[dict], sol_set: set[str]) -> list[dict]:
    """Match protests to our solicitations and aggregate per solicitation."""
    from collections import defaultdict

    matched = defaultdict(list)
    for p in protests:
        sol = (p.get("solicitation_number") or "").strip().upper()
        if sol and sol in sol_set:
            matched[sol].append(p)

    rows = []
    for sol_id in sorted(matched, key=lambda s: -len(matched[s])):
        protests_list = matched[sol_id]
        outcomes = [p.get("outcome") or "Pending" for p in protests_list]
        agencies = sorted({(p.get("agency") or "").split(":")[0].strip()
                          for p in protests_list if p.get("agency")})
        docket_urls = sorted({p.get("docket_url") or ""
                             for p in protests_list if p.get("docket_url")})
        rows.append({
            "solicitation_identifier": sol_id,
            "protest_count":     len(protests_list),
            "sustained_count":   outcomes.count("Sustained"),
            "denied_count":      outcomes.count("Denied"),
            "withdrawn_count":   outcomes.count("Withdrawn"),
            "dismissed_count":   outcomes.count("Dismissed"),
            "pending_count":     outcomes.count("Pending"),
            "agencies":          "; ".join(agencies),
            "outcomes":          "; ".join(sorted(set(outcomes))),
            "docket_urls":       "; ".join(docket_urls),
        })
    return rows


def main():
    api_key = os.environ.get("TANGO_API_KEY")
    if not api_key:
        print("TANGO_API_KEY not set in .env")
        return

    client = TangoClient(api_key=api_key)
    Path("data").mkdir(exist_ok=True)

    # Fetch protests from FY2022 onward (Oct 2021)
    start_date = "2021-10-01"
    print(f"Fetching GAO protests filed after {start_date}...")
    protests = fetch_all_protests(client, start_date)
    print(f"  Total protests fetched: {len(protests):,}")

    # Save raw protests
    with open(RAW_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RAW_FIELDS)
        w.writeheader()
        w.writerows(protests)
    print(f"  Wrote {len(protests):,} protests → {RAW_CSV}")

    # Match to our IT contracts
    print("\nMatching protests to IT contract solicitations...")
    sol_set = load_solicitations_from_bulk()
    if not sol_set:
        sol_set = load_solicitation_index()

    matched = match_and_aggregate(protests, sol_set)

    with open(MATCHED_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MATCHED_FIELDS)
        w.writeheader()
        w.writerows(matched)

    total_protests = sum(r["protest_count"] for r in matched)
    sustained = sum(r["sustained_count"] for r in matched)
    print(f"  {len(matched):,} solicitations with matched protests ({total_protests:,} protests total)")
    print(f"  Sustained: {sustained}  Denied: {sum(r['denied_count'] for r in matched)}")
    print(f"  Wrote → {MATCHED_CSV}")

    # Print interesting matches
    if matched:
        print("\n  Top protested solicitations:")
        for row in matched[:10]:
            flag = " *** SUSTAINED ***" if row["sustained_count"] else ""
            print(f"    {row['solicitation_identifier']}: {row['protest_count']} protests "
                  f"({row['outcomes']}){flag}")


if __name__ == "__main__":
    main()
