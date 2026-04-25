"""
lookup.py — Search federal contracts by vendor name, UEI, or description keyword.

Uses USASpending's free Search API (no auth, generous rate limits).
Results include contract ID, dollars, agency, date, description.

Run:
    python3 lookup.py vendor CodeSignal
    python3 lookup.py vendor "HackerRank" "Karat"          # multiple vendors
    python3 lookup.py keyword "tech talent"                # description search
    python3 lookup.py keyword "coding assessment" "technical interview"
    python3 lookup.py vendor CodeSignal --naics 541511 541512
"""

import argparse
import json
import sys
from typing import Any

import requests

API = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
CONTRACT_TYPE_CODES = ["A", "B", "C", "D"]  # BPA call, purchase order, delivery order, definitive contract


def search(filters: dict, limit: int = 100) -> list[dict]:
    """Paginate through USASpending search results."""
    results: list[dict] = []
    page = 1
    while True:
        payload = {
            "filters": filters,
            "fields": [
                "Award ID", "Recipient Name", "Recipient UEI",
                "Award Amount", "Awarding Agency", "Awarding Sub Agency",
                "Start Date", "End Date", "Description", "NAICS", "naics_description",
                "Contract Award Type",
            ],
            "limit": limit,
            "page": page,
        }
        r = requests.post(API, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("results", []))
        if not data.get("page_metadata", {}).get("hasNext"):
            break
        page += 1
        if page > 100:  # safety
            break
    return results


def build_filters(
    vendor: list[str] | None = None,
    uei: list[str] | None = None,
    keyword: list[str] | None = None,
    naics: list[str] | None = None,
    start: str = "2020-01-01",
    end: str = "2026-12-31",
) -> dict:
    f: dict[str, Any] = {
        "award_type_codes": CONTRACT_TYPE_CODES,
        "time_period": [{"start_date": start, "end_date": end}],
    }
    if vendor:
        # recipient_search_text is OR across terms, matches any of the names
        f["recipient_search_text"] = vendor
    if uei:
        f["recipient_search_text"] = uei  # same field handles UEIs
    if keyword:
        f["keywords"] = keyword
    if naics:
        f["naics_codes"] = naics
    return f


def print_table(rows: list[dict]) -> None:
    if not rows:
        print("  (no results)")
        return
    # Sort by dollars descending
    rows = sorted(rows, key=lambda r: r.get("Award Amount") or 0, reverse=True)
    total_dollars = sum(r.get("Award Amount") or 0 for r in rows)
    print(f"  {len(rows)} contracts  |  ${total_dollars/1e6:.2f}M total")
    print(f"  {'DATE':10}  {'AMOUNT':>12}  {'AGENCY':30}  {'VENDOR':30}  {'PIID':20}  DESCRIPTION")
    print(f"  {'-'*10}  {'-'*12}  {'-'*30}  {'-'*30}  {'-'*20}  {'-'*40}")
    for r in rows[:50]:
        amt = r.get("Award Amount") or 0
        agency = (r.get("Awarding Agency") or "")[:30]
        vendor = (r.get("Recipient Name") or "")[:30]
        piid = (r.get("Award ID") or "")[:20]
        date = (r.get("Start Date") or "")[:10]
        desc = (r.get("Description") or "")[:40].replace("\n", " ")
        print(f"  {date:10}  ${amt:>11,.0f}  {agency:30}  {vendor:30}  {piid:20}  {desc}")
    if len(rows) > 50:
        print(f"  ... and {len(rows)-50} more")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="mode", required=True)

    p_vendor = sub.add_parser("vendor", help="Search by vendor name(s) or UEI(s)")
    p_vendor.add_argument("terms", nargs="+", help="Vendor names or UEIs")
    p_vendor.add_argument("--naics", nargs="+", default=None)
    p_vendor.add_argument("--start", default="2020-01-01")
    p_vendor.add_argument("--end", default="2026-12-31")

    p_kw = sub.add_parser("keyword", help="Search award descriptions for keywords")
    p_kw.add_argument("terms", nargs="+", help="Keywords to search in descriptions")
    p_kw.add_argument("--naics", nargs="+", default=None)
    p_kw.add_argument("--start", default="2020-01-01")
    p_kw.add_argument("--end", default="2026-12-31")

    p_batch = sub.add_parser("batch", help="Run multiple vendor searches and aggregate")
    p_batch.add_argument("vendors", nargs="+")
    p_batch.add_argument("--naics", nargs="+", default=None)
    p_batch.add_argument("--start", default="2020-01-01")
    p_batch.add_argument("--end", default="2026-12-31")

    p_export = sub.add_parser("export", help="Export results to CSV")
    p_export.add_argument("mode", choices=["vendor", "keyword"])
    p_export.add_argument("terms", nargs="+")
    p_export.add_argument("--out", required=True)
    p_export.add_argument("--naics", nargs="+", default=None)
    p_export.add_argument("--start", default="2020-01-01")
    p_export.add_argument("--end", default="2026-12-31")

    args = ap.parse_args()

    if args.mode == "vendor":
        filters = build_filters(vendor=args.terms, naics=args.naics, start=args.start, end=args.end)
        print(f"Searching vendor(s): {', '.join(args.terms)}")
        rows = search(filters)
        print_table(rows)

    elif args.mode == "keyword":
        filters = build_filters(keyword=args.terms, naics=args.naics, start=args.start, end=args.end)
        print(f"Searching keyword(s): {', '.join(args.terms)}")
        rows = search(filters)
        print_table(rows)

    elif args.mode == "batch":
        grand_total = 0
        for v in args.vendors:
            print(f"\n=== {v} ===")
            f = build_filters(vendor=[v], naics=args.naics, start=args.start, end=args.end)
            rows = search(f)
            print_table(rows)
            grand_total += sum(r.get("Award Amount") or 0 for r in rows)
        print(f"\n{'='*60}")
        print(f"GRAND TOTAL across {len(args.vendors)} vendors: ${grand_total/1e6:.2f}M")

    elif args.mode == "export":
        import csv
        if args.mode == "vendor":
            filters = build_filters(vendor=args.terms, naics=args.naics, start=args.start, end=args.end)
        else:
            filters = build_filters(keyword=args.terms, naics=args.naics, start=args.start, end=args.end)
        rows = search(filters)
        if rows:
            keys = list(rows[0].keys())
            with open(args.out, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=keys)
                w.writeheader()
                w.writerows(rows)
            print(f"Wrote {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
