"""
fetch_bulk.py — Download IT services contracts from USASpending bulk archives.

Downloads agency/FY ZIP files from files.usaspending.gov, filters to IT-related
NAICS codes (5415xx), and saves transaction-level rows with ~75 columns covering
competition method, evaluation preference, contract pricing, and firm characteristics.

Each agency/FY is checkpointed individually. Re-running skips completed files.
Safe to interrupt and resume.

Run:
    python3 fetch_bulk.py                      # all agencies, FY2022-FY2026
    python3 fetch_bulk.py --fy 2026            # one year
    python3 fetch_bulk.py --agencies 097 036   # specific agencies
    python3 fetch_bulk.py --force              # re-download everything
"""

import argparse
import csv
import io
import os
import re
import tempfile
import time
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ARCHIVE_BASE   = "https://files.usaspending.gov/award_data_archive/"
CHECKPOINT_DIR = Path("data/bulk_checkpoints")
OUTPUT_CSV     = Path("data/contracts_bulk.csv")

# NAICS prefixes to keep — all computer/IT services
NAICS_PREFIXES = ("5415",)  # 541511, 541512, 541513, 541519, etc.

def _get_latest_datestamp(fallback: str = "20260306") -> str:
    """Fetch the archive index page and find the most recent datestamp."""
    try:
        r = requests.get(ARCHIVE_BASE, timeout=15)
        r.raise_for_status()
        dates = re.findall(r'Contracts_Full_(\d{8})\.zip', r.text)
        if dates:
            latest = max(dates)
            print(f"Auto-detected datestamp: {latest}")
            return latest
    except Exception as exc:
        print(f"Could not auto-detect datestamp ({exc}), using fallback {fallback}")
    return fallback

DATESTAMP = _get_latest_datestamp()

def _current_fy() -> int:
    today = date.today()
    return today.year + 1 if today.month >= 10 else today.year

DEFAULT_YEARS = list(range(_current_fy(), _current_fy() - 5, -1))  # 5 years

# ---------------------------------------------------------------------------
# Columns to keep
# ---------------------------------------------------------------------------

KEEP_COLUMNS = [
    # Identity & key
    "contract_award_unique_key",
    "award_id_piid",
    "modification_number",
    "transaction_number",
    "parent_award_id_piid",

    # Dollars
    "federal_action_obligation",
    "total_dollars_obligated",
    "base_and_exercised_options_value",
    "current_total_value_of_award",
    "base_and_all_options_value",
    "potential_total_value_of_award",

    # Dates
    "action_date",
    "action_date_fiscal_year",
    "period_of_performance_start_date",
    "period_of_performance_current_end_date",
    "period_of_performance_potential_end_date",
    "solicitation_date",

    # Agency
    "awarding_agency_code",
    "awarding_agency_name",
    "awarding_sub_agency_name",
    "awarding_office_code",
    "awarding_office_name",

    # Recipient / vendor
    "recipient_uei",
    "recipient_name",
    "recipient_doing_business_as_name",
    "recipient_parent_uei",
    "recipient_parent_name",
    "cage_code",
    "recipient_state_code",
    "recipient_country_code",

    # Competition & evaluation (the good stuff)
    "evaluated_preference_code",
    "evaluated_preference",
    "extent_competed_code",
    "extent_competed",
    "type_of_contract_pricing_code",
    "type_of_contract_pricing",
    "solicitation_procedures_code",
    "solicitation_procedures",
    "type_of_set_aside_code",
    "type_of_set_aside",
    "other_than_full_and_open_competition_code",
    "other_than_full_and_open_competition",
    "fair_opportunity_limited_sources_code",
    "fair_opportunity_limited_sources",
    "number_of_offers_received",
    "commercial_item_acquisition_procedures_code",
    "commercial_item_acquisition_procedures",
    "price_evaluation_adjustment_preference_percent_difference",
    "contract_bundling_code",
    "contract_bundling",
    "subcontracting_plan_code",
    "subcontracting_plan",
    "cost_or_pricing_data_code",
    "cost_or_pricing_data",
    "interagency_contracting_authority_code",
    "interagency_contracting_authority",
    "clinger_cohen_act_planning_code",
    "clinger_cohen_act_planning",

    # Parent IDV / vehicle info
    "parent_award_agency_name",
    "parent_award_type_code",
    "parent_award_type",
    "type_of_idc_code",
    "type_of_idc",

    # What they were buying
    "award_type_code",
    "award_type",
    "award_description",
    "idv_type_code",
    "idv_type",
    "multiple_or_single_award_idv_code",
    "multiple_or_single_award_idv",
    "naics_code",
    "naics_description",
    "product_or_service_code",
    "product_or_service_code_description",
    "information_technology_commercial_item_category_code",
    "information_technology_commercial_item_category",
    "performance_based_service_acquisition_code",
    "performance_based_service_acquisition",
    "contract_financing_code",
    "contract_financing",
    "transaction_description",
    "prime_award_base_transaction_description",

    # Modification reason (termination tracking)
    "action_type_code",
    "action_type_description",

    # Place of performance
    "primary_place_of_performance_state_code",
    "primary_place_of_performance_country_code",

    # Contract characteristics
    "number_of_actions",
    "solicitation_identifier",
    "national_interest_action_code",
    "national_interest_action",
    "multi_year_contract_code",
    "multi_year_contract",
    "consolidated_contract_code",
    "consolidated_contract",
    "undefinitized_action_code",
    "undefinitized_action",

    # Firm characteristics
    "domestic_or_foreign_entity_code",
    "domestic_or_foreign_entity",
    "contracting_officers_determination_of_business_size_code",
    "contracting_officers_determination_of_business_size",
    "emerging_small_business",
    "woman_owned_business",
    "veteran_owned_business",
    "service_disabled_veteran_owned_business",
    "minority_owned_business",
    "small_disadvantaged_business",
    "c8a_program_participant",
    "historically_underutilized_business_zone_hubzone_firm",
    "sba_certified_8a_joint_venture",
    "foreign_owned",

    # Link
    "usaspending_permalink",
]

# ---------------------------------------------------------------------------
# Agency list
# ---------------------------------------------------------------------------

def get_agencies() -> dict[str, str]:
    url = "https://files.usaspending.gov/reference_data/agency_codes.csv"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(r.text)))
    agencies = {}
    for row in rows:
        c = row.get("CGAC AGENCY CODE", "").strip()
        if row.get("TOPTIER_FLAG", "").strip() == "TRUE" and c and c not in agencies:
            agencies[c] = row["AGENCY NAME"]
    print(f"Loaded {len(agencies)} toptier agency codes\n")
    return agencies

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

NOT_FOUND  = "NOT_FOUND"
IP_BLOCKED = "IP_BLOCKED"
FAILED     = "FAILED"

def download_zip(url: str, max_retries: int = 3) -> str:
    """Download zip to a temp file. Returns temp path or sentinel string."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, stream=True, timeout=600)
            if r.status_code == 404:
                return NOT_FOUND
            if r.status_code >= 500:
                return IP_BLOCKED
            r.raise_for_status()
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
            downloaded = 0
            last_print = 0
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    tmp.write(chunk)
                    downloaded += len(chunk)
                    mb = downloaded / 1024 / 1024
                    if mb - last_print >= 10:
                        print(f"{mb:.0f}MB...", end=" ", flush=True)
                        last_print = mb
            tmp.close()
            return tmp.name
        except requests.exceptions.ConnectionError:
            return IP_BLOCKED
        except Exception as exc:
            wait = min(30 * (attempt + 1), 180)
            print(f"\n    retry {attempt+1}/{max_retries} in {wait}s ({exc})...")
            time.sleep(wait)
    return FAILED

# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def checkpoint_path(fy: int, code: str) -> Path:
    return CHECKPOINT_DIR / f"FY{fy}_{code}.csv"

def not_found_path(fy: int, code: str) -> Path:
    return CHECKPOINT_DIR / f"FY{fy}_{code}.not_found"

def is_done(fy: int, code: str) -> bool:
    return checkpoint_path(fy, code).exists() or not_found_path(fy, code).exists()

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Download IT contracts from USASpending bulk archives")
    parser.add_argument("--fy", nargs="+", type=int, default=DEFAULT_YEARS,
                        help=f"Fiscal years to download (default: {DEFAULT_YEARS})")
    parser.add_argument("--agencies", nargs="+", default=None,
                        help="Specific agency codes (default: all)")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if checkpoint exists")
    parser.add_argument("--force-current-fy", action="store_true",
                        help="Re-download current FY only (for monthly refresh)")
    args = parser.parse_args()

    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    agencies = get_agencies()
    if args.agencies:
        agencies = {c: agencies.get(c, f"Agency {c}") for c in args.agencies}

    if args.force:
        for fy in args.fy:
            for code in agencies:
                for p in [checkpoint_path(fy, code), not_found_path(fy, code)]:
                    if p.exists():
                        print(f"  --force: removing {p.name}")
                        p.unlink()

    if args.force_current_fy:
        cur = _current_fy()
        print(f"  --force-current-fy: clearing FY{cur}")
        for code in agencies:
            for p in [checkpoint_path(cur, code), not_found_path(cur, code)]:
                if p.exists():
                    p.unlink()

    already = sum(1 for fy in args.fy for c in agencies if is_done(fy, c))
    todo    = sum(1 for fy in args.fy for c in agencies if not is_done(fy, c))
    print(f"Already done: {already}  To download: {todo}")
    print(f"Years: {args.fy}")
    print(f"Agencies: {len(agencies)}")
    print(f"NAICS prefixes: {NAICS_PREFIXES}\n")

    ip_blocked = False
    total_kept = 0
    total_scanned = 0

    for fy in args.fy:
        if ip_blocked:
            break
        fy_done = sum(1 for c in agencies if is_done(fy, c))
        fy_todo = len(agencies) - fy_done
        print(f"\n{'='*60}")
        print(f"FY{fy}  —  {fy_done} done, {fy_todo} to download")
        print(f"{'='*60}")

        for code, name in agencies.items():
            if is_done(fy, code):
                continue

            url = f"{ARCHIVE_BASE}FY{fy}_{code}_Contracts_Full_{DATESTAMP}.zip"
            print(f"  [{code}] {name}: downloading...", end=" ", flush=True)
            resp = download_zip(url)

            if resp is IP_BLOCKED:
                print("IP BLOCKED — stopping.")
                ip_blocked = True
                break
            if resp is FAILED:
                print("FAILED — will retry next run")
                continue
            if resp is NOT_FOUND:
                print("404")
                not_found_path(fy, code).touch()
                continue

            zip_path = resp
            zip_mb = os.path.getsize(zip_path) / 1024 / 1024
            print(f"{zip_mb:.1f} MB  |  scanning...", end=" ", flush=True)

            rows_scanned = 0
            rows_kept = 0
            cp = checkpoint_path(fy, code)

            try:
                with zipfile.ZipFile(zip_path) as zf:
                    csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
                    if not csv_names:
                        print("no CSV in zip")
                        cp.touch()
                        continue

                    with open(cp, "w", newline="", encoding="utf-8") as out_f:
                        writer = csv.DictWriter(out_f, fieldnames=KEEP_COLUMNS, extrasaction="ignore")
                        writer.writeheader()

                        for csv_name in csv_names:
                            with zf.open(csv_name) as raw:
                                reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
                                for row in reader:
                                    rows_scanned += 1
                                    if rows_scanned % 100_000 == 0:
                                        print(f"{rows_scanned//1000}k...", end=" ", flush=True)

                                    naics = (row.get("naics_code") or "").strip()
                                    if not any(naics.startswith(p) for p in NAICS_PREFIXES):
                                        continue

                                    kept = {col: row.get(col, "") for col in KEEP_COLUMNS}
                                    writer.writerow(kept)
                                    rows_kept += 1

            except Exception as exc:
                print(f"\n    ERROR: {exc}")
                if cp.exists():
                    cp.unlink()
                os.unlink(zip_path)
                continue

            os.unlink(zip_path)
            total_kept += rows_kept
            total_scanned += rows_scanned
            print(f"scanned {rows_scanned:,}  →  kept {rows_kept:,} IT rows")

    # Merge all checkpoints into one CSV
    print(f"\n{'='*60}")
    print("Merging checkpoints...")
    frames = []
    for cp in sorted(CHECKPOINT_DIR.glob("FY*.csv")):
        if cp.stat().st_size > 0:
            try:
                with open(cp, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    if rows:
                        frames.append((cp.name, rows))
            except Exception:
                pass

    if not frames:
        print("No IT contracts found yet.")
        return

    total_rows = sum(len(rows) for _, rows in frames)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=KEEP_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for _, rows in frames:
            writer.writerows(rows)

    print(f"Wrote {total_rows:,} rows to {OUTPUT_CSV}")
    print(f"From {len(frames)} agency/FY files")

    # Write status for GitHub Actions chaining
    status = "blocked" if ip_blocked else "done"
    Path("data/scan_status.txt").write_text(status)

    if ip_blocked:
        print(f"\nIP blocked — re-run to continue. Progress saved.")
    else:
        print("\nDone!")


if __name__ == "__main__":
    main()
