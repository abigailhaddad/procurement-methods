"""
fetch_solicitations.py — Download + filter SAM.gov opportunities by NAICS.

Sources (all public, no auth required):
  1. ContractOpportunitiesFullCSV.csv  — rolling "current" file, 2019-present
     https://s3.amazonaws.com/falextracts/Contract%20Opportunities/datagov/
  2. FY{YYYY}_archived_opportunities.csv  — per-fiscal-year archives, 1970-2026
     https://s3.amazonaws.com/falextracts/Contract%20Opportunities/Archived%20Data/

The archived yearly files are huge (~1 GB each for recent years), so this
script streams each file, filters rows by NAICS in one pass, and writes only
matching rows to the output CSV. Temp files are deleted after filtering.

Output:
  data/solicitations/filtered.csv   (all years merged, filtered to our NAICS)
  data/solicitations/pdfs/{nid}/*   (only when --attachments)

Run:
    python3 fetch_solicitations.py                           # current file only
    python3 fetch_solicitations.py --years 2020 2021 2022    # specific FYs
    python3 fetch_solicitations.py --years-from 2015         # 2015 → current
    python3 fetch_solicitations.py --attachments             # also fetch PDFs

NAICS defaults: 541511 + 541512 (IT services in this project's scope).
"""

import argparse
import csv
import io
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

DATA_DIR     = Path("data/solicitations")
FILTERED_CSV = DATA_DIR / "filtered.csv"
PDF_DIR      = DATA_DIR / "pdfs"

S3_BASE       = "https://s3.amazonaws.com/falextracts/Contract%20Opportunities"
CURRENT_URL   = f"{S3_BASE}/datagov/ContractOpportunitiesFullCSV.csv"
ARCHIVED_URL  = f"{S3_BASE}/Archived%20Data/FY{{year}}_archived_opportunities.csv"

OPP_DETAIL_API = "https://api.sam.gov/prod/opportunities/v2/"
API_KEY        = os.environ.get("SAM_API_KEY")

DEFAULT_NAICS = ["541511", "541512"]
DEFAULT_TYPES = ["Solicitation", "Combined Synopsis/Solicitation", "Sources Sought",
                 "Presolicitation", "Award Notice"]


def stream_filter(url: str, ncodes: set[str], types: set[str] | None,
                  writer: csv.DictWriter, header_written: bool) -> tuple[int, int, bool]:
    """
    Stream a CSV from S3, filter in-memory by NAICS / notice type, write matches.

    Returns (rows_scanned, rows_kept, header_written).
    """
    print(f"  streaming: {url}", flush=True)
    r = requests.get(url, stream=True, timeout=600)
    if r.status_code == 404:
        print(f"    [404] skipping", flush=True)
        return (0, 0, header_written)
    r.raise_for_status()

    # Stream bytes, decode as Latin-1, split into lines for csv.DictReader.
    def line_iter():
        for chunk in r.iter_lines(chunk_size=1024 * 1024, decode_unicode=False):
            if chunk is not None:
                yield chunk.decode("latin-1")

    reader = csv.DictReader(line_iter())

    if not header_written and reader.fieldnames:
        writer.fieldnames = reader.fieldnames
        writer.writeheader()
        header_written = True

    scanned = kept = 0
    last_reported_mb = 0
    for row in reader:
        scanned += 1
        if scanned % 50_000 == 0:
            # Rough progress — the Content-Length gives us total size
            print(f"    {scanned:>8,} scanned  {kept:>6,} kept", flush=True)

        naics = (row.get("NaicsCode") or "").strip()
        if ncodes and naics not in ncodes:
            continue
        if types and row.get("Type") not in types:
            continue

        writer.writerow(row)
        kept += 1

    print(f"    done: {scanned:,} scanned, {kept:,} kept", flush=True)
    return (scanned, kept, header_written)


def fetch_attachments(notice_id: str, session: requests.Session) -> list[str]:
    """Fetch resourceLinks for an opportunity and download each attachment."""
    r = session.get(f"{OPP_DETAIL_API}{notice_id}",
                    params={"api_key": API_KEY}, timeout=60)
    if r.status_code == 429:
        raise SystemExit("SAM rate limit hit. Quota resets midnight UTC.")
    if r.status_code == 404:
        return []
    if r.status_code != 200:
        print(f"    HTTP {r.status_code} fetching {notice_id}: {r.text[:200]}",
              file=sys.stderr)
        return []

    data = r.json()
    opp = data.get("opportunitiesData", [None])[0] if "opportunitiesData" in data else data
    if not opp:
        return []

    links = opp.get("resourceLinks") or []
    if not links:
        return []

    target = PDF_DIR / notice_id
    target.mkdir(parents=True, exist_ok=True)
    saved: list[str] = []
    for i, url in enumerate(links):
        if "sam.gov" in url and "api_key=" not in url:
            url = f"{url}{'&' if '?' in url else '?'}api_key={API_KEY}"
        try:
            resp = session.get(url, timeout=120, stream=True, allow_redirects=True)
        except requests.RequestException as e:
            print(f"    download err: {e}", file=sys.stderr)
            continue
        if resp.status_code != 200:
            continue

        fname = None
        cd = resp.headers.get("content-disposition", "")
        if "filename=" in cd:
            fname = cd.split("filename=")[-1].strip().strip('"').strip("'")
        if not fname:
            fname = url.rstrip("/").split("/")[-1].split("?")[0] or f"attachment_{i}"

        out = target / fname
        with open(out, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
        saved.append(str(out))
    return saved


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", nargs="+", type=int, default=None,
                    help="Specific fiscal years to fetch archived files for")
    ap.add_argument("--years-from", type=int, default=None,
                    help="Fetch archived files from this FY to current FY")
    ap.add_argument("--current", action="store_true", default=None,
                    help="Include ContractOpportunitiesFullCSV (rolling current)")
    ap.add_argument("--no-current", dest="current", action="store_false",
                    help="Skip the current file")
    ap.add_argument("--ncode", nargs="+", default=DEFAULT_NAICS,
                    help=f"NAICS codes to keep (default: {DEFAULT_NAICS})")
    ap.add_argument("--types", nargs="+", default=DEFAULT_TYPES,
                    help=f"Notice types to keep (default: {DEFAULT_TYPES})")
    ap.add_argument("--output", default=str(FILTERED_CSV),
                    help=f"Output CSV path (default: {FILTERED_CSV})")
    ap.add_argument("--attachments", action="store_true",
                    help="Fetch attachment PDFs via SAM API (consumes quota)")
    ap.add_argument("--max-attachments", type=int, default=None)
    args = ap.parse_args()

    # Default: include the current file if nothing else specified
    if args.current is None:
        args.current = (args.years is None and args.years_from is None)

    # Build year list
    years: list[int] = []
    if args.years:
        years = sorted(set(args.years))
    elif args.years_from is not None:
        cur_fy = datetime.utcnow().year + (1 if datetime.utcnow().month >= 10 else 0)
        years = list(range(args.years_from, cur_fy + 1))

    ncodes = set(args.ncode)
    types  = set(args.types) if args.types else None

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"=== Filtering opportunities ===")
    print(f"  NAICS: {sorted(ncodes)}")
    print(f"  Types: {sorted(types) if types else 'all'}")
    print(f"  Current file: {args.current}")
    print(f"  Archived years: {years or 'none'}")
    print(f"  Output: {out_path}\n")

    total_scanned = total_kept = 0
    with open(out_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=[])  # filled after first header read
        header_written = False

        if args.current:
            s, k, header_written = stream_filter(CURRENT_URL, ncodes, types, writer, header_written)
            total_scanned += s; total_kept += k

        for fy in years:
            url = ARCHIVED_URL.format(year=fy)
            s, k, header_written = stream_filter(url, ncodes, types, writer, header_written)
            total_scanned += s; total_kept += k

    print(f"\n=== Summary ===")
    print(f"  Total rows scanned: {total_scanned:,}")
    print(f"  Matching rows:      {total_kept:,}")
    print(f"  Wrote: {out_path}  ({out_path.stat().st_size/1e6:.1f} MB)")

    if not args.attachments:
        return

    print(f"\n=== Downloading attachment PDFs ===")
    if not API_KEY:
        sys.exit("  SAM_API_KEY not set — skipping attachment downloads")

    import pandas as pd
    df = pd.read_csv(out_path, low_memory=False, dtype=str)
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    already = {p.name for p in PDF_DIR.iterdir()} if PDF_DIR.exists() else set()
    todo = df[~df["NoticeId"].isin(already)]
    if args.max_attachments:
        todo = todo.head(args.max_attachments)
    print(f"  {len(already):,} already have PDF dir;  {len(todo):,} to fetch")

    session = requests.Session()
    hits = misses = 0
    for i, (_, row) in enumerate(todo.iterrows(), 1):
        saved = fetch_attachments(row["NoticeId"], session)
        if saved:
            hits += 1
            print(f"  [{i}/{len(todo)}] {row['NoticeId'][:12]}  {len(saved)} files")
        else:
            misses += 1
        time.sleep(0.3)
    print(f"\nDone.  hits={hits}  misses={misses}")


if __name__ == "__main__":
    main()
