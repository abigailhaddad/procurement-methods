"""
rfp_text_pipeline.py — Daily pull of RFP attachment text for 541xxx opportunities.

Runs once a day (or ad-hoc). Flow:

  1. Pull R2 state: processed.json (set of noticeIds already bundled) and
     queue.json (FIFO of noticeIds we still want to fetch).
  2. Stream ContractOpportunitiesFullCSV.csv (no quota, free S3). Filter to
     the notice types + NAICS prefixes configured below. Add any new noticeId
     (not in processed, not in queue) to the tail of the queue with its CSV
     metadata captured.
  3. Dequeue up to --max-calls noticeIds. For each:
       - GET api.sam.gov/prod/opportunities/v2/{noticeId} (consumes 1 API call
         from the 1,000/day free-tier quota).
       - Download every PDF attachment from the S3 links the API returns.
       - Extract text with pypdf.
       - Write bundles/{noticeId}.json = CSV metadata + API metadata +
         per-attachment { filename, bytes, sha256, pages, chars, text }.
       - Add noticeId to processed, remove from queue.
  4. Push state + bundles back to R2.

Budget: hard-capped at --max-calls so the quota never gets slammed.
Anything we don't get to stays in the queue and drains over subsequent days.

Run:
    python3 rfp_text_pipeline.py                    # daily run, 950 calls
    python3 rfp_text_pipeline.py --max-calls 100    # cheaper test
    python3 rfp_text_pipeline.py --dry-run          # discover only, no API
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

try:
    from pypdf import PdfReader
except ImportError as exc:  # pragma: no cover
    raise SystemExit("pypdf missing — pip install pypdf") from exc

try:
    from docx import Document  # python-docx
except ImportError:
    Document = None  # docx extraction optional; warn at call time

try:
    from openpyxl import load_workbook  # xlsx
except ImportError:
    load_workbook = None

load_dotenv()

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATA_DIR       = Path("data/rfp_text")
STATE_DIR      = DATA_DIR / "state"
BUNDLE_DIR     = DATA_DIR / "bundles"
PROCESSED_JSON = STATE_DIR / "processed.json"
QUEUE_JSON     = STATE_DIR / "queue.json"
QUOTA_JSON     = STATE_DIR / "quota.json"

S3_CSV_URL     = "https://s3.amazonaws.com/falextracts/Contract%20Opportunities/datagov/ContractOpportunitiesFullCSV.csv"
OPP_SEARCH_API = "https://api.sam.gov/prod/opportunities/v2/search"

# Keep anything likely to carry a PWS / SOW / capability list.
# Award Notice is skipped — metadata is enough, attachments are admin paperwork.
DEFAULT_NOTICE_TYPES = {
    "Solicitation",
    "Combined Synopsis/Solicitation",
    "Sources Sought",
    "Presolicitation",
    "Special Notice",
    "Justification",
    "Fair Opportunity / Limited Sources Justification",
}

# NAICS prefix filter. 541xxx = Professional, Scientific, Technical Services.
# Covers legal, accounting, engineering, IT (5415), consulting, R&D, advertising,
# and the "other" bucket. Matches the user's "use all 54-series" ask.
DEFAULT_NAICS_PREFIXES = ("541",)

API_KEY = os.environ.get("SAM_API_KEY")
R2_PREFIX = "it_rfps/"


# ---------------------------------------------------------------------------
# State I/O
# ---------------------------------------------------------------------------

def _load_json(path: Path, default: Any) -> Any:
    if path.exists() and path.stat().st_size > 0:
        return json.loads(path.read_text())
    return default


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _maybe_hydrate_from_r2() -> None:
    """If R2 creds are set and local state is empty, pull state from R2."""
    if not os.environ.get("CF_R2_ACCOUNT_ID"):
        return
    if PROCESSED_JSON.exists() and QUEUE_JSON.exists():
        return
    print("Hydrating state from R2...")
    import r2_sync  # local module

    s3 = r2_sync._client()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=r2_sync.BUCKET, Prefix=R2_PREFIX + "state/"):
        for obj in page.get("Contents", []):
            name = Path(obj["Key"]).name
            local = STATE_DIR / name
            local.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(r2_sync.BUCKET, obj["Key"], str(local))
            print(f"  R2 -> {name}")


def _push_state_to_r2() -> None:
    if not os.environ.get("CF_R2_ACCOUNT_ID"):
        return
    import r2_sync

    s3 = r2_sync._client()
    for f in STATE_DIR.glob("*.json"):
        key = R2_PREFIX + "state/" + f.name
        s3.upload_file(str(f), r2_sync.BUCKET, key)
        print(f"  R2 <- {f.name}")


def _push_bundles_to_r2(new_noticeids: list[str]) -> None:
    if not os.environ.get("CF_R2_ACCOUNT_ID") or not new_noticeids:
        return
    import r2_sync

    s3 = r2_sync._client()
    for nid in new_noticeids:
        local = BUNDLE_DIR / f"{nid}.json"
        if not local.exists():
            continue
        key = R2_PREFIX + f"bundles/{nid}.json"
        s3.upload_file(str(local), r2_sync.BUCKET, key)
    print(f"  Uploaded {len(new_noticeids)} bundle(s) to R2")


# ---------------------------------------------------------------------------
# Discovery: stream the rolling CSV, enqueue new NAICS-matching notices
# ---------------------------------------------------------------------------

def _naics_matches(naics: str, prefixes: tuple[str, ...]) -> bool:
    return any(naics.startswith(p) for p in prefixes)


def discover_new_notices(
    processed: set[str],
    queued_ids: set[str],
    notice_types: set[str],
    naics_prefixes: tuple[str, ...],
) -> list[dict]:
    """Stream the current CSV and return new queue entries (not yet processed,
    not already queued). Each entry carries a metadata snapshot so we don't
    have to re-scan the CSV to rebuild context."""
    print(f"Streaming {S3_CSV_URL} ...")
    r = requests.get(S3_CSV_URL, stream=True, timeout=600)
    r.raise_for_status()

    def lines():
        for chunk in r.iter_lines(chunk_size=1024 * 1024, decode_unicode=False):
            if chunk is not None:
                yield chunk.decode("latin-1")

    reader = csv.DictReader(lines())
    new_entries: list[dict] = []
    scanned = 0
    for row in reader:
        scanned += 1
        if scanned % 100_000 == 0:
            print(f"  {scanned:,} scanned  {len(new_entries):,} new")

        nid = (row.get("NoticeId") or "").strip()
        if not nid or nid in processed or nid in queued_ids:
            continue
        if row.get("Type") not in notice_types:
            continue
        naics = (row.get("NaicsCode") or "").strip()
        if not naics or not _naics_matches(naics, naics_prefixes):
            continue

        new_entries.append({
            "notice_id":    nid,
            "title":        row.get("Title"),
            "type":         row.get("Type"),
            "naics":        naics,
            "posted_date":  row.get("PostedDate"),
            "response_deadline": row.get("ResponseDeadLine"),
            "department":   row.get("Department/Ind.Agency"),
            "sub_agency":   row.get("Sub-Tier"),
            "office":       row.get("Office"),
            "psc":          row.get("ClassificationCode"),
            "solicitation_number": row.get("Sol#"),
            "sam_link":     row.get("Link"),
            "enqueued_at":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
        })

    print(f"  scanned {scanned:,} rows, {len(new_entries):,} new queue entries")
    return new_entries


# ---------------------------------------------------------------------------
# API fetch + PDF extract
# ---------------------------------------------------------------------------

def fetch_opp_detail(session: requests.Session, queue_entry: dict) -> dict | None:
    """One API call. Returns parsed opportunity dict or None on 404.

    SAM's /v2/search requires a posted-date window. We derive a ±7-day window
    around the entry's PostedDate so the query hits the right record.
    """
    notice_id = queue_entry["notice_id"]
    posted = (queue_entry.get("posted_date") or "")[:10]
    try:
        d = datetime.strptime(posted, "%Y-%m-%d")
    except ValueError:
        d = datetime.now(timezone.utc)
    # SAM wants MM/dd/yyyy.
    start = (d.replace(day=1)).strftime("%m/%d/%Y")
    end_d = d.replace(day=28)  # safe upper bound within any month
    end = end_d.strftime("%m/%d/%Y")

    r = session.get(
        OPP_SEARCH_API,
        params={
            "api_key":    API_KEY,
            "noticeid":   notice_id,
            "postedFrom": start,
            "postedTo":   end,
            "limit":      1,
        },
        timeout=60,
    )
    if r.status_code == 429:
        raise SystemExit("SAM 429 — daily quota exhausted. Resumes midnight UTC.")
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        print(f"  HTTP {r.status_code} on {notice_id}: {r.text[:200]}", file=sys.stderr)
        return None
    data = r.json()
    opps = data.get("opportunitiesData") or []
    return opps[0] if opps else None


def _extract_pdf_text(pdf_bytes: bytes) -> tuple[int | None, str | None]:
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for p in reader.pages:
            try:
                parts.append(p.extract_text() or "")
            except Exception:
                parts.append("")
        return len(reader.pages), "\n\n".join(parts).strip()
    except Exception as exc:
        return None, f"[pypdf extract failed: {exc}]"


def _extract_docx_text(docx_bytes: bytes) -> tuple[int | None, str | None]:
    if Document is None:
        return None, "[python-docx not installed]"
    try:
        doc = Document(io.BytesIO(docx_bytes))
        paragraphs = [p.text for p in doc.paragraphs if p.text]
        # Tables — SOWs often lay out CLINs, deliverables, schedules in tables.
        for table in doc.tables:
            for row in table.rows:
                cells = [c.text for c in row.cells if c.text]
                if cells:
                    paragraphs.append(" | ".join(cells))
        return len(doc.paragraphs), "\n\n".join(paragraphs).strip()
    except Exception as exc:
        return None, f"[python-docx extract failed: {exc}]"


def _extract_xlsx_text(xlsx_bytes: bytes) -> tuple[int | None, str | None]:
    """Flatten each sheet into pipe-delimited rows; preserves CLIN/pricing structure."""
    if load_workbook is None:
        return None, "[openpyxl not installed]"
    try:
        wb = load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)
        out = []
        total_rows = 0
        for ws in wb.worksheets:
            out.append(f"### Sheet: {ws.title}")
            for row in ws.iter_rows(values_only=True):
                cells = ["" if v is None else str(v).strip() for v in row]
                if any(cells):
                    out.append(" | ".join(cells))
                    total_rows += 1
            out.append("")
        return total_rows, "\n".join(out).strip()
    except Exception as exc:
        return None, f"[openpyxl extract failed: {exc}]"


def _extract_by_ext(filename: str, content_type: str, data: bytes) -> tuple[int | None, str | None]:
    lower = filename.lower()
    ct = (content_type or "").lower()
    if lower.endswith(".pdf") or ct.startswith("application/pdf"):
        return _extract_pdf_text(data)
    if lower.endswith(".docx") or "officedocument.wordprocessingml" in ct:
        return _extract_docx_text(data)
    if lower.endswith(".xlsx") or "officedocument.spreadsheetml" in ct:
        return _extract_xlsx_text(data)
    return None, None


def download_and_extract(session: requests.Session, opp: dict) -> list[dict]:
    """For each resourceLink on the opportunity, download + extract text."""
    links = opp.get("resourceLinks") or []
    out: list[dict] = []
    for i, url in enumerate(links):
        # SAM endpoints want the api_key appended, S3 links don't.
        fetch_url = url
        if "sam.gov" in url and "api_key=" not in url:
            fetch_url = f"{url}{'&' if '?' in url else '?'}api_key={API_KEY}"
        try:
            resp = session.get(fetch_url, timeout=180, stream=True, allow_redirects=True)
        except requests.RequestException as exc:
            out.append({"index": i, "url": url, "error": str(exc)})
            continue
        if resp.status_code != 200:
            out.append({"index": i, "url": url, "error": f"HTTP {resp.status_code}"})
            continue

        data = resp.content
        filename = None
        cd = resp.headers.get("content-disposition", "")
        if "filename=" in cd:
            filename = cd.split("filename=", 1)[1].strip().strip('"').strip("'")
        if not filename:
            filename = re.split(r"[?#]", url.rstrip("/").split("/")[-1])[0] or f"attachment_{i}"

        sha = hashlib.sha256(data).hexdigest()
        pages, text = _extract_by_ext(filename, resp.headers.get("content-type", ""), data)
        chars = len(text) if text else 0

        out.append({
            "index":    i,
            "url":      url,
            "filename": filename,
            "bytes":    len(data),
            "sha256":   sha,
            "pages":    pages,
            "chars":    chars,
            "text":     text,
        })
    return out


def build_bundle(queue_entry: dict, opp: dict, attachments: list[dict]) -> dict:
    """Compose the per-opportunity output JSON."""
    return {
        "notice_id":          queue_entry["notice_id"],
        "fetched_at":         datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
        "csv_metadata":       queue_entry,
        "api_metadata": {
            "title":           opp.get("title"),
            "type":            opp.get("type"),
            "base_type":       opp.get("baseType"),
            "naics_code":      opp.get("naicsCode"),
            "classification":  opp.get("classificationCode"),
            "active":          opp.get("active"),
            "solicitation_number": opp.get("solicitationNumber"),
            "posted_date":     opp.get("postedDate"),
            "response_deadline": opp.get("responseDeadLine"),
            "archive_date":    opp.get("archiveDate"),
            "set_aside":       opp.get("typeOfSetAside"),
            "set_aside_desc":  opp.get("typeOfSetAsideDescription"),
            "description":     opp.get("description"),
            "department_name": (opp.get("fullParentPathName") or "").split(".")[0] if opp.get("fullParentPathName") else None,
            "full_path_name":  opp.get("fullParentPathName"),
            "point_of_contact": opp.get("pointOfContact"),
            "award":           opp.get("award"),
            "place_of_performance": opp.get("placeOfPerformance"),
            "ui_link":         opp.get("uiLink"),
        },
        "attachments":        attachments,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-calls", type=int, default=950,
                    help="Hard cap on SAM API calls this run (default: 950)")
    ap.add_argument("--naics-prefix", nargs="+", default=list(DEFAULT_NAICS_PREFIXES),
                    help="NAICS prefixes to keep (default: 541)")
    ap.add_argument("--types", nargs="+", default=sorted(DEFAULT_NOTICE_TYPES),
                    help="Notice types to keep (default: all RFP-adjacent)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Discover only; make no API calls")
    ap.add_argument("--summary-file", default="",
                    help="Append a markdown summary (e.g. $GITHUB_STEP_SUMMARY)")
    args = ap.parse_args()

    if not args.dry_run and not API_KEY:
        raise SystemExit("SAM_API_KEY not set")

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    BUNDLE_DIR.mkdir(parents=True, exist_ok=True)
    _maybe_hydrate_from_r2()

    processed: set[str] = set(_load_json(PROCESSED_JSON, []))
    queue: list[dict] = _load_json(QUEUE_JSON, [])
    queued_ids = {q["notice_id"] for q in queue}

    print(f"State: {len(processed):,} processed, {len(queue):,} queued")

    notice_types = set(args.types)
    naics_prefixes = tuple(args.naics_prefix)

    new_entries = discover_new_notices(processed, queued_ids, notice_types, naics_prefixes)
    queue.extend(new_entries)
    _save_json(QUEUE_JSON, queue)

    if args.dry_run:
        print(f"\n--dry-run: added {len(new_entries):,} new entries to queue, no API calls.")
        _push_state_to_r2()
        return

    session = requests.Session()
    budget = args.max_calls
    calls_spent = 0
    bundles_written: list[str] = []
    errors: list[dict] = []

    print(f"\nProcessing up to {budget} opportunities from queue...")
    remaining: list[dict] = []

    for entry in queue:
        if calls_spent >= budget:
            remaining.append(entry)
            continue

        nid = entry["notice_id"]
        try:
            opp = fetch_opp_detail(session, entry)
            calls_spent += 1
        except SystemExit:
            # 429 — stop, preserve the rest of the queue.
            remaining.append(entry)
            # Everything after this is also still pending.
            idx = queue.index(entry)
            remaining.extend(queue[idx + 1:])
            print(f"Quota exhausted after {calls_spent} calls")
            break

        if opp is None:
            # 404 → mark processed so we don't waste tomorrow's quota on it.
            processed.add(nid)
            errors.append({"notice_id": nid, "error": "not found"})
            continue

        attachments = download_and_extract(session, opp)
        bundle = build_bundle(entry, opp, attachments)
        path = BUNDLE_DIR / f"{nid}.json"
        path.write_text(json.dumps(bundle, indent=2, default=str))
        bundles_written.append(nid)
        processed.add(nid)

        if calls_spent % 25 == 0:
            print(f"  {calls_spent}/{budget}  last: {nid}  attachments: {len(attachments)}")

        # Light throttle so we don't hammer the API.
        time.sleep(0.25)

    _save_json(PROCESSED_JSON, sorted(processed))
    _save_json(QUEUE_JSON, remaining)
    _save_json(QUOTA_JSON, {
        "run_at":         datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
        "calls_spent":    calls_spent,
        "bundles_written": len(bundles_written),
        "errors":         len(errors),
        "queue_size_after": len(remaining),
    })

    _push_state_to_r2()
    _push_bundles_to_r2(bundles_written)

    print(f"\nRun complete: {calls_spent} calls, {len(bundles_written)} bundles, "
          f"{len(remaining):,} still queued")

    if args.summary_file:
        lines = [f"## RFP text pipeline\n"]
        lines.append(f"- API calls spent: **{calls_spent}** / {budget}")
        lines.append(f"- Bundles written: **{len(bundles_written)}**")
        lines.append(f"- 404 / errors: **{len(errors)}**")
        lines.append(f"- New queue entries discovered: **{len(new_entries):,}**")
        lines.append(f"- Queue remaining: **{len(remaining):,}**")
        lines.append(f"- Total processed to date: **{len(processed):,}**\n")
        if errors[:10]:
            lines.append("### First errors")
            for e in errors[:10]:
                lines.append(f"- `{e['notice_id']}`: {e['error']}")
            lines.append("")
        with open(args.summary_file, "a") as f:
            f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
