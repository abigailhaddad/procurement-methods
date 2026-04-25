"""
build_combined_table.py — RFP-centric table enriched with USASpending contract data.

Each row is one SAM.gov RFP bundle. Where the bundle's solicitation_number matches
a contract's solicitation_id in contracts_raw.csv, we attach the matched contracts
as a list so the frontend can show them in the detail modal.

Many-to-many is expected (one solicitation → many task orders).  Dollar aggregation
uses the matched contracts' obligated values summed — but the dashboard cards
de-duplicate by solicitation_id to avoid double-counting when filtering.
"""

import csv, json
from collections import defaultdict
from pathlib import Path

OUT = Path("web/data/combined_table.json")

CT_LABELS = {"J": "FFP", "Y": "T&M", "Z": "Labor Hrs"}

def fmt_dollars(v):
    try:
        f = float(v)
        if abs(f) >= 1e9: return f"${f/1e9:.1f}B"
        if abs(f) >= 1e6: return f"${f/1e6:.1f}M"
        if abs(f) >= 1e3: return f"${f/1e3:.0f}K"
        return f"${int(f):,}"
    except (TypeError, ValueError):
        return "—"

# ── Load contracts keyed by solicitation_id ───────────────────────────────────
# Source 1: pre-fetched contracts_raw.csv (NAICS 541511/512 only)
print("Loading contracts from contracts_raw.csv…")
contracts_by_sol = defaultdict(list)
with open("data/contracts_raw.csv", newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        sid = (row.get("solicitation_id") or "").strip()
        if not sid:
            continue
        ob_raw = row.get("obligated", "")
        try:
            ob_float = float(ob_raw)
        except (TypeError, ValueError):
            ob_float = 0.0
        contracts_by_sol[sid].append({
            "key":    row.get("key", ""),
            "vendor": row.get("recipient_name", ""),
            "em":     row.get("eval_method", ""),
            "ob":     fmt_dollars(ob_raw),
            "ob_raw": ob_float,
            "ct":     CT_LABELS.get(row.get("contract_type", ""), row.get("contract_type", "")),
            "sa":     row.get("set_aside", ""),
            "fy":     row.get("fiscal_year", ""),
            "date":   (row.get("award_date") or "")[:10],
        })

total_with_sol = sum(len(v) for v in contracts_by_sol.values())
print(f"  {total_with_sol:,} contracts have a solicitation_id ({len(contracts_by_sol):,} unique)")

# Source 2: live USASpending API matches (all NAICS, from fetch_rfp_contracts.py)
rfp_matches_path = Path("data/rfp_contract_matches.json")
if rfp_matches_path.exists():
    live = json.loads(rfp_matches_path.read_text())
    added = 0
    for sol, rows in live.items():
        for row in rows:
            ob_raw = row.get("ob") or 0
            try:
                ob_float = float(ob_raw)
            except (TypeError, ValueError):
                ob_float = 0.0
            entry = {
                "key":    row.get("piid", ""),
                "vendor": row.get("vendor", ""),
                "em":     "",   # USASpending search API doesn't return eval method
                "ob":     fmt_dollars(str(ob_float)),
                "ob_raw": ob_float,
                "ct":     row.get("ct", ""),
                "sa":     row.get("sa", ""),
                "fy":     "",
                "date":   row.get("date", ""),
            }
            # Add only if not already in contracts_by_sol
            existing_keys = {c["key"] for c in contracts_by_sol[sol]}
            if entry["key"] not in existing_keys:
                contracts_by_sol[sol].append(entry)
                added += 1
    print(f"  +{added} from rfp_contract_matches.json")

# ── Load RFP bundles and enrich with matched contracts ────────────────────────
print("Loading RFP bundles…")
bundles = json.loads(Path("web/data/rfp_bundles.json").read_text())

rows = []
matched_bundles = 0
for b in bundles:
    sn = (b.get("solicitation_number") or "").strip()
    sa = b.get("set_aside", "")
    if isinstance(sa, dict):
        sa = sa.get("description", "")

    matched = contracts_by_sol.get(sn, []) if sn else []
    if matched:
        matched_bundles += 1

    rows.append({
        # Core RFP fields
        "nid":    b.get("notice_id", ""),
        "sol":    sn or None,
        "date":   (b.get("posted_date") or "")[:10],
        "dept":   b.get("department", ""),
        "title":  b.get("title", ""),
        "type":   b.get("type", ""),
        "naics":  b.get("naics", ""),
        "sa":     sa or "",
        "link":   b.get("ui_link", ""),
        "labels": b.get("label_hits", {}),
        "att":    b.get("attachment_count", 0),
        # Matched contract summary (may be empty list)
        "contracts": matched,
    })

rows.sort(key=lambda r: r["date"], reverse=True)

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(rows, separators=(",", ":")))
kb = OUT.stat().st_size / 1024
print(f"\nWrote {OUT}  ({len(rows):,} RFP rows, {matched_bundles} with contract matches, {kb:.0f} KB)")
