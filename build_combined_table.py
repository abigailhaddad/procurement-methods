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
print("Loading contracts…")
contracts_by_sol = defaultdict(list)
with open("data/contracts_raw.csv", newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        sid = (row.get("solicitation_id") or "").strip()
        if not sid:
            continue
        ob_raw = row.get("obligated", "")
        contracts_by_sol[sid].append({
            "key":    row.get("key", ""),
            "vendor": row.get("recipient_name", ""),
            "em":     row.get("eval_method", ""),
            "ob":     fmt_dollars(ob_raw),
            "ob_raw": float(ob_raw) if ob_raw.replace(".", "", 1).lstrip("-").isdigit() else 0,
            "ct":     CT_LABELS.get(row.get("contract_type", ""), row.get("contract_type", "")),
            "sa":     row.get("set_aside", ""),
            "fy":     row.get("fiscal_year", ""),
            "date":   (row.get("award_date") or "")[:10],
        })

total_with_sol = sum(len(v) for v in contracts_by_sol.values())
print(f"  {total_with_sol:,} contracts have a solicitation_id ({len(contracts_by_sol):,} unique)")

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
