"""
update_baseline.py — Snapshot current data IDs into tests/data/baseline.json.

Run after a successful rebuild to raise the floor. The baseline is committed
to git; tests fail if any previously-seen ID disappears in a future rebuild.
"""
import csv, json
from datetime import datetime, timezone
from pathlib import Path

BASELINE = Path(__file__).parent / "baseline.json"


def build_baseline():
    baseline = {"created_at": datetime.now(timezone.utc).isoformat()}

    # ── Contracts (from contracts_raw.csv) ────────────────────────────────
    contracts_csv = Path("data/contracts_raw.csv")
    if contracts_csv.exists():
        with open(contracts_csv, newline="", encoding="utf-8") as f:
            keys = [row["key"] for row in csv.DictReader(f) if row.get("key")]
        baseline["contracts"] = {"count": len(keys), "keys": sorted(set(keys))}
        print(f"  contracts: {len(keys):,} keys")
    else:
        print("  contracts_raw.csv not found — skipping")

    # ── RFP bundles (from rfp_bundles.json) ───────────────────────────────
    rfp_path = Path("web/data/rfp_bundles.json")
    if rfp_path.exists():
        bundles = json.loads(rfp_path.read_text())
        nids = sorted(set(b["notice_id"] for b in bundles if b.get("notice_id")))
        baseline["rfp_bundles"] = {"count": len(nids), "notice_ids": nids}
        print(f"  rfp_bundles: {len(nids):,} notice IDs")

    # ── Combined table (from combined_table.json) ─────────────────────────
    ct_path = Path("web/data/combined_table.json")
    if ct_path.exists():
        ct = json.loads(ct_path.read_text())
        nids = sorted(set(b["nid"] for b in ct if b.get("nid")))
        baseline["combined_table"] = {"count": len(nids), "nids": nids}
        print(f"  combined_table: {len(nids):,} bundle nids")

    # ── Eval method roster (from by_eval_method.json) ─────────────────────
    em_path = Path("web/data/by_eval_method.json")
    if em_path.exists():
        em = json.loads(em_path.read_text())
        baseline["eval_methods"] = sorted(d["method"] for d in em if d.get("method"))
        print(f"  eval_methods: {baseline['eval_methods']}")

    BASELINE.write_text(json.dumps(baseline, indent=2))
    print(f"\nWrote {BASELINE}")
    return baseline


if __name__ == "__main__":
    build_baseline()
