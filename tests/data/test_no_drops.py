"""
test_no_drops.py — Verify nothing previously seen has been dropped.

Loads baseline.json (committed to git) and checks that every ID present
in the baseline still exists in the current data. Tests fail if any
previously-known record has disappeared.
"""
import csv, json
import pytest
from pathlib import Path

BASELINE_PATH = Path(__file__).parent / "baseline.json"


def load_baseline():
    if not BASELINE_PATH.exists():
        pytest.skip("No baseline.json yet — run tests/data/update_baseline.py")
    return json.loads(BASELINE_PATH.read_text())


def test_no_contracts_dropped():
    baseline = load_baseline()
    if "contracts" not in baseline:
        pytest.skip("No contract baseline")

    contracts_csv = Path("data/contracts_raw.csv")
    if not contracts_csv.exists():
        pytest.skip("contracts_raw.csv not available in this environment")

    with open(contracts_csv, newline="", encoding="utf-8") as f:
        current_keys = {row["key"] for row in csv.DictReader(f) if row.get("key")}

    baseline_keys = set(baseline["contracts"]["keys"])
    dropped = baseline_keys - current_keys
    assert not dropped, (
        f"CRITICAL: {len(dropped)} contracts dropped from baseline.\n"
        f"Examples: {sorted(dropped)[:5]}"
    )


def test_no_rfp_bundles_dropped():
    baseline = load_baseline()
    if "rfp_bundles" not in baseline:
        pytest.skip("No rfp_bundles baseline")

    rfp_path = Path("web/data/rfp_bundles.json")
    if not rfp_path.exists():
        pytest.skip("rfp_bundles.json not available")

    current_ids = {b["notice_id"] for b in json.loads(rfp_path.read_text()) if b.get("notice_id")}
    baseline_ids = set(baseline["rfp_bundles"]["notice_ids"])
    dropped = baseline_ids - current_ids
    assert not dropped, (
        f"CRITICAL: {len(dropped)} RFP bundles dropped from baseline.\n"
        f"Examples: {sorted(dropped)[:5]}"
    )


def test_no_combined_table_entries_dropped():
    baseline = load_baseline()
    if "combined_table" not in baseline:
        pytest.skip("No combined_table baseline")

    ct_path = Path("web/data/combined_table.json")
    if not ct_path.exists():
        pytest.skip("combined_table.json not available")

    current_nids = {b["nid"] for b in json.loads(ct_path.read_text()) if b.get("nid")}
    baseline_nids = set(baseline["combined_table"]["nids"])
    dropped = baseline_nids - current_nids
    assert not dropped, (
        f"CRITICAL: {len(dropped)} combined_table entries dropped from baseline.\n"
        f"Examples: {sorted(dropped)[:5]}"
    )


def test_no_eval_methods_dropped():
    baseline = load_baseline()
    if "eval_methods" not in baseline:
        pytest.skip("No eval_methods baseline")

    em_path = Path("web/data/by_eval_method.json")
    if not em_path.exists():
        pytest.skip("by_eval_method.json not available")

    current_methods = {d["method"] for d in json.loads(em_path.read_text()) if d.get("method")}
    baseline_methods = set(baseline["eval_methods"])
    dropped = baseline_methods - current_methods
    assert not dropped, (
        f"CRITICAL: eval_method categories dropped: {dropped}"
    )
