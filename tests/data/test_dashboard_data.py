"""Data integrity tests for web/data/ JSON files before pushing to Vercel."""
import json
from pathlib import Path

DATA = Path("web/data")


def load(name):
    return json.loads((DATA / name).read_text())


def test_required_files_exist():
    for f in [
        "summary.json", "by_eval_method.json", "by_eval_method_fy.json",
        "by_fy.json", "by_agency.json", "rfp_bundles.json", "combined_table.json",
    ]:
        assert (DATA / f).exists(), f"Missing {f}"


def test_summary_contract_count():
    s = load("summary.json")
    assert s.get("total_contracts", 0) > 1000, \
        f"Expected >1000 contracts, got {s.get('total_contracts')}"


def test_summary_obligated():
    s = load("summary.json")
    assert s.get("total_obligated_b", 0) > 1, \
        f"Expected >$1B obligated, got {s.get('total_obligated_b')}"


def test_eval_method_no_tango_categories():
    """eval_method is USASpending-only — LPTA/BVT belong in tradeoff_code."""
    methods = {d["method"] for d in load("by_eval_method.json")}
    assert "LPTA" not in methods, "LPTA should not be in eval_method (use tradeoff_code)"
    assert "Best-Value Tradeoff" not in methods, \
        "Best-Value Tradeoff should not be in eval_method (use tradeoff_code)"


def test_eval_method_expected_categories():
    methods = {d["method"] for d in load("by_eval_method.json")}
    for expected in ("Fair Opportunity", "Negotiated Proposal", "Simplified Acquisition"):
        assert expected in methods, f"Expected eval_method '{expected}' not found"


def test_eval_method_nonzero_counts():
    for d in load("by_eval_method.json"):
        assert d.get("count", 0) > 0, f"eval_method '{d['method']}' has zero count"


def test_rfp_bundles_naics_scope():
    """All bundles must be 541511 or 541512 — no off-scope NAICS."""
    bundles = load("rfp_bundles.json")
    assert len(bundles) > 0, "rfp_bundles.json is empty"
    bad = [b.get("naics") for b in bundles if b.get("naics") not in ("541511", "541512")]
    assert not bad, f"Off-scope NAICS in rfp_bundles: {set(bad)}"


def test_combined_table_naics_scope():
    ct = load("combined_table.json")
    assert len(ct) > 0, "combined_table.json is empty"
    bad = [b.get("naics") for b in ct if b.get("naics") not in ("541511", "541512", "")]
    assert not bad, f"Off-scope NAICS in combined_table: {set(bad)}"


def test_by_fy_has_recent_year():
    data = load("by_fy.json")
    assert "2026" in data or "2025" in data, \
        f"No recent fiscal year in by_fy.json, found: {list(data.keys())}"


def test_by_agency_nonzero():
    data = load("by_agency.json")
    assert len(data) >= 5, f"Expected ≥5 agencies, got {len(data)}"
