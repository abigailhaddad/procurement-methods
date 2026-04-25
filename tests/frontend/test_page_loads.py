"""Tests that the page loads correctly with data and no JS errors."""

import pytest


@pytest.mark.frontend
def test_no_js_errors(page, server):
    """Page should load without JavaScript errors."""
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.goto(f"{server}/index.html")
    page.wait_for_selector("#rfpTable tbody tr", timeout=30_000)
    # Favicon 404 is expected — filter it out
    real_errors = [e for e in errors if "favicon" not in e]
    assert real_errors == [], f"JS errors: {real_errors}"


@pytest.mark.frontend
def test_header_visible(page, server):
    """Dark green header with title and subtitle should be present."""
    page.goto(f"{server}/index.html")
    header = page.locator("header.site-header")
    assert header.is_visible()
    assert "Federal Government Buys IT Services" in header.inner_text()
    subtitle = page.locator(".site-subtitle")
    assert "541511/512" in subtitle.inner_text()


@pytest.mark.frontend
def test_cards_show_data(page_loaded):
    """Reactive summary cards should display numbers after data loads."""
    total = page_loaded.locator("#rcTotal").inner_text()
    assert total.replace(",", "").isdigit(), f"rcTotal not a number: {total}"

    shall = page_loaded.locator("#rcShall").inner_text()
    assert "%" in shall, f"rcShall missing %: {shall}"

    contracts = page_loaded.locator("#ccContracts").inner_text()
    assert contracts != "—", "contracts card should show a value"


@pytest.mark.frontend
def test_table_has_rows(page_loaded):
    """DataTable should render at least 25 rows (default page size)."""
    rows = page_loaded.locator("#rfpTable tbody tr")
    assert rows.count() >= 25


@pytest.mark.frontend
def test_table_columns(page_loaded):
    """All expected columns should be present in the table header."""
    headers = page_loaded.locator("#rfpTable thead th").all_inner_texts()
    expected = ["Title", "Date", "Type", "Department", "NAICS",
                "Set-aside", "Labels", "Contracts", "Eval Method", "Tradeoff Code", "SAM.gov", "Docs"]
    for col in expected:
        assert col in headers, f"Missing column: {col}"


@pytest.mark.frontend
def test_filter_bar_visible(page_loaded):
    """'No filters applied' bar and '+ Add filter' button should be visible."""
    assert page_loaded.locator("#filterBarEmpty").is_visible()
    assert page_loaded.locator("#addFilterBtn").is_visible()
    assert "No filters applied" in page_loaded.locator("#filterBarEmpty").inner_text()
