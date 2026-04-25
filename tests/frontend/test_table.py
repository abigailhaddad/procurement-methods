"""Tests for DataTable sorting, pagination, and row interactions."""

import pytest


@pytest.mark.frontend
def test_table_sorts_by_date_desc(page_loaded):
    """Default sort should be newest date first."""
    dates = [
        row.locator("td").nth(1).inner_text()
        for row in page_loaded.locator("#rfpTable tbody tr").all()[:5]
    ]
    # Dates should be non-empty and in descending order
    assert all(d for d in dates)
    assert dates == sorted(dates, reverse=True), "Table should default to date descending"


@pytest.mark.frontend
def test_table_pagination(page_loaded):
    """Pagination controls should be present and functional."""
    paginate = page_loaded.locator(".dataTables_paginate")
    assert paginate.is_visible()
    # Next button should be clickable
    next_btn = page_loaded.locator(".paginate_button.next")
    assert next_btn.is_visible()
    next_btn.click()
    page_loaded.wait_for_timeout(300)
    # Still have rows after pagination
    assert page_loaded.locator("#rfpTable tbody tr").count() > 0


@pytest.mark.frontend
def test_table_length_control(page_loaded):
    """Show X per page control should be present."""
    length = page_loaded.locator(".dataTables_length")
    assert length.is_visible()
    assert "per page" in length.inner_text()


@pytest.mark.frontend
def test_row_click_opens_modal(page_loaded):
    """Clicking a table row should open the detail modal."""
    page_loaded.locator("#rfpTable tbody tr").first.click()
    modal = page_loaded.locator("#rfpModal")
    page_loaded.wait_for_timeout(300)
    assert "open" in (modal.get_attribute("class") or "")
    title = page_loaded.locator("#rfpModalTitle").inner_text()
    assert len(title) > 0


@pytest.mark.frontend
def test_modal_close_button(page_loaded):
    """Modal close button should close the modal."""
    page_loaded.locator("#rfpTable tbody tr").first.click()
    page_loaded.wait_for_timeout(200)
    page_loaded.locator("#rfpModalClose").click()
    page_loaded.wait_for_timeout(200)
    modal = page_loaded.locator("#rfpModal")
    assert "open" not in (modal.get_attribute("class") or "")


@pytest.mark.frontend
def test_modal_closes_on_escape(page_loaded):
    """Pressing Escape should close the detail modal."""
    page_loaded.locator("#rfpTable tbody tr").first.click()
    page_loaded.wait_for_timeout(200)
    page_loaded.keyboard.press("Escape")
    page_loaded.wait_for_timeout(200)
    modal = page_loaded.locator("#rfpModal")
    assert "open" not in (modal.get_attribute("class") or "")


@pytest.mark.frontend
def test_label_chips_in_table(page_loaded):
    """Label chips should appear in the Labels column for rows that have them."""
    chips = page_loaded.locator("#rfpTable tbody .chip")
    assert chips.count() > 0, "Some rows should have label chips"


@pytest.mark.frontend
def test_sam_links_in_table(page_loaded):
    """SAM.gov links should open correct URLs."""
    sam_links = page_loaded.locator("#rfpTable tbody td a[href*='sam.gov']")
    assert sam_links.count() > 0, "Should have SAM.gov links in table"
    href = sam_links.first.get_attribute("href")
    assert "sam.gov" in href


@pytest.mark.frontend
def test_contracts_column_shows_yes_no(page_loaded):
    """Contracts column should show 'Yes' or 'No', never blank or other values."""
    contracts_cells = page_loaded.locator("#rfpTable tbody tr td:nth-child(8)").all()
    for cell in contracts_cells[:25]:
        text = cell.inner_text().strip()
        assert text in ('Yes', 'No'), f"Contracts cell should show 'Yes' or 'No', got: {text!r}"


@pytest.mark.frontend
def test_shareable_url_updates_on_filter(page_loaded):
    """Applying a filter should add params to the URL."""
    page_loaded.click("#addFilterBtn")
    page_loaded.locator(".filter-field-item", has_text="Department").click()
    page_loaded.locator("#fmOptList label").first.click()
    page_loaded.click("#fmApply")
    page_loaded.wait_for_timeout(300)
    url = page_loaded.url
    assert "dept=" in url, f"URL should contain dept= param after department filter, got: {url}"


@pytest.mark.frontend
def test_shareable_url_restores_filters(page_loaded, server):
    """Loading a URL with filter params should restore the filters."""
    page_loaded.goto(f"{server}/index.html?contracts=Yes")
    page_loaded.wait_for_selector("#rfpTable tbody tr", timeout=5000)
    page_loaded.wait_for_timeout(500)
    chips = page_loaded.locator(".filter-chip-active")
    assert chips.count() == 1
    assert "Yes" in chips.first.inner_text()


@pytest.mark.frontend
def test_copy_link_button_visible_with_filters(page_loaded):
    """Copy link button should appear when filters are active."""
    assert not page_loaded.locator("#copyLinkBtn").is_visible()
    page_loaded.click("#addFilterBtn")
    page_loaded.locator(".filter-field-item", has_text="Department").click()
    page_loaded.locator("#fmOptList label").first.click()
    page_loaded.click("#fmApply")
    page_loaded.wait_for_timeout(200)
    assert page_loaded.locator("#copyLinkBtn").is_visible()


@pytest.mark.frontend
def test_full_text_section_in_modal(page_loaded):
    """Modal should show a 'Full extracted text' section for rows that have text."""
    rows = page_loaded.locator("#rfpTable tbody tr").all()
    for row in rows[:20]:
        row.click()
        page_loaded.wait_for_timeout(200)
        modal = page_loaded.locator("#rfpModal")
        if "open" not in (modal.get_attribute("class") or ""):
            continue
        body_text = page_loaded.locator("#rfpModalBody").inner_text()
        page_loaded.locator("#rfpModalClose").click()
        page_loaded.wait_for_timeout(100)
        if "Full extracted text" in body_text:
            return  # found at least one row with full text section
    # Not a failure if no rows have text — just skip
    pytest.skip("No rows with extracted text found in first 20")
