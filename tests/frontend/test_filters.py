"""Tests for the two-step filter modal and chip system."""

import pytest


@pytest.mark.frontend
def test_add_filter_opens_modal(page_loaded):
    """Clicking '+ Add filter' should open the filter modal."""
    page_loaded.click("#addFilterBtn")
    overlay = page_loaded.locator("#filterOverlay")
    assert overlay.evaluate("el => el.classList.contains('open')")
    assert "Add filter" in page_loaded.locator("#filterDialogTitle").inner_text()


@pytest.mark.frontend
def test_filter_modal_shows_all_columns(page_loaded):
    """Filter modal step 1 should list all filterable columns."""
    page_loaded.click("#addFilterBtn")
    items = page_loaded.locator(".filter-field-item").all_inner_texts()
    expected_fields = [
        "Title", "Posted Date", "Notice Type", "Department",
        "NAICS", "Set-aside", "Label", "Has matched contracts", "Full text search",
    ]
    for field in expected_fields:
        assert any(field in item for item in items), f"Field missing from modal: {field}"


@pytest.mark.frontend
def test_filter_department_multiselect(page_loaded):
    """Selecting Department should show a searchable checkbox list."""
    page_loaded.click("#addFilterBtn")
    page_loaded.locator(".filter-field-item", has_text="Department").click()
    # Step 2: should show search input and options list
    assert page_loaded.locator("#fmOptSearch").is_visible()
    assert page_loaded.locator("#fmOptList").is_visible()
    options = page_loaded.locator("#fmOptList label").count()
    assert options > 5, "Department should have many options"


@pytest.mark.frontend
def test_filter_text_search_input(page_loaded):
    """Selecting Full text search should show a text input."""
    page_loaded.click("#addFilterBtn")
    page_loaded.locator(".filter-field-item", has_text="Full text search").click()
    assert page_loaded.locator("#fmTextVal").is_visible()


@pytest.mark.frontend
def test_apply_department_filter(page_loaded):
    """Applying a department filter should add a chip and filter the table."""
    initial_count = page_loaded.locator("#rfpTable tbody tr").count()

    page_loaded.click("#addFilterBtn")
    page_loaded.locator(".filter-field-item", has_text="Department").click()

    # Click first option label and apply (label click is more reliable than checkbox.check())
    page_loaded.locator("#fmOptList label").first.click()
    page_loaded.click("#fmApply")

    # A chip should appear
    chips = page_loaded.locator(".filter-chip-active")
    assert chips.count() == 1
    assert "Department" in chips.first.inner_text()

    # Table should be filtered (fewer or equal rows)
    filtered_count = page_loaded.locator("#rfpTable tbody tr").count()
    assert filtered_count <= initial_count


@pytest.mark.frontend
def test_clear_all_filters(page_loaded):
    """Clear all should remove chips and restore full table."""
    # Apply a filter first
    page_loaded.click("#addFilterBtn")
    page_loaded.locator(".filter-field-item", has_text="Department").click()
    page_loaded.locator("#fmOptList input[type='checkbox']").first.check()
    page_loaded.click("#fmApply")

    assert page_loaded.locator(".filter-chip-active").count() == 1

    # Clear all
    page_loaded.locator("#filterClearAll").click()

    assert page_loaded.locator(".filter-chip-active").count() == 0
    assert page_loaded.locator("#filterBarEmpty").is_visible()
    # Table should show all rows again
    assert page_loaded.locator("#rfpTable tbody tr").count() >= 25


@pytest.mark.frontend
def test_multiselect_search_filters_options(page_loaded):
    """Typing in the multiselect search box should filter options."""
    page_loaded.click("#addFilterBtn")
    page_loaded.locator(".filter-field-item", has_text="Department").click()

    search = page_loaded.locator("#fmOptSearch")
    search.fill("defense")
    page_loaded.wait_for_timeout(200)

    visible_options = [
        l for l in page_loaded.locator("#fmOptList label").all()
        if l.is_visible()
    ]
    assert len(visible_options) >= 1
    for opt in visible_options:
        assert "defense" in opt.inner_text().lower()


@pytest.mark.frontend
def test_escape_closes_modal(page_loaded):
    """Pressing Escape should close the filter modal."""
    page_loaded.click("#addFilterBtn")
    assert page_loaded.locator("#filterOverlay").evaluate("el => el.classList.contains('open')")
    page_loaded.keyboard.press("Escape")
    page_loaded.wait_for_timeout(100)
    assert not page_loaded.locator("#filterOverlay").evaluate("el => el.classList.contains('open')")
