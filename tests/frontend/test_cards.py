"""Tests for reactive summary cards and vocabulary popups."""

import pytest


@pytest.mark.frontend
def test_rfp_cards_show_percentages(page_loaded):
    """RFP signal cards should show percentage values."""
    for card_id in ["rcShall", "rcUser", "rcAgile", "rcRtm"]:
        val = page_loaded.locator(f"#{card_id}").inner_text()
        assert "%" in val, f"#{card_id} should show a percentage, got: {val}"


@pytest.mark.frontend
def test_matched_awards_card(page_loaded):
    """Matched awards card should show a count."""
    val = page_loaded.locator("#ccContracts").inner_text()
    assert val != "—", "Should show matched contract count"
    assert "matched" in val.lower() or val.replace(",", "").split()[0].isdigit()


@pytest.mark.frontend
def test_card_click_shows_vocab_popup(page_loaded):
    """Clicking an agile card should show a vocabulary popup."""
    agile_card = page_loaded.locator(".rfp-card-clickable[data-label-key='has_agile_vocab']")
    agile_card.click()
    page_loaded.wait_for_timeout(200)

    popup = page_loaded.locator(".vocab-popup")
    assert popup.is_visible()
    text = popup.inner_text()
    # Should list agile vocabulary terms
    assert any(word in text.lower() for word in ["sprint", "scrum", "kanban", "agile"])


@pytest.mark.frontend
def test_vocab_popup_filter_link(page_loaded):
    """Vocabulary popup should have a 'Filter to these RFPs' link."""
    page_loaded.locator(".rfp-card-clickable[data-label-key='has_agile_vocab']").click()
    page_loaded.wait_for_timeout(200)

    filter_link = page_loaded.locator(".vocab-filter-link")
    assert filter_link.is_visible()
    assert "Filter" in filter_link.inner_text()


@pytest.mark.frontend
def test_vocab_popup_filter_link_adds_chip(page_loaded):
    """Clicking 'Filter to these RFPs' should add a filter chip."""
    page_loaded.locator(".rfp-card-clickable[data-label-key='has_agile_vocab']").click()
    page_loaded.wait_for_timeout(200)
    page_loaded.locator(".vocab-filter-link").click()
    page_loaded.wait_for_timeout(300)

    chips = page_loaded.locator(".filter-chip-active")
    assert chips.count() >= 1
    assert "agile" in chips.first.inner_text().lower()


@pytest.mark.frontend
def test_cards_update_after_filter(page_loaded):
    """Cards should recalculate when a department filter is applied."""
    initial_total = page_loaded.locator("#rcTotal").inner_text()

    # Apply a department filter that results in fewer RFPs
    page_loaded.click("#addFilterBtn")
    page_loaded.locator(".filter-field-item", has_text="Department").click()
    page_loaded.locator("#fmOptSearch").fill("defense")
    page_loaded.wait_for_timeout(200)
    # Click first visible label (DOM-first item may be hidden by search filter)
    visible_labels = [l for l in page_loaded.locator("#fmOptList label").all() if l.is_visible()]
    assert visible_labels, "Should have visible 'defense' options"
    visible_labels[0].click()
    page_loaded.click("#fmApply")
    page_loaded.wait_for_timeout(300)

    filtered_total = page_loaded.locator("#rcTotal").inner_text()
    assert filtered_total != initial_total, "Cards should update after filtering"
