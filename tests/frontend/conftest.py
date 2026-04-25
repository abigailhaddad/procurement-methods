"""Shared fixtures for procurement-methods frontend tests."""

import subprocess
import time
import socket
import pytest

BASE_URL = "http://localhost:8765"
WEB_DIR = "web"


def port_open(port: int) -> bool:
    try:
        with socket.create_connection(("localhost", port), timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def server():
    """Start a local HTTP server if one isn't already running."""
    if port_open(8765):
        yield BASE_URL
        return

    proc = subprocess.Popen(
        ["python3", "-m", "http.server", "8765", "--directory", WEB_DIR],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(20):
        if port_open(8765):
            break
        time.sleep(0.25)
    else:
        proc.kill()
        pytest.skip("Could not start local server")

    yield BASE_URL
    proc.kill()


@pytest.fixture
def page_loaded(page, server):
    """Navigate to the main page and wait for the DataTable to render."""
    page.goto(f"{server}/index.html")
    # Wait for DataTables to initialize (table body gets rows)
    page.wait_for_selector("#rfpTable tbody tr", timeout=30_000)
    return page
