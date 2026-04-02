"""Shared test fixtures for valkey-oncall tests."""

import pytest


@pytest.fixture
def temp_db_path(tmp_path):
    """Provide a temporary SQLite database path for tests."""
    return str(tmp_path / "test_cache.db")
