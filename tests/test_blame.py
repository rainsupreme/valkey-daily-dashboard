"""Tests for the blame module."""

from unittest.mock import MagicMock

import pytest

from valkey_oncall.blame import compute_blame
from valkey_oncall.cache import Cache


@pytest.fixture
def cache(tmp_path):
    return Cache(str(tmp_path / "test.db"))


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.repo = "valkey-io/valkey"
    client.compare_commits.return_value = [
        {
            "sha": "aaa111",
            "message": "fix: something",
            "author": "dev1",
            "date": "2026-05-03T10:00:00Z",
        },
        {
            "sha": "bbb222",
            "message": "feat: new thing",
            "author": "dev2",
            "date": "2026-05-03T11:00:00Z",
        },
    ]
    return client


def _make_run(run_id, date_str, status="failure", sha=None):
    return {
        "run_id": run_id,
        "repo": "valkey-io/valkey",
        "workflow_file": "daily.yml",
        "status": status,
        "branch": "unstable",
        "commit_sha": sha or f"sha{run_id}",
        "run_date": f"{date_str}T06:00:00Z",
    }


def _make_job(job_id, run_id, conclusion="failure"):
    return {
        "job_id": job_id,
        "name": f"job-{job_id}",
        "status": "completed",
        "conclusion": conclusion,
    }


class TestComputeBlame:
    def test_empty_cache(self, cache, mock_client):
        result = compute_blame(cache, mock_client, days=14)
        assert result == []

    def test_green_to_red_transition(self, cache, mock_client):
        """Test that a green→red transition identifies blame commits."""
        # Day 1: passing
        cache.store_runs([_make_run(100, "2026-05-01", "success", "aaa")])
        cache.store_jobs(100, [_make_job(200, 100, "success")])

        # Day 2: passing
        cache.store_runs([_make_run(101, "2026-05-02", "success", "bbb")])
        cache.store_jobs(101, [_make_job(201, 101, "success")])

        # Day 3: failing with a new test failure
        cache.store_runs([_make_run(102, "2026-05-03", "failure", "ccc")])
        cache.store_jobs(102, [_make_job(202, 102)])
        cache.store_failures(
            202,
            [
                {
                    "test_name": "flaky test in tests/unit/foo.tcl",
                    "error_summary": "assertion failed",
                    "log_lines": "x",
                }
            ],
        )

        result = compute_blame(cache, mock_client, days=30)
        assert len(result) == 1

        rec = result[0]
        assert rec["test_name"] == "flaky test in tests/unit/foo.tcl"
        assert rec["regression_date"] == "2026-05-03"
        assert rec["last_pass_date"] == "2026-05-02"
        assert rec["last_pass_sha"] == "bbb"
        assert rec["first_fail_sha"] == "ccc"
        assert rec["commit_count"] == 2
        mock_client.compare_commits.assert_called_once_with("bbb", "ccc")

    def test_already_failing_at_window_start(self, cache, mock_client):
        """Test that tests failing from the start get a note."""
        cache.store_runs([_make_run(100, "2026-05-01", "failure", "aaa")])
        cache.store_jobs(100, [_make_job(200, 100)])
        cache.store_failures(
            200,
            [
                {
                    "test_name": "old_flaky",
                    "error_summary": "err",
                    "log_lines": "x",
                }
            ],
        )

        result = compute_blame(cache, mock_client, days=30)
        assert len(result) == 1
        assert result[0]["last_pass_sha"] is None
        assert "extend --days" in result[0]["note"]
        mock_client.compare_commits.assert_not_called()

    def test_multiple_tests_different_regression_dates(self, cache, mock_client):
        """Tests regressing on different days are sorted newest first."""
        # Day 1: all green
        cache.store_runs([_make_run(100, "2026-05-01", "success", "a1")])
        cache.store_jobs(100, [_make_job(200, 100, "success")])

        # Day 2: test_A starts failing
        cache.store_runs([_make_run(101, "2026-05-02", "failure", "a2")])
        cache.store_jobs(101, [_make_job(201, 101)])
        cache.store_failures(
            201,
            [
                {
                    "test_name": "test_A",
                    "error_summary": "err",
                    "log_lines": "x",
                }
            ],
        )

        # Day 3: test_B also starts failing
        cache.store_runs([_make_run(102, "2026-05-03", "failure", "a3")])
        cache.store_jobs(102, [_make_job(202, 102)])
        cache.store_failures(
            202,
            [
                {"test_name": "test_A", "error_summary": "err", "log_lines": "x"},
                {"test_name": "test_B", "error_summary": "err", "log_lines": "x"},
            ],
        )

        result = compute_blame(cache, mock_client, days=30)
        assert len(result) == 2
        # Newest regression first
        assert result[0]["test_name"] == "test_B"
        assert result[0]["regression_date"] == "2026-05-03"
        assert result[1]["test_name"] == "test_A"
        assert result[1]["regression_date"] == "2026-05-02"

    def test_api_error_gracefully_handled(self, cache, mock_client):
        """If compare_commits fails, blame_commits is empty."""
        mock_client.compare_commits.side_effect = Exception("API error")

        cache.store_runs([_make_run(100, "2026-05-01", "success", "aaa")])
        cache.store_jobs(100, [_make_job(200, 100, "success")])
        cache.store_runs([_make_run(101, "2026-05-02", "failure", "bbb")])
        cache.store_jobs(101, [_make_job(201, 101)])
        cache.store_failures(
            201,
            [
                {
                    "test_name": "test_X",
                    "error_summary": "err",
                    "log_lines": "x",
                }
            ],
        )

        result = compute_blame(cache, mock_client, days=30)
        assert len(result) == 1
        assert result[0]["blame_commits"] == []
        assert result[0]["commit_count"] == 0
