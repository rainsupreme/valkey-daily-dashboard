"""Tests for the scorecard module."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from valkey_oncall.cache import Cache
from valkey_oncall.scorecard import (
    _classify,
    _extract_category,
    _trend,
    compute_scorecards,
)

# Anchor test runs to a recent window so they fall inside compute_scorecards'
# rolling `days` window regardless of the calendar date the suite runs on.
_BASE = datetime.now(timezone.utc) - timedelta(days=15)

# --- Unit tests for helpers ---


class TestClassify:
    def test_persistent(self):
        assert _classify(0.5) == "persistent"  # boundary
        assert _classify(0.8) == "persistent"
        assert _classify(1.0) == "persistent"

    def test_flaky(self):
        assert _classify(0.01) == "flaky"  # 100-run-bar boundary
        assert _classify(0.1) == "flaky"
        assert _classify(0.49) == "flaky"

    def test_rare(self):
        assert _classify(0.009) == "rare"
        assert _classify(0.0) == "rare"


class TestTrend:
    def test_insufficient_data(self):
        assert _trend([]) == 0.0
        assert _trend([1]) == 0.0
        assert _trend([1, 2]) == 0.0

    def test_increasing(self):
        # Clearly increasing: 0, 1, 2, 3
        slope = _trend([0, 1, 2, 3])
        assert slope > 0

    def test_decreasing(self):
        slope = _trend([3, 2, 1, 0])
        assert slope < 0

    def test_flat(self):
        assert _trend([1, 1, 1, 1]) == 0.0


class TestExtractCategory:
    def test_unit(self):
        assert _extract_category("some test in tests/unit/foo.tcl") == "unit"

    def test_cluster(self):
        assert _extract_category("some test in tests/cluster/bar.tcl") == "cluster"

    def test_sentinel(self):
        assert _extract_category("some test in tests/sentinel/baz.tcl") == "sentinel"

    def test_gtest(self):
        assert _extract_category("GTest FAILED: SomeTest.Case") == "unit"

    def test_sentinel_keyword(self):
        assert _extract_category("Sentinel failover test") == "sentinel"

    def test_other(self):
        assert _extract_category("random test name") == "other"


# --- Integration tests with real Cache ---


@pytest.fixture
def cache(tmp_path):
    return Cache(str(tmp_path / "test.db"))


def _make_run(run_id, date_str, status="failure"):
    return {
        "run_id": run_id,
        "repo": "valkey-io/valkey",
        "workflow_file": "daily.yml",
        "status": status,
        "branch": "unstable",
        "commit_sha": f"abc{run_id}",
        "run_date": f"{date_str}T06:00:00Z",
    }


def _make_job(job_id, run_id, conclusion="failure"):
    return {
        "job_id": job_id,
        "name": f"test-job-{job_id}",
        "status": "completed",
        "conclusion": conclusion,
    }


class TestComputeScorecards:
    def test_empty_cache(self, cache):
        result = compute_scorecards(cache, days=14)
        assert result["meta"]["total_runs"] == 0
        assert result["scorecards"] == []

    def test_basic_scorecard(self, cache):
        # Create 5 runs over 5 days, test fails on 3 of them
        base = _BASE
        for i in range(5):
            date = (base + timedelta(days=i)).strftime("%Y-%m-%d")
            run = _make_run(100 + i, date, "failure" if i < 3 else "success")
            cache.store_runs([run])
            if i < 3:
                job = _make_job(200 + i, 100 + i)
                cache.store_jobs(100 + i, [job])
                cache.store_failures(
                    200 + i,
                    [
                        {
                            "test_name": "flaky test in tests/unit/foo.tcl",
                            "error_summary": "assertion failed",
                            "log_lines": "line1\nline2",
                        }
                    ],
                )
            else:
                # Passing run still needs jobs stored for completeness
                cache.store_jobs(100 + i, [_make_job(200 + i, 100 + i, "success")])

        result = compute_scorecards(cache, days=30)
        assert result["meta"]["total_runs"] == 5
        assert len(result["scorecards"]) == 1

        sc = result["scorecards"][0]
        assert sc["test_name"] == "flaky test in tests/unit/foo.tcl"
        assert sc["category"] == "unit"
        assert sc["days_failed"] == 3
        assert sc["total_hits"] == 3
        assert sc["total_runs"] == 5
        assert sc["failure_rate"] == 0.6
        assert sc["classification"] == "flaky"
        assert len(sc["daily_series"]) == 5

    def test_multiple_tests_sorted_by_rate(self, cache):
        base = _BASE
        for i in range(10):
            date = (base + timedelta(days=i)).strftime("%Y-%m-%d")
            cache.store_runs([_make_run(100 + i, date)])
            cache.store_jobs(100 + i, [_make_job(200 + i, 100 + i)])

            failures = []
            # "always_fails" fails every day
            failures.append(
                {
                    "test_name": "always_fails in tests/cluster/x.tcl",
                    "error_summary": "err",
                    "log_lines": "x",
                }
            )
            # "sometimes_fails" fails on even days only
            if i % 2 == 0:
                failures.append(
                    {
                        "test_name": "sometimes_fails in tests/unit/y.tcl",
                        "error_summary": "err",
                        "log_lines": "x",
                    }
                )
            cache.store_failures(200 + i, failures)

        result = compute_scorecards(cache, days=30)
        assert len(result["scorecards"]) == 2
        # First should be the one with higher failure rate
        assert (
            result["scorecards"][0]["test_name"]
            == "always_fails in tests/cluster/x.tcl"
        )
        assert result["scorecards"][0]["failure_rate"] == 1.0
        assert result["scorecards"][0]["classification"] == "persistent"
        assert result["scorecards"][1]["failure_rate"] == 0.5

    def test_trend_positive_when_getting_worse(self, cache):
        base = _BASE
        for i in range(7):
            date = (base + timedelta(days=i)).strftime("%Y-%m-%d")
            cache.store_runs([_make_run(100 + i, date)])
            cache.store_jobs(100 + i, [_make_job(200 + i, 100 + i)])
            # Only fails on later days (getting worse)
            if i >= 4:
                cache.store_failures(
                    200 + i,
                    [
                        {
                            "test_name": "worsening in tests/unit/z.tcl",
                            "error_summary": "err",
                            "log_lines": "x",
                        }
                    ],
                )

        result = compute_scorecards(cache, days=30)
        assert len(result["scorecards"]) == 1
        assert result["scorecards"][0]["trend"] > 0

    def test_first_last_seen(self, cache):
        base = _BASE
        for i in range(5):
            date = (base + timedelta(days=i)).strftime("%Y-%m-%d")
            cache.store_runs([_make_run(100 + i, date)])
            cache.store_jobs(100 + i, [_make_job(200 + i, 100 + i)])
            # Fails only on days 1 and 3
            if i in (1, 3):
                cache.store_failures(
                    200 + i,
                    [
                        {
                            "test_name": "sporadic",
                            "error_summary": "err",
                            "log_lines": "x",
                        }
                    ],
                )

        result = compute_scorecards(cache, days=30)
        sc = result["scorecards"][0]
        assert sc["first_seen"] == (base + timedelta(days=1)).strftime("%Y-%m-%d")
        assert sc["last_seen"] == (base + timedelta(days=3)).strftime("%Y-%m-%d")

    def test_json_serializable(self, cache):
        """Ensure the output is fully JSON-serializable."""
        base = _BASE
        for i in range(3):
            date = (base + timedelta(days=i)).strftime("%Y-%m-%d")
            cache.store_runs([_make_run(100 + i, date)])
            cache.store_jobs(100 + i, [_make_job(200 + i, 100 + i)])
            cache.store_failures(
                200 + i,
                [
                    {
                        "test_name": "test",
                        "error_summary": "err",
                        "log_lines": "x",
                    }
                ],
            )

        result = compute_scorecards(cache, days=30)
        # Should not raise
        serialized = json.dumps(result)
        assert "scorecards" in serialized
