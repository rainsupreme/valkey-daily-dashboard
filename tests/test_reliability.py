"""Tests for the reliability SLA inputs (valkey_oncall.reliability)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from valkey_oncall.cache import Cache
from valkey_oncall.flakes import FlakeEntry, FlakeRegistry
from valkey_oncall.reliability import (
    FLAKE_RATE_BUDGET,
    compute_flake_rates,
    compute_run_health,
    flake_rate_series,
)
from valkey_oncall.stats import rate_credible_interval

NOW = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
TEST_A = "Active defrag - AOF loading in tests/unit/memefficiency.tcl"
TEST_B = "COPY Preserves TTLs in tests/unit/hashexpire.tcl"


@pytest.fixture
def cache(tmp_path):
    return Cache(str(tmp_path / "test.db"))


def _day(offset: int) -> str:
    return (NOW - timedelta(days=offset)).strftime("%Y-%m-%d")


def _store_run(
    cache: Cache,
    run_id: int,
    day: str,
    jobs,
    status: str = "failure",
    event: str = "schedule",
    workflow: str = "daily.yml",
) -> None:
    """One run with *jobs*: ``(name, conclusion, [(test, summary), ...])``."""
    cache.store_runs(
        [
            {
                "run_id": run_id,
                "workflow_file": workflow,
                "status": status,
                "branch": "unstable",
                "commit_sha": f"sha{run_id}",
                "run_date": f"{day}T06:00:00Z",
                "raw_json": json.dumps({"event": event}),
            }
        ]
    )
    rows = []
    for i, (name, conclusion, _failures) in enumerate(jobs):
        rows.append(
            {
                "job_id": run_id * 100 + i,
                "name": name,
                "status": "completed",
                "conclusion": conclusion,
                "raw_json": "{}",
            }
        )
    cache.store_jobs(run_id, rows)
    for i, (_name, _conclusion, failures) in enumerate(jobs):
        if failures:
            cache.store_failures(
                run_id * 100 + i,
                [
                    {"test_name": t, "error_summary": s, "log_lines": "..."}
                    for t, s in failures
                ],
            )


class TestRateCredibleInterval:
    def test_no_data_is_whole_range(self):
        assert rate_credible_interval(0, 0) == (0.0, 1.0)

    def test_brackets_the_observed_rate(self):
        lo, hi = rate_credible_interval(3, 30)
        assert lo < 0.1 < hi
        assert 0.0 < lo and hi < 1.0

    def test_narrows_with_more_executions(self):
        lo1, hi1 = rate_credible_interval(1, 10)
        lo2, hi2 = rate_credible_interval(10, 100)
        assert hi2 - lo2 < hi1 - lo1


class TestQueryRunsEvent:
    def test_event_filter_reads_raw_json(self, cache):
        _store_run(cache, 1, _day(1), [("j", "success", [])], event="schedule")
        _store_run(cache, 2, _day(2), [("j", "success", [])], event="workflow_dispatch")
        runs = cache.query_runs(workflow="daily.yml", event="schedule")
        assert [r["run_id"] for r in runs] == [1]
        assert "raw_json" not in runs[0]

    def test_no_event_returns_all(self, cache):
        _store_run(cache, 1, _day(1), [("j", "success", [])], event="schedule")
        _store_run(cache, 2, _day(2), [("j", "success", [])], event="workflow_dispatch")
        assert len(cache.query_runs(workflow="daily.yml")) == 2


class TestComputeFlakeRates:
    def test_executions_count_settled_jobs_of_the_tests_jobs(self, cache):
        # Three runs. TEST_A fails in job-x on day 1 only; job-x ran three
        # times, job-y also ran but TEST_A was never seen there.
        for i, day in enumerate((_day(1), _day(2), _day(3))):
            fails = [(TEST_A, "Expected 1 to be 0")] if i == 0 else []
            conclusion = "failure" if fails else "success"
            _store_run(
                cache,
                10 + i,
                day,
                [("job-x", conclusion, fails), ("job-y", "success", [])],
            )
        data = compute_flake_rates(cache, now=NOW)
        assert data["meta"]["runs"] == 3
        (row,) = data["tests"]
        assert row["test_name"] == TEST_A
        assert row["fails"] == 1
        assert row["executions"] == 3
        assert row["rate"] == pytest.approx(1 / 3, abs=1e-4)
        assert row["over_budget"] is True
        assert row["jobs"] == [
            {
                "job_name": "job-x",
                "fails": 1,
                "executions": 3,
                "rate": row["rate"],
                "rate_ci90": row["rate_ci90"],
            }
        ]
        lo, hi = row["rate_ci90"]
        assert lo < 1 / 3 < hi

    def test_cancelled_and_skipped_jobs_are_not_executions(self, cache):
        _store_run(cache, 1, _day(1), [("job-x", "failure", [(TEST_A, "e")])])
        _store_run(cache, 2, _day(2), [("job-x", "cancelled", [])], status="cancelled")
        _store_run(cache, 3, _day(3), [("job-x", "skipped", [])])
        (row,) = compute_flake_rates(cache, now=NOW)["tests"]
        assert row["executions"] == 1

    def test_per_job_rows_and_aggregate(self, cache):
        # TEST_A fails once in each of two jobs over two runs: 2 fails over
        # 4 executions (each job ran twice).
        _store_run(
            cache,
            1,
            _day(1),
            [("job-x", "failure", [(TEST_A, "e")]), ("job-y", "success", [])],
        )
        _store_run(
            cache,
            2,
            _day(2),
            [("job-x", "success", []), ("job-y", "failure", [(TEST_A, "e")])],
        )
        (row,) = compute_flake_rates(cache, now=NOW)["tests"]
        assert row["fails"] == 2
        assert row["executions"] == 4
        assert [j["job_name"] for j in row["jobs"]] == ["job-x", "job-y"]
        assert all(j["fails"] == 1 and j["executions"] == 2 for j in row["jobs"])

    def test_window_excludes_old_runs(self, cache):
        _store_run(cache, 1, _day(1), [("job-x", "failure", [(TEST_A, "e")])])
        _store_run(cache, 2, _day(40), [("job-x", "failure", [(TEST_B, "e")])])
        _store_run(cache, 3, _day(45), [("job-x", "success", [])])
        data = compute_flake_rates(cache, days=30, now=NOW)
        assert [t["test_name"] for t in data["tests"]] == [TEST_A]
        assert data["tests"][0]["executions"] == 1

    def test_event_filter_drops_dispatch_runs(self, cache):
        _store_run(cache, 1, _day(1), [("job-x", "failure", [(TEST_A, "e")])])
        _store_run(
            cache,
            2,
            _day(2),
            [("job-x", "success", [])],
            event="workflow_dispatch",
        )
        with_all = compute_flake_rates(cache, now=NOW)
        scheduled = compute_flake_rates(cache, now=NOW, event="schedule")
        assert with_all["tests"][0]["executions"] == 2
        assert scheduled["tests"][0]["executions"] == 1

    def test_budget_flag_and_counts(self, cache):
        # TEST_A: 1 fail in 200 executions (0.5%) stays under budget;
        # TEST_B: 3 of 200 is over.
        for i in range(200):
            fails = []
            if i == 0:
                fails.append((TEST_A, "e"))
            if i < 3:
                fails.append((TEST_B, "e"))
            _store_run(
                cache,
                100 + i,
                _day(1),
                [("job-x", "failure" if fails else "success", fails)],
            )
        data = compute_flake_rates(cache, now=NOW)
        by_name = {t["test_name"]: t for t in data["tests"]}
        assert by_name[TEST_A]["over_budget"] is False
        assert by_name[TEST_A]["rate"] < FLAKE_RATE_BUDGET
        assert by_name[TEST_B]["over_budget"] is True
        assert data["tests_over_budget"] == 1
        assert data["tests"][0]["test_name"] == TEST_B  # worst first

    def test_confident_over_budget_needs_a_narrow_interval(self, cache):
        # TEST_A fails once in 30 executions (3.3%): over budget by point
        # estimate, but the interval reaches below 1%. TEST_B fails 4 times:
        # the whole interval is above 1%.
        for i in range(30):
            fails = []
            if i == 0:
                fails.append((TEST_A, "e"))
            if i < 4:
                fails.append((TEST_B, "e"))
            _store_run(
                cache,
                100 + i,
                _day(1),
                [("job-x", "failure" if fails else "success", fails)],
            )
        data = compute_flake_rates(cache, now=NOW)
        by_name = {t["test_name"]: t for t in data["tests"]}
        assert by_name[TEST_A]["over_budget"] is True
        assert by_name[TEST_A]["over_budget_confident"] is False
        assert by_name[TEST_B]["over_budget_confident"] is True
        assert data["tests_over_budget"] == 2
        assert data["tests_over_budget_confident"] == 1

    def test_tracked_requires_exact_registry_match(self, cache):
        _store_run(
            cache,
            1,
            _day(1),
            [
                (
                    "job-x",
                    "failure",
                    [(TEST_A, "Expected '1' to be '0'"), (TEST_B, "boom")],
                )
            ],
        )
        registry = FlakeRegistry(
            [
                FlakeEntry(test_name=TEST_A, summary="Expected '9' to be '0'", issue=1),
                FlakeEntry(test_name=TEST_B, summary="other mode", issue=2),
            ]
        )
        data = compute_flake_rates(cache, now=NOW, registry=registry)
        by_name = {t["test_name"]: t for t in data["tests"]}
        # Quoted numbers are normalized away, so A's mode matches exactly.
        assert by_name[TEST_A]["tracked"] is True
        # B's registered mode differs from what was seen: test-only match.
        assert by_name[TEST_B]["tracked"] is False
        assert data["tests_tracked"] == 1

    def test_junk_names_are_dropped(self, cache):
        _store_run(cache, 1, _day(1), [("job-x", "failure", [("pid:12345", "e")])])
        assert compute_flake_rates(cache, now=NOW)["tests"] == []

    def test_empty_cache(self, cache):
        data = compute_flake_rates(cache, now=NOW)
        assert data["tests"] == []
        assert data["tests_over_budget"] == 0
        assert data["meta"]["runs"] == 0


class TestFlakeRateSeries:
    def test_one_point_per_run_day_with_rolling_window(self, cache):
        # TEST_A fails on day 50 only; it is over budget for the 30 days
        # that follow and gone afterwards.
        for off in (50, 40, 30, 10):
            fails = [(TEST_A, "e")] if off == 50 else []
            _store_run(
                cache,
                off,
                _day(off),
                [("job-x", "failure" if fails else "success", fails)],
            )
        series = flake_rate_series(cache, days=60, window_days=30, now=NOW)
        by_date = {p["date"]: p for p in series}
        assert list(by_date) == [_day(50), _day(40), _day(30), _day(10)]
        assert by_date[_day(50)]["tests_over_budget"] == 1
        assert by_date[_day(50)]["tests_over_budget_confident"] == 1  # 1 of 1
        assert by_date[_day(40)]["tests_over_budget"] == 1
        assert by_date[_day(30)]["runs"] == 3
        # Day 50 is exactly 30 days before day 20's window start: excluded.
        assert by_date[_day(10)]["tests_over_budget"] == 0
        assert by_date[_day(10)]["failing_tests"] == 0

    def test_empty(self, cache):
        assert flake_rate_series(cache, now=NOW) == []


class TestComputeRunHealth:
    def _seed(self, cache, registry_summary="Expected '1' to be '0'"):
        """Four scheduled runs, oldest first:

        day 4: fully green (status success, both jobs ok).
        day 3: one failed job, its only failure is the tracked flake.
        day 2: two failed jobs: one tracked flake, one untracked test.
        day 1: cancelled run with one cancelled job.
        """
        _store_run(
            cache,
            4,
            _day(4),
            [("job-x", "success", []), ("job-y", "success", [])],
            status="success",
        )
        _store_run(
            cache,
            3,
            _day(3),
            [
                ("job-x", "failure", [(TEST_A, registry_summary)]),
                ("job-y", "success", []),
            ],
        )
        _store_run(
            cache,
            2,
            _day(2),
            [
                ("job-x", "failure", [(TEST_A, registry_summary)]),
                ("job-y", "failure", [(TEST_B, "boom")]),
            ],
        )
        _store_run(
            cache,
            1,
            _day(1),
            [("job-x", "success", []), ("job-y", "cancelled", [])],
            status="cancelled",
        )
        return FlakeRegistry(
            [FlakeEntry(test_name=TEST_A, summary=registry_summary, issue=1)]
        )

    def test_per_run_fields(self, cache):
        registry = self._seed(cache)
        data = compute_run_health(cache, registry, now=NOW, event="schedule")
        runs = data["runs"]
        assert [r["date"] for r in runs] == [_day(4), _day(3), _day(2), _day(1)]

        green, tracked, mixed, cancelled = runs
        assert green["fully_green"] is True
        assert green["failed_jobs"] == 0
        assert green["green_share"] == 1.0
        assert green["green_if_tracked_fixed"] is True

        assert tracked["fully_green"] is False
        assert tracked["failed_jobs"] == 1
        assert tracked["green_share"] == 0.5
        assert tracked["unexplained_failed_jobs"] == []
        assert tracked["green_if_tracked_fixed"] is True

        assert mixed["failed_jobs"] == 2
        assert mixed["failed_job_names"] == ["job-x", "job-y"]
        assert mixed["unexplained_failed_jobs"] == ["job-y"]
        assert mixed["unexplained_failed_job_count"] == 1
        assert mixed["green_if_tracked_fixed"] is False

        assert cancelled["fully_green"] is False
        assert cancelled["cancelled_jobs"] == 1
        assert cancelled["cancelled_job_names"] == ["job-y"]
        assert cancelled["failed_jobs"] == 0
        # A cancelled run has no unexplained failed job but is still not green.
        assert cancelled["green_if_tracked_fixed"] is False

    def test_failed_job_without_parsed_rows_is_unexplained(self, cache):
        registry = FlakeRegistry([FlakeEntry(test_name=TEST_A, issue=1)])
        _store_run(cache, 1, _day(1), [("job-x", "failure", [])])
        (run,) = compute_run_health(cache, registry, now=NOW)["runs"]
        assert run["unexplained_failed_jobs"] == ["job-x"]

    def test_without_registry_nothing_is_explained(self, cache):
        self._seed(cache)
        data = compute_run_health(cache, None, now=NOW, event="schedule")
        assert data["runs"][1]["unexplained_failed_jobs"] == ["job-x"]
        assert data["meta"]["registry_entries"] == 0

    def test_rolling_windows(self, cache):
        registry = self._seed(cache)
        data = compute_run_health(cache, registry, now=NOW, event="schedule")
        r30 = data["rolling30"]
        assert [p["runs"] for p in r30] == [1, 2, 3, 4]
        last = r30[-1]
        assert last["green_runs"] == 1
        assert last["green_rate"] == 0.25
        assert last["green_if_fixed_rate"] == 0.5  # days 4 and 3
        assert last["mean_failed_jobs"] == 0.75  # 0 + 1 + 2 + 0
        assert last["median_failed_jobs"] == 0.5
        assert last["mean_unexplained_failed_jobs"] == 0.25
        # green shares: 1.0, 0.5, 0.0, 1.0 (cancelled run: one settled ok job)
        assert last["mean_green_share"] == pytest.approx(0.625)

        r7 = data["rolling7"]
        assert [p["runs"] for p in r7] == [1, 2, 3, 4]
        assert r7[0]["green_rate"] == 1.0

    def test_rolling_window_length(self, cache):
        for i in range(10):
            _store_run(
                cache, i + 1, _day(i + 1), [("job-x", "success", [])], status="success"
            )
        data = compute_run_health(cache, None, now=NOW)
        assert [p["runs"] for p in data["rolling7"]][-1] == 7
        assert [p["runs"] for p in data["rolling30"]][-1] == 10

    def test_event_filter_and_cancelled_exclusion(self, cache):
        self._seed(cache)
        _store_run(
            cache,
            9,
            _day(5),
            [("job-x", "success", [])],
            status="success",
            event="workflow_dispatch",
        )
        scheduled = compute_run_health(cache, None, now=NOW, event="schedule")
        assert len(scheduled["runs"]) == 4
        everything = compute_run_health(cache, None, now=NOW)
        assert len(everything["runs"]) == 5
        no_cancel = compute_run_health(
            cache, None, now=NOW, event="schedule", include_cancelled=False
        )
        assert [r["status"] for r in no_cancel["runs"]] == [
            "success",
            "failure",
            "failure",
        ]

    def test_empty(self, cache):
        data = compute_run_health(cache, None, now=NOW)
        assert data["runs"] == []
        assert data["rolling7"] == [] and data["rolling30"] == []
