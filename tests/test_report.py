"""Tests for report data generation (scorecard leaderboard wiring)."""

from datetime import datetime, timedelta, timezone

import pytest

from valkey_oncall.cache import Cache
from valkey_oncall.report import generate_report_data, render_html


@pytest.fixture
def cache(tmp_path):
    return Cache(str(tmp_path / "test.db"))


def _day(offset):
    return (datetime.now(timezone.utc) - timedelta(days=offset)).strftime("%Y-%m-%d")


def _make_run(run_id, date_str, status="failure"):
    return {
        "run_id": run_id,
        "repo": "valkey-io/valkey",
        "workflow_file": "daily.yml",
        "status": status,
        "branch": "unstable",
        "commit_sha": f"sha{run_id}",
        "run_date": f"{date_str}T06:00:00Z",
    }


def _make_job(job_id, run_id):
    return {
        "job_id": job_id,
        "name": f"job-{job_id}",
        "status": "completed",
        "conclusion": "failure",
    }


def _store_failure(cache, run_id, job_id, date, test_name):
    cache.store_runs([_make_run(run_id, date)])
    cache.store_jobs(run_id, [_make_job(job_id, run_id)])
    cache.store_failures(
        job_id,
        [{"test_name": test_name, "error_summary": "err", "log_lines": "x"}],
    )


class TestScorecardWiring:
    def test_report_data_includes_scorecard_block(self, cache):
        _store_failure(cache, 100, 200, _day(2), "recent in tests/unit/b.tcl")
        data = generate_report_data(cache, days=14)
        assert "scorecard" in data
        assert "scorecards" in data["scorecard"]

    def test_test_outside_recent_window_still_in_scorecard(self, cache):
        # Failed 25 days ago: outside the 14-day heatmap, inside the 90-day roster.
        _store_failure(cache, 100, 200, _day(25), "old_flaky in tests/unit/a.tcl")
        # A recent failure so the heatmap window is non-empty.
        _store_failure(cache, 101, 201, _day(2), "recent in tests/unit/b.tcl")

        data = generate_report_data(cache, days=14)

        # Heatmap (recent) does NOT show the old test...
        assert "old_flaky in tests/unit/a.tcl" not in data["tests"]
        assert "recent in tests/unit/b.tcl" in data["tests"]

        # ...but the 90-day scorecard leaderboard does.
        roster = {s["test_name"] for s in data["scorecard"]["scorecards"]}
        assert "old_flaky in tests/unit/a.tcl" in roster
        assert "recent in tests/unit/b.tcl" in roster

    def test_scorecard_carries_analytics_fields(self, cache):
        _store_failure(cache, 100, 200, _day(3), "some test in tests/unit/c.tcl")
        data = generate_report_data(cache, days=14)
        sc = data["scorecard"]["scorecards"][0]
        for field in (
            "classification",
            "trend",
            "category",
            "first_seen",
            "daily_series",
        ):
            assert field in sc

    def test_scorecard_drops_junk_names(self, cache):
        _store_failure(cache, 100, 200, _day(3), "pid:99999")
        data = generate_report_data(cache, days=14)
        roster = {s["test_name"] for s in data["scorecard"]["scorecards"]}
        assert "pid:99999" not in roster


class TestTabLayout:
    def test_render_has_tabs_and_panels(self, cache):
        _store_failure(cache, 100, 200, _day(2), "recent in tests/unit/b.tcl")
        html = render_html(generate_report_data(cache, days=14))

        # Four tab buttons.
        for attr in (
            'data-tab="heatmap"',
            'data-tab="scorecard"',
            'data-tab="rundetails"',
            'data-tab="regressions"',
        ):
            assert attr in html
        # Four panels, and the heatmap is active by default.
        for pid in (
            'id="tab-heatmap"',
            'id="tab-scorecard"',
            'id="tab-rundetails"',
            'id="tab-regressions"',
        ):
            assert pid in html
        assert 'class="tab-panel active" id="tab-heatmap"' in html
        # Exactly four panels (balanced open/close with the four sections).
        assert html.count('class="tab-panel') == 4


class TestResolvedSubList:
    def test_resolved_test_goes_to_collapsed_block(self, cache):
        from valkey_oncall.scorecard import RESOLVED_QUIET_RUNS

        n = RESOLVED_QUIET_RUNS + 3
        # An old test that failed once long ago (now resolved/quiet)...
        for i in range(n):
            date = (datetime.now(timezone.utc) - timedelta(days=n - i)).strftime(
                "%Y-%m-%d"
            )
            cache.store_runs([_make_run(300 + i, date)])
            cache.store_jobs(300 + i, [_make_job(400 + i, 300 + i)])
            if i == 0:
                cache.store_failures(
                    400 + i,
                    [
                        {
                            "test_name": "fixed in tests/unit/old.tcl",
                            "error_summary": "e",
                            "log_lines": "x",
                        }
                    ],
                )
        # ...and a fresh failure today (active).
        _store_failure(cache, 100, 200, _day(0), "live in tests/unit/new.tcl")

        html = render_html(generate_report_data(cache, days=90))

        assert '<details class="resolved-block"' in html
        assert "Resolved / quiet (1)" in html
        block = html.index('<details class="resolved-block"')
        body = html.index('id="scorecard-body"')
        assert body < block
        active_region = html[body:block]
        resolved_region = html[block:]
        # Active test is in the main scorecard body, not the resolved block.
        assert "live in tests/unit/new.tcl" in active_region
        assert "fixed in tests/unit/old.tcl" not in active_region
        # The presumed-fixed test lives in the resolved block.
        assert "fixed in tests/unit/old.tcl" in resolved_region

    def test_no_resolved_block_when_all_active(self, cache):
        _store_failure(cache, 100, 200, _day(1), "live in tests/unit/new.tcl")
        html = render_html(generate_report_data(cache, days=90))
        assert 'class="resolved-block"' not in html


class TestRegressions:
    def _green_run(self, cache, run_id, job_id, date, sha):
        cache.store_runs(
            [
                {
                    "run_id": run_id,
                    "repo": "valkey-io/valkey",
                    "workflow_file": "daily.yml",
                    "status": "success",
                    "branch": "unstable",
                    "commit_sha": sha,
                    "run_date": f"{date}T06:00:00Z",
                }
            ]
        )
        cache.store_jobs(
            run_id,
            [
                {
                    "job_id": job_id,
                    "name": "j",
                    "status": "completed",
                    "conclusion": "success",
                }
            ],
        )

    def test_regression_detected_with_compare_link(self, cache):
        # Green run (older), then a red run (newer) with a failing test.
        self._green_run(cache, 500, 600, _day(3), "aaaaaaa1111")
        _store_failure(
            cache, 501, 601, _day(2), "flap in tests/unit/x.tcl"
        )  # sha = sha501

        data = generate_report_data(cache, days=14)
        assert "regressions" in data
        assert any(r["test_name"].startswith("flap") for r in data["regressions"])

        html = render_html(data)
        reg = html[html.index('id="tab-regressions"') :]
        # Permission-free compare link between last-green and first-red SHAs.
        assert "compare/aaaaaaa1111...sha501" in reg
        assert "flap in tests/unit/x.tcl" in reg
        # High confidence (failed every run since onset).
        assert "high" in reg

    def test_run_detail_has_compare_link_without_client(self, cache):
        # Two consecutive failing runs with different SHAs -> compare link.
        self._green_run(cache, 500, 600, _day(3), "aaaaaaa1111")
        _store_failure(cache, 501, 601, _day(2), "flap in tests/unit/x.tcl")
        html = render_html(generate_report_data(cache, days=14))
        # No client passed, yet the Run Details column links the diff.
        assert "compare/aaaaaaa1111...sha501" in html

    def test_legend_reflects_threshold_constants(self, cache):
        from valkey_oncall.scorecard import (
            COOLING_QUIET_RUNS,
            PERSISTENT_STREAK_DAYS,
            RESOLVED_QUIET_RUNS,
        )

        # Thresholds show in the legend regardless of whether anything resolved.
        _store_failure(cache, 100, 200, _day(1), "live in tests/unit/new.tcl")
        html = render_html(generate_report_data(cache, days=90))
        assert f"last {PERSISTENT_STREAK_DAYS} runs straight" in html
        assert f"last {COOLING_QUIET_RUNS}+ runs" in html
        assert f"{RESOLVED_QUIET_RUNS}+ runs drop to" in html
