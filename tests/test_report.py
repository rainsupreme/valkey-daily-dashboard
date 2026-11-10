"""Tests for report data generation (scorecard leaderboard wiring)."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from valkey_oncall.cache import Cache
from valkey_oncall.report import generate_report_data, render_html, stale_reason


@pytest.fixture
def cache(tmp_path):
    return Cache(str(tmp_path / "test.db"))


def _day(offset):
    return (datetime.now(timezone.utc) - timedelta(days=offset)).strftime("%Y-%m-%d")


def _make_run(run_id, date_str, status="failure", event="schedule"):
    return {
        "run_id": run_id,
        "repo": "valkey-io/valkey",
        "workflow_file": "daily.yml",
        "status": status,
        "branch": "unstable",
        "commit_sha": f"sha{run_id}",
        "run_date": f"{date_str}T06:00:00Z",
        "raw_json": json.dumps({"event": event}),
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

        # Five tab buttons.
        for attr in (
            'data-tab="heatmap"',
            'data-tab="sla"',
            'data-tab="scorecard"',
            'data-tab="rundetails"',
            'data-tab="regressions"',
        ):
            assert attr in html
        # Five panels, and the heatmap is active by default.
        for pid in (
            'id="tab-heatmap"',
            'id="tab-sla"',
            'id="tab-scorecard"',
            'id="tab-rundetails"',
            'id="tab-regressions"',
        ):
            assert pid in html
        assert 'class="tab-panel active" id="tab-heatmap"' in html
        # Exactly five panels (balanced open/close with the five sections).
        assert html.count('class="tab-panel') == 5


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
                    "raw_json": '{"event": "schedule"}',
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
        # A confidence badge (data-conf) is rendered for the regression row,
        # and the record carries the prior-aware fields.
        assert "data-conf=" in reg
        rec = next(r for r in data["regressions"] if r["test_name"].startswith("flap"))
        assert rec["confidence"] in ("high", "medium", "low", "unknown")
        assert "p0_hat" in rec and "burst_p" in rec

    def test_methodology_note_present(self, cache):
        self._green_run(cache, 500, 600, _day(3), "aaaaaaa1111")
        _store_failure(cache, 501, 601, _day(2), "flap in tests/unit/x.tcl")
        reg = render_html(generate_report_data(cache, days=14))
        reg = reg[reg.index('id="tab-regressions"') :]
        assert "How this works" in reg
        assert ">Baseline<" in reg  # column header
        # Wikipedia references for the math, opening in a new tab.
        assert "en.wikipedia.org/wiki/Beta-binomial_distribution" in reg
        assert "en.wikipedia.org/wiki/Bayesian_inference" in reg
        assert 'target="_blank"' in reg
        assert ">Surprise<" in reg  # column renamed from Confidence

    def test_active_tab_encoded_in_url_hash(self):
        from valkey_oncall.report import _asset

        js = _asset("report.js")
        assert "hashchange" in js
        assert "replaceState" in js or "location.hash" in js

    def test_ongoing_vs_fixed_split_and_surprise_pct(self, cache):
        from valkey_oncall.blame import REGRESSION_ONGOING_QUIET_RUNS as q

        # NOTE: _day(offset) = offset days AGO, so offset 0 is the newest run.
        # ONGOING: green yesterday, fails today (quiet 0 runs).
        self._green_run(cache, 700, 9700, _day(1), "og")
        _store_failure(cache, 701, 9701, _day(0), "livebreak in tests/unit/b.tcl")
        # FIXED: fails q+3 days ago, then clean on every more-recent run.
        self._green_run(cache, 702, 9702, _day(q + 4), "gf")
        _store_failure(cache, 703, 9703, _day(q + 3), "oldbreak in tests/unit/a.tcl")
        rid = 704
        for off in range(2, q + 3):  # q+1 clean days newer than the failure
            self._green_run(cache, rid, rid + 9000, _day(off), f"c{off}")
            rid += 1

        html = render_html(generate_report_data(cache, days=90))
        reg = html[html.index('id="tab-regressions"') :]
        assert '<details class="resolved-block"' in reg
        assert "Likely fixed" in reg
        block = reg.index('<details class="resolved-block"')
        # ongoing sits in the main table (above the fixed block)...
        assert reg.index("livebreak in tests/unit/b.tcl") < block
        # ...and the fixed one sits inside the collapsed block.
        assert reg.index("oldbreak in tests/unit/a.tcl") > block
        # confidence badge renders a surprise percentage.
        assert "%</span>" in reg

    def test_run_detail_has_compare_link_without_client(self, cache):
        # Two consecutive failing runs with different SHAs -> compare link.
        self._green_run(cache, 500, 600, _day(3), "aaaaaaa1111")
        _store_failure(cache, 501, 601, _day(2), "flap in tests/unit/x.tcl")
        html = render_html(generate_report_data(cache, days=14))
        # No client passed, yet the Run Details column links the diff.
        assert "compare/aaaaaaa1111...sha501" in html

    def test_commit_fetch_failure_is_logged(self, cache, caplog):
        import logging

        self._green_run(cache, 500, 600, _day(3), "aaaaaaa1111")
        _store_failure(cache, 501, 601, _day(2), "flap in tests/unit/x.tcl")

        class BoomClient:
            def compare_commits(self, base, head):
                raise RuntimeError("403 Forbidden")

            def get_commit(self, sha):
                raise RuntimeError("403 Forbidden")

        with caplog.at_level(logging.WARNING):
            generate_report_data(cache, client=BoomClient(), days=14)
        assert any("compare_commits" in r.message for r in caplog.records)

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


class TestStaleReason:
    """Freshness guard: stale_reason() drives the workflow's fail-loud check."""

    def test_none_when_fresh_today(self):
        assert stale_reason(_day(0)) is None

    def test_none_within_tolerance(self):
        # 2 days old is within the default MAX_RUN_AGE_DAYS tolerance.
        assert stale_reason(_day(2)) is None

    def test_stale_when_too_old(self):
        reason = stale_reason(_day(5))
        assert reason is not None
        assert "days old" in reason

    def test_missing_date_is_stale(self):
        reason = stale_reason(None)
        assert reason is not None
        assert "no runs" in reason

    def test_unparseable_date_is_stale(self):
        reason = stale_reason("not-a-date")
        assert reason is not None
        assert "unparseable" in reason

    def test_custom_now_and_threshold(self):
        now = datetime(2026, 7, 10, tzinfo=timezone.utc)
        # 3 days old with a 1-day tolerance -> stale.
        assert stale_reason("2026-07-07", now=now, max_age_days=1) is not None
        # Same date with a 5-day tolerance -> fresh.
        assert stale_reason("2026-07-07", now=now, max_age_days=5) is None


class TestLatestRunDate:
    """generate_report_data exposes the newest run date for the guard."""

    def test_latest_run_date_is_newest(self, cache):
        cache.store_runs(
            [
                _make_run(1, _day(3), status="success"),
                _make_run(2, _day(1), status="success"),
            ]
        )
        data = generate_report_data(cache, days=14)
        assert data["summary"]["latest_run_date"] == _day(1)

    def test_latest_run_date_none_when_empty(self, cache):
        data = generate_report_data(cache, days=14)
        assert data["summary"]["latest_run_date"] is None


class TestRegressionWarnings:
    """Heatmap ⚠️ marker for ongoing likely-regression rows."""

    def _rec(self, name, fails, total, ongoing=True):
        # onset at index 0; post-onset = fails failures then clean runs.
        series = [1] * fails + [0] * (total - fails)
        return {
            "test_name": name,
            "ongoing": ongoing,
            "daily_series": series,
            "onset_index": 0,
        }

    def test_effect_size_gate_matches_live_calibration(self):
        from valkey_oncall.report import _regression_warnings

        regs = [
            self._rec("dual", 3, 6),  # lower90 ~21% -> flag
            self._rec("assert", 2, 2),  # lower90 ~43% -> flag
            self._rec("defrag", 7, 66),  # lower90 ~5.6% -> flag (just over)
            self._rec("iothreads", 10, 88),  # lower90 ~6.7% -> flag
            self._rec("migration", 6, 75),  # lower90 <5% -> NO (cluster noise)
            self._rec("blip", 1, 1),  # < min failures -> NO
            self._rec("done", 5, 5, ongoing=False),  # not ongoing -> NO
        ]
        warn = _regression_warnings(regs)
        assert set(warn) == {"dual", "assert", "defrag", "iothreads"}
        # returns the lower bound, and it clears the meaningful-rate threshold
        assert warn["defrag"] >= 0.05
        assert warn["assert"] > warn["defrag"]

    def test_min_failures_floor(self):
        from valkey_oncall.report import _regression_warnings

        # A single failure on the most recent run must not flag, even though an
        # uninformative prior would push its lower bound high.
        assert _regression_warnings([self._rec("x", 1, 1)]) == {}

    def test_missing_series_skipped(self):
        from valkey_oncall.report import _regression_warnings

        assert _regression_warnings([{"test_name": "x", "ongoing": True}]) == {}

    def test_gate_empty(self):
        from valkey_oncall.report import _regression_warnings

        assert _regression_warnings([]) == {}

    def test_marker_rendered_for_durable_regression(self, cache):
        # ~10 clean days establish a baseline, then 4 consecutive failing runs
        # produce a durable, ongoing regression (meaningful post-onset rate).
        tr = TestRegressions()
        for i, off in enumerate(range(14, 4, -1)):
            tr._green_run(cache, 700 + i, 800 + i, _day(off), f"sha7{i:02d}")
        for i, off in enumerate([3, 2, 1, 0]):
            _store_failure(
                cache, 720 + i, 820 + i, _day(off), "regr in tests/unit/y.tcl"
            )

        data = generate_report_data(cache, days=14)
        rec = next(r for r in data["regressions"] if r["test_name"].startswith("regr"))
        assert rec["ongoing"] is True

        html = render_html(data)
        heatmap = html[
            html.index('id="tab-heatmap"') : html.index('id="tab-scorecard"')
        ]
        # ⚠️ marker present in the heatmap, linking to the regressions tab.
        assert 'class="regwarn"' in heatmap
        assert 'href="#regressions"' in heatmap

    def test_no_marker_when_no_regressions(self, cache):
        # A one-off flake (single failing day) is not an ongoing regression.
        _store_failure(cache, 900, 1000, _day(1), "oneoff in tests/unit/z.tcl")
        data = generate_report_data(cache, days=14)
        html = render_html(data)
        heatmap = html[
            html.index('id="tab-heatmap"') : html.index('id="tab-scorecard"')
        ]
        assert 'class="regwarn"' not in heatmap


class TestSparklineOnset:
    """_sparkline draws an onset tick when mark_index is given."""

    def test_mark_draws_amber_tick(self):
        from valkey_oncall.report import _sparkline

        assert "#d29922" in _sparkline([0, 0, 1, 1], mark_index=2)

    def test_no_mark_no_tick(self):
        from valkey_oncall.report import _sparkline

        assert "#d29922" not in _sparkline([0, 0, 1, 1])

    def test_out_of_range_mark_ignored(self):
        from valkey_oncall.report import _sparkline

        assert "#d29922" not in _sparkline([0, 1], mark_index=9)

    def test_empty_series_empty_string(self):
        from valkey_oncall.report import _sparkline

        assert _sparkline([], mark_index=0) == ""


class TestRegressionSparklineRender:
    """The Regressions tab renders an onset sparkline per row."""

    def test_onset_column_and_spark_present(self, cache):
        tr = TestRegressions()
        for i, off in enumerate(range(14, 4, -1)):
            tr._green_run(cache, 600 + i, 700 + i, _day(off), f"sha6{i:02d}")
        for i, off in enumerate([3, 2, 1, 0]):
            _store_failure(
                cache, 620 + i, 720 + i, _day(off), "regr in tests/unit/w.tcl"
            )

        data = generate_report_data(cache, days=14)
        html = render_html(data)
        reg = html[html.index('id="tab-regressions"') :]
        assert ">Onset<" in reg  # new column header
        assert 'class="spark-cell"' in reg  # sparkline cell rendered
        assert "#d29922" in reg  # onset tick drawn


class TestHeatmapMethodologyNote:
    """The heatmap carries a methodology note explaining the ⚠️ gate."""

    def test_note_and_links_present(self, cache):
        _store_failure(cache, 900, 1000, _day(1), "x in tests/unit/z.tcl")
        html = render_html(generate_report_data(cache, days=14))
        heat = html[html.index('id="tab-heatmap"') : html.index('id="tab-scorecard"')]
        assert "What the ⚠️ means" in heat
        assert "en.wikipedia.org/wiki/Credible_interval" in heat
        assert "en.wikipedia.org/wiki/Effect_size" in heat
        assert "en.wikipedia.org/wiki/Beta_distribution" in heat
        assert 'target="_blank"' in heat
        # Injected gate constants (kept DRY via template vars).
        assert "90% credible interval" in heat
        assert "5%" in heat


class TestDualWorkflowRender:
    """CI (per-run) stacks on top of Daily across the tabs, with guardrails."""

    def _ci_cache(self, cache):
        base = datetime(2026, 7, 12, 6, 0, 0, tzinfo=timezone.utc)
        for i in range(20):
            ts = (base + timedelta(minutes=20 * i)).strftime("%Y-%m-%dT%H:%M:%SZ")
            fail = i >= 15
            cache.store_runs(
                [
                    {
                        "run_id": 500 + i,
                        "repo": "valkey-io/valkey",
                        "workflow_file": "ci.yml",
                        "status": "failure" if fail else "success",
                        "branch": "unstable",
                        "commit_sha": f"abc{i:03d}def",
                        "run_date": ts,
                    }
                ]
            )
            cache.store_jobs(
                500 + i,
                [
                    {
                        "job_id": 600 + i,
                        "name": "build",
                        "status": "completed",
                        "conclusion": "failure" if fail else "success",
                    }
                ],
            )
            if fail:
                cache.store_failures(
                    600 + i,
                    [
                        {
                            "test_name": "ci regr in tests/unit/b.tcl",
                            "error_summary": "e",
                            "log_lines": "x",
                        }
                    ],
                )

    def test_ci_on_top_and_guardrails(self, cache):
        _store_failure(cache, 1, 2, _day(0), "daily regr in tests/unit/a.tcl")
        daily = generate_report_data(cache, days=14)
        import tempfile

        ci_cache = Cache(tempfile.mktemp(suffix=".ci.db"))
        self._ci_cache(ci_cache)
        ci = generate_report_data(
            ci_cache, workflow="ci.yml", per_run=True, max_runs=50
        )

        html = render_html(daily, ci_data=ci)
        # CI heading appears before the Daily heading in the heatmap.
        assert 0 < html.find("CI · last") < html.find("Daily · nightly full suite")
        assert "Columns are merge runs (one per commit)" in html  # CI caption
        assert "Columns are days" in html  # Daily caption
        assert 'class="wf-daily"' in html  # Daily heatmap collapsed
        assert "CI · per-commit regressions" in html
        # Single interactive scorecard body (Daily) -> no JS id collision.
        assert html.count('id="scorecard-body"') == 1
        # Shared methodology notes, one each.
        assert html.count("What the ⚠️ means") == 1
        assert html.count("How this works") == 1
        assert html.count("<div") == html.count("</div>")
        assert html.count("<details") == html.count("</details>")

    def test_daily_only_render_unchanged(self, cache):
        _store_failure(cache, 1, 2, _day(0), "x in tests/unit/a.tcl")
        html = render_html(generate_report_data(cache, days=14))
        assert "Columns are days" in html
        assert "CI · " not in html
        assert 'class="wf-daily"' not in html


class TestFailureClusters:
    """Rows with identical failure-day signatures collapse into cluster rows."""

    def _store_many(self, cache, run_id, job_id, date, names):
        cache.store_runs([_make_run(run_id, date)])
        cache.store_jobs(run_id, [_make_job(job_id, run_id)])
        cache.store_failures(
            job_id,
            [{"test_name": n, "error_summary": "err", "log_lines": "x"} for n in names],
        )

    def test_five_plus_identical_rows_collapse(self, cache):
        names = [f"regr case {i} in tests/unit/a.tcl" for i in range(6)]
        # Same six tests fail on the same two days -> one signature, 6 members.
        self._store_many(cache, 100, 200, _day(3), names)
        self._store_many(cache, 101, 201, _day(2), names)
        # A test with a different signature stays a normal row.
        _store_failure(cache, 102, 202, _day(1), "unique in tests/unit/b.tcl")

        html = render_html(generate_report_data(cache, days=14))

        assert 'class="hm-cluster-toggle"' in html
        assert "6 tests · identical failure days" in html
        # All six member rows present but hidden by default.
        assert html.count('class="hm-cluster-member"') == 6
        assert html.count('class="hm-cluster-member" data-group="0"') == 6
        # Caption mentions the collapse.
        assert "collapsed into cluster rows" in html
        # The unique test is not part of any cluster.
        unique_pos = html.index("unique in tests/unit/b.tcl")
        row_start = html.rindex("<tr", 0, unique_pos)
        assert "hm-cluster-member" not in html[row_start:unique_pos]

    def test_below_threshold_rows_stay_flat(self, cache):
        names = [f"few case {i} in tests/unit/a.tcl" for i in range(4)]
        self._store_many(cache, 100, 200, _day(2), names)

        html = render_html(generate_report_data(cache, days=14))

        assert 'class="hm-cluster-toggle"' not in html
        assert 'class="hm-cluster-member"' not in html
        assert "collapsed into cluster rows" not in html

    def test_different_signatures_not_merged(self, cache):
        # Five tests share days {3,2}; five others share only day {2}.
        group_a = [f"a case {i} in tests/unit/a.tcl" for i in range(5)]
        group_b = [f"b case {i} in tests/unit/b.tcl" for i in range(5)]
        self._store_many(cache, 100, 200, _day(3), group_a)
        self._store_many(cache, 101, 201, _day(2), group_a + group_b)

        html = render_html(generate_report_data(cache, days=14))

        # Two distinct clusters, not one merged blob.
        assert html.count('class="hm-cluster-toggle"') == 2
        assert html.count("5 tests · identical failure days") == 2


class TestEwmaRate:
    def test_empty_series_is_zero(self):
        from valkey_oncall.report import _ewma_rate

        assert _ewma_rate([]) == 0.0

    def test_constant_series_matches_flat_rate(self):
        from valkey_oncall.report import _ewma_rate

        assert _ewma_rate([0] * 90) == 0.0
        assert _ewma_rate([1] * 90) == pytest.approx(1.0)

    def test_fixed_test_decays_below_flat_average(self):
        from valkey_oncall.report import _ewma_rate

        # Flaky for the first 45 days, clean for the last 45: flat average
        # says 50%, the recency weighting should read it as mostly fixed.
        series = [1] * 45 + [0] * 45
        rate = _ewma_rate(series)
        assert rate < 0.25

    def test_fresh_regression_reads_above_flat_average(self):
        from valkey_oncall.report import _ewma_rate

        # Clean for 80 days, failing the last 10.
        series = [0] * 80 + [1] * 10
        assert _ewma_rate(series) > 10 / 90


class TestHeatmapTrendColumn:
    def test_trend_sparkline_replaces_numeric_columns(self, cache):
        _store_failure(cache, 100, 200, _day(2), "recent in tests/unit/b.tcl")

        html = render_html(generate_report_data(cache, days=14))

        assert "90d trend" in html
        assert 'class="spark-cell"' in html
        assert "recent-weighted" in html
        # The old numeric heatmap columns are gone.
        assert 'title="Failure rate over last 90 days"' not in html

    def test_tooltip_carries_recency_facts(self, cache):
        _store_failure(cache, 100, 200, _day(2), "recent in tests/unit/b.tcl")

        html = render_html(generate_report_data(cache, days=14))

        assert "last failed" in html
        assert "in window" in html

    def test_cluster_row_gets_envelope_trend(self, cache):
        names = [f"regr case {i} in tests/unit/a.tcl" for i in range(6)]
        cache.store_runs([_make_run(100, _day(2))])
        cache.store_jobs(100, [_make_job(200, 100)])
        cache.store_failures(
            200,
            [{"test_name": n, "error_summary": "err", "log_lines": "x"} for n in names],
        )

        html = render_html(generate_report_data(cache, days=14))

        assert 'class="hm-cluster-toggle"' in html
        assert "cluster envelope" in html


class TestReliabilityTab:
    """The Reliability SLA block in the data and its tab in the HTML."""

    def _run(self, cache, run_id, day, jobs, status="failure", event="schedule"):
        import json

        cache.store_runs(
            [
                {
                    **_make_run(run_id, day, status),
                    "raw_json": json.dumps({"event": event}),
                }
            ]
        )
        rows = []
        for i, (name, conclusion, failures) in enumerate(jobs):
            jid = run_id * 100 + i
            rows.append(
                {
                    "job_id": jid,
                    "name": name,
                    "status": "completed",
                    "conclusion": conclusion,
                }
            )
        cache.store_jobs(run_id, rows)
        for i, (_n, _c, failures) in enumerate(jobs):
            if failures:
                cache.store_failures(
                    run_id * 100 + i,
                    [
                        {"test_name": t, "error_summary": s, "log_lines": "x"}
                        for t, s in failures
                    ],
                )

    def _seed(self, cache):
        from valkey_oncall.flakes import FlakeEntry, FlakeRegistry

        tracked = "tracked flake in tests/unit/a.tcl"
        untracked = "untracked test in tests/unit/b.tcl"
        self._run(cache, 1, _day(3), [("job-x", "success", [])], status="success")
        self._run(cache, 2, _day(2), [("job-x", "failure", [(tracked, "err")])])
        self._run(
            cache,
            3,
            _day(1),
            [("job-x", "failure", [(tracked, "err"), (untracked, "err")])],
        )
        # A hand-started run that must not count.
        self._run(
            cache,
            4,
            _day(1),
            [("job-x", "success", [])],
            status="success",
            event="workflow_dispatch",
        )
        return FlakeRegistry([FlakeEntry(test_name=tracked, issue=42)])

    def test_data_block(self, cache):
        registry = self._seed(cache)
        data = generate_report_data(cache, days=14, registry=registry)
        rel = data["reliability"]
        assert rel["targets"] == {"green_rate": 0.5, "flake_rate": 0.01}
        health = rel["run_health"]
        assert health["meta"]["event"] == "schedule"
        assert [r["run_id"] for r in health["runs"]] == [1, 2, 3]
        last = health["rolling30"][-1]
        assert last["green_runs"] == 1 and last["runs"] == 3
        assert last["mean_unexplained_failed_jobs"] == pytest.approx(1 / 3, abs=1e-3)
        rates = rel["flake_rates"]
        assert rates["tests_tracked"] == 1
        assert {t["test_name"]: t["executions"] for t in rates["tests"]} == {
            "tracked flake in tests/unit/a.tcl": 3,
            "untracked test in tests/unit/b.tcl": 3,
        }
        assert rel["over_budget_series"][-1]["failing_tests"] == 2

    def test_html_tab_shows_numbers_and_targets(self, cache):
        registry = self._seed(cache)
        data = generate_report_data(cache, days=14, registry=registry)
        html = render_html(data)
        assert 'id="tab-sla"' in html
        assert "1 / 3" in html  # fully green nightly runs
        assert "target 50%" in html
        assert "1 / 2" in html  # tracked failing tests
        assert "untracked test" in html
        assert (
            "tracked flake"
            not in html.split("without a tracking")[1].split("</table>")[0]
        )
        # Inline SVG charts carry the dashed target line, and nothing fetches.
        assert 'class="sla-chart"' in html
        assert 'class="sla-target"' in html
        assert "<script src=" not in html

    def test_ci_green_rate_rendered_from_ci_data(self, cache):
        registry = self._seed(cache)
        data = generate_report_data(cache, days=14, registry=registry)
        import json

        for i, status in enumerate(("success", "success", "failure", "cancelled")):
            cache.store_runs(
                [
                    {
                        **_make_run(50 + i, _day(1), status),
                        "workflow_file": "ci.yml",
                        "raw_json": json.dumps({"event": "push"}),
                    }
                ]
            )
            cache.store_jobs(
                50 + i,
                [
                    {
                        "job_id": 5000 + i,
                        "name": "test-ubuntu-latest",
                        "status": "completed",
                        "conclusion": "success" if status == "success" else status,
                    }
                ],
            )
        ci = generate_report_data(
            cache, workflow="ci.yml", per_run=True, registry=registry
        )
        ci_last = ci["reliability"]["run_health"]["rolling30"][-1]
        assert ci_last["runs"] == 3  # the cancelled push run is left out
        assert ci_last["green_rate"] == pytest.approx(2 / 3, abs=1e-3)
        html = render_html(data, ci_data=ci)
        assert "CI push runs fully green" in html
        assert "target 95%" in html

    def test_empty_window_renders(self, cache):
        data = generate_report_data(cache, days=14)
        html = render_html(data)
        assert "no scheduled nightly runs in the window" in html
        assert "no runs in the window" in html

    def test_target_label_has_at_most_two_decimals(self, cache):
        registry = self._seed(cache)
        html = render_html(generate_report_data(cache, days=14, registry=registry))
        # The failed-jobs budget is -ln(0.5) = 0.693147...
        assert "target 0.69<" in html
        assert "0.693" not in html

    def test_cancelled_night_without_jobs_is_a_gap_not_a_zero(self, cache):
        registry = self._seed(cache)
        # A cancelled scheduled run with no job rows, as sync stores it.
        cache.store_runs(
            [
                {
                    **_make_run(9, _day(0), "cancelled"),
                    "raw_json": '{"event": "schedule"}',
                }
            ]
        )
        data = generate_report_data(cache, days=14, registry=registry)
        last = data["reliability"]["run_health"]["rolling30"][-1]
        assert last["runs"] == 4 and last["job_data_runs"] == 3
        assert last["mean_failed_jobs"] == pytest.approx(2 / 3, abs=1e-3)
        html = render_html(data)
        chart = html.split("failed jobs per nightly run</h3>")[1].split("</svg>")[0]
        assert chart.count('class="sla-gap"') == 1
        assert f"{_day(0)} · cancelled, no job data" in chart
        assert "no job data</span>" in html  # legend entry
        # The tooltip says what the stat is averaged over.
        assert "the 3 of the last 4 runs with job data" in html


class TestSlaChart:
    def test_number_format(self):
        from valkey_oncall.report import _sla_chart

        pts = [{"date": "2026-11-01", "v": 12.0}, {"date": "2026-11-02", "v": 0.5}]
        svg = _sla_chart(pts, [{"key": "v", "label": "v", "color": "#f00"}], 0.693147)
        assert "target 0.69<" in svg
        assert ">12<" in svg  # axis top, no trailing .00
        assert "v: 0.5<" in svg
        assert "0.693" not in svg

    def test_percent_format_unchanged(self):
        from valkey_oncall.report import _sla_chart

        pts = [{"date": "2026-11-01", "v": 0.25}]
        svg = _sla_chart(
            pts,
            [{"key": "v", "label": "v", "color": "#f00"}],
            target=0.5,
            ymax=1.0,
            percent=True,
        )
        assert "target 50%" in svg and "v: 25%" in svg

    def test_no_gap_no_hatch(self):
        from valkey_oncall.report import _sla_chart

        pts = [{"date": "2026-11-01", "v": 1}]
        svg = _sla_chart(pts, [{"key": "v", "label": "v", "color": "#f00"}])
        assert "sla-gap" not in svg and "sla-hatch" not in svg


class TestScheduleOnlyDaily:
    """Only the nightly schedule runs of daily.yml are Daily runs.

    daily.yml is also started by hand (workflow_dispatch) to confirm fixes
    against a PR head. Those runs must never stand in for a nightly, and
    every chart has to end on the same latest scheduled run.
    """

    def _run(self, cache, run_id, day, status, event, failing_test=None):
        cache.store_runs([_make_run(run_id, day, status, event=event)])
        job_id = run_id * 100
        cache.store_jobs(
            run_id,
            [
                {
                    "job_id": job_id,
                    "name": "test-ubuntu",
                    "status": "completed",
                    "conclusion": "failure" if failing_test else "success",
                }
            ],
        )
        if failing_test:
            cache.store_failures(
                job_id,
                [{"test_name": failing_test, "error_summary": "e", "log_lines": "x"}],
            )

    def test_schedule_run_wins_over_dispatch_on_same_day(self, cache):
        # The hand-started run finished first (success); the nightly on the
        # same date failed. Before the fix the first run of the day won.
        self._run(cache, 10, _day(1), "success", "workflow_dispatch")
        self._run(cache, 11, _day(1), "failure", "schedule", "x in tests/unit/a.tcl")
        data = generate_report_data(cache, days=7)

        assert [r["run_id"] for r in data["runs"]] == [11]
        assert data["runs"][0]["status"] == "failure"
        assert data["summary"]["failed_runs"] == 1
        assert "x in tests/unit/a.tcl" in data["tests"]
        health = data["reliability"]["run_health"]["runs"]
        assert [r["run_id"] for r in health] == [11]
        assert [t["test_name"] for t in data["scorecard"]["scorecards"]] == [
            "x in tests/unit/a.tcl"
        ]

    def test_day_with_only_a_dispatch_run_has_no_daily_entry(self, cache):
        self._run(cache, 20, _day(2), "failure", "schedule", "x in tests/unit/a.tcl")
        self._run(
            cache, 21, _day(1), "failure", "workflow_dispatch", "y in tests/unit/b.tcl"
        )
        data = generate_report_data(cache, days=7)

        assert data["dates"] == [_day(2)]
        assert data["summary"]["latest_run_date"] == _day(2)
        assert "y in tests/unit/b.tcl" not in data["tests"]
        assert all(
            t["test_name"] != "y in tests/unit/b.tcl"
            for t in data["scorecard"]["scorecards"]
        )
        assert all(
            r["test_name"] != "y in tests/unit/b.tcl" for r in data["regressions"]
        )

    def test_event_all_keeps_dispatch_runs(self, cache):
        self._run(
            cache, 30, _day(1), "failure", "workflow_dispatch", "x in tests/unit/a.tcl"
        )
        assert generate_report_data(cache, days=7)["runs"] == []
        data = generate_report_data(cache, days=7, event="all")
        assert [r["run_id"] for r in data["runs"]] == [30]

    def test_every_chart_series_ends_on_the_latest_scheduled_run(self, cache):
        # A scheduled nightly every day, plus a newer hand-started run that
        # must not extend any chart.
        for i in range(5, 0, -1):
            self._run(
                cache, 100 + i, _day(i), "failure", "schedule", "x in tests/unit/a.tcl"
            )
        self._run(cache, 200, _day(0), "success", "workflow_dispatch")
        data = generate_report_data(cache, days=30)
        rel = data["reliability"]

        last = {
            "heatmap": data["dates"][-1],
            "runs": data["runs"][-1]["day"],
            "run_health": rel["run_health"]["runs"][-1]["date"],
            "rolling7": rel["run_health"]["rolling7"][-1]["date"],
            "rolling30": rel["run_health"]["rolling30"][-1]["date"],
            "over_budget_series": rel["over_budget_series"][-1]["date"],
            "flake_rates_last_failure": max(
                t["last_failure"] for t in rel["flake_rates"]["tests"]
            ),
            "scorecard_last_seen": max(
                t["last_seen"] for t in data["scorecard"]["scorecards"]
            ),
        }
        assert set(last.values()) == {_day(1)}, last
        assert data["summary"]["latest_run_date"] == _day(1)
