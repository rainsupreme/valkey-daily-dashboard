"""Tests for the flake registry (valkey_oncall.flakes) and its CLI."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from valkey_oncall.cache import Cache
from valkey_oncall.cli import cli
from valkey_oncall.flakes import (
    TIER_EXACT,
    TIER_TEST_ONLY,
    FlakeEntry,
    FlakeRegistry,
    RegistryError,
    entry_report,
    maintenance,
    refresh_registry,
    signature_id,
    tracking_for_test,
)

TEST = "Active defrag - AOF loading in tests/unit/memefficiency.tcl"
SUMMARY_A = "Expected 1 to be equal to 0"
SUMMARY_B = "timeout waiting for defrag"


def _seed(cache: Cache, run_id: int, day: str, failures, branch="unstable") -> None:
    """Store one failed run on *day* with the given (test_name, summary) rows."""
    cache.store_runs(
        [
            {
                "run_id": run_id,
                "workflow_file": "daily.yml",
                "status": "failure",
                "branch": branch,
                "commit_sha": f"sha{run_id}",
                "run_date": f"{day}T06:00:00Z",
                "raw_json": "{}",
            }
        ]
    )
    job_id = run_id * 10
    cache.store_jobs(
        run_id,
        [
            {
                "job_id": job_id,
                "name": "test-ubuntu-latest",
                "status": "completed",
                "conclusion": "failure",
                "raw_json": "{}",
            }
        ],
    )
    cache.store_failures(
        job_id,
        [{"test_name": t, "error_summary": s, "log_lines": "..."} for t, s in failures],
    )


def _today(offset_days: int = 0) -> str:
    from datetime import datetime, timedelta, timezone

    return (datetime.now(timezone.utc) - timedelta(days=offset_days)).strftime(
        "%Y-%m-%d"
    )


class TestEntry:
    def test_requires_owner(self) -> None:
        with pytest.raises(RegistryError):
            FlakeEntry(test_name=TEST)

    def test_fix_sha_requires_date(self) -> None:
        with pytest.raises(RegistryError):
            FlakeEntry(test_name=TEST, issue=1, fix_sha="abc")

    def test_summary_is_normalized(self) -> None:
        e = FlakeEntry(test_name=TEST, issue=1, summary="pid 12345 died at 0xdeadbeef")
        assert e.summary == "pid <pid> died at 0x<addr>"

    def test_summary_keeps_first_line_and_scrubs_run_values(self) -> None:
        # Shape of a real cached error_summary: assertion line, then the
        # server banner with pids, ports and timestamps that differ per run.
        raw = (
            "Expected '5' to be less than or equal to '4' (context: type source "
            "line 26 file /Users/runner/work/valkey/valkey/tests/integration/"
            "replication-flow-control.tcl cmd {assert_lessthan_equal ...} proc ::test)\n"
            "### Starting server for test \n"
            "50728:M 23 Oct 2026 10:47:58.216 * Valkey version=9.2.0, pid:50728\n"
        )
        again = raw.replace("'5'", "'7'").replace("line 26", "line 31")
        again = again.replace("/Users/runner", "/home/runner").replace("50728", "611")
        a = FlakeEntry(test_name=TEST, issue=1, summary=raw)
        b = FlakeEntry(test_name=TEST, issue=1, summary=again)
        assert a.summary == b.summary
        assert a.summary.startswith("Expected '<n>' to be less than or equal to '<n>'")
        assert "line <n> file tests/integration/replication-flow-control.tcl" in (
            a.summary
        )
        assert "\n" not in a.summary

    def test_id_is_deterministic(self) -> None:
        a = FlakeEntry(test_name=TEST, issue=1, summary=SUMMARY_A)
        b = FlakeEntry(test_name=TEST, issue=2, summary=SUMMARY_A)
        assert a.id == b.id == signature_id(TEST, SUMMARY_A)
        assert a.id != FlakeEntry(test_name=TEST, issue=1).id

    def test_status(self) -> None:
        e = FlakeEntry(test_name=TEST, issue=1)
        assert e.status == "tracked"
        e.fix_sha, e.fix_merged_at = "abc", "2026-10-01"
        assert e.status == "fixed"


class TestRegistry:
    def test_roundtrip(self, tmp_path) -> None:
        path = str(tmp_path / "flakes.json")
        reg = FlakeRegistry()
        reg.add(FlakeEntry(test_name=TEST, issue=4001, summary=SUMMARY_A, note="n"))
        reg.add(FlakeEntry(test_name=TEST, fix_pr=4002))
        reg.save(path)
        back = FlakeRegistry.load(path)
        assert [e.to_dict() for e in back.entries()] == [
            e.to_dict() for e in reg.entries()
        ]

    def test_missing_file_is_empty(self, tmp_path) -> None:
        assert FlakeRegistry.load(str(tmp_path / "none.json")).entries() == []

    def test_bad_version_rejected(self, tmp_path) -> None:
        p = tmp_path / "flakes.json"
        p.write_text(json.dumps({"version": 99, "entries": []}))
        with pytest.raises(RegistryError):
            FlakeRegistry.load(str(p))

    def test_duplicate_signature_rejected(self) -> None:
        reg = FlakeRegistry()
        reg.add(FlakeEntry(test_name=TEST, issue=1, summary=SUMMARY_A))
        with pytest.raises(RegistryError):
            reg.add(FlakeEntry(test_name=TEST, issue=2, summary=SUMMARY_A))

    def test_match_tiers(self) -> None:
        reg = FlakeRegistry()
        a = reg.add(FlakeEntry(test_name=TEST, issue=1, summary=SUMMARY_A))
        any_mode = reg.add(FlakeEntry(test_name=TEST, issue=2))

        exact = reg.match(TEST, SUMMARY_A)
        assert [(m.entry.id, m.tier) for m in exact] == sorted(
            [(a.id, TIER_EXACT), (any_mode.id, TIER_EXACT)]
        )

        other = reg.match(TEST, SUMMARY_B)
        tiers = {m.entry.id: m.tier for m in other}
        assert tiers == {a.id: TIER_TEST_ONLY, any_mode.id: TIER_EXACT}
        assert other[0].tier == TIER_EXACT  # exact sorts first

        assert reg.match("some other test", SUMMARY_A) == []

    def test_match_normalizes_incoming_summary(self) -> None:
        reg = FlakeRegistry()
        reg.add(FlakeEntry(test_name=TEST, issue=1, summary="pid 1 died"))
        assert reg.match(TEST, "pid 999 died")[0].tier == TIER_EXACT

    def test_mark_fixed(self) -> None:
        reg = FlakeRegistry()
        e = reg.add(FlakeEntry(test_name=TEST, issue=1))
        reg.mark_fixed(e.id, fix_sha="abc", fix_merged_at="2026-10-01", fix_pr=5)
        assert (e.status, e.fix_pr) == ("fixed", 5)
        with pytest.raises(RegistryError):
            reg.mark_fixed("sig:nope", "x", "2026-10-01")


class TestSightings:
    def test_entry_report_counts_matching_mode_only(self, temp_db_path) -> None:
        cache = Cache(temp_db_path)
        _seed(cache, 1, _today(3), [(TEST, SUMMARY_A)])
        _seed(cache, 2, _today(2), [(TEST, SUMMARY_B)])
        _seed(cache, 3, _today(1), [(TEST, SUMMARY_A)])
        _seed(cache, 4, _today(60), [(TEST, SUMMARY_A)])  # outside window

        specific = FlakeEntry(test_name=TEST, issue=1, summary=SUMMARY_A)
        rep = entry_report(cache, specific, days=30)
        assert (rep["sightings"], rep["last_sighting"]) == (2, _today(1))

        any_mode = FlakeEntry(test_name=TEST, issue=1)
        assert entry_report(cache, any_mode, days=30)["sightings"] == 3

    def test_recurrence_after_fix(self, temp_db_path) -> None:
        cache = Cache(temp_db_path)
        _seed(cache, 1, _today(10), [(TEST, SUMMARY_A)])
        _seed(cache, 2, _today(1), [(TEST, SUMMARY_A)])
        e = FlakeEntry(
            test_name=TEST,
            issue=1,
            fix_pr=2,
            fix_sha="abc",
            fix_merged_at=_today(5),
        )
        rep = entry_report(cache, e, days=30)
        assert (rep["status"], rep["post_fix_sightings"]) == ("fixed", 1)

        reg = FlakeRegistry([e])
        kinds = [f["kind"] for f in maintenance(cache, reg, days=30)]
        assert kinds == ["recurrence"]

    def test_fixed_without_recurrence_is_silent(self, temp_db_path) -> None:
        cache = Cache(temp_db_path)
        _seed(cache, 1, _today(10), [(TEST, SUMMARY_A)])
        e = FlakeEntry(test_name=TEST, issue=1, fix_sha="abc", fix_merged_at=_today(5))
        assert maintenance(cache, FlakeRegistry([e]), days=30) == []

    def test_drift_and_quiet(self, temp_db_path) -> None:
        cache = Cache(temp_db_path)
        _seed(cache, 1, _today(2), [(TEST, SUMMARY_B)])
        drifted = FlakeEntry(test_name=TEST, issue=1, summary=SUMMARY_A)
        quiet = FlakeEntry(test_name="never fails anymore", issue=2)
        findings = maintenance(cache, FlakeRegistry([drifted, quiet]), days=30)
        by_id = {f["id"]: f for f in findings}
        assert by_id[drifted.id]["kind"] == "drift"
        assert by_id[drifted.id]["other_summaries"] == [SUMMARY_B]
        assert by_id[quiet.id]["kind"] == "quiet"

    def test_tracked_with_sightings_needs_no_attention(self, temp_db_path) -> None:
        cache = Cache(temp_db_path)
        _seed(cache, 1, _today(2), [(TEST, SUMMARY_A)])
        e = FlakeEntry(test_name=TEST, issue=1, summary=SUMMARY_A)
        assert maintenance(cache, FlakeRegistry([e]), days=30) == []


class TestCli:
    def _run(self, db, reg, *args):
        runner = CliRunner()
        return runner.invoke(
            cli,
            ["--db", db, "flakes", "--registry", reg, *args],
            catch_exceptions=False,
        )

    def test_add_list_match_fixed_check(self, temp_db_path, tmp_path) -> None:
        reg = str(tmp_path / "flakes.json")
        cache = Cache(temp_db_path)
        _seed(cache, 1, _today(1), [(TEST, SUMMARY_A)])

        r = self._run(temp_db_path, reg, "add", "--from-job", "10", "--issue", "4001")
        assert r.exit_code == 0, r.output
        entry = json.loads(r.output)
        assert (entry["test_name"], entry["summary"]) == (TEST, SUMMARY_A)
        assert json.load(open(reg))["version"] == 1

        r = self._run(temp_db_path, reg, "list")
        rows = json.loads(r.output)
        assert rows[0]["sightings"] == 1 and rows[0]["status"] == "tracked"

        r = self._run(
            temp_db_path, reg, "match", "--test", TEST, "--summary", SUMMARY_B
        )
        assert json.loads(r.output)[0]["tier"] == "test_only"

        r = self._run(
            temp_db_path,
            reg,
            "fixed",
            entry["id"],
            "--sha",
            "abc",
            "--merged-at",
            _today(5),
            "--pr",
            4002,
        )
        assert json.loads(r.output)["fix_pr"] == 4002

        r = self._run(temp_db_path, reg, "check")
        assert [f["kind"] for f in json.loads(r.output)] == ["recurrence"]

        r = self._run(temp_db_path, reg, "remove", entry["id"])
        assert r.exit_code == 0
        assert json.loads(self._run(temp_db_path, reg, "list").output) == []

    def test_add_without_owner_fails(self, temp_db_path, tmp_path) -> None:
        reg = str(tmp_path / "flakes.json")
        r = CliRunner().invoke(
            cli,
            ["--db", temp_db_path, "flakes", "--registry", reg, "add", "--test", TEST],
        )
        assert r.exit_code == 1
        assert "needs an issue or a fix_pr" in r.output

    def test_from_job_ambiguous(self, temp_db_path, tmp_path) -> None:
        reg = str(tmp_path / "flakes.json")
        cache = Cache(temp_db_path)
        _seed(cache, 1, _today(1), [(TEST, SUMMARY_A), ("other test", "x")])
        r = CliRunner().invoke(
            cli,
            [
                "--db",
                temp_db_path,
                "flakes",
                "--registry",
                reg,
                "add",
                "--from-job",
                "10",
                "--issue",
                "1",
            ],
        )
        assert r.exit_code == 1 and "pick one with --test" in r.output


# ----------------------------------------------------------------------
# refresh: issue / fix-PR state from GitHub
# ----------------------------------------------------------------------


class _FakeGitHub:
    """Stand-in for GitHubActionsClient's issue/PR reads."""

    def __init__(self, issues=None, pulls=None, timelines=None) -> None:
        self.issues = issues or {}
        self.pulls = pulls or {}
        self.timelines = timelines or {}
        self.calls: list = []

    def get_issue(self, number):
        self.calls.append(("issue", number))
        return self.issues[number]

    def get_pull(self, number):
        self.calls.append(("pull", number))
        return self.pulls[number]

    def get_issue_timeline(self, number):
        self.calls.append(("timeline", number))
        return self.timelines.get(number, [])


def _issue(state, title="t", reason=None):
    return {"state": state, "title": title, "state_reason": reason}


class TestRefresh:
    def test_open_issue_caches_state_only(self) -> None:
        reg = FlakeRegistry([FlakeEntry(test_name=TEST, issue=1)])
        gh = _FakeGitHub(issues={1: _issue("open", "flaky defrag")})
        reports = refresh_registry(reg, gh)
        e = reg.entries()[0]
        assert e.issue_state == "open" and e.issue_title == "flaky defrag"
        assert e.status == "tracked" and e.fix_sha is None
        assert reports[0]["changes"] == ["issue_state None -> open"]
        assert ("timeline", 1) not in gh.calls

    def test_issue_closed_by_merged_pr_records_fix(self) -> None:
        # Real shape (valkey issue 4958): the closed event carries no commit;
        # the fix is the merged PR cross-referenced just before the close.
        reg = FlakeRegistry([FlakeEntry(test_name=TEST, issue=1)])
        timeline = [
            {
                "event": "cross-referenced",
                "source": {
                    "issue": {
                        "number": 2,
                        "pull_request": {"merged_at": "2026-10-20T10:00:00Z"},
                    }
                },
            },
            {
                "event": "closed",
                "commit_id": None,
                "created_at": "2026-10-20T10:00:05Z",
            },
        ]
        gh = _FakeGitHub(
            issues={1: _issue("closed")},
            timelines={1: timeline},
            pulls={
                2: {
                    "state": "closed",
                    "merged_at": "2026-10-20T10:00:00Z",
                    "merge_commit_sha": "abcdef1234567890",
                }
            },
        )
        refresh_registry(reg, gh)
        e = reg.entries()[0]
        assert e.status == "fixed"
        assert (e.fix_sha, e.fix_merged_at, e.fix_pr) == (
            "abcdef1234567890",
            "2026-10-20",
            2,
        )

    def test_closed_event_with_commit_needs_no_pr_fetch(self) -> None:
        reg = FlakeRegistry([FlakeEntry(test_name=TEST, issue=1)])
        timeline = [
            {
                "event": "closed",
                "commit_id": "c0ffee",
                "created_at": "2026-10-20T10:00Z",
            }
        ]
        gh = _FakeGitHub(issues={1: _issue("closed")}, timelines={1: timeline})
        refresh_registry(reg, gh)
        e = reg.entries()[0]
        assert (e.fix_sha, e.fix_merged_at, e.fix_pr) == ("c0ffee", "2026-10-20", None)
        assert not any(c[0] == "pull" for c in gh.calls)

    def test_issue_closed_by_hand_is_flagged_not_fixed(self) -> None:
        reg = FlakeRegistry([FlakeEntry(test_name=TEST, issue=1)])
        timeline = [{"event": "closed", "commit_id": None, "created_at": "2026-10-20"}]
        gh = _FakeGitHub(
            issues={1: _issue("closed", reason="not_planned")}, timelines={1: timeline}
        )
        reports = refresh_registry(reg, gh)
        e = reg.entries()[0]
        assert e.status == "closed" and e.fix_sha is None
        assert reports[0]["flag"] == "closed_without_fix"

    def test_registered_fix_pr_merging_marks_fixed(self) -> None:
        reg = FlakeRegistry([FlakeEntry(test_name=TEST, fix_pr=7)])
        gh = _FakeGitHub(
            pulls={
                7: {
                    "state": "closed",
                    "merged_at": "2026-10-21T00:00:00Z",
                    "merge_commit_sha": "feedface00",
                }
            }
        )
        reports = refresh_registry(reg, gh)
        e = reg.entries()[0]
        assert e.status == "fixed" and e.fix_merged_at == "2026-10-21"
        assert reports[0]["fix_pr_state"] == "merged"

    def test_fix_pr_closed_unmerged_is_flagged(self) -> None:
        reg = FlakeRegistry([FlakeEntry(test_name=TEST, fix_pr=7)])
        gh = _FakeGitHub(pulls={7: {"state": "closed", "merged_at": None}})
        reports = refresh_registry(reg, gh)
        assert reports[0]["flag"] == "fix_pr_closed_unmerged"
        assert reg.entries()[0].status == "tracked"

    def test_already_fixed_entry_skips_lookups(self) -> None:
        reg = FlakeRegistry(
            [
                FlakeEntry(
                    test_name=TEST,
                    issue=1,
                    fix_pr=7,
                    fix_sha="abc",
                    fix_merged_at="2026-10-01",
                )
            ]
        )
        gh = _FakeGitHub(issues={1: _issue("closed")})
        refresh_registry(reg, gh)
        assert gh.calls == [("issue", 1)]

    def test_closed_without_fix_in_maintenance(self, temp_db_path) -> None:
        cache = Cache(temp_db_path)
        _seed(cache, 1, _today(1), [(TEST, SUMMARY_A)])
        e = FlakeEntry(test_name=TEST, issue=1, issue_state="closed")
        findings = maintenance(cache, FlakeRegistry([e]))
        assert [f["kind"] for f in findings] == ["closed_without_fix"]
        assert "1 sighting(s)" in findings[0]["detail"]

    def test_cached_fields_roundtrip(self, tmp_path) -> None:
        path = str(tmp_path / "f.json")
        reg = FlakeRegistry(
            [FlakeEntry(test_name=TEST, issue=1, issue_state="open", issue_title="x")]
        )
        reg.save(path)
        again = FlakeRegistry.load(path).entries()[0]
        assert (again.issue_state, again.issue_title) == ("open", "x")


# ----------------------------------------------------------------------
# tracking_for_test: the report's 'tracked' column
# ----------------------------------------------------------------------


class TestTracking:
    def test_confirmed_and_unconfirmed_modes(self) -> None:
        reg = FlakeRegistry(
            [
                FlakeEntry(test_name=TEST, summary=SUMMARY_A, issue=1),
                FlakeEntry(test_name=TEST, summary=SUMMARY_B, issue=2),
                FlakeEntry(test_name="other", issue=3),
            ]
        )
        rows = tracking_for_test(reg, TEST, {SUMMARY_A + "\nbanner"}, {"2026-10-20"})
        assert [(r["issue"], r["confirmed"]) for r in rows] == [(1, True), (2, False)]

    def test_recurred_only_after_fix_date(self) -> None:
        fixed = FlakeEntry(
            test_name=TEST, issue=1, fix_sha="abc", fix_merged_at="2026-10-15"
        )
        reg = FlakeRegistry([fixed])
        assert not tracking_for_test(reg, TEST, set(), {"2026-10-10"})[0]["recurred"]
        assert tracking_for_test(reg, TEST, set(), {"2026-10-16"})[0]["recurred"]

    def test_report_renders_tracked_column(self, temp_db_path) -> None:
        from valkey_oncall.report import generate_report_data, render_html

        cache = Cache(temp_db_path)
        _seed(cache, 1, _today(1), [(TEST, SUMMARY_A), ("lonely test", "x")])
        reg = FlakeRegistry(
            [
                FlakeEntry(test_name=TEST, issue=4001, issue_state="open"),
            ]
        )
        data = generate_report_data(cache, days=14, registry=reg)
        assert data["tests"][TEST]["tracking"][0]["issue"] == 4001
        assert data["tests"]["lonely test"]["tracking"] == []

        page = render_html(data)
        assert '<th class="flake-col"' in page
        assert "https://github.com/valkey-io/valkey/issues/4001" in page
        assert 'class="flake-link"' in page
        assert 'class="flake-link flake-recurred"' not in page
        # Row for the untracked test carries an empty tracked cell.
        pos = page.index("lonely test")
        row = page[page.rindex("<tr", 0, pos) : page.index("</tr>", pos)]
        assert 'title="no registry entry"' in row

    def test_report_marks_recurrence_red(self, temp_db_path) -> None:
        from valkey_oncall.report import generate_report_data, render_html

        cache = Cache(temp_db_path)
        _seed(cache, 1, _today(1), [(TEST, SUMMARY_A)])
        reg = FlakeRegistry(
            [
                FlakeEntry(
                    test_name=TEST,
                    issue=4001,
                    fix_pr=4002,
                    fix_sha="abc",
                    fix_merged_at=_today(10),
                )
            ]
        )
        page = render_html(generate_report_data(cache, days=14, registry=reg))
        assert 'class="flake-link flake-recurred"' in page and "RECURRED" in page

    def test_report_without_registry_has_empty_column(self, temp_db_path) -> None:
        from valkey_oncall.report import generate_report_data, render_html

        cache = Cache(temp_db_path)
        _seed(cache, 1, _today(1), [(TEST, SUMMARY_A)])
        page = render_html(generate_report_data(cache, days=14))
        assert '<th class="flake-col"' in page
        assert '<a class="flake-link' not in page


class TestRefreshCli:
    def test_refresh_writes_unless_dry_run(self, temp_db_path, tmp_path, monkeypatch):
        from valkey_oncall import cli as cli_mod

        reg_path = str(tmp_path / "flakes.json")
        FlakeRegistry([FlakeEntry(test_name=TEST, issue=1)]).save(reg_path)
        monkeypatch.setenv("GITHUB_TOKEN", "x")
        monkeypatch.setattr(
            cli_mod,
            "_make_client",
            lambda repo: _FakeGitHub(issues={1: _issue("open", "title")}),
        )
        runner = CliRunner()
        r = runner.invoke(
            cli,
            [
                "--db",
                temp_db_path,
                "flakes",
                "--registry",
                reg_path,
                "refresh",
                "--dry-run",
            ],
            catch_exceptions=False,
        )
        assert r.exit_code == 0, r.output
        assert "dry run" in r.output
        assert "issue_state" not in open(reg_path).read()

        r = runner.invoke(
            cli,
            ["--db", temp_db_path, "flakes", "--registry", reg_path, "refresh"],
            catch_exceptions=False,
        )
        assert r.exit_code == 0, r.output
        saved = json.load(open(reg_path))["entries"][0]
        assert saved["issue_state"] == "open" and saved["issue_title"] == "title"
