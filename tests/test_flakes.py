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
    signature_id,
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
