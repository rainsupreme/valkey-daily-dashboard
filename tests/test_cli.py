"""Property-based and integration tests for the CLI layer.

Feature: valkey-oncall-dashboard, Property 3: All command output is valid JSON
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Dict, List
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from hypothesis import given, settings
import hypothesis.strategies as st

from valkey_oncall.cache import Cache
from valkey_oncall.cli import cli
from valkey_oncall.service import _map_run, _map_job


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_db() -> str:
    """Create a fresh temporary SQLite DB path."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


def _api_run(run_id: int, conclusion: str = "failure", branch: str = "unstable",
             workflow_path: str = ".github/workflows/daily.yml") -> Dict:
    """Return a dict resembling a GitHub API workflow-run object."""
    return {
        "id": run_id,
        "path": workflow_path,
        "name": "daily.yml",
        "conclusion": conclusion,
        "status": "completed",
        "head_branch": branch,
        "head_sha": f"sha_{run_id}",
        "run_started_at": "2024-01-15T08:00:00Z",
        "created_at": "2024-01-15T08:00:00Z",
        "updated_at": "2024-01-15T09:00:00Z",
    }


def _api_job(job_id: int, conclusion: str = "failure") -> Dict:
    """Return a dict resembling a GitHub API job object."""
    return {
        "id": job_id,
        "name": f"build-{job_id}",
        "status": "completed",
        "conclusion": conclusion,
    }


def _populate_cache(db_path: str, num_runs: int, num_jobs_per_run: int,
                    add_failures: bool = False) -> None:
    """Populate a cache DB with sample data for query commands."""
    cache = Cache(db_path)
    for i in range(1, num_runs + 1):
        run = _map_run(_api_run(i), "valkey-io/valkey")
        cache.store_runs([run])
        jobs = []
        for j in range(1, num_jobs_per_run + 1):
            job_id = i * 1000 + j
            jobs.append(_map_job(_api_job(job_id, conclusion="failure")))
        if jobs:
            cache.store_jobs(i, jobs)
            if add_failures:
                for job in jobs:
                    jid = job["job_id"]
                    log_text = f"[err]: test_example_{jid} in tests/unit.tcl\nExpected OK\n"
                    cache.store_log(jid, log_text)
                    cache.store_failures(jid, [
                        {
                            "test_name": f"test_example_{jid} in tests/unit.tcl",
                            "error_summary": "Expected OK",
                            "log_lines": log_text,
                        }
                    ])


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

# Strategy for number of runs to populate (keep small for speed)
st_num_runs = st.integers(min_value=0, max_value=5)
st_num_jobs = st.integers(min_value=1, max_value=3)


# ---------------------------------------------------------------------------
# Property 3: All command output is valid JSON
# ---------------------------------------------------------------------------

class TestAllCommandOutputIsValidJSON:
    """Property 3: All command output is valid JSON.

    For any successful command execution (query runs/jobs/failures, sync,
    fetch-runs, fetch-jobs, parse-log), the stdout output should be
    parseable as valid JSON.

    **Validates: Requirements 1.5, 2.4, 4.5, 5.4, 7.4**
    """

    @given(num_runs=st_num_runs)
    @settings(max_examples=100)
    def test_query_runs_outputs_valid_json(self, num_runs: int) -> None:
        """query runs always outputs valid JSON."""
        db_path = _fresh_db()
        _populate_cache(db_path, num_runs=num_runs, num_jobs_per_run=1)

        runner = CliRunner()
        result = runner.invoke(cli, ["--db", db_path, "query", "runs"])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)

    @given(num_runs=st.integers(min_value=1, max_value=5),
           num_jobs=st_num_jobs)
    @settings(max_examples=100)
    def test_query_jobs_outputs_valid_json(self, num_runs: int, num_jobs: int) -> None:
        """query jobs always outputs valid JSON."""
        db_path = _fresh_db()
        _populate_cache(db_path, num_runs=num_runs, num_jobs_per_run=num_jobs)

        runner = CliRunner()
        # Query jobs for run_id=1 (always exists since num_runs >= 1)
        result = runner.invoke(cli, ["--db", db_path, "query", "jobs", "--run-id", "1"])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)

    @given(num_runs=st_num_runs)
    @settings(max_examples=100)
    def test_query_failures_outputs_valid_json(self, num_runs: int) -> None:
        """query failures always outputs valid JSON."""
        db_path = _fresh_db()
        _populate_cache(db_path, num_runs=num_runs, num_jobs_per_run=1,
                        add_failures=True)

        runner = CliRunner()
        result = runner.invoke(cli, ["--db", db_path, "query", "failures"])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)

    @given(num_runs=st.integers(min_value=0, max_value=3))
    @settings(max_examples=100)
    def test_sync_outputs_valid_json(self, num_runs: int) -> None:
        """sync command always outputs valid JSON summary."""
        db_path = _fresh_db()

        # Build mock API responses
        api_runs = [_api_run(i, conclusion="failure") for i in range(1, num_runs + 1)]
        api_jobs_map = {}
        for run in api_runs:
            rid = run["id"]
            api_jobs_map[rid] = [_api_job(rid * 1000 + 1, conclusion="failure")]

        mock_client = MagicMock(repo="valkey-io/valkey")
        mock_client.get_workflow_runs.return_value = api_runs
        mock_client.get_jobs_for_run.side_effect = lambda rid: api_jobs_map.get(rid, [])
        mock_client.get_job_log.return_value = "some log content\n"

        runner = CliRunner(env={"GITHUB_TOKEN": "fake-token"})
        with patch("valkey_oncall.cli._make_client", return_value=mock_client):
            result = runner.invoke(cli, ["--db", db_path, "sync", "--workflow", "daily"])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        parsed = json.loads(result.output)
        assert isinstance(parsed, dict)
        assert "new_runs_fetched" in parsed

    @given(num_runs=st.integers(min_value=0, max_value=3))
    @settings(max_examples=100)
    def test_fetch_runs_outputs_valid_json(self, num_runs: int) -> None:
        """fetch-runs command always outputs valid JSON."""
        db_path = _fresh_db()
        api_runs = [_api_run(i) for i in range(1, num_runs + 1)]

        mock_client = MagicMock(repo="valkey-io/valkey")
        mock_client.get_workflow_runs.return_value = api_runs

        runner = CliRunner(env={"GITHUB_TOKEN": "fake-token"})
        with patch("valkey_oncall.cli._make_client", return_value=mock_client):
            result = runner.invoke(cli, ["--db", db_path, "fetch-runs", "--workflow", "daily"])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)

    @given(num_jobs=st.integers(min_value=0, max_value=5))
    @settings(max_examples=100)
    def test_fetch_jobs_outputs_valid_json(self, num_jobs: int) -> None:
        """fetch-jobs command always outputs valid JSON."""
        db_path = _fresh_db()
        # Pre-populate a run so the FK constraint is satisfied
        cache = Cache(db_path)
        cache.store_runs([_map_run(_api_run(1), "valkey-io/valkey")])

        api_jobs = [_api_job(1000 + j) for j in range(1, num_jobs + 1)]

        mock_client = MagicMock(repo="valkey-io/valkey")
        mock_client.get_jobs_for_run.return_value = api_jobs

        runner = CliRunner(env={"GITHUB_TOKEN": "fake-token"})
        with patch("valkey_oncall.cli._make_client", return_value=mock_client):
            result = runner.invoke(cli, ["--db", db_path, "fetch-jobs", "--run-id", "1"])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)

    @given(st.data())
    @settings(max_examples=100)
    def test_parse_log_outputs_valid_json(self, data: st.DataObject) -> None:
        """parse-log command always outputs valid JSON."""
        db_path = _fresh_db()
        cache = Cache(db_path)

        # Store a run and job so FK constraints are met
        cache.store_runs([_map_run(_api_run(1), "valkey-io/valkey")])
        cache.store_jobs(1, [_map_job(_api_job(100))])

        # Generate a random log — may or may not contain failure patterns
        has_failure = data.draw(st.booleans())
        if has_failure:
            test_name = data.draw(st.from_regex(r"[a-z_]{3,20}", fullmatch=True))
            log_text = f"[err]: {test_name} in tests/unit.tcl\nExpected OK but got ERR\n"
        else:
            log_text = data.draw(st.text(min_size=0, max_size=200))

        cache.store_log(100, log_text)

        mock_client = MagicMock(repo="valkey-io/valkey")
        # mix_stderr=False keeps stderr separate so JSON on stdout is clean
        runner = CliRunner(env={"GITHUB_TOKEN": "fake-token"}, mix_stderr=False)
        with patch("valkey_oncall.cli._make_client", return_value=mock_client):
            result = runner.invoke(cli, ["--db", db_path, "parse-log", "--job-id", "100"])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)


# ---------------------------------------------------------------------------
# Property 5: Query commands make no API calls
# ---------------------------------------------------------------------------

class TestQueryCommandsMakeNoAPICalls:
    """Property 5: Query commands make no API calls.

    For any query command invocation (query runs, query jobs, query failures),
    regardless of parameters, zero HTTP requests should be made to any external
    service. All data should come exclusively from the local cache.

    **Validates: Requirements 5.5**
    """

    @given(
        num_runs=st.integers(min_value=0, max_value=5),
        workflow=st.one_of(st.none(), st.sampled_from(["daily.yml", "weekly.yml"])),
        status=st.one_of(st.none(), st.sampled_from(["success", "failure", "cancelled"])),
        branch=st.one_of(st.none(), st.sampled_from(["unstable", "main"])),
    )
    @settings(max_examples=100)
    def test_query_runs_makes_no_api_calls(
        self, num_runs: int, workflow, status, branch
    ) -> None:
        """query runs never invokes _make_client, regardless of filter params."""
        db_path = _fresh_db()
        _populate_cache(db_path, num_runs=num_runs, num_jobs_per_run=1)

        runner = CliRunner()
        args = ["--db", db_path, "query", "runs"]
        if workflow is not None:
            args += ["--workflow", workflow]
        if status is not None:
            args += ["--status", status]
        if branch is not None:
            args += ["--branch", branch]

        with patch("valkey_oncall.cli._make_client") as mock_make_client:
            result = runner.invoke(cli, args)

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        mock_make_client.assert_not_called()

    @given(
        num_runs=st.integers(min_value=1, max_value=5),
        num_jobs=st.integers(min_value=1, max_value=3),
        failed_only=st.booleans(),
    )
    @settings(max_examples=100)
    def test_query_jobs_makes_no_api_calls(
        self, num_runs: int, num_jobs: int, failed_only: bool
    ) -> None:
        """query jobs never invokes _make_client, regardless of filter params."""
        db_path = _fresh_db()
        _populate_cache(db_path, num_runs=num_runs, num_jobs_per_run=num_jobs)

        runner = CliRunner()
        args = ["--db", db_path, "query", "jobs", "--run-id", "1"]
        if failed_only:
            args.append("--failed-only")

        with patch("valkey_oncall.cli._make_client") as mock_make_client:
            result = runner.invoke(cli, args)

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        mock_make_client.assert_not_called()

    @given(
        num_runs=st.integers(min_value=0, max_value=5),
        job_id_filter=st.one_of(st.none(), st.just(1001)),
        test_name=st.one_of(st.none(), st.just("%example%")),
    )
    @settings(max_examples=100)
    def test_query_failures_makes_no_api_calls(
        self, num_runs: int, job_id_filter, test_name
    ) -> None:
        """query failures never invokes _make_client, regardless of filter params."""
        db_path = _fresh_db()
        _populate_cache(db_path, num_runs=num_runs, num_jobs_per_run=1,
                        add_failures=True)

        runner = CliRunner()
        args = ["--db", db_path, "query", "failures"]
        if job_id_filter is not None:
            args += ["--job-id", str(job_id_filter)]
        if test_name is not None:
            args += ["--test-name", test_name]

        with patch("valkey_oncall.cli._make_client") as mock_make_client:
            result = runner.invoke(cli, args)

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        mock_make_client.assert_not_called()


# ---------------------------------------------------------------------------
# Imports needed for integration tests
# ---------------------------------------------------------------------------
from valkey_oncall.github_client import GitHubAPIError


# ---------------------------------------------------------------------------
# Task 7.4: CLI Integration Tests
# Requirements: 1.5, 1.6, 2.4, 2.5, 6.3
# ---------------------------------------------------------------------------

class TestCLIIntegration:
    """Integration tests for CLI commands covering success/error exit codes,
    JSON output structure, --db override, and GITHUB_TOKEN warning."""

    # 1. fetch-runs success
    def test_fetch_runs_success(self) -> None:
        db_path = _fresh_db()
        mock_client = MagicMock(repo="valkey-io/valkey")
        mock_client.get_workflow_runs.return_value = [_api_run(1), _api_run(2)]

        runner = CliRunner(env={"GITHUB_TOKEN": "fake-token"})
        with patch("valkey_oncall.cli._make_client", return_value=mock_client):
            result = runner.invoke(cli, ["--db", db_path, "fetch-runs", "--workflow", "daily"])

        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)

    # 2. fetch-runs API error
    def test_fetch_runs_api_error(self) -> None:
        db_path = _fresh_db()
        mock_client = MagicMock(repo="valkey-io/valkey")
        mock_client.get_workflow_runs.side_effect = GitHubAPIError(500, "Internal Server Error")

        runner = CliRunner(env={"GITHUB_TOKEN": "fake-token"}, mix_stderr=False)
        with patch("valkey_oncall.cli._make_client", return_value=mock_client):
            result = runner.invoke(cli, ["--db", db_path, "fetch-runs", "--workflow", "daily"])

        assert result.exit_code == 1
        assert "Error" in result.stderr

    # 3. fetch-jobs success
    def test_fetch_jobs_success(self) -> None:
        db_path = _fresh_db()
        # Pre-populate a run so FK constraint is satisfied
        cache = Cache(db_path)
        cache.store_runs([_map_run(_api_run(1), "valkey-io/valkey")])

        mock_client = MagicMock(repo="valkey-io/valkey")
        mock_client.get_jobs_for_run.return_value = [_api_job(1001), _api_job(1002)]

        runner = CliRunner(env={"GITHUB_TOKEN": "fake-token"})
        with patch("valkey_oncall.cli._make_client", return_value=mock_client):
            result = runner.invoke(cli, ["--db", db_path, "fetch-jobs", "--run-id", "1"])

        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)

    # 4. fetch-jobs API error
    def test_fetch_jobs_api_error(self) -> None:
        db_path = _fresh_db()
        mock_client = MagicMock(repo="valkey-io/valkey")
        mock_client.get_jobs_for_run.side_effect = GitHubAPIError(404, "Run not found")

        runner = CliRunner(env={"GITHUB_TOKEN": "fake-token"}, mix_stderr=False)
        with patch("valkey_oncall.cli._make_client", return_value=mock_client):
            result = runner.invoke(cli, ["--db", db_path, "fetch-jobs", "--run-id", "99999"])

        assert result.exit_code == 1
        assert "Error" in result.stderr

    # 5. fetch-log success
    def test_fetch_log_success(self) -> None:
        db_path = _fresh_db()
        cache = Cache(db_path)
        cache.store_runs([_map_run(_api_run(1), "valkey-io/valkey")])
        cache.store_jobs(1, [_map_job(_api_job(1001))])

        mock_client = MagicMock(repo="valkey-io/valkey")
        mock_client.get_job_log.return_value = "raw log line 1\nraw log line 2\n"

        runner = CliRunner(env={"GITHUB_TOKEN": "fake-token"})
        with patch("valkey_oncall.cli._make_client", return_value=mock_client):
            result = runner.invoke(cli, ["--db", db_path, "fetch-log", "--job-id", "1001"])

        assert result.exit_code == 0
        assert "raw log line 1" in result.output

    # 6. parse-log success
    def test_parse_log_success(self) -> None:
        db_path = _fresh_db()
        cache = Cache(db_path)
        cache.store_runs([_map_run(_api_run(1), "valkey-io/valkey")])
        cache.store_jobs(1, [_map_job(_api_job(1001))])
        cache.store_log(1001, "[err]: test_acl_setuser in tests/unit.tcl\nExpected OK but got ERR\n")

        mock_client = MagicMock(repo="valkey-io/valkey")
        runner = CliRunner(env={"GITHUB_TOKEN": "fake-token"}, mix_stderr=False)
        with patch("valkey_oncall.cli._make_client", return_value=mock_client):
            result = runner.invoke(cli, ["--db", db_path, "parse-log", "--job-id", "1001"])

        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)

    # 7. query runs JSON structure
    def test_query_runs_json_structure(self) -> None:
        db_path = _fresh_db()
        _populate_cache(db_path, num_runs=2, num_jobs_per_run=1)

        runner = CliRunner()
        result = runner.invoke(cli, ["--db", db_path, "query", "runs"])

        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)
        expected_keys = {"run_id", "workflow_file", "status", "branch", "commit_sha", "run_date", "duration_secs"}
        for item in parsed:
            assert expected_keys.issubset(item.keys()), f"Missing keys: {expected_keys - item.keys()}"

    # 8. query jobs JSON structure
    def test_query_jobs_json_structure(self) -> None:
        db_path = _fresh_db()
        _populate_cache(db_path, num_runs=1, num_jobs_per_run=2)

        runner = CliRunner()
        result = runner.invoke(cli, ["--db", db_path, "query", "jobs", "--run-id", "1"])

        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)
        expected_keys = {"job_id", "run_id", "name", "status", "conclusion"}
        for item in parsed:
            assert expected_keys.issubset(item.keys()), f"Missing keys: {expected_keys - item.keys()}"

    # 9. query failures JSON structure
    def test_query_failures_json_structure(self) -> None:
        db_path = _fresh_db()
        _populate_cache(db_path, num_runs=1, num_jobs_per_run=1, add_failures=True)

        runner = CliRunner()
        result = runner.invoke(cli, ["--db", db_path, "query", "failures"])

        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)
        assert len(parsed) > 0, "Expected at least one failure"
        expected_keys = {"job_id", "test_name", "error_summary", "log_lines"}
        for item in parsed:
            assert expected_keys.issubset(item.keys()), f"Missing keys: {expected_keys - item.keys()}"

    # 10. sync JSON structure
    def test_sync_json_structure(self) -> None:
        db_path = _fresh_db()
        mock_client = MagicMock(repo="valkey-io/valkey")
        mock_client.get_workflow_runs.return_value = [_api_run(1, conclusion="failure")]
        mock_client.get_jobs_for_run.return_value = [_api_job(1001, conclusion="failure")]
        mock_client.get_job_log.return_value = "some log\n"

        runner = CliRunner(env={"GITHUB_TOKEN": "fake-token"})
        with patch("valkey_oncall.cli._make_client", return_value=mock_client):
            result = runner.invoke(cli, ["--db", db_path, "sync", "--workflow", "daily"])

        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert isinstance(parsed, dict)
        expected_keys = {"new_runs_fetched", "new_jobs_fetched", "new_logs_fetched", "new_failures_parsed", "errors"}
        assert expected_keys.issubset(parsed.keys()), f"Missing keys: {expected_keys - parsed.keys()}"

    # 11. --db option overrides default path
    def test_db_option_overrides_default(self, tmp_path) -> None:
        custom_db = str(tmp_path / "custom" / "my.db")

        runner = CliRunner()
        result = runner.invoke(cli, ["--db", custom_db, "query", "runs"])

        assert result.exit_code == 0
        from pathlib import Path
        assert Path(custom_db).exists(), "Custom DB file was not created at the specified path"

    # 12. stderr warning when GITHUB_TOKEN is absent (fetch-runs still works)
    def test_stderr_warning_when_no_github_token(self) -> None:
        db_path = _fresh_db()
        mock_client = MagicMock(repo="valkey-io/valkey")
        mock_client.get_workflow_runs.return_value = []

        runner = CliRunner(env={"GITHUB_TOKEN": ""}, mix_stderr=False)
        # Let _make_client run so the warning is emitted, but patch the
        # GitHubActionsClient constructor to avoid real HTTP setup.
        with patch("valkey_oncall.cli.GitHubActionsClient", return_value=mock_client):
            result = runner.invoke(cli, ["--db", db_path, "fetch-runs", "--workflow", "daily"])

        assert result.exit_code == 0
        assert "GITHUB_TOKEN" in result.stderr
        assert "log" in result.stderr.lower()

    # 13. fetch-log fails without GITHUB_TOKEN
    def test_fetch_log_requires_token(self) -> None:
        db_path = _fresh_db()
        runner = CliRunner(env={"GITHUB_TOKEN": ""}, mix_stderr=False)
        result = runner.invoke(cli, ["--db", db_path, "fetch-log", "--job-id", "123"])

        assert result.exit_code == 1
        assert "GITHUB_TOKEN" in result.stderr
        assert "required" in result.stderr.lower()

    # 14. sync fails without GITHUB_TOKEN
    def test_sync_requires_token(self) -> None:
        db_path = _fresh_db()
        runner = CliRunner(env={"GITHUB_TOKEN": ""}, mix_stderr=False)
        result = runner.invoke(cli, ["--db", db_path, "sync", "--workflow", "daily"])

        assert result.exit_code == 1
        assert "GITHUB_TOKEN" in result.stderr
        assert "required" in result.stderr.lower()
