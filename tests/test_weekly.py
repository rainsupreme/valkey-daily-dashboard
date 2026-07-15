"""Tests for weekly release-branch splitting and budgeted ingest."""

from __future__ import annotations

from typing import Dict, List

from valkey_oncall.cache import Cache
from valkey_oncall.github_client import GitHubAPIError
from valkey_oncall.service import OnCallService
from valkey_oncall.weekly import (
    WEEKLY_SPLIT_WORKFLOW,
    build_synthetic_runs,
    parse_branch_job,
    split_jobs_by_branch,
    synthetic_run_id,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _api_weekly_run(run_id: int, date: str = "2026-07-12") -> Dict:
    return {
        "id": run_id,
        "path": ".github/workflows/weekly.yml",
        "name": "Weekly Test Workflow for Released Branches",
        "conclusion": "failure",
        "status": "completed",
        "head_branch": "unstable",
        "head_sha": f"sha_{run_id}",
        "run_started_at": f"{date}T06:00:00Z",
        "created_at": f"{date}T06:00:00Z",
        "updated_at": f"{date}T18:00:00Z",
    }


def _api_branch_job(job_id: int, branch: str, name: str, conclusion: str) -> Dict:
    return {
        "id": job_id,
        "name": f"run-daily-for-release-branches ({branch}) / {name}",
        "status": "completed",
        "conclusion": conclusion,
    }


class _CountingClient:
    """Fake GitHubActionsClient that counts requests like the real one."""

    def __init__(self, runs: List[Dict], jobs: Dict[int, List[Dict]], logs=None):
        self.repo = "valkey-io/valkey"
        self.requests_made = 0
        self._runs = runs
        self._jobs = jobs
        self._logs = logs or {}

    def get_workflow_runs(self, **kwargs) -> List[Dict]:
        self.requests_made += 1
        return self._runs

    def get_jobs_for_run(self, run_id: int) -> List[Dict]:
        self.requests_made += 1
        return self._jobs.get(run_id, [])

    def get_job_log(self, job_id: int) -> str:
        self.requests_made += 1
        result = self._logs.get(job_id, "")
        if isinstance(result, Exception):
            raise result
        return result


# ---------------------------------------------------------------------------
# Splitter unit tests
# ---------------------------------------------------------------------------


class TestParseBranchJob:
    def test_parses_branch_and_strips_prefix(self) -> None:
        assert parse_branch_job(
            "run-daily-for-release-branches (8.0) / test-ubuntu"
        ) == ("8.0", "test-ubuntu")

    def test_setup_job_returns_none(self) -> None:
        assert parse_branch_job("determine-release-branches") is None

    def test_empty_and_none_safe(self) -> None:
        assert parse_branch_job("") is None
        assert parse_branch_job(None) is None


class TestSplitJobsByBranch:
    def test_partitions_and_drops_setup(self) -> None:
        jobs = [
            {
                "job_id": 1,
                "name": "determine-release-branches",
                "conclusion": "success",
            },
            {
                "job_id": 2,
                "name": "run-daily-for-release-branches (7.2) / build",
                "conclusion": "success",
            },
            {
                "job_id": 3,
                "name": "run-daily-for-release-branches (8.0) / build",
                "conclusion": "failure",
            },
        ]
        by = split_jobs_by_branch(jobs)
        assert set(by) == {"7.2", "8.0"}
        assert by["7.2"][0]["name"] == "build"
        assert by["8.0"][0]["conclusion"] == "failure"


class TestBuildSyntheticRuns:
    def test_per_branch_status_and_stable_ids(self) -> None:
        run = {
            "run_id": 555,
            "repo": "valkey-io/valkey",
            "run_date": "2026-07-12T06:00:00Z",
            "duration_secs": 100,
        }
        by = {
            "8.0": [{"job_id": 1, "conclusion": "failure"}],
            "7.2": [{"job_id": 2, "conclusion": "success"}],
        }
        runs = build_synthetic_runs(run, by)
        assert [r["branch"] for r in runs] == ["7.2", "8.0"]
        assert runs[0]["status"] == "success"
        assert runs[1]["status"] == "failure"
        assert runs[0]["run_id"] == synthetic_run_id(555, 0)
        assert runs[1]["run_id"] == synthetic_run_id(555, 1)
        assert all(r["workflow_file"] == WEEKLY_SPLIT_WORKFLOW for r in runs)
        assert all(r["run_id"] < 0 for r in runs)
        assert all(r["commit_sha"] == "" for r in runs)


# ---------------------------------------------------------------------------
# Budgeted ingest tests
# ---------------------------------------------------------------------------


def _two_branch_setup(temp_db_path: str, log_result="[err]: Test foo in tests/t"):
    """One weekly run, two branches, one failed job per branch."""
    runs = [_api_weekly_run(1000)]
    jobs = {
        1000: [
            {
                "id": 1,
                "name": "determine-release-branches",
                "status": "completed",
                "conclusion": "success",
            },
            _api_branch_job(11, "7.2", "test-a", "failure"),
            _api_branch_job(12, "8.0", "test-a", "success"),
        ]
    }
    logs = {11: log_result}
    client = _CountingClient(runs, jobs, logs)
    cache = Cache(temp_db_path)
    return OnCallService(client, cache), client, cache


class TestSyncWeeklyBranches:
    def test_full_ingest_creates_synthetic_series(self, temp_db_path: str) -> None:
        svc, client, cache = _two_branch_setup(temp_db_path)
        summary = svc.sync_weekly_branches(budget=100)
        assert summary["runs_split"] == 1
        assert summary["runs_completed"] == 1
        assert not summary["budget_exhausted"]

        split_72 = cache.query_runs(workflow=WEEKLY_SPLIT_WORKFLOW, branch="7.2")
        split_80 = cache.query_runs(workflow=WEEKLY_SPLIT_WORKFLOW, branch="8.0")
        assert len(split_72) == 1 and split_72[0]["status"] == "failure"
        assert len(split_80) == 1 and split_80[0]["status"] == "success"
        # Jobs re-homed to synthetic runs with stripped names
        jobs_72 = cache.query_jobs(split_72[0]["run_id"])
        assert [j["name"] for j in jobs_72] == ["test-a"]

    def test_second_sync_is_nearly_free(self, temp_db_path: str) -> None:
        svc, client, cache = _two_branch_setup(temp_db_path)
        svc.sync_weekly_branches(budget=100)
        used_first = client.requests_made
        summary = svc.sync_weekly_branches(budget=100)
        # Only the run-list refresh should hit the API on the second pass.
        assert client.requests_made - used_first == 1
        assert summary["runs_split"] == 0
        assert summary["runs_completed"] == 0

    def test_budget_exhaustion_resumes_next_sync(self, temp_db_path: str) -> None:
        svc, client, cache = _two_branch_setup(temp_db_path)
        # Budget 2: run-list (1) + jobs (1) -> exhausted before the log fetch.
        summary1 = svc.sync_weekly_branches(budget=2)
        assert summary1["runs_split"] == 1
        assert summary1["budget_exhausted"]
        assert cache.get_weekly_ingest_status(1000) == "split"

        summary2 = svc.sync_weekly_branches(budget=100)
        assert summary2["logs_fetched"] == 1
        assert summary2["runs_completed"] == 1
        assert cache.get_weekly_ingest_status(1000) == "done"

    def test_expired_log_marks_and_completes(self, temp_db_path: str) -> None:
        svc, client, cache = _two_branch_setup(
            temp_db_path, log_result=GitHubAPIError(410, "Gone")
        )
        summary = svc.sync_weekly_branches(budget=100)
        assert summary["logs_expired"] == 1
        assert summary["runs_completed"] == 1
        # Marked so it is never re-fetched
        assert cache.has_failures_for_job(11)

    def test_reuses_jobs_cached_by_generic_sync(self, temp_db_path: str) -> None:
        """Jobs already cached under the real run_id cost zero API calls."""
        svc, client, cache = _two_branch_setup(temp_db_path)
        # Simulate the generic sync having cached the weekly run's jobs.
        from valkey_oncall.service import _map_job, _map_run

        cache.store_runs([_map_run(_api_weekly_run(1000), client.repo)])
        cache.store_jobs(1000, [_map_job(j) for j in client._jobs[1000]])
        summary = svc.sync_weekly_branches(budget=100)
        # run-list refresh (1) + log fetch (1); no jobs fetch.
        assert client.requests_made == 2
        assert summary["runs_split"] == 1
        assert summary["runs_completed"] == 1
