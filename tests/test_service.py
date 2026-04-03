"""Unit tests for the OnCallService layer."""

from __future__ import annotations

import json
from typing import Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from valkey_oncall.cache import Cache
from valkey_oncall.service import OnCallService, _map_run, _map_job


# ---------------------------------------------------------------------------
# Helpers — fake GitHub API responses
# ---------------------------------------------------------------------------

def _api_run(run_id: int, conclusion: str = "failure", branch: str = "unstable") -> Dict:
    """Return a dict resembling a GitHub API workflow-run object."""
    return {
        "id": run_id,
        "path": ".github/workflows/daily.yml",
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


def _make_service(temp_db_path: str, client: Optional[MagicMock] = None) -> OnCallService:
    cache = Cache(temp_db_path)
    if client is None:
        client = MagicMock(repo="valkey-io/valkey")
    return OnCallService(client, cache)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFetchRunsReturnsCachedAndNew:
    """fetch_runs should return all matching runs (cached + newly fetched)."""

    def test_returns_cached_and_new(self, temp_db_path: str) -> None:
        cache = Cache(temp_db_path)
        # Pre-populate cache with run 1
        cache.store_runs([_map_run(_api_run(1), "valkey-io/valkey")])

        client = MagicMock(repo="valkey-io/valkey")
        # API returns run 1 (cached) and run 2 (new)
        client.get_workflow_runs.return_value = [_api_run(1), _api_run(2)]

        svc = OnCallService(client, cache)
        runs = svc.fetch_runs("daily")

        run_ids = {r["run_id"] for r in runs}
        assert 1 in run_ids
        assert 2 in run_ids
        assert cache.has_run(2)

    def test_all_cached_returns_cached(self, temp_db_path: str) -> None:
        cache = Cache(temp_db_path)
        cache.store_runs([_map_run(_api_run(1), "valkey-io/valkey")])

        client = MagicMock(repo="valkey-io/valkey")
        client.get_workflow_runs.return_value = [_api_run(1)]

        svc = OnCallService(client, cache)
        runs = svc.fetch_runs("daily")
        assert len(runs) == 1
        assert runs[0]["run_id"] == 1


class TestFetchJobsReturnsCached:
    """fetch_jobs should return cached jobs without re-fetching from API."""

    def test_returns_cached_jobs_without_api_call(self, temp_db_path: str) -> None:
        cache = Cache(temp_db_path)
        cache.store_runs([_map_run(_api_run(1), "valkey-io/valkey")])
        cache.store_jobs(1, [_map_job(_api_job(100))])

        client = MagicMock(repo="valkey-io/valkey")
        svc = OnCallService(client, cache)
        result = svc.fetch_jobs(1)

        assert len(result) == 1
        assert result[0]["job_id"] == 100
        client.get_jobs_for_run.assert_not_called()

    def test_fetches_and_returns_when_not_cached(self, temp_db_path: str) -> None:
        cache = Cache(temp_db_path)
        cache.store_runs([_map_run(_api_run(1), "valkey-io/valkey")])

        client = MagicMock(repo="valkey-io/valkey")
        client.get_jobs_for_run.return_value = [_api_job(200), _api_job(201)]

        svc = OnCallService(client, cache)
        result = svc.fetch_jobs(1)

        assert len(result) == 2
        client.get_jobs_for_run.assert_called_once_with(1)


class TestFetchLogReturnsCached:
    """fetch_log should return cached log without calling the API."""

    def test_returns_cached_log(self, temp_db_path: str) -> None:
        cache = Cache(temp_db_path)
        cache.store_runs([_map_run(_api_run(1), "valkey-io/valkey")])
        cache.store_jobs(1, [_map_job(_api_job(100))])
        cache.store_log(100, "cached log content")

        client = MagicMock(repo="valkey-io/valkey")
        svc = OnCallService(client, cache)
        log = svc.fetch_log(100)

        assert log == "cached log content"
        client.get_job_log.assert_not_called()

    def test_fetches_from_api_when_not_cached(self, temp_db_path: str) -> None:
        cache = Cache(temp_db_path)
        cache.store_runs([_map_run(_api_run(1), "valkey-io/valkey")])
        cache.store_jobs(1, [_map_job(_api_job(100))])

        client = MagicMock(repo="valkey-io/valkey")
        client.get_job_log.return_value = "fresh log"

        svc = OnCallService(client, cache)
        log = svc.fetch_log(100)

        assert log == "fresh log"
        client.get_job_log.assert_called_once_with(100)
        # Should now be cached
        assert cache.get_log(100) == "fresh log"


class TestSyncOrchestration:
    """sync should orchestrate the full pipeline correctly."""

    def test_full_sync_pipeline(self, temp_db_path: str) -> None:
        cache = Cache(temp_db_path)
        client = MagicMock(repo="valkey-io/valkey")

        # API returns one failed run
        client.get_workflow_runs.return_value = [_api_run(10, conclusion="failure")]
        # That run has one failed job and one successful job
        client.get_jobs_for_run.return_value = [
            _api_job(500, conclusion="failure"),
            _api_job(501, conclusion="success"),
        ]
        # The failed job's log contains a recognisable failure pattern
        client.get_job_log.return_value = (
            "[err]: some_test in tests/unit.tcl\n"
            "Expected OK but got ERR\n"
        )

        svc = OnCallService(client, cache)
        summary = svc.sync(workflow="daily")

        assert summary["new_runs_fetched"] == 1
        assert summary["new_jobs_fetched"] == 2
        assert summary["new_logs_fetched"] == 1
        assert summary["new_failures_parsed"] >= 1
        assert summary["errors"] == []

    def test_sync_collects_nonfatal_errors(self, temp_db_path: str) -> None:
        cache = Cache(temp_db_path)
        client = MagicMock(repo="valkey-io/valkey")

        client.get_workflow_runs.return_value = [_api_run(20, conclusion="failure")]
        client.get_jobs_for_run.return_value = [_api_job(600, conclusion="failure")]
        # Log fetch fails
        client.get_job_log.side_effect = RuntimeError("network error")

        svc = OnCallService(client, cache)
        summary = svc.sync(workflow="daily")

        assert summary["new_runs_fetched"] == 1
        assert summary["new_jobs_fetched"] == 1
        assert summary["new_logs_fetched"] == 0
        assert len(summary["errors"]) == 1
        assert "network error" in summary["errors"][0]

    def test_sync_both_workflows_when_none_specified(self, temp_db_path: str) -> None:
        cache = Cache(temp_db_path)
        client = MagicMock(repo="valkey-io/valkey")
        client.get_workflow_runs.return_value = []

        svc = OnCallService(client, cache)
        summary = svc.sync()

        # Should call get_workflow_runs for both daily and weekly
        assert client.get_workflow_runs.call_count == 2
        assert summary["new_runs_fetched"] == 0
        assert summary["errors"] == []

    def test_sync_skips_success_runs(self, temp_db_path: str) -> None:
        cache = Cache(temp_db_path)
        client = MagicMock(repo="valkey-io/valkey")

        # Only successful runs — no jobs should be fetched
        client.get_workflow_runs.return_value = [_api_run(30, conclusion="success")]

        svc = OnCallService(client, cache)
        summary = svc.sync(workflow="daily")

        assert summary["new_runs_fetched"] == 1
        assert summary["new_jobs_fetched"] == 0
        client.get_jobs_for_run.assert_not_called()


# ---------------------------------------------------------------------------
# Property-based tests (hypothesis)
# ---------------------------------------------------------------------------

import tempfile
import os

from hypothesis import given, settings, assume
import hypothesis.strategies as st


def _disjoint_id_sets():
    """Strategy that produces two disjoint sets of positive integer IDs:
    (cached_ids, new_ids).  Both sets are non-empty.
    """
    return (
        st.lists(st.integers(min_value=1, max_value=10_000), min_size=1, max_size=10, unique=True)
        .flatmap(
            lambda cached: st.tuples(
                st.just(frozenset(cached)),
                st.lists(
                    st.integers(min_value=1, max_value=10_000).filter(lambda x: x not in cached),
                    min_size=1,
                    max_size=10,
                    unique=True,
                ).map(frozenset),
            )
        )
    )


def _fresh_db() -> str:
    """Create a fresh temporary SQLite DB path for each hypothesis example."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


class TestIncrementalSyncNeverRefetchesCached:
    """Property 2: Incremental sync never re-fetches cached data.

    **Validates: Requirements 1.4, 2.3, 3.3, 7.1, 7.2, 7.3**
    """

    @given(data=_disjoint_id_sets())
    @settings(max_examples=100)
    def test_fetch_runs_skips_cached_ids(self, data) -> None:
        """fetch_runs stores only new runs but returns all (cached + new)."""
        cached_ids, new_ids = data
        db_path = _fresh_db()

        cache = Cache(db_path)
        # Pre-populate cache with the "cached" runs
        cache.store_runs([_map_run(_api_run(rid), "valkey-io/valkey") for rid in cached_ids])

        # API returns ALL runs (cached + new)
        all_api_runs = [_api_run(rid) for rid in sorted(cached_ids | new_ids)]

        client = MagicMock(repo="valkey-io/valkey")
        client.get_workflow_runs.return_value = all_api_runs

        svc = OnCallService(client, cache)
        runs = svc.fetch_runs("daily")

        returned_ids = {r["run_id"] for r in runs}
        # Returned IDs must include both cached and new
        assert set(new_ids).issubset(returned_ids)
        assert set(cached_ids).issubset(returned_ids)

    @given(data=_disjoint_id_sets())
    @settings(max_examples=100)
    def test_fetch_jobs_skips_cached_runs(self, data) -> None:
        """get_jobs_for_run is NOT called for runs whose jobs are already cached."""
        cached_run_ids, new_run_ids = data
        db_path = _fresh_db()

        cache = Cache(db_path)
        all_run_ids = sorted(cached_run_ids | new_run_ids)

        # Store all runs in cache (they need to exist for FK)
        cache.store_runs([_map_run(_api_run(rid, conclusion="failure"), "valkey-io/valkey") for rid in all_run_ids])

        # Pre-populate jobs for the "cached" runs
        for rid in cached_run_ids:
            job_id = rid * 1000  # deterministic job ID
            cache.store_jobs(rid, [_map_job(_api_job(job_id))])

        client = MagicMock(repo="valkey-io/valkey")
        # For new runs, return a job when asked
        client.get_jobs_for_run.side_effect = lambda run_id: [_api_job(run_id * 1000 + 1)]

        svc = OnCallService(client, cache)

        # Call fetch_jobs for every run
        for rid in all_run_ids:
            svc.fetch_jobs(rid)

        # get_jobs_for_run should only have been called for new (non-cached) runs
        called_run_ids = {call.args[0] for call in client.get_jobs_for_run.call_args_list}
        assert called_run_ids == set(new_run_ids)
        assert called_run_ids.isdisjoint(cached_run_ids)

    @given(data=_disjoint_id_sets())
    @settings(max_examples=100)
    def test_fetch_log_skips_cached_jobs(self, data) -> None:
        """get_job_log is NOT called for jobs whose logs are already cached."""
        cached_job_ids, new_job_ids = data
        db_path = _fresh_db()

        cache = Cache(db_path)
        all_job_ids = sorted(cached_job_ids | new_job_ids)

        # We need a run to satisfy FK constraints
        run_id = 99999
        cache.store_runs([_map_run(_api_run(run_id), "valkey-io/valkey")])
        cache.store_jobs(run_id, [_map_job(_api_job(jid)) for jid in all_job_ids])

        # Pre-populate logs for cached jobs
        for jid in cached_job_ids:
            cache.store_log(jid, f"cached log for {jid}")

        client = MagicMock(repo="valkey-io/valkey")
        client.get_job_log.side_effect = lambda job_id: f"fresh log for {job_id}"

        svc = OnCallService(client, cache)

        # Fetch log for every job
        for jid in all_job_ids:
            svc.fetch_log(jid)

        # get_job_log should only have been called for new (non-cached) jobs
        called_job_ids = {call.args[0] for call in client.get_job_log.call_args_list}
        assert called_job_ids == set(new_job_ids)
        assert called_job_ids.isdisjoint(cached_job_ids)


class TestSyncRespectsWorkflowTypeFilter:
    """Property 8: Sync respects workflow type filter.

    **Validates: Requirements 7.5**
    """

    @given(workflow_type=st.sampled_from(["daily", "weekly"]))
    @settings(max_examples=100)
    def test_sync_only_fetches_specified_workflow(self, workflow_type: str) -> None:
        """When sync is called with a specific workflow type, only that
        workflow's file is passed to get_workflow_runs."""
        from valkey_oncall.service import _WORKFLOW_FILES

        db_path = _fresh_db()
        cache = Cache(db_path)

        expected_file = _WORKFLOW_FILES[workflow_type]

        # Build a run whose workflow_file matches the expected filter
        api_run = _api_run(1, conclusion="success", branch="unstable")
        # Override the path so _map_run produces the correct workflow_file
        api_run["path"] = f".github/workflows/{expected_file}"

        client = MagicMock(repo="valkey-io/valkey")
        client.get_workflow_runs.return_value = [api_run]

        svc = OnCallService(client, cache)
        svc.sync(workflow=workflow_type)

        # get_workflow_runs should be called exactly once, with the correct file
        client.get_workflow_runs.assert_called_once()
        call_kwargs = client.get_workflow_runs.call_args
        assert call_kwargs.kwargs.get("workflow_file") == expected_file or \
               (call_kwargs.args and call_kwargs.args[0] == expected_file) or \
               call_kwargs[1].get("workflow_file") == expected_file

        # All runs stored in cache should have the matching workflow_file
        stored = cache.query_runs()
        for run in stored:
            assert run["workflow_file"] == expected_file
