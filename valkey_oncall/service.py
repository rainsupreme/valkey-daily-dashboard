"""Service layer orchestrating sync between GitHub API, cache, and log parser."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from valkey_oncall.cache import Cache
from valkey_oncall.github_client import GitHubActionsClient
from valkey_oncall.log_parser import parse_job_log


# Type alias for the optional progress callback
ProgressCallback = Optional[Callable[[str], None]]


# Workflow files used by valkey-io/valkey CI
_WORKFLOW_FILES = {
    "daily": "daily.yml",
    "weekly": "weekly.yml",
}


def _compute_duration(run: Dict) -> Optional[int]:
    """Compute run duration in seconds from API timestamps.

    Uses ``run_started_at`` and ``updated_at``.  Returns *None* when the
    required fields are missing or unparseable.
    """
    started = run.get("run_started_at") or run.get("created_at")
    ended = run.get("updated_at")
    if not started or not ended:
        return None
    try:
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        start_dt = datetime.strptime(started, fmt).replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(ended, fmt).replace(tzinfo=timezone.utc)
        delta = int((end_dt - start_dt).total_seconds())
        return delta if delta >= 0 else None
    except (ValueError, TypeError):
        return None


def _map_run(api_run: Dict, repo: str) -> Dict:
    """Map a GitHub API workflow-run dict to our cache schema."""
    return {
        "run_id": api_run["id"],
        "repo": repo,
        "workflow_file": api_run.get("path", "").rsplit("/", 1)[-1] if api_run.get("path") else api_run.get("name", ""),
        "status": api_run.get("conclusion") or api_run.get("status", "unknown"),
        "branch": api_run.get("head_branch", ""),
        "commit_sha": api_run.get("head_sha", ""),
        "run_date": api_run.get("run_started_at") or api_run.get("created_at", ""),
        "duration_secs": _compute_duration(api_run),
        "raw_json": json.dumps(api_run),
    }


def _map_job(api_job: Dict) -> Dict:
    """Map a GitHub API job dict to our cache schema."""
    return {
        "job_id": api_job["id"],
        "name": api_job.get("name", ""),
        "status": api_job.get("status", ""),
        "conclusion": api_job.get("conclusion"),
        "raw_json": json.dumps(api_job),
    }


class OnCallService:
    """Orchestrates fetching, caching, and parsing of Valkey CI data."""

    def __init__(self, client: GitHubActionsClient, cache: Cache) -> None:
        self._client = client
        self._cache = cache

    # ------------------------------------------------------------------
    # Individual fetch / parse operations
    # ------------------------------------------------------------------

    def fetch_runs(
        self,
        workflow: str,
        branch: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> List[Dict]:
        """Fetch workflow runs from the API, store new ones, return all.

        Fetches from the API, stores any runs not already cached, then
        returns the full set of cached runs matching the given filters.
        """
        workflow_file = _WORKFLOW_FILES.get(workflow, workflow)
        api_runs = self._client.get_workflow_runs(
            workflow_file=workflow_file,
            branch=branch,
            created_after=since,
            created_before=until,
        )
        new_runs: List[Dict] = []
        for raw in api_runs:
            run = _map_run(raw, self._client.repo)
            if not self._cache.has_run(run["run_id"]):
                new_runs.append(run)

        if new_runs:
            self._cache.store_runs(new_runs)

        return self._cache.query_runs(
            repo=self._client.repo,
            workflow=workflow_file, branch=branch, since=since, until=until,
        )

    def fetch_jobs(self, run_id: int) -> List[Dict]:
        """Fetch jobs for a run from the API if not cached, return all.

        Ensures jobs are fetched and cached, then returns the full set
        of cached jobs for the run.  If previously cached jobs are still
        in progress, re-fetches from the API to pick up final results.
        """
        if not self._cache.has_jobs_for_run(run_id) or self._cache.has_incomplete_jobs(run_id):
            api_jobs = self._client.get_jobs_for_run(run_id)
            mapped: List[Dict] = [_map_job(j) for j in api_jobs]
            if mapped:
                self._cache.store_jobs(run_id, mapped)

        return self._cache.query_jobs(run_id)

    def fetch_log(self, job_id: int) -> str:
        """Return the raw log for a job, fetching from API only if not cached."""
        cached = self._cache.get_log(job_id)
        if cached is not None:
            return cached

        raw_log = self._client.get_job_log(job_id)
        self._cache.store_log(job_id, raw_log)
        return raw_log

    def parse_log(self, job_id: int) -> List[Dict]:
        """Parse a cached log for test failures, store results."""
        raw_log = self._cache.get_log(job_id)
        if raw_log is None:
            return []

        failures = parse_job_log(raw_log)
        if failures:
            failure_dicts = [
                {
                    "test_name": f.test_name,
                    "error_summary": f.error_summary,
                    "log_lines": f.log_lines,
                }
                for f in failures
            ]
            self._cache.store_failures(job_id, failure_dicts)
            return failure_dicts

        # No recognisable failures — mark as unparseable
        self._cache.mark_unparseable(job_id)
        return []

    # ------------------------------------------------------------------
    # Full incremental sync
    # ------------------------------------------------------------------

    def sync(
        self,
        workflow: Optional[str] = None,
        branch: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        progress: ProgressCallback = None,
    ) -> Dict:
        """Run a full incremental sync and return a summary dict.

        For each workflow type (or the one specified), fetch new runs.
        For each *failed* new run, fetch jobs.  For each *failed* job
        whose log is not yet cached, fetch the log and parse it.

        Non-fatal errors (e.g. a single log fetch failing) are collected
        in the summary rather than aborting the whole sync.

        If *progress* is provided, it is called with human-readable status
        messages during the sync (intended for stderr output).
        """
        def _log(msg: str) -> None:
            if progress:
                progress(msg)

        summary: Dict = {
            "new_runs_fetched": 0,
            "new_jobs_fetched": 0,
            "new_logs_fetched": 0,
            "new_failures_parsed": 0,
            "errors": [],
        }

        workflows: List[str]
        if workflow:
            workflows = [workflow]
        else:
            workflows = list(_WORKFLOW_FILES.keys())

        for wf in workflows:
            workflow_file = _WORKFLOW_FILES.get(wf, wf)
            _log(f"Fetching {wf} runs...")

            # Snapshot cached run IDs before fetching so we can count new ones
            try:
                existing_run_ids = {r["run_id"] for r in self._cache.query_runs(repo=self._client.repo, workflow=workflow_file)}
                api_runs = self._client.get_workflow_runs(
                    workflow_file=workflow_file,
                    branch=branch,
                    created_after=since,
                    created_before=until,
                )
                new_runs: List[Dict] = []
                for raw in api_runs:
                    run = _map_run(raw, self._client.repo)
                    if run["run_id"] not in existing_run_ids:
                        new_runs.append(run)
                if new_runs:
                    self._cache.store_runs(new_runs)
            except Exception as exc:
                summary["errors"].append(f"fetch_runs({wf}): {exc}")
                _log(f"  Error fetching {wf} runs: {exc}")
                continue

            _log(f"  {len(new_runs)} new runs")
            summary["new_runs_fetched"] += len(new_runs)

            failed_runs = [r for r in new_runs if r["status"] == "failure"]
            if failed_runs:
                _log(f"  {len(failed_runs)} failed runs to process")

            for run in failed_runs:
                run_id = run["run_id"]
                _log(f"  Fetching jobs for run {run_id}...")
                try:
                    had_jobs = self._cache.has_jobs_for_run(run_id)
                    jobs = self.fetch_jobs(run_id)
                    new_job_count = 0 if had_jobs else len(jobs)
                except Exception as exc:
                    summary["errors"].append(f"fetch_jobs(run={run_id}): {exc}")
                    _log(f"    Error: {exc}")
                    continue
                summary["new_jobs_fetched"] += new_job_count

                failed_jobs = [j for j in jobs if j.get("conclusion") == "failure"]
                _log(f"    {len(failed_jobs)} failed jobs")

                for job in failed_jobs:
                    job_id = job["job_id"]
                    job_name = job.get("name", str(job_id))

                    # Fetch log
                    try:
                        if not self._cache.has_log(job_id):
                            _log(f"    Fetching log for {job_name}...")
                            self.fetch_log(job_id)
                            summary["new_logs_fetched"] += 1
                    except Exception as exc:
                        summary["errors"].append(f"fetch_log(job={job_id}): {exc}")
                        _log(f"    Error fetching log: {exc}")
                        continue

                    # Parse log
                    try:
                        if not self._cache.has_failures_for_job(job_id):
                            _log(f"    Parsing log for {job_name}...")
                            failures = self.parse_log(job_id)
                            summary["new_failures_parsed"] += len(failures)
                            _log(f"      {len(failures)} failures found")
                    except Exception as exc:
                        summary["errors"].append(f"parse_log(job={job_id}): {exc}")
                        _log(f"    Error parsing log: {exc}")

        _log(f"Sync complete: {summary['new_runs_fetched']} runs, "
             f"{summary['new_failures_parsed']} failures parsed")
        return summary
