"""Service layer orchestrating sync between GitHub API, cache, and log parser."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from valkey_oncall.cache import Cache
from valkey_oncall.github_client import (
    GitHubActionsClient,
    GitHubAPIError,
    RateLimitError,
)
from valkey_oncall.log_parser import parse_job_log
from valkey_oncall.weekly import (
    WEEKLY_SPLIT_WORKFLOW,
    build_synthetic_runs,
    split_jobs_by_branch,
)

# Type alias for the optional progress callback
ProgressCallback = Optional[Callable[[str], None]]


# Workflow files used by valkey-io/valkey CI
_WORKFLOW_FILES = {
    "daily": "daily.yml",
    "weekly": "weekly.yml",
    "ci": "ci.yml",
}

# Trigger-event filter per workflow file. CI (ci.yml) fires on every push and
# every PR; we only want post-merge runs on the target branch, so we restrict
# it to the "push" event. Workflows absent from this map are fetched
# unfiltered (all events).
_WORKFLOW_EVENTS = {
    "ci.yml": "push",
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
        "workflow_file": api_run.get("path", "").rsplit("/", 1)[-1]
        if api_run.get("path")
        else api_run.get("name", ""),
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
            event=_WORKFLOW_EVENTS.get(workflow_file),
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
            workflow=workflow_file,
            branch=branch,
            since=since,
            until=until,
        )

    def fetch_jobs(self, run_id: int) -> List[Dict]:
        """Fetch jobs for a run from the API if not cached, return all.

        Ensures jobs are fetched and cached, then returns the full set
        of cached jobs for the run.  If previously cached jobs are still
        in progress, re-fetches from the API to pick up final results.
        """
        if not self._cache.has_jobs_for_run(run_id) or self._cache.has_incomplete_jobs(
            run_id
        ):
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

        job_name = self._cache.get_job_name(job_id)
        failures = parse_job_log(raw_log, job_name=job_name)
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

    def failures_summary(self, run_id: int, failed_only: bool = False) -> List[Dict]:
        """One-shot summary of jobs for a run with first error line per failed job.

        Fetches jobs (and their logs/failures) as needed, then returns a
        list of dicts with keys: job_id, name, conclusion, first_error.
        """
        jobs = self.fetch_jobs(run_id)
        results: List[Dict] = []

        for job in jobs:
            conclusion = job.get("conclusion")
            if failed_only and conclusion != "failure":
                continue

            first_error: Optional[str] = None
            if conclusion == "failure":
                job_id = job["job_id"]
                # Ensure log is fetched and parsed
                if not self._cache.has_log(job_id):
                    try:
                        self.fetch_log(job_id)
                    except Exception:
                        pass
                if not self._cache.has_failures_for_job(job_id):
                    try:
                        self.parse_log(job_id)
                    except Exception:
                        pass
                # Grab the first failure's error summary
                failures = self._cache.query_failures(job_id=job_id)
                if failures:
                    first_error = failures[0].get("error_summary", "")

            results.append(
                {
                    "job_id": job["job_id"],
                    "name": job["name"],
                    "conclusion": conclusion or "running",
                    "first_error": first_error,
                }
            )

        return results

    def fetch_log_grep(
        self,
        job_id: int,
        pattern: str,
        context: int = 0,
    ) -> List[str]:
        """Fetch a job log and return only lines matching *pattern*.

        *pattern* is compiled as a regex (case-insensitive).  When
        *context* > 0, surrounding lines are included (like ``grep -C``).
        """
        raw_log = self.fetch_log(job_id)
        lines = raw_log.splitlines()
        regex = re.compile(pattern, re.IGNORECASE)

        if context <= 0:
            return [line for line in lines if regex.search(line)]

        # Collect indices of matching lines, then expand with context
        match_indices = {i for i, line in enumerate(lines) if regex.search(line)}
        include = set()
        for idx in match_indices:
            for offset in range(-context, context + 1):
                include.add(idx + offset)

        return [lines[i] for i in sorted(include) if 0 <= i < len(lines)]

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
            # Set True if a fetch fails with an authentication error (401, or a
            # non-rate-limit 403). A dead/expired/under-scoped token trips this,
            # and the CLI treats it as fatal so the failure is never silent.
            "auth_failed": False,
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
                existing_run_ids = {
                    r["run_id"]
                    for r in self._cache.query_runs(
                        repo=self._client.repo, workflow=workflow_file
                    )
                }
                api_runs = self._client.get_workflow_runs(
                    workflow_file=workflow_file,
                    branch=branch,
                    created_after=since,
                    created_before=until,
                    event=_WORKFLOW_EVENTS.get(workflow_file),
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
                if (
                    isinstance(exc, GitHubAPIError)
                    and not isinstance(exc, RateLimitError)
                    and exc.status_code in (401, 403)
                ):
                    summary["auth_failed"] = True
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

        # Re-parse any cached logs that lost their parse results (e.g. parser version bump)
        unparsed_jobs = self._cache.query_unparsed_jobs_with_logs()
        if unparsed_jobs:
            _log(
                f"Re-parsing {len(unparsed_jobs)} jobs with stale/missing parse results..."
            )
            for job_id in unparsed_jobs:
                try:
                    failures = self.parse_log(job_id)
                    summary["new_failures_parsed"] += len(failures)
                except Exception as exc:
                    summary["errors"].append(f"reparse(job={job_id}): {exc}")

        _log(
            f"Sync complete: {summary['new_runs_fetched']} runs, "
            f"{summary['new_failures_parsed']} failures parsed"
        )
        return summary

    # ------------------------------------------------------------------
    # Weekly release-branch ingest (budgeted, resumable)
    # ------------------------------------------------------------------

    def sync_weekly_branches(
        self,
        budget: int = 300,
        progress: ProgressCallback = None,
    ) -> Dict:
        """Ingest weekly release-branch runs as per-branch synthetic series.

        Each ``weekly.yml`` run fans out ``daily.yml`` per release branch,
        with all jobs in one run distinguished by a job-name prefix. This
        pass splits every weekly run into synthetic per-branch runs
        (``workflow_file='weekly-split'``, ``branch='X.Y'``) and fetches
        failure logs where GitHub still retains them (~90 days).

        Bounded by *budget* API calls per invocation; processing is
        newest-first with per-run completion markers, so successive
        invocations make monotonic progress through history and steady
        state costs only the newest run. Cached data (from prior passes
        or the generic sync) is reused at zero API cost.
        """

        def _log(msg: str) -> None:
            if progress:
                progress(msg)

        start_requests = self._client.requests_made

        def _budget_left() -> int:
            return budget - (self._client.requests_made - start_requests)

        summary: Dict = {
            "runs_split": 0,
            "runs_completed": 0,
            "logs_fetched": 0,
            "logs_expired": 0,
            "failures_parsed": 0,
            "budget_used": 0,
            "budget_exhausted": False,
            "errors": [],
            "auth_failed": False,
        }

        # Refresh the weekly run list (scheduled runs only — PR-triggered
        # runs of the workflow file carry no release-branch fan-out).
        try:
            self.fetch_runs("weekly", branch="unstable")
        except Exception as exc:
            summary["errors"].append(f"fetch_runs(weekly): {exc}")
            if (
                isinstance(exc, GitHubAPIError)
                and not isinstance(exc, RateLimitError)
                and exc.status_code in (401, 403)
            ):
                summary["auth_failed"] = True
            summary["budget_used"] = self._client.requests_made - start_requests
            return summary

        weekly_runs = [
            r
            for r in self._cache.query_runs(
                repo=self._client.repo, workflow="weekly.yml", branch="unstable"
            )
            if r["status"] in ("success", "failure")
        ]  # newest-first (query_runs orders by run_date DESC)

        for run in weekly_runs:
            run_id = run["run_id"]
            status = self._cache.get_weekly_ingest_status(run_id)
            if status == "done":
                continue
            if _budget_left() <= 0:
                summary["budget_exhausted"] = True
                _log(f"Budget exhausted before run {run_id}; resuming next sync")
                break

            # ---- Tier 1: jobs + synthetic per-branch runs ----
            if status is None:
                _log(f"Splitting weekly run {run_id} ({run['run_date'][:10]})...")
                try:
                    jobs = self.fetch_jobs(run_id)
                except Exception as exc:
                    summary["errors"].append(f"fetch_jobs(run={run_id}): {exc}")
                    continue
                by_branch = split_jobs_by_branch(jobs)
                if not by_branch:
                    _log(f"  No release-branch jobs in run {run_id}; marking done")
                    self._cache.set_weekly_ingest_status(run_id, "done")
                    summary["runs_completed"] += 1
                    continue
                synthetic = build_synthetic_runs(run, by_branch)
                self._cache.store_runs(synthetic)
                # Re-point each branch's jobs at its synthetic run (job_id is
                # the PK, so this re-homes rows; logs/failures key off job_id
                # and are unaffected).
                for srun in synthetic:
                    branch = srun["branch"]
                    self._cache.store_jobs(srun["run_id"], by_branch[branch])
                self._cache.set_weekly_ingest_status(run_id, "split")
                summary["runs_split"] += 1
                status = "split"

            # ---- Tier 2: failure logs (where retention allows) ----
            failed_jobs: List[Dict] = []
            for srun in self._cache.query_runs(
                repo=self._client.repo, workflow=WEEKLY_SPLIT_WORKFLOW
            ):
                if abs(srun["run_id"]) // 100 == run_id:
                    failed_jobs.extend(
                        self._cache.query_jobs(srun["run_id"], failed_only=True)
                    )

            pending = [
                j
                for j in failed_jobs
                if not self._cache.has_failures_for_job(j["job_id"])
            ]
            exhausted_mid_run = False
            for job in pending:
                job_id = job["job_id"]
                if _budget_left() <= 0:
                    summary["budget_exhausted"] = True
                    exhausted_mid_run = True
                    _log(f"Budget exhausted mid-run {run_id}; resuming next sync")
                    break
                try:
                    if not self._cache.has_log(job_id):
                        self.fetch_log(job_id)
                        summary["logs_fetched"] += 1
                    failures = self.parse_log(job_id)
                    summary["failures_parsed"] += len(failures)
                except RateLimitError as exc:
                    summary["errors"].append(f"fetch_log(job={job_id}): {exc}")
                    summary["budget_exhausted"] = True
                    exhausted_mid_run = True
                    break
                except GitHubAPIError as exc:
                    if exc.status_code in (404, 410):
                        # Log fell out of GitHub's retention window; job
                        # conclusions still feed the summary tier.
                        self._cache.mark_log_expired(job_id)
                        summary["logs_expired"] += 1
                    else:
                        summary["errors"].append(f"fetch_log(job={job_id}): {exc}")
                        if exc.status_code in (401, 403):
                            summary["auth_failed"] = True
                            exhausted_mid_run = True
                            break
                except Exception as exc:
                    summary["errors"].append(f"log(job={job_id}): {exc}")

            if summary["auth_failed"]:
                break
            if not exhausted_mid_run:
                self._cache.set_weekly_ingest_status(run_id, "done")
                summary["runs_completed"] += 1

        summary["budget_used"] = self._client.requests_made - start_requests
        _log(
            f"Weekly ingest: {summary['runs_split']} runs split, "
            f"{summary['runs_completed']} completed, "
            f"{summary['logs_fetched']} logs fetched, "
            f"{summary['logs_expired']} expired, "
            f"budget {summary['budget_used']}/{budget}"
        )
        return summary
