"""Blame narrowing: identify commits likely responsible for test regressions."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from valkey_oncall.cache import Cache
from valkey_oncall.github_client import GitHubActionsClient


def compute_blame(
    cache: Cache,
    client: GitHubActionsClient,
    days: int = 30,
    branch: str = "unstable",
    workflow: str = "daily.yml",
    repo: str = "valkey-io/valkey",
) -> List[Dict]:
    """For each test that has a green→red transition, identify blame candidates.

    Finds the last passing run before the first failure and uses the GitHub
    compare API to list commits between those two SHAs.

    Returns a list of blame records sorted by recency (newest regressions first).
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT00:00:00Z"
    )

    runs = cache.query_runs(repo=repo, workflow=workflow, branch=branch, since=since)
    # Deduplicate, sort chronologically
    seen_dates: set = set()
    valid_runs: List[Dict] = []
    for r in sorted(runs, key=lambda x: x["run_date"]):
        if r["status"] in ("in_progress", "queued", "skipped"):
            continue
        date_key = r["run_date"][:10]
        if date_key in seen_dates:
            continue
        seen_dates.add(date_key)
        valid_runs.append(r)

    if not valid_runs:
        return []

    # Build per-run failure sets
    run_failures: List[Dict] = []  # [{run, failing_tests: set}]
    for run in valid_runs:
        jobs = cache.query_jobs(run["run_id"], failed_only=True)
        failing = set()
        for job in jobs:
            for f in cache.query_failures(job_id=job["job_id"]):
                failing.add(f["test_name"])
        run_failures.append({"run": run, "failing_tests": failing})

    # For each test, find the first appearance (green→red transition)
    # A transition is: test NOT in run[i-1] failures, but IS in run[i] failures
    all_tests = set()
    for rf in run_failures:
        all_tests.update(rf["failing_tests"])

    blame_records: List[Dict] = []
    for test_name in all_tests:
        # Find the first run where this test failed
        first_fail_idx = None
        for i, rf in enumerate(run_failures):
            if test_name in rf["failing_tests"]:
                first_fail_idx = i
                break

        if first_fail_idx is None:
            continue

        # The "last green" is the run immediately before first_fail_idx
        if first_fail_idx == 0:
            # Test was already failing at the start of our window — no transition visible
            blame_records.append({
                "test_name": test_name,
                "regression_date": run_failures[0]["run"]["run_date"][:10],
                "first_fail_sha": run_failures[0]["run"].get("commit_sha", ""),
                "last_pass_sha": None,
                "blame_commits": [],
                "note": "Already failing at start of window — extend --days for full history",
            })
            continue

        last_pass = run_failures[first_fail_idx - 1]["run"]
        first_fail = run_failures[first_fail_idx]["run"]

        base_sha = last_pass.get("commit_sha", "")
        head_sha = first_fail.get("commit_sha", "")

        commits: List[Dict] = []
        if base_sha and head_sha and base_sha != head_sha:
            try:
                commits = client.compare_commits(base_sha, head_sha)
            except Exception:
                pass

        blame_records.append({
            "test_name": test_name,
            "regression_date": first_fail["run_date"][:10],
            "last_pass_date": last_pass["run_date"][:10],
            "first_fail_sha": head_sha,
            "last_pass_sha": base_sha,
            "blame_commits": commits,
            "commit_count": len(commits),
        })

    # Sort by regression date descending (newest regressions first)
    blame_records.sort(key=lambda r: r.get("regression_date", ""), reverse=True)
    return blame_records
