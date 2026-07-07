"""Per-test flakiness scorecards computed from cached CI data."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from valkey_oncall.cache import Cache
from valkey_oncall.log_parser import sanitize_cached_failure


def _classify(rate: float) -> str:
    """Classify a failure rate into a flakiness category.

    Bands (rate = fraction of runs in which the test failed):
      * persistent (>= 50%): fails the majority of runs -- a clean
        green->red boundary, so blame is reliable; treat as a real
        regression.
      * flaky (1% - 50%): intermittent. The 1% floor matches the
        "must pass 100 runs" fix bar, so anything at/above it is worth
        fixing.
      * rare (< 1%): clears the 100-run bar -- effectively noise.
    """
    if rate >= 0.5:
        return "persistent"
    if rate >= 0.01:
        return "flaky"
    return "rare"


def _trend(daily_hits: List[int]) -> float:
    """Simple linear regression slope over daily failure counts.

    Positive = getting worse, negative = improving.
    Returns 0.0 if insufficient data.
    """
    n = len(daily_hits)
    if n < 3:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(daily_hits) / n
    num = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(daily_hits))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return round(num / den, 4) if den else 0.0


def _extract_category(test_name: str) -> str:
    """Extract test category from the file path in the test name."""
    if " in tests/" in test_name:
        path = test_name.split(" in tests/")[1]
        # e.g. "unit/foo.tcl" -> "unit", "cluster/bar.tcl" -> "cluster"
        parts = path.split("/")
        if len(parts) >= 2:
            return parts[0]
    if "GTest" in test_name:
        return "unit"
    if "sentinel" in test_name.lower():
        return "sentinel"
    return "other"


def compute_scorecards(
    cache: Cache,
    days: int = 30,
    branch: str = "unstable",
    workflow: str = "daily.yml",
    repo: str = "valkey-io/valkey",
) -> Dict:
    """Compute per-test flakiness scorecards over the given window.

    Returns a dict with metadata and a list of test scorecards sorted by
    failure rate (descending).
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT00:00:00Z"
    )

    runs = cache.query_runs(repo=repo, workflow=workflow, branch=branch, since=since)
    # Deduplicate to one run per day, skip in-progress
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
        return {"meta": {"days": days, "total_runs": 0}, "scorecards": []}

    all_dates = sorted(seen_dates)
    total_runs = len(valid_runs)

    # Gather failures per test per date
    # test_name -> {date -> count}
    test_failures: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    # Track first/last seen
    test_first_seen: Dict[str, str] = {}
    test_last_seen: Dict[str, str] = {}

    for run in valid_runs:
        date_key = run["run_date"][:10]
        jobs = cache.query_jobs(run["run_id"], failed_only=True)
        for job in jobs:
            failures = cache.query_failures(job_id=job["job_id"])
            for f in failures:
                name = sanitize_cached_failure(f["test_name"])
                if name is None:
                    continue
                test_failures[name][date_key] += 1
                if name not in test_first_seen or date_key < test_first_seen[name]:
                    test_first_seen[name] = date_key
                if name not in test_last_seen or date_key > test_last_seen[name]:
                    test_last_seen[name] = date_key

    # Build scorecards
    scorecards: List[Dict] = []
    for test_name, date_counts in test_failures.items():
        days_failed = len(date_counts)
        total_hits = sum(date_counts.values())
        failure_rate = round(days_failed / total_runs, 4)

        # Build daily series for trend (0 on days with no failure)
        daily_series = [date_counts.get(d, 0) for d in all_dates]

        scorecards.append(
            {
                "test_name": test_name,
                "category": _extract_category(test_name),
                "first_seen": test_first_seen[test_name],
                "last_seen": test_last_seen[test_name],
                "days_failed": days_failed,
                "total_hits": total_hits,
                "total_runs": total_runs,
                "failure_rate": failure_rate,
                "classification": _classify(failure_rate),
                "trend": _trend(daily_series),
                "daily_series": daily_series,
            }
        )

    # Sort by failure rate descending, then total hits
    scorecards.sort(key=lambda s: (-s["failure_rate"], -s["total_hits"]))

    return {
        "meta": {
            "days": days,
            "branch": branch,
            "workflow": workflow,
            "repo": repo,
            "total_runs": total_runs,
            "window_start": all_dates[0],
            "window_end": all_dates[-1],
            "generated": datetime.now(timezone.utc).isoformat(),
        },
        "scorecards": scorecards,
    }
