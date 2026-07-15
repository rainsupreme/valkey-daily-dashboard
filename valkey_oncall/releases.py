"""Per-release-branch report generation for the releases page.

Consumes the synthetic ``weekly-split`` per-branch series produced by
``sync_weekly_branches`` and builds, for every release branch, the same
report dataset the daily/CI views use (heatmap timeline, scorecard,
regressions) plus a compact summary row for the top-of-page strip.

Weekly runs land on Sundays, so per-day column keys are naturally sparse
(one column per week). Synthetic runs carry no commit SHA — the weekly
run's head SHA is the *unstable* tip, not the release branch tip — so
compare links and blame commit lists are intentionally absent; onset
dates remain.
"""

from __future__ import annotations

from typing import Dict, List

from valkey_oncall.cache import Cache
from valkey_oncall.report import generate_report_data
from valkey_oncall.weekly import WEEKLY_SPLIT_WORKFLOW

#: A branch whose latest week has >= this share of jobs failing is "crit".
CRIT_JOB_FAIL_RATIO = 0.20

#: Substring marking parser pseudo-rows for jobs that died before any test
#: ran (build/compile/setup failures) — no test name could be attributed.
UNATTRIBUTED_MARKER = "unattributed failure"


def _branch_sort_key(branch: str) -> List[int]:
    return [int(x) for x in branch.split(".")]


def discover_release_branches(
    cache: Cache, repo: str = "valkey-io/valkey"
) -> List[str]:
    """Release branches present in the weekly-split series, newest first."""
    runs = cache.query_runs(repo=repo, workflow=WEEKLY_SPLIT_WORKFLOW)
    branches = sorted({r["branch"] for r in runs}, key=_branch_sort_key, reverse=True)
    return branches


def _health_tier(failed_jobs: int, total_jobs: int) -> str:
    """'ok' (no failures), 'warn' (some), or 'crit' (>=20% of jobs red)."""
    if failed_jobs <= 0:
        return "ok"
    if total_jobs and failed_jobs / total_jobs >= CRIT_JOB_FAIL_RATIO:
        return "crit"
    return "warn"


def _summary_row(branch: str, data: Dict) -> Dict:
    """Compact summary-strip row for one branch's report dataset."""
    runs = data.get("runs", [])
    latest = runs[-1] if runs else None

    failure_names = (latest or {}).get("failure_names", [])
    attributed = [n for n in failure_names if UNATTRIBUTED_MARKER not in n]
    unattributed = len(failure_names) - len(attributed)

    failed_jobs = (latest or {}).get("failed_jobs", 0)
    total_jobs = (latest or {}).get("total_jobs", 0)
    tier = _health_tier(failed_jobs, total_jobs)

    return {
        "branch": branch,
        "latest_week": (latest or {}).get("day"),
        "failed_jobs": failed_jobs,
        "total_jobs": total_jobs,
        "tier": tier,
        # Distinct failing tests in the latest week, split by whether a
        # test name could be attributed. High unattributed + crit tier
        # reads as "branch is structurally broken (build/setup)", not
        # "N flaky tests".
        "failing_tests": len(attributed),
        "unattributed_jobs": unattributed,
        "build_broken": tier == "crit" and unattributed > len(attributed),
        # Weeks trend, oldest-first, for the summary sparkline.
        "trend": [
            {
                "day": r.get("day"),
                "failed_jobs": r.get("failed_jobs", 0),
                "total_jobs": r.get("total_jobs", 0),
            }
            for r in runs
        ],
    }


def generate_releases_data(
    cache: Cache,
    repo: str = "valkey-io/valkey",
    days: int = 119,
) -> Dict:
    """Build the releases-page dataset: per-branch reports + summary rows.

    *days* bounds the recent window (default 119 = 17 weeks of Sundays).
    Branch order is newest-first throughout.
    """
    branches = discover_release_branches(cache, repo=repo)
    per_branch: Dict[str, Dict] = {}
    summary_rows: List[Dict] = []

    for branch in branches:
        data = generate_report_data(
            cache,
            days=days,
            branch=branch,
            workflow=WEEKLY_SPLIT_WORKFLOW,
            repo=repo,
        )
        per_branch[branch] = data
        summary_rows.append(_summary_row(branch, data))

    return {
        "branches": branches,
        "per_branch": per_branch,
        "summary_rows": summary_rows,
        "summary": {
            "repo": repo,
            "days": days,
            "branch_count": len(branches),
            "latest_week": max(
                (row["latest_week"] for row in summary_rows if row["latest_week"]),
                default=None,
            ),
        },
    }
