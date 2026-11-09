"""Reliability SLA inputs: per-execution flake rates over a rolling window.

The scorecard measures how many CI *days* a test failed. The SLA asks a
different question: out of the times a test actually ran, how often did it
fail? That needs an execution count, which the cache does not store
directly (it records failures, not passes). This module derives it.

How executions are derived
--------------------------
A test is assumed to run in every finished job that carries a job name in
which the test has been seen failing inside the window. So for a test T:

* ``jobs(T)``        = job names with at least one failure row for T in the
                       window;
* ``executions(T)``  = number of jobs in the window whose name is in
                       ``jobs(T)`` and whose conclusion is ``success`` or
                       ``failure`` (cancelled and skipped jobs did not run
                       the suite to the end, so they are not executions);
* ``fails(T)``       = number of failure rows for T in those jobs (the cache
                       keeps one row per job and test, so fails <= executions).

Rates are reported per (test, job) and aggregated per test over its jobs.
The credible interval is the Beta posterior from :mod:`valkey_oncall.stats`.

Limits
------
* A job in which T runs but never failed inside the window is not counted,
  so executions are undercounted and the rate overestimated. Over a 30-day
  window with daily runs this matters for tests with one sighting.
* A failed job whose log expired or did not parse contributes an execution
  but no failure rows, so a test that failed there is undercounted.
* Matrix jobs (``test-valgrind-test (unit)``) run a subset of the suite; the
  per-job rows stay correct because T is only counted in jobs where it was
  seen, but the per-test aggregate mixes jobs with different suites.
* A test renamed inside the window appears as two tests.

Run health
----------
:func:`compute_run_health` looks at whole runs: failed jobs, the share of
green jobs, whether the run was fully green, and how many failed jobs are
*not explained* by the flake registry. A failed job is explained when it has
at least one parsed failure row and every row matches a registry entry
exactly (same test, and the entry's failure mode when it has one). A failed
job with no parsed rows (build error, expired log, timeout) is never
explained. A run is "green if tracked flakes were fixed" when it was not
cancelled and every failed job is explained. Rolling 7-run and 30-run
values are given per run, oldest first.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from valkey_oncall.cache import Cache
from valkey_oncall.flakes import TIER_EXACT, FlakeRegistry
from valkey_oncall.log_parser import sanitize_cached_failure
from valkey_oncall.stats import rate_credible_interval
from valkey_oncall.windowing import select_runs

# Per-test budget: a test failing in more than this share of its executions
# is over budget (the "under 1% per execution" goal).
FLAKE_RATE_BUDGET = 0.01

# Rolling window for per-execution rates.
RATE_WINDOW_DAYS = 30

# Credible interval reported next to each rate.
RATE_CREDIBLE = 0.90

# Job conclusions that count as an execution of the job's suite.
SETTLED_JOB_CONCLUSIONS = ("success", "failure")


def _day(run: Dict) -> str:
    return run["run_date"][:10]


def _load_runs(
    cache: Cache,
    repo: str,
    workflow: str,
    branch: str,
    since_day: str,
    event: Optional[str] = None,
) -> List[Dict]:
    """Settled runs since *since_day*, oldest first, with jobs and failures.

    Each run dict gains ``jobs`` (name, conclusion, job_id) and ``failures``:
    one ``(test_name, error_summary, job_name)`` per parsed failure row in a
    failed job, with the parser's cleanup applied to the name.
    """
    all_runs = cache.query_runs(
        repo=repo,
        workflow=workflow,
        branch=branch,
        since=f"{since_day}T00:00:00Z",
        event=event,
    )
    runs = select_runs(all_runs, per_run=True)
    for run in runs:
        jobs = cache.query_jobs(run["run_id"])
        run["jobs"] = jobs
        failures: List[tuple] = []
        for j in jobs:
            if j["conclusion"] != "failure":
                continue
            for f in cache.query_failures(job_id=j["job_id"]):
                name = sanitize_cached_failure(f["test_name"])
                if name is None:
                    continue
                failures.append((name, f["error_summary"], j["name"]))
        run["failures"] = failures
    return runs


def _rates_from_runs(runs: List[Dict], registry: Optional[FlakeRegistry]) -> List[Dict]:
    """Per-test rate rows for one window of loaded runs, worst first."""
    executions: Dict[str, int] = defaultdict(int)
    for run in runs:
        for j in run["jobs"]:
            if j["conclusion"] in SETTLED_JOB_CONCLUSIONS:
                executions[j["name"]] += 1

    # test -> job -> fails; test -> summaries seen; test -> days failed
    fails: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    summaries: Dict[str, set] = defaultdict(set)
    first: Dict[str, str] = {}
    last: Dict[str, str] = {}
    for run in runs:
        day = _day(run)
        for name, summary, job in run["failures"]:
            fails[name][job] += 1
            summaries[name].add(summary)
            first[name] = min(first.get(name, day), day)
            last[name] = max(last.get(name, day), day)

    rows: List[Dict] = []
    for name, per_job in fails.items():
        jobs: List[Dict] = []
        n_fail = 0
        n_exec = 0
        for job, k in sorted(per_job.items()):
            n = executions.get(job, 0)
            lo, hi = rate_credible_interval(k, n, RATE_CREDIBLE)
            jobs.append(
                {
                    "job_name": job,
                    "fails": k,
                    "executions": n,
                    "rate": round(k / n, 4) if n else None,
                    "rate_ci90": [round(lo, 4), round(hi, 4)],
                }
            )
            n_fail += k
            n_exec += n
        lo, hi = rate_credible_interval(n_fail, n_exec, RATE_CREDIBLE)
        rate = n_fail / n_exec if n_exec else None
        tracked = False
        if registry is not None:
            tracked = any(
                m.tier == TIER_EXACT
                for s in summaries[name]
                for m in registry.match(name, s)
            )
        rows.append(
            {
                "test_name": name,
                "fails": n_fail,
                "executions": n_exec,
                "rate": round(rate, 4) if rate is not None else None,
                "rate_ci90": [round(lo, 4), round(hi, 4)],
                "over_budget": bool(rate is not None and rate > FLAKE_RATE_BUDGET),
                # Effect-size view: the whole credible interval sits above
                # the budget. One failure in 30 executions is 3.3% by point
                # estimate but its interval reaches below 1%, so it is not
                # confidently over budget; two failures are.
                "over_budget_confident": bool(lo > FLAKE_RATE_BUDGET),
                "tracked": tracked,
                "first_failure": first[name],
                "last_failure": last[name],
                "jobs": jobs,
            }
        )
    rows.sort(key=lambda r: (-(r["rate"] or 0.0), -r["fails"], r["test_name"]))
    return rows


def compute_flake_rates(
    cache: Cache,
    days: int = RATE_WINDOW_DAYS,
    branch: str = "unstable",
    workflow: str = "daily.yml",
    repo: str = "valkey-io/valkey",
    registry: Optional[FlakeRegistry] = None,
    event: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Dict:
    """Per-execution fail rates over the last *days* days (see module doc).

    With a *registry*, each test carries ``tracked``: whether one of its
    failure modes in the window matches a registry entry exactly. *event*
    restricts runs to one trigger event (``schedule`` for the nightly run).
    """
    now = now or datetime.now(timezone.utc)
    since_day = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    runs = _load_runs(cache, repo, workflow, branch, since_day, event)
    tests = _rates_from_runs(runs, registry)
    return {
        "meta": {
            "days": days,
            "branch": branch,
            "workflow": workflow,
            "repo": repo,
            "runs": len(runs),
            "window_start": since_day,
            "window_end": now.strftime("%Y-%m-%d"),
            "budget": FLAKE_RATE_BUDGET,
        },
        "tests": tests,
        "tests_over_budget": sum(1 for t in tests if t["over_budget"]),
        "tests_over_budget_confident": sum(
            1 for t in tests if t["over_budget_confident"]
        ),
        "tests_tracked": sum(1 for t in tests if t["tracked"]),
    }


def flake_rate_series(
    cache: Cache,
    days: int = 90,
    window_days: int = RATE_WINDOW_DAYS,
    branch: str = "unstable",
    workflow: str = "daily.yml",
    repo: str = "valkey-io/valkey",
    event: Optional[str] = None,
    now: Optional[datetime] = None,
) -> List[Dict]:
    """How many tests were over budget, per run day, over the last *days*.

    For each run day ``d`` in the span the rolling window is the runs with
    ``d - window_days <= day <= d``, the same inclusive edges as
    :func:`compute_flake_rates`. One point per day, oldest first.
    """
    now = now or datetime.now(timezone.utc)
    span_start = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    load_start = (now - timedelta(days=days + window_days)).strftime("%Y-%m-%d")
    runs = _load_runs(cache, repo, workflow, branch, load_start, event)
    out: List[Dict] = []
    for day in sorted({_day(r) for r in runs if _day(r) >= span_start}):
        lo = (
            datetime.strptime(day, "%Y-%m-%d") - timedelta(days=window_days)
        ).strftime("%Y-%m-%d")
        window = [r for r in runs if lo <= _day(r) <= day]
        rows = _rates_from_runs(window, None)
        out.append(
            {
                "date": day,
                "runs": len(window),
                "failing_tests": len(rows),
                "tests_over_budget": sum(1 for r in rows if r["over_budget"]),
                "tests_over_budget_confident": sum(
                    1 for r in rows if r["over_budget_confident"]
                ),
            }
        )
    return out


# ----------------------------------------------------------------------
# Run health
# ----------------------------------------------------------------------

ROLLING_SHORT = 7
ROLLING_LONG = 30


def _explained(registry: Optional[FlakeRegistry], rows: List[tuple]) -> bool:
    """True when every parsed failure row of a job is a tracked flake."""
    if registry is None or not rows:
        return False
    for name, summary, _job in rows:
        if not any(m.tier == TIER_EXACT for m in registry.match(name, summary)):
            return False
    return True


def _run_health(run: Dict, registry: Optional[FlakeRegistry]) -> Dict:
    by_conclusion: Dict[str, List[Dict]] = defaultdict(list)
    for j in run["jobs"]:
        by_conclusion[j["conclusion"]].append(j)
    failed = sorted(by_conclusion.get("failure", []), key=lambda j: j["name"])
    rows_by_job: Dict[str, List[tuple]] = defaultdict(list)
    for row in run["failures"]:
        rows_by_job[row[2]].append(row)
    unexplained = [
        j["name"] for j in failed if not _explained(registry, rows_by_job[j["name"]])
    ]
    n_ok = len(by_conclusion.get("success", []))
    n_fail = len(failed)
    cancelled = sorted(j["name"] for j in by_conclusion.get("cancelled", []))
    settled = n_ok + n_fail
    return {
        "run_id": run["run_id"],
        "date": _day(run),
        "commit_sha": run.get("commit_sha", ""),
        "status": run["status"],
        "total_jobs": len(run["jobs"]),
        "success_jobs": n_ok,
        "failed_jobs": n_fail,
        "cancelled_jobs": len(cancelled),
        "skipped_jobs": len(by_conclusion.get("skipped", [])),
        "failed_job_names": [j["name"] for j in failed],
        "cancelled_job_names": cancelled,
        "green_share": round(n_ok / settled, 4) if settled else None,
        "fully_green": run["status"] == "success" and n_fail == 0 and not cancelled,
        "unexplained_failed_jobs": unexplained,
        "unexplained_failed_job_count": len(unexplained),
        "green_if_tracked_fixed": run["status"] != "cancelled" and not unexplained,
    }


def _rolling(runs: List[Dict], n: int) -> List[Dict]:
    """Rolling values over the last *n* runs ending at each run."""
    out: List[Dict] = []
    for i in range(len(runs)):
        w = runs[max(0, i - n + 1) : i + 1]
        shares = [x["green_share"] for x in w if x["green_share"] is not None]
        out.append(
            {
                "date": w[-1]["date"],
                "runs": len(w),
                "green_runs": sum(1 for x in w if x["fully_green"]),
                "green_rate": round(sum(1 for x in w if x["fully_green"]) / len(w), 4),
                "green_if_fixed_rate": round(
                    sum(1 for x in w if x["green_if_tracked_fixed"]) / len(w), 4
                ),
                "mean_failed_jobs": round(
                    statistics.mean(x["failed_jobs"] for x in w), 3
                ),
                "median_failed_jobs": statistics.median(x["failed_jobs"] for x in w),
                "mean_unexplained_failed_jobs": round(
                    statistics.mean(x["unexplained_failed_job_count"] for x in w), 3
                ),
                "mean_green_share": round(statistics.mean(shares), 4)
                if shares
                else None,
            }
        )
    return out


def compute_run_health(
    cache: Cache,
    registry: Optional[FlakeRegistry] = None,
    days: int = 90,
    branch: str = "unstable",
    workflow: str = "daily.yml",
    repo: str = "valkey-io/valkey",
    event: Optional[str] = None,
    include_cancelled: bool = True,
    now: Optional[datetime] = None,
) -> Dict:
    """Per-run health over the last *days* days with rolling values.

    *event* restricts runs to one trigger (``schedule`` keeps hand-started
    nightly runs out of the green rate). *include_cancelled* is for workflows
    with cancel-in-progress concurrency (ci.yml): a run cancelled by a newer
    push never reached a verdict and is left out when False. A cancelled
    nightly run is kept: it is a night without a green result.
    """
    now = now or datetime.now(timezone.utc)
    since_day = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    runs = _load_runs(cache, repo, workflow, branch, since_day, event)
    if not include_cancelled:
        runs = [r for r in runs if r["status"] != "cancelled"]
    health = [_run_health(r, registry) for r in runs]
    return {
        "meta": {
            "days": days,
            "branch": branch,
            "workflow": workflow,
            "repo": repo,
            "event": event,
            "include_cancelled": include_cancelled,
            "runs": len(health),
            "window_start": since_day,
            "window_end": now.strftime("%Y-%m-%d"),
            "registry_entries": len(registry.entries()) if registry else 0,
        },
        "runs": health,
        "rolling7": _rolling(health, ROLLING_SHORT),
        "rolling30": _rolling(health, ROLLING_LONG),
    }
