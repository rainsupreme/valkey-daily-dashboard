"""HTML report generator for Valkey CI failure trends."""

from __future__ import annotations

import html
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from valkey_oncall.cache import Cache
from valkey_oncall.log_parser import sanitize_cached_failure
from valkey_oncall.scorecard import compute_scorecards


def generate_report_data(
    cache: Cache,
    days: int = 14,
    branch: str = "unstable",
    workflow: str = "daily.yml",
    repo: str = "valkey-io/valkey",
    client=None,
) -> Dict:
    """Build the data structure for the failure trend report.

    If *client* (a ``GitHubActionsClient``) is provided, the report will
    include the list of commits between consecutive runs.
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT00:00:00Z"
    )

    all_runs = cache.query_runs(
        repo=repo, workflow=workflow, branch=branch, since=since
    )
    # Keep only completed scheduled runs (one per day, skip duplicates)
    seen_dates: set[str] = set()
    runs: List[Dict] = []
    for r in all_runs:
        if r["status"] in ("in_progress", "queued", "skipped", "action_required"):
            continue
        date_key = r["run_date"][:10]
        if date_key in seen_dates:
            continue
        seen_dates.add(date_key)
        runs.append(r)

    # Oldest first for the timeline
    runs.sort(key=lambda r: r["run_date"])

    # For each run, gather jobs and failures
    run_details: List[Dict] = []
    # test_name -> [{run_date, status, error_summary, job_names}]
    test_timeline: Dict[str, Dict[str, Dict]] = defaultdict(dict)

    for run in runs:
        rid = run["run_id"]
        date_key = run["run_date"][:10]
        all_jobs = cache.query_jobs(rid)
        failed_jobs = cache.query_jobs(rid, failed_only=True)

        run_info = {
            "run_id": rid,
            "date": date_key,
            "status": run["status"],
            "commit_sha": run.get("commit_sha", ""),
            "total_jobs": len(all_jobs),
            "failed_jobs": len(failed_jobs),
            "failed_job_names": sorted(j["name"] for j in failed_jobs),
        }

        # Gather failures for this run
        run_failures: List[Dict] = []
        for j in failed_jobs:
            for f in cache.query_failures(job_id=j["job_id"]):
                # Clean stale cached noise at display time (mirrors the parser).
                clean = sanitize_cached_failure(f["test_name"])
                if clean is None:
                    continue
                run_failures.append({**f, "test_name": clean, "job_name": j["name"]})

        # Group by test_name
        by_test: Dict[str, List[Dict]] = defaultdict(list)
        for f in run_failures:
            by_test[f["test_name"]].append(f)

        run_info["unique_failures"] = len(by_test)
        run_info["failure_names"] = sorted(by_test.keys())
        # Map each failure name to its job_ids for linking
        run_info["failure_jobs"] = {
            test_name: sorted(set(inst["job_id"] for inst in instances))
            for test_name, instances in by_test.items()
        }
        run_details.append(run_info)

        # Record in timeline
        for test_name, instances in by_test.items():
            error_summaries = sorted(
                set(inst["error_summary"][:120] for inst in instances)
            )
            job_names = sorted(set(inst["job_name"] for inst in instances))
            test_timeline[test_name][date_key] = {
                "count": len(instances),
                "errors": error_summaries,
                "jobs": job_names,
            }

    # Sort tests by total failure count (most frequent first)
    test_totals = {
        name: sum(d["count"] for d in dates.values())
        for name, dates in test_timeline.items()
    }
    sorted_tests = sorted(test_totals.keys(), key=lambda n: -test_totals[n])

    dates = [r["date"] for r in run_details]

    # Fetch commits between consecutive runs if a client is provided
    if client:
        for i in range(1, len(run_details)):
            prev_sha = run_details[i - 1]["commit_sha"]
            curr_sha = run_details[i]["commit_sha"]
            if prev_sha and curr_sha and prev_sha != curr_sha:
                try:
                    commits = client.compare_commits(prev_sha, curr_sha)
                    run_details[i]["commits_since_prev"] = commits
                except Exception:
                    run_details[i]["commits_since_prev"] = []
            else:
                run_details[i]["commits_since_prev"] = []

        # Fetch commit messages for each unique SHA
        seen_shas: Dict[str, str] = {}
        for run in run_details:
            sha = run.get("commit_sha", "")
            if sha and sha not in seen_shas:
                try:
                    info = client.get_commit(sha)
                    seen_shas[sha] = info.get("message_full", "")
                except Exception:
                    seen_shas[sha] = ""
            run["commit_message"] = seen_shas.get(sha, "")

    # Compute long-term (90-day) failure rates for each test
    long_term_since = (datetime.now(timezone.utc) - timedelta(days=90)).strftime(
        "%Y-%m-%dT00:00:00Z"
    )
    lt_runs = cache.query_runs(
        repo=repo, workflow=workflow, branch=branch, since=long_term_since
    )
    lt_seen_dates: set[str] = set()
    for r in lt_runs:
        if r["status"] not in ("in_progress", "queued", "skipped", "action_required"):
            lt_seen_dates.add(r["run_date"][:10])
    lt_total = len(lt_seen_dates) or 1

    # Count days each test failed in the 90-day window
    lt_test_days: Dict[str, set] = defaultdict(set)
    for r in lt_runs:
        if r["status"] in ("in_progress", "queued", "skipped", "action_required"):
            continue
        date_key = r["run_date"][:10]
        for j in cache.query_jobs(r["run_id"], failed_only=True):
            for f in cache.query_failures(job_id=j["job_id"]):
                clean = sanitize_cached_failure(f["test_name"])
                if clean is None:
                    continue
                lt_test_days[clean].add(date_key)

    return {
        "dates": dates,
        "runs": run_details,
        "tests": {
            name: {
                "total": test_totals[name],
                "days_failed": len(test_timeline[name]),
                "score_90d": round(
                    len(lt_test_days.get(name, set())) / lt_total * 100, 1
                ),
                "timeline": {d: test_timeline[name].get(d) for d in dates},
            }
            for name in sorted_tests
        },
        "summary": {
            "days": days,
            "repo": repo,
            "branch": branch,
            "workflow": workflow,
            "total_runs": len(run_details),
            "failed_runs": sum(1 for r in run_details if r["status"] == "failure"),
            "unique_tests_failed": len(sorted_tests),
        },
        # Full 90-day flakiness roster (the "board of shame"), independent of
        # the recent-window heatmap above. Ranked worst-first by compute_scorecards.
        "scorecard": compute_scorecards(
            cache, days=90, branch=branch, workflow=workflow, repo=repo
        ),
    }


def render_html(data: Dict) -> str:
    """Render the report data as a self-contained HTML file."""
    summary = data["summary"]
    dates = data["dates"]
    tests = data["tests"]
    runs = data["runs"]
    repo = summary.get("repo", "valkey-io/valkey")

    # Build date headers — M/D format, no leading zeros
    date_headers = ""
    for d in dates:
        month = int(d[5:7])
        day = int(d[8:10])
        date_headers += f'<th class="date-col" title="{d}">{month}/{day}</th>'

    # Build run status row (overall pass/fail per day)
    run_status_cells = ""
    for run in runs:
        cls = "pass" if run["status"] == "success" else "fail"
        title = f"{run['date']}: {run['status']} ({run['failed_jobs']}/{run['total_jobs']} jobs failed)"
        run_status_cells += f'<td class="cell {cls}" title="{html.escape(title)}"></td>'

    # Build test rows
    test_rows = ""
    for test_name, info in tests.items():
        # Shorten the display name
        short_name = _short_test_name(test_name)
        freq_pct = round(info["days_failed"] / len(dates) * 100) if dates else 0
        freq = f"{freq_pct}%"
        score_90d = info.get("score_90d", 0)
        score_str = f"{score_90d:.0f}%" if score_90d >= 1 else f"{score_90d:.1f}%"

        cells = ""
        for d in dates:
            entry = info["timeline"][d]
            if entry is None:
                cells += '<td class="cell none" title="no failure"></td>'
            else:
                n = entry["count"]
                jobs = ", ".join(entry["jobs"][:3])
                if len(entry["jobs"]) > 3:
                    jobs += f" +{len(entry['jobs']) - 3}"
                errs = "; ".join(entry["errors"][:2])
                tip = html.escape(f"{n}x on {d}\nJobs: {jobs}\nError: {errs}")
                cells += f'<td class="cell fail" title="{tip}">{n}</td>'

        test_rows += f"""<tr>
            <td class="test-name" title="{html.escape(test_name)}">{html.escape(short_name)}</td>
            <td class="freq">{freq}</td>
            <td class="freq" title="Failed {score_90d:.1f}% of runs in last 90 days">{score_str}</td>
            {cells}
        </tr>"""

    # Build per-run detail rows for the bottom table
    run_detail_rows = ""
    for run in reversed(runs):  # newest first
        if run["status"] == "success":
            status_badge = '<span class="badge pass">PASS</span>'
        else:
            status_badge = f'<span class="badge fail">FAIL ({run["failed_jobs"]}/{run["total_jobs"]})</span>'
        jobs_list = run["failed_job_names"][:5]
        jobs_extra = len(run["failed_job_names"]) - 5
        if jobs_list:
            jobs_html = '<div class="job-list">'
            for jn in jobs_list:
                jobs_html += f'<div class="job-entry">{html.escape(jn)}</div>'
            if jobs_extra > 0:
                jobs_html += f'<div class="job-entry" style="color:#8b949e">+{jobs_extra} more</div>'
            jobs_html += "</div>"
        else:
            jobs_html = "—"
        sha = run.get("commit_sha", "")
        commit_msg = run.get("commit_message", "") or ""
        sha_link = _commit_link(sha, repo, title=commit_msg) if sha else "—"

        # Commits since previous run
        commits = run.get("commits_since_prev", [])
        if commits:
            commits_html = '<div class="commit-list">'
            for c in commits:
                full_msg = c.get("message", "")
                c_link = _commit_link(c["sha"], repo, title=full_msg)
                author = html.escape(c.get("author", "")[:20])
                msg_short = html.escape(full_msg[:80])
                msg_tip = html.escape(full_msg, quote=True)
                commits_html += (
                    f'<div class="commit-entry">{c_link} '
                    f'<span class="commit-author">{author}</span> '
                    f'<span title="{msg_tip}">{msg_short}</span></div>'
                )
            commits_html += "</div>"
        else:
            commits_html = '<span class="no-commits">—</span>'

        run_detail_rows += f"""<tr>
            <td>{run["date"]}</td>
            <td>{status_badge}</td>
            <td>{sha_link}</td>
            <td>{run["unique_failures"]}</td>
            <td class="failures-cell">{_render_failure_names(run.get("failure_names", []), run.get("failure_jobs", {}), repo, run.get("run_id", 0))}</td>
            <td class="jobs-cell">{jobs_html}</td>
            <td class="commits-cell">{commits_html}</td>
        </tr>"""

    scorecard_rows = _render_scorecard_rows(
        data.get("scorecard", {}).get("scorecards", [])
    )

    return _HTML_TEMPLATE.format(
        repo=html.escape(repo),
        branch=html.escape(summary["branch"]),
        workflow=html.escape(summary["workflow"]),
        days=summary["days"],
        total_runs=summary["total_runs"],
        failed_runs=summary["failed_runs"],
        unique_tests=summary["unique_tests_failed"],
        generated=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        date_headers=date_headers,
        run_status_cells=run_status_cells,
        test_rows=test_rows,
        run_detail_rows=run_detail_rows,
        scorecard_rows=scorecard_rows,
        report_json=html.escape(json.dumps(data, indent=2)),
    )


def render_markdown(data: Dict) -> str:
    """Render the report data as GitHub-Flavored Markdown."""
    summary = data["summary"]
    repo = summary.get("repo", "valkey-io/valkey")
    dates = data["dates"]
    tests = data["tests"]
    runs = data["runs"]

    lines: List[str] = []
    lines.append("# Valkey CI Failure Report")
    lines.append("")
    lines.append(
        f"`{summary['workflow']}` · `{summary['branch']}` · `{repo}` · "
        f"last {summary['days']} days · generated "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    lines.append("")
    lines.append(
        f"| Runs | Failed | Unique failures |\n"
        f"|------|--------|----------------|\n"
        f"| {summary['total_runs']} | {summary['failed_runs']} "
        f"| {summary['unique_tests_failed']} |"
    )

    # Failure heatmap table
    if tests:
        lines.append("")
        lines.append("## Failure Heatmap")
        lines.append("")
        day_labels = [d[5:] for d in dates]  # "04-01" from "2026-04-01"
        header = "| Test | Freq | " + " | ".join(day_labels) + " |"
        sep = "|------|------|" + "|".join("---" for _ in dates) + "|"
        lines.append(header)
        lines.append(sep)
        for test_name, info in tests.items():
            short = _short_test_name(test_name)
            freq = f"{info['days_failed']}/{len(dates)}d"
            cells = []
            for d in dates:
                entry = info["timeline"][d]
                if entry is None:
                    cells.append("·")
                else:
                    cells.append(f"**{entry['count']}**")
            lines.append(f"| `{short}` | {freq} | " + " | ".join(cells) + " |")

    # Run details
    lines.append("")
    lines.append("## Run Details (newest first)")
    lines.append("")
    lines.append("| Date | Status | Commit | # | Failures |")
    lines.append("|------|--------|--------|---|----------|")
    for run in reversed(runs):
        status = (
            "✅"
            if run["status"] == "success"
            else f"❌ {run['failed_jobs']}/{run['total_jobs']}"
        )
        sha = run.get("commit_sha", "")
        sha_md = (
            f"[`{sha[:7]}`](https://github.com/{repo}/commit/{sha})" if sha else "—"
        )
        run_id = run.get("run_id", 0)
        failure_jobs = run.get("failure_jobs", {})
        failure_parts = []
        for n in run.get("failure_names", [])[:5]:
            short = f"`{_short_test_name(n)}`"
            job_ids = failure_jobs.get(n, [])
            if job_ids:
                job_links = "".join(
                    f"[[{i + 1}]](https://github.com/{repo}/actions/runs/{run_id}/job/{jid})"
                    for i, jid in enumerate(job_ids)
                )
                short += f" {job_links}"
            failure_parts.append(short)
        if len(run.get("failure_names", [])) > 5:
            failure_parts.append(f"+{len(run['failure_names']) - 5} more")
        failures = ", ".join(failure_parts) if failure_parts else "—"
        lines.append(
            f"| {run['date']} | {status} | {sha_md} | {run['unique_failures']} | {failures} |"
        )

    lines.append("")
    return "\n".join(lines)


def render_slack(data: Dict) -> str:
    """Render the report data as Slack mrkdwn."""
    summary = data["summary"]
    repo = summary.get("repo", "valkey-io/valkey")
    dates = data["dates"]
    tests = data["tests"]
    runs = data["runs"]

    lines: List[str] = []
    lines.append("*Valkey CI Failure Report*")
    lines.append(
        f"`{summary['workflow']}` · `{summary['branch']}` · `{repo}` · "
        f"last {summary['days']} days · "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    lines.append("")
    lines.append(
        f"*{summary['total_runs']}* runs · "
        f"*{summary['failed_runs']}* failed · "
        f"*{summary['unique_tests_failed']}* unique failures"
    )

    # Top failing tests
    if tests:
        lines.append("")
        lines.append("*Top Failing Tests:*")
        for test_name, info in list(tests.items())[:15]:
            short = _short_test_name(test_name)
            freq = f"{info['days_failed']}/{len(dates)}d"
            lines.append(f"• `{short}` — {freq}, {info['total']} total hits")

    # Recent runs
    lines.append("")
    lines.append("*Recent Runs:*")
    for run in list(reversed(runs))[:10]:
        status = ":white_check_mark:" if run["status"] == "success" else ":x:"
        sha = run.get("commit_sha", "")
        sha_link = f"<https://github.com/{repo}/commit/{sha}|{sha[:7]}>" if sha else "—"
        detail = ""
        if run["unique_failures"] > 0:
            run_id = run.get("run_id", 0)
            failure_jobs = run.get("failure_jobs", {})
            parts = []
            for n in run.get("failure_names", [])[:3]:
                short = f"`{_short_test_name(n)}`"
                job_ids = failure_jobs.get(n, [])
                if job_ids:
                    job_links = "".join(
                        f"<https://github.com/{repo}/actions/runs/{run_id}/job/{jid}|[{i + 1}]>"
                        for i, jid in enumerate(job_ids)
                    )
                    short += f" {job_links}"
                parts.append(short)
            extra = len(run.get("failure_names", [])) - 3
            if extra > 0:
                parts.append(f"+{extra} more")
            detail = f" — {', '.join(parts)}"
        lines.append(
            f"{status} {run['date']} {sha_link} "
            f"({run['failed_jobs']}/{run['total_jobs']} jobs failed){detail}"
        )

    lines.append("")
    return "\n".join(lines)


def _short_test_name(name: str) -> str:
    """Shorten a test name for display in the grid."""
    # Strip " in tests/..." suffix for the grid, keep it in the tooltip
    if " in tests/" in name:
        name = name.split(" in tests/")[0]
    # Strip "(exception)" suffix
    name = name.replace(" (exception)", "")
    # Truncate
    if len(name) > 60:
        name = name[:57] + "..."
    return name


def _commit_link(sha: str, repo: str = "valkey-io/valkey", title: str = "") -> str:
    """Render a short commit SHA as a GitHub link."""
    if not sha:
        return ""
    short = sha[:7]
    tip = html.escape(title, quote=True) if title else sha
    return (
        f'<a href="https://github.com/{repo}/commit/{sha}" '
        f'class="sha" title="{tip}">{short}</a>'
    )


def _render_failure_names(
    names: List[str],
    failure_jobs: Dict[str, List[int]] = None,
    repo: str = "valkey-io/valkey",
    run_id: int = 0,
) -> str:
    """Render a list of failure names with linked job IDs."""
    if not names:
        return "—"
    failure_jobs = failure_jobs or {}
    items = ""
    for n in names:
        short = html.escape(_short_test_name(n))
        job_ids = failure_jobs.get(n, [])
        if job_ids:
            links = " ".join(
                f'<a href="https://github.com/{repo}/actions/runs/{run_id}/job/{jid}" '
                f'class="job-link">[{i + 1}]</a>'
                for i, jid in enumerate(job_ids)
            )
            items += f'<div class="failure-entry">{short} {links}</div>'
        else:
            items += f'<div class="failure-entry">{short}</div>'
    return f'<div class="failure-list">{items}</div>'


def _sparkline(series: List[int], width: int = 90, height: int = 16) -> str:
    """Render a per-day failure-count series as a compact inline SVG bar chart."""
    if not series:
        return ""
    n = len(series)
    mx = max(series) or 1
    bar_w = max(1, width // n)
    bars = []
    for i, v in enumerate(series):
        h = round(v / mx * (height - 2)) if v else 0
        x = i * bar_w
        y = height - max(h, 1)
        color = "#da3633" if v else "#30363d"
        bars.append(
            f'<rect x="{x}" y="{y}" width="{max(1, bar_w - 1)}" '
            f'height="{max(h, 1)}" fill="{color}"/>'
        )
    return (
        f'<svg class="spark" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">{"".join(bars)}</svg>'
    )


def _render_scorecard_rows(scorecards: List[Dict]) -> str:
    """Render the 90-day flaky-test leaderboard rows (ranked worst-first)."""
    rows = ""
    for i, sc in enumerate(scorecards, 1):
        name = sc["test_name"]
        short = _short_test_name(name)
        cls = sc.get("classification", "rare")
        trend = sc.get("trend", 0.0)
        cat = sc.get("category", "other")
        rate = sc.get("failure_rate", 0.0) * 100
        days_failed = sc.get("days_failed", 0)
        total_runs = sc.get("total_runs", 0)
        series = sc.get("daily_series", [])

        if trend > 0.05:
            arrow, tr_cls, tr_title = "↑", "trend-up", "getting worse"
        elif trend < -0.05:
            arrow, tr_cls, tr_title = "↓", "trend-down", "improving"
        else:
            arrow, tr_cls, tr_title = "→", "trend-flat", "flat"
        tr_title = f"{tr_title} (slope {trend:+.3f}/day)"
        rate_str = f"{rate:.0f}%" if rate >= 1 else f"{rate:.1f}%"

        rows += (
            f'<tr data-cat="{html.escape(cat)}" data-class="{cls}" '
            f'data-trend="{trend}" data-rate="{rate:.4f}" data-days="{days_failed}">'
            f'<td class="rank">{i}</td>'
            f'<td class="test-name" title="{html.escape(name)}">{html.escape(short)}</td>'
            f'<td><span class="badge-{cls}" title="{cls}">{cls}</span></td>'
            f'<td class="{tr_cls}" title="{tr_title}">{arrow}</td>'
            f'<td><span class="cat-chip">{html.escape(cat)}</span></td>'
            f'<td class="freq" title="failed {days_failed} of {total_runs} runs (90d)">{rate_str}</td>'
            f'<td class="freq">{days_failed}/{total_runs}</td>'
            f"<td>{_sparkline(series)}</td>"
            f"</tr>"
        )
    return rows


_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Valkey CI Failure Report — {branch}</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
         background: #0d1117; color: #c9d1d9; padding: 20px; font-size: 13px; }}
  h1 {{ font-size: 18px; margin-bottom: 4px; color: #f0f6fc; }}
  .meta {{ color: #8b949e; margin-bottom: 16px; font-size: 12px; }}
  .stats {{ display: flex; gap: 24px; margin-bottom: 20px; }}
  .stat {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px;
           padding: 10px 16px; }}
  .stat-val {{ font-size: 22px; font-weight: 600; color: #f0f6fc; }}
  .stat-label {{ font-size: 11px; color: #8b949e; }}
  table {{ border-collapse: collapse; margin-bottom: 24px; }}
  th, td {{ padding: 3px 6px; text-align: center; font-size: 12px; }}
  th {{ color: #8b949e; font-weight: 500; position: sticky; top: 0; background: #0d1117; }}
  .test-name {{ text-align: left; max-width: 340px; overflow: hidden;
                text-overflow: ellipsis; white-space: nowrap; font-family: monospace;
                font-size: 11px; padding-right: 8px; }}
  .freq {{ color: #8b949e; font-size: 11px; white-space: nowrap; padding-right: 4px; }}
  .cell {{ width: 22px; height: 22px; min-width: 22px; border-radius: 3px;
           font-size: 10px; line-height: 22px; cursor: default; }}
  .date-col {{ width: 32px; min-width: 32px; max-width: 32px; font-size: 11px; white-space: nowrap; }}
  .cell.fail {{ background: #da3633; color: #fff; font-weight: 600; }}
  .cell.pass {{ background: #238636; }}
  .cell.none {{ background: #21262d; }}
  .badge {{ padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }}
  .badge.pass {{ background: #238636; color: #fff; }}
  .badge.fail {{ background: #da3633; color: #fff; }}
  .section {{ margin-top: 24px; }}
  .section h2 {{ font-size: 14px; color: #f0f6fc; margin-bottom: 8px; }}
  .detail-table {{ width: 100%; }}
  .detail-table th {{ text-align: left; border-bottom: 1px solid #30363d; padding: 6px 8px; }}
  .detail-table td {{ text-align: left; border-bottom: 1px solid #21262d; padding: 6px 8px; }}
  .jobs-cell {{ font-size: 11px; vertical-align: top; text-align: left; }}
  .job-list {{ max-height: 150px; overflow-y: auto; }}
  .job-entry {{ white-space: nowrap; padding: 1px 0; }}
  .commits-cell {{ font-size: 11px; vertical-align: top; }}
  .commit-list {{ max-height: 150px; overflow-y: auto; }}
  .commit-entry {{ white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
                   max-width: 600px; padding: 1px 0; }}
  .commit-author {{ color: #8b949e; }}
  .no-commits {{ color: #484f58; }}
  .failures-cell {{ font-size: 11px; vertical-align: top; text-align: left; }}
  .failure-list {{ max-height: 150px; overflow-y: auto; }}
  .failure-entry {{ white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
                    max-width: 400px; padding: 1px 0; color: #f85149; }}
  .job-link {{ color: #58a6ff; text-decoration: none; font-size: 10px; font-family: monospace; }}
  .job-link:hover {{ text-decoration: underline; }}
  .sha {{ color: #58a6ff; text-decoration: none; font-family: monospace; font-size: 11px; }}
  .sha:hover {{ text-decoration: underline; }}
  .hint {{ color: #8b949e; font-size: 11px; margin-bottom: 12px; line-height: 1.5; max-width: 720px; }}
  details {{ margin-top: 20px; }}
  summary {{ cursor: pointer; color: #8b949e; font-size: 12px; }}
  pre {{ background: #161b22; padding: 12px; border-radius: 6px; overflow-x: auto;
         font-size: 11px; max-height: 400px; }}
  .scorecard-table {{ width: 100%; }}
  .scorecard-table th {{ text-align: left; border-bottom: 1px solid #30363d; padding: 6px 8px; }}
  .scorecard-table td {{ text-align: left; border-bottom: 1px solid #21262d; padding: 4px 8px; vertical-align: middle; }}
  .rank {{ color: #8b949e; font-size: 11px; }}
  .badge-persistent {{ background:#da3633; color:#fff; padding:1px 7px; border-radius:10px; font-size:10px; font-weight:600; }}
  .badge-flaky {{ background:#9e6a03; color:#fff; padding:1px 7px; border-radius:10px; font-size:10px; font-weight:600; }}
  .badge-rare {{ background:#30363d; color:#c9d1d9; padding:1px 7px; border-radius:10px; font-size:10px; font-weight:600; }}
  .trend-up {{ color:#f85149; font-weight:700; }}
  .trend-down {{ color:#3fb950; font-weight:700; }}
  .trend-flat {{ color:#8b949e; }}
  .cat-chip {{ background:#161b22; border:1px solid #30363d; color:#8b949e; padding:1px 6px; border-radius:4px; font-size:10px; }}
  .spark {{ vertical-align: middle; }}
</style>
</head>
<body>
<h1>Valkey CI Failure Report</h1>
<p class="meta">{workflow} · {branch} · {repo} · last {days} days · generated {generated}</p>
<p class="hint">Daily CI failure trends for the <b>{branch}</b> branch. Tracks which tests fail, how often, and whether they are getting better or worse.</p>

<div class="stats">
  <div class="stat"><div class="stat-val">{total_runs}</div><div class="stat-label">runs</div></div>
  <div class="stat"><div class="stat-val">{failed_runs}</div><div class="stat-label">failed</div></div>
  <div class="stat"><div class="stat-val">{unique_tests}</div><div class="stat-label">unique failures</div></div>
</div>

<table>
  <caption class="hint" style="text-align:left; caption-side:top; margin-bottom:8px;">
    Columns are days, rows are unique test failures. Each cell shows how many jobs hit that failure on that day.
    <span style="display:inline-block; width:10px; height:10px; background:#da3633; border-radius:2px; vertical-align:middle;"></span> failed
    <span style="display:inline-block; width:10px; height:10px; background:#238636; border-radius:2px; vertical-align:middle;"></span> passed
    <span style="display:inline-block; width:10px; height:10px; background:#21262d; border-radius:2px; vertical-align:middle;"></span> no failure.
    Freq = days failed / total days.
  </caption>
  <thead>
    <tr><th class="test-name">Test</th><th class="freq" title="Failure rate over last 14 days">14d</th><th class="freq" title="Failure rate over last 90 days">90d</th>{date_headers}</tr>
    <tr><td class="test-name" style="color:#8b949e">Run status</td><td></td><td></td>{run_status_cells}</tr>
  </thead>
  <tbody>
    {test_rows}
  </tbody>
</table>

<div class="section">
  <h2>Flaky Test Scorecard — last 90 days</h2>
  <p class="hint">Every test that failed in the last 90 days, ranked worst-first — the full flaky roster, independent of the recent heatmap above.
    Class: <span class="badge-persistent">persistent</span> ≥50% ·
    <span class="badge-flaky">flaky</span> 1–50% ·
    <span class="badge-rare">rare</span> &lt;1%.
    Trend: <span class="trend-up">↑</span> worse / <span class="trend-down">↓</span> better / <span class="trend-flat">→</span> flat.
    "90d activity" sparkline shows per-day failure counts.
  </p>
  <div id="scorecard-controls"></div>
  <table class="scorecard-table">
    <thead><tr><th>#</th><th>Test</th><th>Class</th><th>Trend</th><th>Category</th><th title="Failure rate over last 90 days">90d rate</th><th>Days</th><th>90d activity</th></tr></thead>
    <tbody id="scorecard-body">{scorecard_rows}</tbody>
  </table>
</div>

<div class="section">
  <h2>Run Details (newest first)</h2>
  <p class="hint">Each row is one daily CI run. Status shows failed/total jobs. Numbered links like [1][2] go to the specific job logs on GitHub. Hover over a commit SHA to see the commit message.</p>
  <table class="detail-table">
    <thead><tr><th>Date</th><th>Status</th><th>Commit</th><th>#</th><th>Unique Failures</th><th>Failed Jobs</th><th>Commits since prev run</th></tr></thead>
    <tbody>{run_detail_rows}</tbody>
  </table>
</div>

<details>
  <summary>Raw JSON data</summary>
  <pre>{report_json}</pre>
</details>
</body>
</html>
"""
