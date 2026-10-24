"""CLI entry point for the Valkey OnCall toolkit."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Optional

import click

from valkey_oncall.cache import Cache
from valkey_oncall.flakes import (
    DEFAULT_REGISTRY_PATH,
    FlakeEntry,
    FlakeRegistry,
    RegistryError,
    entry_report,
    maintenance,
    refresh_registry,
)
from valkey_oncall.github_client import (
    DEFAULT_REPO,
    GitHubActionsClient,
    GitHubAPIError,
)
from valkey_oncall.service import OnCallService


def _make_cache(db_path: str) -> Cache:
    """Create parent directories and return a Cache instance."""
    parent = Path(db_path).expanduser().parent
    parent.mkdir(parents=True, exist_ok=True)
    return Cache(str(Path(db_path).expanduser()))


def _make_client(repo: str = DEFAULT_REPO) -> GitHubActionsClient:
    """Build a GitHubActionsClient, warning if no token is set."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        click.echo(
            "Warning: GITHUB_TOKEN not set. Fetching runs and jobs works "
            "(60 requests/hour) but downloading logs requires authentication. "
            "Set GITHUB_TOKEN to enable log fetching and higher rate limits "
            "(5,000 requests/hour).",
            err=True,
        )
    return GitHubActionsClient(token=token, repo=repo)


def _release_strip(cache, repo: str) -> str:
    """Best-effort release-health strip HTML for the main page.

    Empty string when there is no weekly-split data yet, and never raises —
    the strip must not be able to break the main report render.
    """
    try:
        from valkey_oncall.releases import (
            generate_releases_data,
            render_release_strip,
        )

        rel = generate_releases_data(cache, repo=repo)
        return render_release_strip(rel["summary_rows"])
    except Exception as exc:  # pragma: no cover - defensive
        click.echo(f"Warning: release strip skipped: {exc}", err=True)
        return ""


def _require_token() -> str:
    """Return the GITHUB_TOKEN or exit with an error."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        click.echo(
            "Error: GITHUB_TOKEN is required for this command. "
            "GitHub requires authentication to download job logs. "
            "Set the GITHUB_TOKEN environment variable to a personal access token "
            "with 'actions:read' scope (or 'repo' scope for private repos).",
            err=True,
        )
        sys.exit(1)
    return token


@click.group()
@click.option(
    "--db",
    default="~/.valkey-oncall/cache.db",
    show_default=True,
    help="Path to the SQLite cache database.",
)
@click.option(
    "--repo",
    default=DEFAULT_REPO,
    show_default=True,
    help="GitHub repository (owner/name).",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Print progress messages to stderr during operations.",
)
@click.pass_context
def cli(ctx: click.Context, db: str, repo: str, verbose: bool) -> None:
    """Valkey OnCall CLI toolkit for monitoring CI test health."""
    ctx.ensure_object(dict)
    ctx.obj["db"] = db
    ctx.obj["repo"] = repo
    ctx.obj["verbose"] = verbose


# ------------------------------------------------------------------
# fetch-runs
# ------------------------------------------------------------------


@cli.command("fetch-runs")
@click.option("--workflow", required=True, help="Workflow type (daily or weekly).")
@click.option("--branch", default=None, help="Filter by branch name.")
@click.option(
    "--since", default=None, help="Fetch runs created after this date (ISO 8601)."
)
@click.option(
    "--until",
    "until_",
    default=None,
    help="Fetch runs created before this date (ISO 8601).",
)
@click.pass_context
def fetch_runs(
    ctx: click.Context,
    workflow: str,
    branch: Optional[str],
    since: Optional[str],
    until_: Optional[str],
) -> None:
    """Fetch workflow runs from GitHub Actions API."""
    try:
        cache = _make_cache(ctx.obj["db"])
        client = _make_client(ctx.obj["repo"])
        svc = OnCallService(client, cache)
        runs = svc.fetch_runs(workflow, branch=branch, since=since, until=until_)
        click.echo(json.dumps(runs, indent=2))
    except GitHubAPIError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


# ------------------------------------------------------------------
# fetch-jobs
# ------------------------------------------------------------------


@cli.command("fetch-jobs")
@click.option("--run-id", required=True, type=int, help="Workflow run ID.")
@click.pass_context
def fetch_jobs(ctx: click.Context, run_id: int) -> None:
    """Fetch jobs for a workflow run from GitHub Actions API."""
    try:
        cache = _make_cache(ctx.obj["db"])
        client = _make_client(ctx.obj["repo"])
        svc = OnCallService(client, cache)
        jobs = svc.fetch_jobs(run_id)
        click.echo(json.dumps(jobs, indent=2))
    except GitHubAPIError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


# ------------------------------------------------------------------
# fetch-log
# ------------------------------------------------------------------


@cli.command("fetch-log")
@click.option("--job-id", required=True, type=int, help="Job ID.")
@click.option(
    "--grep", "pattern", default=None, help="Filter log lines by regex pattern."
)
@click.option(
    "--context",
    "-C",
    "context_lines",
    default=0,
    type=int,
    help="Number of surrounding lines to include with --grep matches.",
)
@click.pass_context
def fetch_log(
    ctx: click.Context, job_id: int, pattern: Optional[str], context_lines: int
) -> None:
    """Fetch the raw log for a job from GitHub Actions API."""
    try:
        _require_token()
        cache = _make_cache(ctx.obj["db"])
        client = _make_client(ctx.obj["repo"])
        svc = OnCallService(client, cache)
        if pattern is not None:
            lines = svc.fetch_log_grep(job_id, pattern, context=context_lines)
            click.echo("\n".join(lines))
        else:
            raw_log = svc.fetch_log(job_id)
            click.echo(raw_log)
    except GitHubAPIError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


# ------------------------------------------------------------------
# parse-log
# ------------------------------------------------------------------


@cli.command("parse-log")
@click.option("--job-id", required=True, type=int, help="Job ID.")
@click.pass_context
def parse_log(ctx: click.Context, job_id: int) -> None:
    """Parse a cached job log for test failures."""
    try:
        cache = _make_cache(ctx.obj["db"])
        client = _make_client(ctx.obj["repo"])
        svc = OnCallService(client, cache)
        failures = svc.parse_log(job_id)
        if not failures:
            click.echo("No parseable test failures found.", err=True)
        click.echo(json.dumps(failures, indent=2))
    except GitHubAPIError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


# ------------------------------------------------------------------
# failures
# ------------------------------------------------------------------


@cli.command("failures")
@click.option("--run-id", required=True, type=int, help="Workflow run ID.")
@click.option(
    "--failed-only", is_flag=True, default=False, help="Show only failed jobs."
)
@click.pass_context
def failures(ctx: click.Context, run_id: int, failed_only: bool) -> None:
    """One-shot summary of jobs for a run with first error per failed job."""
    try:
        _require_token()
        cache = _make_cache(ctx.obj["db"])
        client = _make_client(ctx.obj["repo"])
        svc = OnCallService(client, cache)
        summary = svc.failures_summary(run_id, failed_only=failed_only)
        click.echo(json.dumps(summary, indent=2))
    except GitHubAPIError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


# ------------------------------------------------------------------
# query subgroup
# ------------------------------------------------------------------


@cli.group()
def query() -> None:
    """Query the local cache (no API calls)."""


@query.command("runs")
@click.option(
    "--workflow", default=None, help="Filter by workflow type (daily or weekly)."
)
@click.option(
    "--status", default=None, help="Filter by status (success, failure, cancelled)."
)
@click.option("--branch", default=None, help="Filter by branch name.")
@click.option(
    "--since", default=None, help="Filter runs on or after this date (ISO 8601)."
)
@click.option(
    "--until",
    "until_",
    default=None,
    help="Filter runs on or before this date (ISO 8601).",
)
@click.pass_context
def query_runs(
    ctx: click.Context,
    workflow: Optional[str],
    status: Optional[str],
    branch: Optional[str],
    since: Optional[str],
    until_: Optional[str],
) -> None:
    """List cached workflow runs."""
    cache = _make_cache(ctx.obj["db"])
    runs = cache.query_runs(
        workflow=workflow, status=status, branch=branch, since=since, until=until_
    )
    click.echo(json.dumps(runs, indent=2))


@query.command("jobs")
@click.option("--run-id", required=True, type=int, help="Workflow run ID.")
@click.option(
    "--failed-only", is_flag=True, default=False, help="Show only failed jobs."
)
@click.pass_context
def query_jobs(ctx: click.Context, run_id: int, failed_only: bool) -> None:
    """List cached jobs for a workflow run."""
    cache = _make_cache(ctx.obj["db"])
    jobs = cache.query_jobs(run_id, failed_only=failed_only)
    click.echo(json.dumps(jobs, indent=2))


@query.command("failures")
@click.option("--job-id", default=None, type=int, help="Filter by job ID.")
@click.option(
    "--test-name", default=None, help="Filter by test name pattern (SQL LIKE)."
)
@click.option(
    "--since", default=None, help="Filter failures on or after this date (ISO 8601)."
)
@click.option(
    "--until",
    "until_",
    default=None,
    help="Filter failures on or before this date (ISO 8601).",
)
@click.pass_context
def query_failures(
    ctx: click.Context,
    job_id: Optional[int],
    test_name: Optional[str],
    since: Optional[str],
    until_: Optional[str],
) -> None:
    """List cached test failures."""
    cache = _make_cache(ctx.obj["db"])
    failures = cache.query_failures(
        job_id=job_id, test_name_pattern=test_name, since=since, until=until_
    )
    click.echo(json.dumps(failures, indent=2))


# ------------------------------------------------------------------
# flakes subgroup
# ------------------------------------------------------------------


@cli.group()
@click.option(
    "--registry",
    default=DEFAULT_REGISTRY_PATH,
    show_default=True,
    help="Path to the git-tracked registry file.",
)
@click.pass_context
def flakes(ctx: click.Context, registry: str) -> None:
    """Known flaky failures tied to the issue or PR tracking them."""
    ctx.obj["registry"] = registry


def _load_registry(ctx: click.Context) -> FlakeRegistry:
    try:
        return FlakeRegistry.load(ctx.obj["registry"])
    except RegistryError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


@flakes.command("list")
@click.option("--days", default=30, type=int, show_default=True)
@click.pass_context
def flakes_list(ctx: click.Context, days: int) -> None:
    """List entries with sightings derived from the cache."""
    reg = _load_registry(ctx)
    cache = _make_cache(ctx.obj["db"])
    out = [entry_report(cache, e, days) for e in reg.entries()]
    click.echo(json.dumps(out, indent=2))


@flakes.command("add")
@click.option("--test", "test_name", default=None, help="Exact parser test_name.")
@click.option(
    "--summary",
    default=None,
    help="Failure mode (parser error_summary). Omit to claim every mode.",
)
@click.option(
    "--from-job",
    default=None,
    type=int,
    help="Copy test_name and error_summary from this cached job's failure row.",
)
@click.option("--issue", default=None, type=int, help="Tracking issue number.")
@click.option("--fix-pr", default=None, type=int, help="PR expected to fix it.")
@click.option("--note", default="", help="Free-text note.")
@click.pass_context
def flakes_add(
    ctx: click.Context,
    test_name: Optional[str],
    summary: Optional[str],
    from_job: Optional[int],
    issue: Optional[int],
    fix_pr: Optional[int],
    note: str,
) -> None:
    """Register a failure signature.

    With --from-job the signature is copied from a cached failure row, so
    the stored summary is the parser's own output rather than a hand-typed
    pattern. Use --test alongside it when the job has several failures.
    """
    if from_job is not None:
        cache = _make_cache(ctx.obj["db"])
        rows = cache.query_failures(job_id=from_job)
        if test_name is not None:
            rows = [r for r in rows if r["test_name"] == test_name]
        if not rows:
            click.echo(f"Error: no cached failure for job {from_job}", err=True)
            sys.exit(1)
        if len(rows) > 1:
            names = sorted({r["test_name"] for r in rows})
            click.echo(
                f"Error: job {from_job} has {len(rows)} failures; pick one with "
                "--test:\n  " + "\n  ".join(names),
                err=True,
            )
            sys.exit(1)
        test_name = rows[0]["test_name"]
        if summary is None:
            summary = rows[0]["error_summary"]
    if test_name is None:
        click.echo("Error: --test or --from-job is required", err=True)
        sys.exit(1)
    reg = _load_registry(ctx)
    try:
        entry = reg.add(
            FlakeEntry(
                test_name=test_name,
                summary=summary,
                issue=issue,
                fix_pr=fix_pr,
                note=note,
            )
        )
    except RegistryError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    reg.save(ctx.obj["registry"])
    click.echo(json.dumps(entry.to_dict(), indent=2))


@flakes.command("fixed")
@click.argument("entry_id")
@click.option("--sha", required=True, help="Merge commit of the fix.")
@click.option("--merged-at", required=True, help="Merge date, YYYY-MM-DD.")
@click.option("--pr", default=None, type=int, help="Fix PR, if not set at add.")
@click.pass_context
def flakes_fixed(
    ctx: click.Context, entry_id: str, sha: str, merged_at: str, pr: Optional[int]
) -> None:
    """Record that an entry's fix merged. Later sightings become recurrences."""
    reg = _load_registry(ctx)
    try:
        entry = reg.mark_fixed(
            entry_id, fix_sha=sha, fix_merged_at=merged_at, fix_pr=pr
        )
    except RegistryError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    reg.save(ctx.obj["registry"])
    click.echo(json.dumps(entry.to_dict(), indent=2))


@flakes.command("remove")
@click.argument("entry_id")
@click.pass_context
def flakes_remove(ctx: click.Context, entry_id: str) -> None:
    """Delete an entry."""
    reg = _load_registry(ctx)
    try:
        entry = reg.remove(entry_id)
    except RegistryError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    reg.save(ctx.obj["registry"])
    click.echo(json.dumps(entry.to_dict(), indent=2))


@flakes.command("match")
@click.option("--test", "test_name", required=True, help="Exact parser test_name.")
@click.option("--summary", default=None, help="Failure mode to confirm against.")
@click.pass_context
def flakes_match(ctx: click.Context, test_name: str, summary: Optional[str]) -> None:
    """Look up a failure. Exact matches first; test_only means unconfirmed."""
    reg = _load_registry(ctx)
    out = [
        {"tier": m.tier, **m.entry.to_dict(), "status": m.entry.status}
        for m in reg.match(test_name, summary)
    ]
    click.echo(json.dumps(out, indent=2))


@flakes.command("check")
@click.option("--days", default=30, type=int, show_default=True)
@click.pass_context
def flakes_check(ctx: click.Context, days: int) -> None:
    """Maintenance list: recurrences after a fix, pattern drift, quiet entries."""
    reg = _load_registry(ctx)
    cache = _make_cache(ctx.obj["db"])
    click.echo(json.dumps(maintenance(cache, reg, days), indent=2))


@flakes.command("refresh")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Report what would change without writing the registry.",
)
@click.pass_context
def flakes_refresh(ctx: click.Context, dry_run: bool) -> None:
    """Read issue and fix-PR state from GitHub into the registry.

    Caches each entry's issue state and title, and records the fix when the
    issue was closed by a merged PR (or the registered fix PR merged). An
    issue closed with no fix commit is flagged, not silently dropped.
    """
    _require_token()
    reg = _load_registry(ctx)
    client = _make_client(ctx.obj["repo"])
    try:
        reports = refresh_registry(reg, client)
    except GitHubAPIError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    if not dry_run:
        reg.save(ctx.obj["registry"])
    changed = sum(1 for r in reports if r["changes"])
    flagged = sum(1 for r in reports if r.get("flag"))
    click.echo(json.dumps(reports, indent=2))
    click.echo(
        f"{len(reports)} entries refreshed, {changed} changed, {flagged} flagged"
        + (" (dry run, registry not written)" if dry_run else ""),
        err=True,
    )


# ------------------------------------------------------------------
# sync
# ------------------------------------------------------------------


@cli.command()
@click.option(
    "--workflow", default=None, help="Limit sync to a workflow type (daily or weekly)."
)
@click.option("--branch", default=None, help="Filter by branch name.")
@click.option(
    "--since", default=None, help="Sync runs created after this date (ISO 8601)."
)
@click.option(
    "--until",
    "until_",
    default=None,
    help="Sync runs created before this date (ISO 8601).",
)
@click.pass_context
def sync(
    ctx: click.Context,
    workflow: Optional[str],
    branch: Optional[str],
    since: Optional[str],
    until_: Optional[str],
) -> None:
    """Run a full incremental sync of workflow data."""
    try:
        _require_token()
        cache = _make_cache(ctx.obj["db"])
        client = _make_client(ctx.obj["repo"])
        svc = OnCallService(client, cache)

        progress = None
        if ctx.obj.get("verbose"):

            def progress(msg):
                click.echo(msg, err=True)

        summary = svc.sync(
            workflow=workflow,
            branch=branch,
            since=since,
            until=until_,
            progress=progress,
        )
        click.echo(json.dumps(summary, indent=2))
        if summary.get("auth_failed"):
            click.echo(
                "Error: GitHub authentication failed during sync (token "
                "expired/revoked or lacks scope).",
                err=True,
            )
            sys.exit(1)
    except GitHubAPIError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


# ------------------------------------------------------------------
# sync-releases
# ------------------------------------------------------------------


@cli.command("sync-releases")
@click.option(
    "--budget",
    default=300,
    show_default=True,
    type=int,
    help="Max GitHub API calls this invocation may spend; unfinished "
    "backfill resumes on the next invocation.",
)
@click.pass_context
def sync_releases(ctx: click.Context, budget: int) -> None:
    """Ingest weekly release-branch runs as per-branch series (budgeted)."""
    try:
        _require_token()
        cache = _make_cache(ctx.obj["db"])
        client = _make_client(ctx.obj["repo"])
        svc = OnCallService(client, cache)

        progress = None
        if ctx.obj.get("verbose"):

            def progress(msg):
                click.echo(msg, err=True)

        summary = svc.sync_weekly_branches(budget=budget, progress=progress)
        click.echo(json.dumps(summary, indent=2))
        if summary.get("auth_failed"):
            click.echo(
                "Error: GitHub authentication failed during release sync.",
                err=True,
            )
            sys.exit(1)
    except GitHubAPIError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


# ------------------------------------------------------------------
# report-releases
# ------------------------------------------------------------------


@cli.command("report-releases")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["html", "json"]),
    default="html",
    show_default=True,
    help="Output format.",
)
@click.option(
    "-o",
    "--output",
    default=None,
    help="Write output to this file instead of stdout.",
)
@click.pass_context
def report_releases(ctx: click.Context, fmt: str, output: Optional[str]) -> None:
    """Render the release-branch health page from cached weekly-split data.

    Reads only the cache — run ``sync-releases`` first to ingest data.
    """
    from valkey_oncall.releases import generate_releases_data, render_releases_html

    cache = _make_cache(ctx.obj["db"])
    data = generate_releases_data(cache, repo=ctx.obj["repo"])
    if fmt == "html":
        out = render_releases_html(data)
    else:
        out = json.dumps(data, indent=2)
    if output:
        with open(output, "w") as fh:
            fh.write(out)
        click.echo(f"Wrote {output}", err=True)
    else:
        click.echo(out)


# ------------------------------------------------------------------
# scorecard
# ------------------------------------------------------------------


@cli.command()
@click.option(
    "--days",
    default=30,
    show_default=True,
    help="Number of days of history for scorecard window.",
)
@click.option(
    "--branch", default="unstable", show_default=True, help="Branch to report on."
)
@click.option(
    "--workflow",
    default="daily",
    show_default=True,
    help="Workflow type (daily or weekly).",
)
@click.option(
    "--output", "-o", default=None, help="Output file path (default: stdout)."
)
@click.option(
    "--no-sync", is_flag=True, default=False, help="Skip syncing latest data."
)
@click.pass_context
def scorecard(
    ctx: click.Context,
    days: int,
    branch: str,
    workflow: str,
    output: Optional[str],
    no_sync: bool,
) -> None:
    """Generate per-test flakiness scorecards as JSON."""
    from valkey_oncall.scorecard import compute_scorecards

    cache = _make_cache(ctx.obj["db"])
    repo = ctx.obj["repo"]
    workflow_file = {"daily": "daily.yml", "weekly": "weekly.yml"}.get(
        workflow, workflow
    )

    token = os.environ.get("GITHUB_TOKEN")
    if not no_sync and token:
        client = GitHubActionsClient(token=token, repo=repo)
        verbose = ctx.obj.get("verbose")
        progress = (lambda msg: click.echo(msg, err=True)) if verbose else None
        click.echo(f"Syncing latest data for {repo} {workflow} / {branch}...", err=True)
        svc = OnCallService(client, cache)
        svc.sync(workflow=workflow_file, branch=branch, progress=progress)

    data = compute_scorecards(
        cache, days=days, branch=branch, workflow=workflow_file, repo=repo
    )
    content = json.dumps(data, indent=2)

    if output:
        Path(output).write_text(content)
        click.echo(f"Scorecard written to {output}", err=True)
    else:
        click.echo(content)


# ------------------------------------------------------------------
# blame
# ------------------------------------------------------------------


@cli.command()
@click.option(
    "--days",
    default=30,
    show_default=True,
    help="Number of days of history to search for regressions.",
)
@click.option(
    "--branch", default="unstable", show_default=True, help="Branch to analyze."
)
@click.option(
    "--workflow",
    default="daily",
    show_default=True,
    help="Workflow type (daily or weekly).",
)
@click.option(
    "--output", "-o", default=None, help="Output file path (default: stdout)."
)
@click.option(
    "--no-sync", is_flag=True, default=False, help="Skip syncing latest data."
)
@click.pass_context
def blame(
    ctx: click.Context,
    days: int,
    branch: str,
    workflow: str,
    output: Optional[str],
    no_sync: bool,
) -> None:
    """Identify commits likely responsible for test regressions."""
    from valkey_oncall.blame import compute_blame

    _require_token()
    cache = _make_cache(ctx.obj["db"])
    repo = ctx.obj["repo"]
    workflow_file = {"daily": "daily.yml", "weekly": "weekly.yml"}.get(
        workflow, workflow
    )

    token = os.environ.get("GITHUB_TOKEN")
    client = GitHubActionsClient(token=token, repo=repo)

    if not no_sync:
        verbose = ctx.obj.get("verbose")
        progress = (lambda msg: click.echo(msg, err=True)) if verbose else None
        click.echo(f"Syncing latest data for {repo} {workflow} / {branch}...", err=True)
        svc = OnCallService(client, cache)
        svc.sync(workflow=workflow_file, branch=branch, progress=progress)

    records = compute_blame(
        cache, client, days=days, branch=branch, workflow=workflow_file, repo=repo
    )
    content = json.dumps(records, indent=2)

    if output:
        Path(output).write_text(content)
        click.echo(f"Blame report written to {output}", err=True)
    else:
        click.echo(content)


# ------------------------------------------------------------------
# report
# ------------------------------------------------------------------


@cli.command()
@click.option(
    "--days", default=30, show_default=True, help="Number of days of history."
)
@click.option(
    "--branch", default="unstable", show_default=True, help="Branch to report on."
)
@click.option(
    "--workflow",
    default="daily",
    show_default=True,
    help="Workflow type (daily or weekly).",
)
@click.option(
    "--format",
    "-f",
    "fmt",
    type=click.Choice(["html", "markdown", "slack"], case_sensitive=False),
    default="html",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--output", "-o", default=None, help="Output file path (default depends on format)."
)
@click.option(
    "--no-sync",
    is_flag=True,
    default=False,
    help="Skip syncing latest data before generating the report.",
)
@click.option(
    "--with-ci",
    is_flag=True,
    default=False,
    help="Also render the CI (per-commit) workflow stacked above Daily (HTML only).",
)
@click.option(
    "--registry",
    default=DEFAULT_REGISTRY_PATH,
    show_default=True,
    help="Flake registry to populate the 'tracked' column from (missing file = empty).",
)
@click.pass_context
def report(
    ctx: click.Context,
    days: int,
    branch: str,
    workflow: str,
    fmt: str,
    output: str,
    no_sync: bool,
    with_ci: bool,
    registry: str,
) -> None:
    """Generate a failure trend report, syncing latest data first."""
    from valkey_oncall.report import (
        generate_report_data,
        render_html,
        render_markdown,
        render_slack,
    )

    cache = _make_cache(ctx.obj["db"])
    repo = ctx.obj["repo"]
    workflow_file = {"daily": "daily.yml", "weekly": "weekly.yml"}.get(
        workflow, workflow
    )

    token = os.environ.get("GITHUB_TOKEN")
    client = GitHubActionsClient(token=token, repo=repo) if token else None

    # Sync latest data unless --no-sync is passed
    if not no_sync:
        if not token:
            click.echo(
                "Warning: GITHUB_TOKEN not set, skipping sync. "
                "Use --no-sync to silence this warning.",
                err=True,
            )
        else:
            verbose = ctx.obj.get("verbose")
            progress = (lambda msg: click.echo(msg, err=True)) if verbose else None
            click.echo(
                f"Syncing latest data for {repo} {workflow} / {branch}...", err=True
            )
            svc = OnCallService(client, cache)
            sync_summary = svc.sync(
                workflow=workflow_file,
                branch=branch,
                progress=progress,
            )
            click.echo(
                f"Sync: {sync_summary['new_runs_fetched']} new runs, "
                f"{sync_summary['new_failures_parsed']} new failures parsed",
                err=True,
            )
            if sync_summary["errors"]:
                for err in sync_summary["errors"]:
                    click.echo(f"  sync error: {err}", err=True)
            if sync_summary.get("auth_failed"):
                click.echo(
                    "Error: GitHub authentication failed during sync (token "
                    "expired/revoked or lacks scope). Aborting so the failure "
                    "is not silently masked by stale cached data.",
                    err=True,
                )
                sys.exit(1)

    click.echo(
        f"Generating report for {repo} {workflow} / {branch} (last {days} days)...",
        err=True,
    )
    try:
        flake_registry = FlakeRegistry.load(registry)
    except RegistryError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    data = generate_report_data(
        cache,
        days=days,
        branch=branch,
        workflow=workflow_file,
        repo=repo,
        client=client,
        registry=flake_registry,
    )

    renderers = {
        "html": render_html,
        "markdown": render_markdown,
        "slack": render_slack,
    }
    if fmt == "html":
        ci_data = None
        if with_ci:
            # CI (per-commit) view, read from cache alongside Daily and stacked
            # on top. Empty columns render gracefully if CI isn't synced yet.
            ci_data = generate_report_data(
                cache,
                branch=branch,
                workflow="ci.yml",
                repo=repo,
                client=client,
                per_run=True,
                max_runs=40,
                registry=flake_registry,
            )
        content = render_html(
            data, ci_data=ci_data, releases_strip=_release_strip(cache, repo)
        )
    else:
        content = renderers[fmt](data)

    default_ext = {"html": ".html", "markdown": ".md", "slack": ".txt"}
    if output is None:
        output = f"report{default_ext[fmt]}"

    Path(output).write_text(content)
    click.echo(f"Report written to {output}", err=True)
    click.echo(json.dumps(data["summary"], indent=2))


if __name__ == "__main__":
    cli()
