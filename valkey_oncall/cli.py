"""CLI entry point for the Valkey OnCall toolkit."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Optional

import click

from valkey_oncall.cache import Cache
from valkey_oncall.github_client import GitHubActionsClient, GitHubAPIError
from valkey_oncall.service import OnCallService


def _make_cache(db_path: str) -> Cache:
    """Create parent directories and return a Cache instance."""
    parent = Path(db_path).expanduser().parent
    parent.mkdir(parents=True, exist_ok=True)
    return Cache(str(Path(db_path).expanduser()))


def _make_client() -> GitHubActionsClient:
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
    return GitHubActionsClient(token=token)


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
    "-v", "--verbose",
    is_flag=True,
    default=False,
    help="Print progress messages to stderr during operations.",
)
@click.pass_context
def cli(ctx: click.Context, db: str, verbose: bool) -> None:
    """Valkey OnCall CLI toolkit for monitoring CI test health."""
    ctx.ensure_object(dict)
    ctx.obj["db"] = db
    ctx.obj["verbose"] = verbose


# ------------------------------------------------------------------
# fetch-runs
# ------------------------------------------------------------------

@cli.command("fetch-runs")
@click.option("--workflow", required=True, help="Workflow type (daily or weekly).")
@click.option("--branch", default=None, help="Filter by branch name.")
@click.option("--since", default=None, help="Fetch runs created after this date (ISO 8601).")
@click.option("--until", "until_", default=None, help="Fetch runs created before this date (ISO 8601).")
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
        client = _make_client()
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
        client = _make_client()
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
@click.pass_context
def fetch_log(ctx: click.Context, job_id: int) -> None:
    """Fetch the raw log for a job from GitHub Actions API."""
    try:
        _require_token()
        cache = _make_cache(ctx.obj["db"])
        client = _make_client()
        svc = OnCallService(client, cache)
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
        client = _make_client()
        svc = OnCallService(client, cache)
        failures = svc.parse_log(job_id)
        if not failures:
            click.echo("No parseable test failures found.", err=True)
        click.echo(json.dumps(failures, indent=2))
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
@click.option("--workflow", default=None, help="Filter by workflow type (daily or weekly).")
@click.option("--status", default=None, help="Filter by status (success, failure, cancelled).")
@click.option("--branch", default=None, help="Filter by branch name.")
@click.option("--since", default=None, help="Filter runs on or after this date (ISO 8601).")
@click.option("--until", "until_", default=None, help="Filter runs on or before this date (ISO 8601).")
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
@click.option("--failed-only", is_flag=True, default=False, help="Show only failed jobs.")
@click.pass_context
def query_jobs(ctx: click.Context, run_id: int, failed_only: bool) -> None:
    """List cached jobs for a workflow run."""
    cache = _make_cache(ctx.obj["db"])
    jobs = cache.query_jobs(run_id, failed_only=failed_only)
    click.echo(json.dumps(jobs, indent=2))


@query.command("failures")
@click.option("--job-id", default=None, type=int, help="Filter by job ID.")
@click.option("--test-name", default=None, help="Filter by test name pattern (SQL LIKE).")
@click.option("--since", default=None, help="Filter failures on or after this date (ISO 8601).")
@click.option("--until", "until_", default=None, help="Filter failures on or before this date (ISO 8601).")
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
# sync
# ------------------------------------------------------------------

@cli.command()
@click.option("--workflow", default=None, help="Limit sync to a workflow type (daily or weekly).")
@click.option("--branch", default=None, help="Filter by branch name.")
@click.option("--since", default=None, help="Sync runs created after this date (ISO 8601).")
@click.option("--until", "until_", default=None, help="Sync runs created before this date (ISO 8601).")
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
        client = _make_client()
        svc = OnCallService(client, cache)

        progress = None
        if ctx.obj.get("verbose"):
            progress = lambda msg: click.echo(msg, err=True)

        summary = svc.sync(
            workflow=workflow, branch=branch, since=since, until=until_,
            progress=progress,
        )
        click.echo(json.dumps(summary, indent=2))
    except GitHubAPIError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


# ------------------------------------------------------------------
# report
# ------------------------------------------------------------------

@cli.command()
@click.option("--days", default=14, show_default=True, help="Number of days of history.")
@click.option("--branch", default="unstable", show_default=True, help="Branch to report on.")
@click.option("--workflow", default="daily", show_default=True, help="Workflow type (daily or weekly).")
@click.option("--output", "-o", default="report.html", show_default=True, help="Output HTML file path.")
@click.pass_context
def report(
    ctx: click.Context,
    days: int,
    branch: str,
    workflow: str,
    output: str,
) -> None:
    """Generate an HTML failure trend report from cached data."""
    from valkey_oncall.report import generate_report_data, render_html

    cache = _make_cache(ctx.obj["db"])
    workflow_file = {"daily": "daily.yml", "weekly": "weekly.yml"}.get(workflow, workflow)

    # Use a client for fetching inter-run commits if a token is available
    token = os.environ.get("GITHUB_TOKEN")
    client = GitHubActionsClient(token=token) if token else None

    click.echo(f"Generating report for {workflow} / {branch} (last {days} days)...", err=True)
    data = generate_report_data(cache, days=days, branch=branch, workflow=workflow_file, client=client)
    html_content = render_html(data)

    Path(output).write_text(html_content)
    click.echo(f"Report written to {output}", err=True)
    click.echo(json.dumps(data["summary"], indent=2))


if __name__ == "__main__":
    cli()
