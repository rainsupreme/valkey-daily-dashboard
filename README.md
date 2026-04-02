# valkey-oncall

CLI toolkit for monitoring CI test health in the [valkey-io/valkey](https://github.com/valkey-io/valkey) repository. Fetches GitHub Actions workflow runs, downloads job logs, parses them for test failures, and generates trend reports.

## Quick start

```bash
pip install -e ".[dev]"
export GITHUB_TOKEN=ghp_...   # or: export GITHUB_TOKEN=$(gh auth token)

# Sync the last week of daily CI runs
valkey-oncall sync --workflow daily --since 2026-03-25

# Generate an HTML failure trend report
valkey-oncall report --days 14
open report.html
```

## Authentication

A GitHub personal access token is required for downloading job logs and running sync. Fetching run/job metadata works without a token (at 60 requests/hour), but log downloads and the `sync` command will fail without one.

Create a token at https://github.com/settings/tokens with `public_repo` scope, or a fine-grained token with Actions: Read on `valkey-io/valkey`.

If you have the `gh` CLI installed:
```bash
export GITHUB_TOKEN=$(gh auth token)
```

## Commands

### Fetching data

```bash
# Fetch workflow runs (metadata only, works without token)
valkey-oncall fetch-runs --workflow daily --since 2026-03-01

# Fetch jobs for a specific run
valkey-oncall fetch-jobs --run-id 23825749975

# Fetch a job's raw log (requires token)
valkey-oncall fetch-log --job-id 69448207219

# Parse a cached log for test failures
valkey-oncall parse-log --job-id 69448207219

# Full incremental sync — fetch runs, jobs, logs, parse failures (requires token)
valkey-oncall sync --workflow daily --since 2026-03-25
```

### Querying cached data

All query commands work offline from the local SQLite cache:

```bash
# List cached runs
valkey-oncall query runs --workflow daily.yml --status failure --branch unstable

# List jobs for a run
valkey-oncall query jobs --run-id 23825749975 --failed-only

# Search test failures
valkey-oncall query failures --test-name "%maxmemory%"
```

### Reports

```bash
# Generate a 14-day HTML failure trend report (default)
valkey-oncall report

# Custom range
valkey-oncall report --days 7 --output weekly-report.html

# With commit changelogs between runs (requires token)
GITHUB_TOKEN=$(gh auth token) valkey-oncall report --days 14
```

The report includes:
- A timeline grid showing which tests failed on which days
- Failure frequency per test
- Per-run commit changelogs (commits between consecutive runs)
- Failed job lists and error details on hover

## Options

```
--db PATH    SQLite cache path (default: ~/.valkey-oncall/cache.db)
-v           Verbose output to stderr during operations
```

## How it works

1. **Fetch** — pulls workflow run metadata and job details from the GitHub Actions API
2. **Cache** — stores everything in a local SQLite database to avoid redundant API calls
3. **Parse** — extracts test failure names and error messages from raw job logs using pattern matching
4. **Report** — aggregates cached data into trend reports

The log parser handles Valkey's Tcl test framework (`[err]`, `[exception]` stack traces), Google Test output, sentinel test failures, and GitHub Actions error annotations. It strips GitHub Actions timestamp prefixes automatically.

## Development

```bash
pip install -e ".[dev]"
pytest
```

Tests include unit tests, property-based tests (hypothesis), and real-world log parser regression tests built from actual Valkey CI log snippets.

## Project structure

```
valkey_oncall/
  cache.py          — SQLite storage layer
  github_client.py  — GitHub Actions API client
  log_parser.py     — CI log failure extraction
  service.py        — orchestration (sync, fetch, parse)
  report.py         — HTML report generation
  cli.py            — click CLI entry point

tests/
  test_cache.py                — cache unit + property tests
  test_github_client.py        — API client tests
  test_log_parser.py           — parser unit + property tests
  test_log_parser_realworld.py — regression tests from real CI logs
  test_service.py              — service layer tests
  test_cli.py                  — CLI integration tests
```
