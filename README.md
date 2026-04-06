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

# Fetch a log and filter lines by regex (like grep)
valkey-oncall fetch-log --job-id 69448207219 --grep "FAILED|error" --context 3

# Parse a cached log for test failures
valkey-oncall parse-log --job-id 69448207219

# One-shot failure summary for a run — jobs, conclusions, first error per failure
valkey-oncall failures --run-id 23825749975
valkey-oncall failures --run-id 23825749975 --failed-only

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

# Markdown for pasting into GitHub issues/PRs
valkey-oncall report --format markdown -o report.md

# Slack mrkdwn format
valkey-oncall report --format slack -o report.txt

# Custom range and branch
valkey-oncall report --days 7 --branch stable --output weekly-report.html

# Skip auto-sync (use cached data only)
valkey-oncall report --no-sync

# Report on a different repo (e.g. your fork)
valkey-oncall --repo yourname/valkey report --branch unstable
```

The report includes:
- A timeline heatmap showing which tests failed on which days
- Failure frequency per test
- Per-run commit changelogs (commits between consecutive runs)
- Clickable job links `[1][2][3]` on each failure for quick access to logs
- Commit message tooltips on hover
- Failed job lists and error details on hover

## Options

```
--db PATH      SQLite cache path (default: ~/.valkey-oncall/cache.db)
--repo OWNER/NAME  GitHub repository (default: valkey-io/valkey)
-v             Verbose output to stderr during operations
```

## How it works

1. **Fetch** — pulls workflow run metadata and job details from the GitHub Actions API
2. **Cache** — stores everything in a local SQLite database to avoid redundant API calls
3. **Parse** — extracts test failure names and error messages from raw job logs using pattern matching
4. **Report** — aggregates cached data into trend reports

The log parser handles Valkey's Tcl test framework (`[err]`, `[exception]` stack traces), Google Test output, sentinel test failures, and GitHub Actions error annotations. It strips GitHub Actions timestamp prefixes automatically.

## Hosted Dashboard (GitHub Pages)

You can set up an auto-updating dashboard that regenerates daily and publishes to GitHub Pages. This works from any repo — it doesn't need to live in `valkey-io/valkey`.

### 1. Create a Personal Access Token

1. Go to [github.com → Settings → Developer settings → Fine-grained tokens](https://github.com/settings/personal-access-tokens/new)
2. Set a descriptive name like `valkey-ci-dashboard`
3. Under **Repository access**, select **Only select repositories** and pick `valkey-io/valkey`
4. Under **Permissions → Repository permissions**, set **Actions** to **Read-only**
5. Click **Generate token** and copy it

### 2. Add the token as a repository secret

1. In your dashboard repo, go to **Settings → Secrets and variables → Actions**
2. Click **New repository secret**
3. Name: `VALKEY_CI_TOKEN`, Value: paste the token from step 1

### 3. Enable GitHub Pages

1. In your repo, go to **Settings → Pages**
2. Under **Source**, select **GitHub Actions**

### 4. Add the workflow

The workflow file at `.github/workflows/dashboard.yml` is included in this repo. It:

- Runs daily at 06:00 UTC (after the nightly CI typically completes)
- Caches the SQLite database between runs for incremental sync
- Generates HTML and Markdown reports
- Deploys to GitHub Pages

You can also trigger it manually from the Actions tab.

### 5. Customize (optional)

Edit the workflow's "Generate reports" step to change the report parameters:

```yaml
- name: Generate reports
  env:
    GITHUB_TOKEN: ${{ secrets.VALKEY_CI_TOKEN }}
  run: |
    mkdir -p _site
    # Change --days, --branch, --workflow, or --repo as needed
    valkey-oncall --repo valkey-io/valkey report --days 14 --branch unstable --format html -o _site/index.html
    valkey-oncall --repo valkey-io/valkey report --days 14 --branch unstable --format markdown -o _site/report.md
```

To monitor a fork or different branch:
```yaml
    valkey-oncall --repo yourname/valkey report --branch my-feature --format html -o _site/index.html
```

### Notes

- The fine-grained PAT expires after at most 1 year. Set a reminder to rotate it.
- The PAT is tied to your GitHub account. For a team setup, consider using a [GitHub App](https://docs.github.com/en/apps) instead.
- The Actions cache persists the SQLite DB between runs. If the cache is evicted (after 7 days of inactivity), the next run does a full sync automatically — it just takes longer.
- Since `valkey-io/valkey` is public, run/job metadata works without auth. The token is mainly needed for downloading job logs and for higher rate limits (5,000/hr vs 60/hr).

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
