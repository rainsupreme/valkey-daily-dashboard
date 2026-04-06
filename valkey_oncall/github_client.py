"""GitHub Actions API client for the valkey-io/valkey repository."""

from __future__ import annotations

from typing import Dict, List, Optional

import httpx


DEFAULT_REPO = "valkey-io/valkey"
BASE_URL = "https://api.github.com"


class GitHubAPIError(Exception):
    """Raised on non-2xx responses from the GitHub API."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(f"GitHub API returned {status_code}: {message}")


class RateLimitError(GitHubAPIError):
    """Raised when GitHub returns 403 or 429 (rate limit exceeded)."""

    def __init__(self, status_code: int, message: str, reset_at: Optional[str] = None) -> None:
        self.reset_at = reset_at
        super().__init__(status_code, message)


class GitHubActionsClient:
    """Read-only client for the GitHub Actions REST API."""

    def __init__(self, token: Optional[str] = None, repo: str = DEFAULT_REPO) -> None:
        self.repo = repo
        headers: Dict[str, str] = {"Accept": "application/vnd.github+json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.Client(base_url=BASE_URL, headers=headers, follow_redirects=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _raise_for_status(self, response: httpx.Response) -> None:
        """Raise the appropriate error for non-2xx responses."""
        if response.is_success:
            return

        status = response.status_code
        try:
            body = response.json()
            message = body.get("message", response.text)
        except Exception:
            message = response.text

        if status in (403, 429):
            reset_at = response.headers.get("X-RateLimit-Reset")
            raise RateLimitError(status, message, reset_at=reset_at)

        raise GitHubAPIError(status, message)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_workflow_runs(
        self,
        workflow_file: str,
        branch: Optional[str] = None,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
        page: int = 1,
        per_page: int = 100,
    ) -> List[Dict]:
        """Fetch workflow runs, automatically paginating through all results.

        Returns the combined list of run dicts from every page.
        """
        all_runs: List[Dict] = []
        current_page = page

        while True:
            params: Dict[str, object] = {
                "page": current_page,
                "per_page": per_page,
            }
            if branch is not None:
                params["branch"] = branch
            if created_after or created_before:
                # GitHub uses the `created` query param with range syntax
                parts: List[str] = []
                if created_after:
                    parts.append(f">={created_after}")
                if created_before:
                    parts.append(f"<={created_before}")
                params["created"] = "..".join(parts) if len(parts) == 2 else parts[0]

            resp = self._client.get(
                f"/repos/{self.repo}/actions/workflows/{workflow_file}/runs",
                params=params,
            )
            self._raise_for_status(resp)

            data = resp.json()
            runs = data.get("workflow_runs", [])
            all_runs.extend(runs)

            # Stop when we've received fewer results than a full page
            if len(runs) < per_page:
                break

            current_page += 1

        return all_runs

    def get_jobs_for_run(self, run_id: int) -> List[Dict]:
        """Fetch all jobs for a given workflow run ID."""
        all_jobs: List[Dict] = []
        current_page = 1
        per_page = 100

        while True:
            resp = self._client.get(
                f"/repos/{self.repo}/actions/runs/{run_id}/jobs",
                params={"page": current_page, "per_page": per_page},
            )
            self._raise_for_status(resp)

            data = resp.json()
            jobs = data.get("jobs", [])
            all_jobs.extend(jobs)

            if len(jobs) < per_page:
                break

            current_page += 1

        return all_jobs

    def get_job_log(self, job_id: int) -> str:
        """Fetch the raw log text for a given job ID."""
        resp = self._client.get(f"/repos/{self.repo}/actions/jobs/{job_id}/logs")
        self._raise_for_status(resp)
        return resp.text

    def compare_commits(self, base: str, head: str) -> List[Dict]:
        """Return the list of commits between *base* and *head*.

        Uses ``GET /repos/{owner}/{repo}/compare/{base}...{head}``.
        Returns a list of compact commit dicts (sha, message, author, date).
        """
        resp = self._client.get(
            f"/repos/{self.repo}/compare/{base}...{head}",
            params={"per_page": 100},
        )
        self._raise_for_status(resp)
        data = resp.json()
        return [
            {
                "sha": c["sha"],
                "message": (c.get("commit", {}).get("message", "") or "").split("\n")[0],
                "author": (c.get("commit", {}).get("author", {}) or {}).get("name", ""),
                "date": (c.get("commit", {}).get("author", {}) or {}).get("date", ""),
            }
            for c in data.get("commits", [])
        ]

    def get_commit(self, sha: str) -> Dict:
        """Fetch a single commit and return a compact dict."""
        resp = self._client.get(f"/repos/{self.repo}/commits/{sha}")
        self._raise_for_status(resp)
        data = resp.json()
        commit = data.get("commit", {})
        message = (commit.get("message", "") or "")
        return {
            "sha": data.get("sha", sha),
            "message_subject": message.split("\n")[0],
            "message_full": message,
            "author": (commit.get("author", {}) or {}).get("name", ""),
        }
