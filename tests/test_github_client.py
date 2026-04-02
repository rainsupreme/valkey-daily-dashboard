"""Property-based tests for GitHubActionsClient."""

import httpx
from hypothesis import given, settings
from hypothesis import strategies as st

from valkey_oncall.github_client import GitHubActionsClient


# Strategy: generate either None or a non-empty ASCII token string.
# Real GitHub tokens are ASCII-only, and HTTP headers require ASCII-encodable values.
token_strategy = st.one_of(
    st.none(),
    st.text(
        alphabet=st.characters(whitelist_categories=("L", "N", "P", "S"), max_codepoint=127),
        min_size=1,
        max_size=50,
    ),
)


def _capture_transport(captured_requests: list):
    """Return an httpx transport that records requests and returns a canned response."""

    def handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(
            200,
            json={"workflow_runs": [], "total_count": 0},
        )

    return httpx.MockTransport(handler)


class TestAuthTokenProperty:
    """Property 6: Auth token is included in all requests when provided.

    Validates: Requirements 6.2, 6.3
    """

    @given(token=token_strategy)
    @settings(max_examples=100)
    def test_auth_header_matches_token_presence(self, token):
        """For any token (including None), the Authorization header on outgoing
        requests must be present iff the token is truthy, and its value must be
        ``Bearer <token>``.

        **Validates: Requirements 6.2, 6.3**
        """
        captured: list[httpx.Request] = []

        client = GitHubActionsClient(token=token)
        # Swap the transport so requests are captured locally instead of hitting the network
        client._client._transport = _capture_transport(captured)

        try:
            client.get_workflow_runs(workflow_file="daily.yml")

            assert len(captured) == 1
            request = captured[0]

            if token:
                assert "authorization" in request.headers
                assert request.headers["authorization"] == f"Bearer {token}"
            else:
                assert "authorization" not in request.headers
        finally:
            client._client.close()


from typing import Optional

import pytest

from valkey_oncall.github_client import GitHubAPIError, RateLimitError


# ---------------------------------------------------------------------------
# Helpers for unit tests
# ---------------------------------------------------------------------------


def _error_transport(status_code: int, headers: Optional[dict] = None):
    """Return a MockTransport that always responds with the given status code."""
    headers = headers or {}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code,
            json={"message": f"Error {status_code}"},
            headers=headers,
        )

    return httpx.MockTransport(handler)


# ---------------------------------------------------------------------------
# Unit tests – error handling (task 3.3)
# ---------------------------------------------------------------------------


class TestGitHubClientErrorHandling:
    """Unit tests for GitHubActionsClient error responses.

    Requirements: 1.6, 2.5, 3.5
    """

    def test_404_raises_github_api_error(self):
        """A 404 response must raise GitHubAPIError with status_code=404."""
        client = GitHubActionsClient(token="fake-token")
        client._client._transport = _error_transport(404)

        try:
            with pytest.raises(GitHubAPIError) as exc_info:
                client.get_workflow_runs(workflow_file="daily.yml")
            assert exc_info.value.status_code == 404
        finally:
            client._client.close()

    def test_410_raises_github_api_error(self):
        """A 410 (Gone) response must raise GitHubAPIError with status_code=410."""
        client = GitHubActionsClient(token="fake-token")
        client._client._transport = _error_transport(410)

        try:
            with pytest.raises(GitHubAPIError) as exc_info:
                client.get_job_log(job_id=999)
            assert exc_info.value.status_code == 410
        finally:
            client._client.close()

    def test_403_raises_rate_limit_error(self):
        """A 403 response with X-RateLimit-Reset must raise RateLimitError."""
        client = GitHubActionsClient(token="fake-token")
        client._client._transport = _error_transport(
            403, headers={"X-RateLimit-Reset": "1700000000"}
        )

        try:
            with pytest.raises(RateLimitError) as exc_info:
                client.get_workflow_runs(workflow_file="daily.yml")
            assert exc_info.value.status_code == 403
            assert exc_info.value.reset_at == "1700000000"
        finally:
            client._client.close()

    def test_429_raises_rate_limit_error(self):
        """A 429 response must raise RateLimitError."""
        client = GitHubActionsClient(token="fake-token")
        client._client._transport = _error_transport(429)

        try:
            with pytest.raises(RateLimitError) as exc_info:
                client.get_workflow_runs(workflow_file="daily.yml")
            assert exc_info.value.status_code == 429
        finally:
            client._client.close()


# ---------------------------------------------------------------------------
# Unit test – pagination (task 3.3)
# ---------------------------------------------------------------------------


class TestPaginationAssembly:
    """Test that get_workflow_runs correctly assembles multiple pages."""

    def test_pagination_assembles_multiple_pages(self):
        """Two pages of workflow runs should be combined into a single list."""
        page_calls: list[int] = []

        def handler(request: httpx.Request) -> httpx.Response:
            page = int(request.url.params.get("page", "1"))
            per_page = int(request.url.params.get("per_page", "100"))
            page_calls.append(page)

            if page == 1:
                # Return a full page → client should request another page
                runs = [{"id": i, "name": f"run-{i}"} for i in range(per_page)]
            else:
                # Return fewer than per_page → signals last page
                runs = [{"id": per_page + j, "name": f"run-{per_page + j}"} for j in range(3)]

            return httpx.Response(
                200,
                json={"workflow_runs": runs, "total_count": per_page + 3},
            )

        client = GitHubActionsClient(token="fake-token")
        client._client._transport = httpx.MockTransport(handler)

        try:
            runs = client.get_workflow_runs(workflow_file="daily.yml", per_page=5)
            # First page: 5 runs, second page: 3 runs → 8 total
            assert len(runs) == 8
            assert page_calls == [1, 2]
            # Verify IDs are correct
            assert [r["id"] for r in runs] == list(range(8))
        finally:
            client._client.close()


# ---------------------------------------------------------------------------
# Unit test – unauthenticated requests (task 3.3)
# ---------------------------------------------------------------------------


class TestUnauthenticatedRequests:
    """Test that unauthenticated clients send no Authorization header."""

    def test_unauthenticated_no_auth_header(self):
        """A client created with no token must not send an Authorization header."""
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(
                200,
                json={"workflow_runs": [], "total_count": 0},
            )

        client = GitHubActionsClient(token=None)
        client._client._transport = httpx.MockTransport(handler)

        try:
            client.get_workflow_runs(workflow_file="daily.yml")
            assert len(captured) == 1
            assert "authorization" not in captured[0].headers
        finally:
            client._client.close()
