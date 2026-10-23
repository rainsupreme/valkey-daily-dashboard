"""Flake registry: known flaky failures tied to the issue or PR tracking them.

The registry is a git-tracked JSON file, not a cache table. The SQLite cache
is disposable (evicted after idle periods, ``test_failures`` wiped on every
parser bump), so curated knowledge cannot live there. The cache is only used
to derive sightings for registry entries.

An entry is a *signature*: a ``test_name`` plus an optional ``summary`` (the
parser's normalized ``error_summary``). An entry with no summary claims every
failure mode of that test; several entries may share one issue, one per mode.
Every entry must carry an ``issue`` or a ``fix_pr``: a failure nobody owns is
never a tracked flake.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

from valkey_oncall.log_parser import _scrub_volatile

REGISTRY_VERSION = 1
DEFAULT_REGISTRY_PATH = "flakes.json"

STATUS_TRACKED = "tracked"
STATUS_FIXED = "fixed"
STATUS_CLOSED = "closed"  # issue closed, no fix recorded: needs a human look

TIER_EXACT = "exact"
TIER_TEST_ONLY = "test_only"


class RegistryError(ValueError):
    """Raised for malformed registry files or invalid entries."""


_QUOTED_NUMBER_RE = re.compile(r"'-?\d+(?:\.\d+)?'")
_LINE_NUMBER_RE = re.compile(r"\bline \d+\b")
_RUNNER_WORKDIR_RE = re.compile(r"/(?:home|Users)/runner/work/[^/\s]+/[^/\s]+/")


def normalize_summary(summary: Optional[str]) -> Optional[str]:
    """Reduce an error summary to a signature that is stable across runs.

    The parser's ``error_summary`` keeps context after the assertion line
    (server banner, pids, ports, timestamps), which differs on every run.
    The signature is the first non-empty line with the parser's volatile
    scrub applied, then run-specific values the parser keeps: quoted
    numbers in assertion messages, Tcl ``line N`` positions, and the
    runner's checkout path. Both sides of a match go through this, so the
    stored form and the incoming failure are compared like for like.
    """
    if summary is None:
        return None
    first = next((ln for ln in summary.splitlines() if ln.strip()), "")
    s = _scrub_volatile(first).strip()
    s = _QUOTED_NUMBER_RE.sub("'<n>'", s)
    s = _LINE_NUMBER_RE.sub("line <n>", s)
    s = _RUNNER_WORKDIR_RE.sub("", s)
    return s


def signature_id(test_name: str, summary: Optional[str]) -> str:
    """Deterministic short id for a (test_name, summary) signature."""
    h = hashlib.sha1()
    h.update(test_name.encode("utf-8"))
    h.update(b"\0")
    h.update((summary or "").encode("utf-8"))
    return "sig:" + h.hexdigest()[:10]


@dataclass
class FlakeEntry:
    """One registered failure signature and the artifact that owns it."""

    test_name: str
    summary: Optional[str] = None
    issue: Optional[int] = None
    fix_pr: Optional[int] = None
    fix_sha: Optional[str] = None
    fix_merged_at: Optional[str] = None  # ISO date; a sighting after it is a recurrence
    added_at: str = ""
    note: str = ""
    # Cached from GitHub by ``flakes refresh``; never authoritative, always
    # overwritten by the next refresh. Lets the report render issue state
    # without a network call.
    issue_state: Optional[str] = None  # "open" | "closed"
    issue_title: Optional[str] = None
    refreshed_at: Optional[str] = None
    id: str = field(default="")

    def __post_init__(self) -> None:
        self.summary = normalize_summary(self.summary)
        if not self.test_name.strip():
            raise RegistryError("entry needs a test_name")
        if self.issue is None and self.fix_pr is None:
            raise RegistryError(
                f"entry for {self.test_name!r} needs an issue or a fix_pr; "
                "a failure nobody owns is not a tracked flake"
            )
        if self.fix_sha and not self.fix_merged_at:
            raise RegistryError(
                f"entry {self.test_name!r}: fix_sha needs fix_merged_at"
            )
        if not self.id:
            self.id = signature_id(self.test_name, self.summary)
        if not self.added_at:
            self.added_at = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    @property
    def status(self) -> str:
        if self.fix_sha:
            return STATUS_FIXED
        if self.issue_state == "closed":
            return STATUS_CLOSED
        return STATUS_TRACKED

    def to_dict(self) -> Dict:
        d = asdict(self)
        return {k: v for k, v in d.items() if v not in (None, "")}


@dataclass
class Match:
    """A registry entry that matched a failure, with how closely it matched."""

    entry: FlakeEntry
    tier: str  # TIER_EXACT or TIER_TEST_ONLY


@dataclass
class Sighting:
    """One cached failure row that matches an entry's signature."""

    job_id: int
    run_id: int
    run_date: str
    branch: str
    commit_sha: str
    job_name: str
    error_summary: str

    @property
    def day(self) -> str:
        return self.run_date[:10]


class FlakeRegistry:
    """In-memory registry with load/save to a JSON file."""

    def __init__(self, entries: Optional[List[FlakeEntry]] = None) -> None:
        self._entries: Dict[str, FlakeEntry] = {}
        for e in entries or []:
            self.add(e)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    @classmethod
    def load(cls, path: str = DEFAULT_REGISTRY_PATH) -> "FlakeRegistry":
        p = Path(path).expanduser()
        if not p.exists():
            return cls()
        try:
            data = json.loads(p.read_text())
        except json.JSONDecodeError as exc:
            raise RegistryError(f"{path}: not valid JSON: {exc}") from exc
        version = data.get("version")
        if version != REGISTRY_VERSION:
            raise RegistryError(
                f"{path}: registry version {version!r}, expected {REGISTRY_VERSION}"
            )
        entries = [FlakeEntry(**raw) for raw in data.get("entries", [])]
        return cls(entries)

    def save(self, path: str = DEFAULT_REGISTRY_PATH) -> None:
        p = Path(path).expanduser()
        payload = {
            "version": REGISTRY_VERSION,
            "entries": [e.to_dict() for e in self.entries()],
        }
        p.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    # ------------------------------------------------------------------
    # Entries
    # ------------------------------------------------------------------

    def entries(self) -> List[FlakeEntry]:
        return sorted(self._entries.values(), key=lambda e: (e.test_name, e.id))

    def get(self, entry_id: str) -> Optional[FlakeEntry]:
        return self._entries.get(entry_id)

    def add(self, entry: FlakeEntry) -> FlakeEntry:
        if entry.id in self._entries:
            raise RegistryError(
                f"signature {entry.id} already registered for {entry.test_name!r}"
            )
        self._entries[entry.id] = entry
        return entry

    def remove(self, entry_id: str) -> FlakeEntry:
        try:
            return self._entries.pop(entry_id)
        except KeyError:
            raise RegistryError(f"no entry {entry_id}") from None

    def mark_fixed(
        self,
        entry_id: str,
        fix_sha: str,
        fix_merged_at: str,
        fix_pr: Optional[int] = None,
    ) -> FlakeEntry:
        entry = self.get(entry_id)
        if entry is None:
            raise RegistryError(f"no entry {entry_id}")
        entry.fix_sha = fix_sha
        entry.fix_merged_at = fix_merged_at
        if fix_pr is not None:
            entry.fix_pr = fix_pr
        return entry

    # ------------------------------------------------------------------
    # Matching
    # ------------------------------------------------------------------

    def match(self, test_name: str, error_summary: Optional[str] = None) -> List[Match]:
        """Return entries matching a failure, exact matches first.

        An entry with no summary matches every failure mode of its test
        (``exact``). An entry with a summary is ``exact`` only when the
        normalized summaries are equal; otherwise it is ``test_only``, a
        candidate the caller must confirm rather than an identification.
        """
        summary = normalize_summary(error_summary)
        out: List[Match] = []
        for e in self._entries.values():
            if e.test_name != test_name:
                continue
            if e.summary is None or (summary is not None and e.summary == summary):
                out.append(Match(e, TIER_EXACT))
            else:
                out.append(Match(e, TIER_TEST_ONLY))
        out.sort(key=lambda m: (m.tier != TIER_EXACT, m.entry.id))
        return out


def tracking_for_test(
    registry: FlakeRegistry,
    test_name: str,
    error_summaries,
    failure_days,
) -> List[Dict]:
    """Registry entries for one report row, with how they relate to this window.

    *error_summaries* are the full summaries seen for the test in the window;
    an entry with a summary is ``confirmed`` only if one of them normalizes
    to it. *failure_days* (``YYYY-MM-DD``) decide ``recurred``: a fixed entry
    whose test failed after the fix merged. Entries are listed exact-first so
    a renderer can show the first as the primary owner.
    """
    normalized = {normalize_summary(s) for s in error_summaries}
    last_day = max(failure_days) if failure_days else None
    out: List[Dict] = []
    for e in registry.entries():
        if e.test_name != test_name:
            continue
        confirmed = e.summary is None or e.summary in normalized
        recurred = bool(
            e.fix_merged_at and last_day and last_day > e.fix_merged_at[:10]
        )
        out.append(
            {
                "id": e.id,
                "issue": e.issue,
                "issue_state": e.issue_state,
                "issue_title": e.issue_title,
                "fix_pr": e.fix_pr,
                "fix_sha": e.fix_sha,
                "fix_merged_at": e.fix_merged_at,
                "status": e.status,
                "confirmed": confirmed,
                "recurred": recurred,
            }
        )
    out.sort(key=lambda t: (not t["confirmed"], t["id"]))
    return out


# ----------------------------------------------------------------------
# Refresh from GitHub (issue state, fix PR merge state)
# ----------------------------------------------------------------------


def _fix_from_timeline(events: List[Dict]) -> Dict:
    """Pull the fixing PR and merge commit out of an issue's timeline.

    Two shapes occur. A ``Fixes #N`` merge sometimes leaves a ``closed``
    event carrying the merge ``commit_id``. More often the ``closed`` event
    has no commit and the fix is the ``cross-referenced`` merged pull request
    whose merge is the latest one at or before the close. An issue closed by
    hand with no merged PR referencing it yields neither.
    """
    out: Dict = {}
    merged: List[tuple] = []  # (merged_at, pr_number)
    for ev in events:
        kind = ev.get("event")
        if kind == "closed":
            out["closed_at"] = (ev.get("created_at") or "")[:10]
            if ev.get("commit_id"):
                out["fix_sha"] = ev["commit_id"]
        elif kind == "cross-referenced":
            src = (ev.get("source") or {}).get("issue") or {}
            pr = src.get("pull_request")
            if pr and pr.get("merged_at") and src.get("number"):
                merged.append((pr["merged_at"], src["number"]))
    if merged:
        closed_at = out.get("closed_at") or "9999"
        # Latest merge at or before the close (merge and auto-close land
        # seconds apart, so compare on the date).
        before = [m for m in merged if m[0][:10] <= closed_at] or merged
        merged_at, number = max(before)
        out["fix_pr"] = number
        out["fix_merged_at"] = merged_at[:10]
    return out


def refresh_entry(entry: FlakeEntry, client) -> Dict:
    """Update one entry's cached GitHub state and record a merged fix.

    *client* needs ``get_issue``, ``get_pull`` and ``get_issue_timeline``.
    Returns a report dict describing what was read and what changed. The
    entry is mutated; the caller saves the registry.
    """
    report: Dict = {"id": entry.id, "test_name": entry.test_name, "changes": []}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if entry.issue is not None:
        issue = client.get_issue(entry.issue)
        state = issue.get("state")
        if state != entry.issue_state:
            report["changes"].append(f"issue_state {entry.issue_state} -> {state}")
        entry.issue_state = state
        entry.issue_title = issue.get("title")
        report["issue_state"] = state
        if state == "closed" and not entry.fix_sha:
            found = _fix_from_timeline(client.get_issue_timeline(entry.issue))
            inferred = bool(found.get("fix_pr")) and not found.get("fix_sha")
            if inferred:
                pr = client.get_pull(found["fix_pr"])
                if pr.get("merged_at"):
                    found["fix_sha"] = pr.get("merge_commit_sha")
                    found["fix_merged_at"] = pr["merged_at"][:10]
            if found.get("fix_sha"):
                entry.fix_sha = found["fix_sha"]
                entry.fix_merged_at = found.get("fix_merged_at") or found["closed_at"]
                if entry.fix_pr is None and found.get("fix_pr"):
                    entry.fix_pr = found["fix_pr"]
                how = "closed by commit"
                if inferred:
                    # The close event carried no commit; the PR is the merged
                    # cross-reference nearest the close. Say so where a human
                    # reading flakes.json will see it.
                    how = "inferred from cross-reference"
                    report["fix_source"] = "cross_reference"
                    if not entry.note:
                        entry.note = (
                            f"fix PR {entry.fix_pr} inferred by refresh: merged "
                            f"cross-reference nearest the issue close on "
                            f"{found.get('closed_at') or '?'}"
                        )
                else:
                    report["fix_source"] = "closed_commit"
                report["changes"].append(
                    f"fixed by {entry.fix_sha[:10]} on {entry.fix_merged_at}"
                    + (f" (PR {entry.fix_pr})" if entry.fix_pr else "")
                    + f" [{how}]"
                )
            else:
                report["flag"] = "closed_without_fix"

    if entry.fix_pr is not None and not entry.fix_sha:
        pr = client.get_pull(entry.fix_pr)
        report["fix_pr_state"] = (
            "merged" if pr.get("merged_at") else pr.get("state", "unknown")
        )
        if pr.get("merged_at"):
            entry.fix_sha = pr.get("merge_commit_sha")
            entry.fix_merged_at = pr["merged_at"][:10]
            report["changes"].append(
                f"fix PR {entry.fix_pr} merged {entry.fix_merged_at} "
                f"as {entry.fix_sha[:10]}"
            )
        elif pr.get("state") == "closed":
            report["flag"] = "fix_pr_closed_unmerged"

    entry.refreshed_at = today
    report["status"] = entry.status
    return report


def refresh_registry(registry: FlakeRegistry, client) -> List[Dict]:
    """Refresh every entry; see :func:`refresh_entry`."""
    return [refresh_entry(e, client) for e in registry.entries()]


# ----------------------------------------------------------------------
# Sightings and maintenance (cache-derived, never stored in the registry)
# ----------------------------------------------------------------------


def _since_iso(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT00:00:00Z"
    )


def sightings(cache, entry: FlakeEntry, days: int = 30) -> List[Sighting]:
    """Cached failures matching *entry* within the last *days*, newest first."""
    rows = cache.query_failure_sightings(entry.test_name, since=_since_iso(days))
    out: List[Sighting] = []
    for r in rows:
        if entry.summary is not None and normalize_summary(r["error_summary"]) != (
            entry.summary
        ):
            continue
        out.append(
            Sighting(
                job_id=r["job_id"],
                run_id=r["run_id"],
                run_date=r["run_date"],
                branch=r["branch"],
                commit_sha=r["commit_sha"],
                job_name=r["job_name"],
                error_summary=r["error_summary"],
            )
        )
    out.sort(key=lambda s: s.run_date, reverse=True)
    return out


def post_fix_sightings(entry: FlakeEntry, seen: List[Sighting]) -> List[Sighting]:
    """Sightings dated after the fix merged: the fix did not take, or a new bug."""
    if not entry.fix_merged_at:
        return []
    cutoff = entry.fix_merged_at[:10]
    return [s for s in seen if s.day > cutoff]


def entry_report(cache, entry: FlakeEntry, days: int = 30) -> Dict:
    """Entry plus its derived sightings summary, for list output."""
    seen = sightings(cache, entry, days)
    recur = post_fix_sightings(entry, seen)
    return {
        **entry.to_dict(),
        "status": entry.status,
        "sightings": len(seen),
        "last_sighting": seen[0].day if seen else None,
        "post_fix_sightings": len(recur),
        "window_days": days,
    }


def maintenance(cache, registry: FlakeRegistry, days: int = 30) -> List[Dict]:
    """Entries that need a human look, derived from the data rather than a clock.

    - ``recurrence``: fixed, but sighted after ``fix_merged_at``.
    - ``closed_without_fix``: the issue is closed (per the last refresh) but
      no fix commit is recorded; either record the fix or drop the entry.
    - ``drift``: tracked with a summary, zero sightings in the window, while
      the same test fails under a different summary (pattern no longer fits).
    - ``quiet``: tracked, zero sightings in the window (issue may be closable).
    """
    findings: List[Dict] = []
    for e in registry.entries():
        seen = sightings(cache, e, days)
        if e.status == STATUS_FIXED:
            recur = post_fix_sightings(e, seen)
            if recur:
                findings.append(
                    {
                        "id": e.id,
                        "test_name": e.test_name,
                        "kind": "recurrence",
                        "detail": f"{len(recur)} sighting(s) after fix {e.fix_sha}",
                        "last_sighting": recur[0].day,
                    }
                )
            continue
        if e.status == STATUS_CLOSED:
            findings.append(
                {
                    "id": e.id,
                    "test_name": e.test_name,
                    "kind": "closed_without_fix",
                    "detail": (
                        f"issue {e.issue} closed, no fix recorded; "
                        f"{len(seen)} sighting(s) in {days} days"
                    ),
                    "last_sighting": seen[0].day if seen else None,
                }
            )
            continue
        if seen:
            continue
        others = cache.query_failure_sightings(e.test_name, since=_since_iso(days))
        if e.summary is not None and others:
            summaries = sorted({r["error_summary"] for r in others})
            findings.append(
                {
                    "id": e.id,
                    "test_name": e.test_name,
                    "kind": "drift",
                    "detail": (
                        f"test failed {len(others)} time(s) under "
                        f"{len(summaries)} other summary(ies)"
                    ),
                    "other_summaries": summaries[:5],
                }
            )
        else:
            findings.append(
                {
                    "id": e.id,
                    "test_name": e.test_name,
                    "kind": "quiet",
                    "detail": f"no sightings in {days} days",
                }
            )
    return findings
