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
        return STATUS_FIXED if self.fix_sha else STATUS_TRACKED

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
