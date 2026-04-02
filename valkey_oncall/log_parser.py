"""Log parser for Valkey CI job logs.

Extracts test failure information from Valkey's Tcl test framework output,
Google Test (gtest) unit test output, sentinel/cluster test output, and
Tcl exception stack traces.

GitHub Actions prepends ISO timestamps to every log line
(e.g. ``2026-04-01T01:05:06.5089664Z ``).  The parser strips these before
applying patterns.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List


@dataclass
class TestFailure:
    """A single test failure extracted from a CI job log."""

    test_name: str
    error_summary: str
    log_lines: str


# ---------------------------------------------------------------------------
# Timestamp stripping
# ---------------------------------------------------------------------------

# GitHub Actions timestamp prefix: "2026-04-01T01:05:06.5089664Z "
_GHA_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s?")


def _strip_timestamps(raw_log: str) -> str:
    """Remove GitHub Actions timestamp prefixes from every line."""
    return "\n".join(
        _GHA_TIMESTAMP_RE.sub("", line) for line in raw_log.splitlines()
    )


# ---------------------------------------------------------------------------
# Regex patterns for Valkey CI log formats
# ---------------------------------------------------------------------------

# Tcl test framework: "[err]: <test_name> in <file>"
# This appears inline during test execution.
_TCL_ERR_RE = re.compile(
    r"^\[err\]:\s*(.+?)\s+in\s+(tests/.+\.tcl)\s*$", re.MULTILINE
)

# Summary lines at the end: "*** [err]: <test_name> in <file>"
_SUMMARY_ERR_RE = re.compile(
    r"^\*\*\*\s+\[err\]:\s*(.+?)\s+in\s+(tests/.+\.tcl)\s*$", re.MULTILINE
)

# Timeout marker: "[TIMEOUT]: ..."
_TIMEOUT_RE = re.compile(
    r"^\[TIMEOUT\]:\s*(.+)", re.MULTILINE
)

# Google Test failure marker: "[  FAILED  ] TestSuite.TestName"
_GTEST_FAILED_RE = re.compile(
    r"^\[\s+FAILED\s+\]\s+(\S+)", re.MULTILINE
)

# Sentinel / cluster test "FAILED:" lines
_SENTINEL_FAILED_RE = re.compile(
    r"^(.+?):\s*FAILED:\s*(.+)", re.MULTILINE
)

# Tcl exception: "[exception]: Executing test client: <message>"
_TCL_EXCEPTION_RE = re.compile(
    r"^\[exception\]:\s*(.+)", re.MULTILINE
)

# Tcl file reference in stack trace: '(file "tests/unit/foo.tcl" line 123)'
_TCL_FILE_REF_RE = re.compile(
    r'\(file\s+"(tests/.+?\.tcl)"\s+line\s+\d+\)'
)

# Tcl test name in stack trace: 'test "test name here" {' or '"test "name" {'
_TCL_STACK_TEST_RE = re.compile(
    r'(?:^"|^)test\s+"([^"]+)"', re.MULTILINE
)

# Tcl procedure call with test name: 'test_slave_buffers {name here} args'
_TCL_PROC_CALL_RE = re.compile(
    r'^"?(\w+)\s+\{([^}]+)\}', re.MULTILINE
)

# GitHub Actions error annotation: "##[error]Process completed with exit code N"
_GHA_ERROR_RE = re.compile(
    r"^##\[error\](.+)", re.MULTILINE
)


def _context_window(lines: List[str], center: int, radius: int = 5) -> str:
    """Return a window of *radius* lines around *center*, joined."""
    start = max(0, center - radius)
    end = min(len(lines), center + radius + 1)
    return "\n".join(lines[start:end])


def _find_line_index(lines: List[str], substring: str) -> int:
    """Return the index of the first line containing *substring*, or -1."""
    for i, line in enumerate(lines):
        if substring in line:
            return i
    return -1


def _extract_error_block(lines: List[str], start: int) -> str:
    """Extract the error detail lines following a failure marker.

    Collects non-empty lines after *start* until we hit another status
    marker (``[ok]``, ``[err]``, ``[skip]``, ``Passed``, ``Testing``,
    or a blank line after content).
    """
    detail_lines: List[str] = []
    # Status markers that signal the start of a new test result
    stop_markers = ("[ok]", "[err]", "[skip]", "[ignore]", "[TIMEOUT]",
                    "Passed ", "Testing ", "*** [")
    for line in lines[start + 1:]:
        stripped = line.strip()
        if not stripped:
            # Allow one blank line, but stop on a second consecutive blank
            if detail_lines and not detail_lines[-1].strip():
                break
            detail_lines.append(line)
            continue
        if any(stripped.startswith(m) for m in stop_markers):
            break
        detail_lines.append(line)
        # Cap at a reasonable number of lines
        if len(detail_lines) > 30:
            break
    # Trim trailing blank lines
    while detail_lines and not detail_lines[-1].strip():
        detail_lines.pop()
    return "\n".join(detail_lines)


def parse_job_log(raw_log: str) -> List[TestFailure]:
    """Parse a raw CI job log and return extracted test failures.

    Handles:
    * Valkey Tcl test framework ``[err]`` markers
    * End-of-run ``*** [err]`` summary lines
    * ``[TIMEOUT]`` markers
    * Google Test ``[  FAILED  ]`` markers
    * Sentinel/cluster ``FAILED:`` lines
    * Tcl ``[exception]`` stack traces with file references
    * GitHub Actions ``##[error]`` annotations (fallback)

    Returns an empty list when no recognisable failure patterns are found.
    """
    if not raw_log or not raw_log.strip():
        return []

    # Strip GitHub Actions timestamp prefixes so ^-anchored regexes work
    cleaned = _strip_timestamps(raw_log)
    lines = cleaned.splitlines()
    seen: set[str] = set()  # deduplicate by (test_name)
    failures: List[TestFailure] = []

    def _add(test_name: str, error_summary: str, log_lines: str) -> None:
        key = test_name.strip()
        if key and key not in seen:
            seen.add(key)
            failures.append(TestFailure(
                test_name=key,
                error_summary=error_summary.strip(),
                log_lines=log_lines.strip(),
            ))

    # 1. Tcl [err] markers (inline during test run)
    for m in _TCL_ERR_RE.finditer(cleaned):
        test_name = m.group(1).strip()
        test_file = m.group(2).strip()
        full_name = f"{test_name} in {test_file}"
        idx = _find_line_index(lines, m.group(0).strip())
        if idx >= 0:
            error_detail = _extract_error_block(lines, idx)
            context = _context_window(lines, idx, radius=8)
        else:
            error_detail = ""
            context = m.group(0)
        _add(full_name, error_detail or test_name, context)

    # 2. Summary *** [err] lines (end-of-run summary)
    for m in _SUMMARY_ERR_RE.finditer(cleaned):
        test_name = m.group(1).strip()
        test_file = m.group(2).strip()
        full_name = f"{test_name} in {test_file}"
        idx = _find_line_index(lines, m.group(0).strip())
        context = _context_window(lines, idx, radius=3) if idx >= 0 else m.group(0)
        # Only add if not already captured by the inline [err]
        _add(full_name, test_name, context)

    # 3. TIMEOUT markers
    for m in _TIMEOUT_RE.finditer(cleaned):
        detail = m.group(1).strip()
        # Try to extract a test name from the detail
        # Format is often: "test_name in tests/file.tcl" or just a description
        test_name = detail
        idx = _find_line_index(lines, m.group(0).strip())
        context = _context_window(lines, idx, radius=5) if idx >= 0 else m.group(0)
        _add(test_name, f"TIMEOUT: {detail}", context)

    # 4. Google Test [  FAILED  ] markers
    #    GTest prints a summary block at the end listing all failed tests.
    #    We want the summary entries, not the inline ones (which duplicate).
    gtest_summary = False
    for i, line in enumerate(lines):
        stripped = line.strip()
        # Detect the GTest summary section
        if re.match(r"^\[\s+FAILED\s+\]\s+\d+\s+test", stripped):
            gtest_summary = True
            continue
        if gtest_summary and stripped.startswith("[  FAILED  ]"):
            gm = _GTEST_FAILED_RE.match(stripped)
            if gm:
                test_name = gm.group(1).strip()
                # Remove trailing timing info like " (123 ms)"
                test_name = re.sub(r"\s*\(\d+\s*ms\)\s*$", "", test_name)
                context = _context_window(lines, i, radius=3)
                _add(test_name, f"GTest FAILED: {test_name}", context)

    # If no summary section found, pick up inline [  FAILED  ] markers
    if not gtest_summary:
        for m in _GTEST_FAILED_RE.finditer(cleaned):
            test_name = m.group(1).strip()
            test_name = re.sub(r"\s*\(\d+\s*ms\)\s*$", "", test_name)
            if test_name and not re.match(r"^\d+\s+test", test_name):
                idx = _find_line_index(lines, m.group(0).strip())
                context = _context_window(lines, idx, radius=5) if idx >= 0 else m.group(0)
                _add(test_name, f"GTest FAILED: {test_name}", context)

    # 5. Sentinel/cluster "FAILED:" lines
    for m in _SENTINEL_FAILED_RE.finditer(cleaned):
        prefix = m.group(1).strip()
        message = m.group(2).strip()
        # The prefix often contains a timestamp and test name
        # e.g. "00:36:47>Master reboot in very short time"
        # Strip leading timestamps
        test_name = re.sub(r"^\d{2}:\d{2}:\d{2}\s*>\s*", "", prefix)
        if not test_name:
            test_name = message
        idx = _find_line_index(lines, m.group(0).strip())
        context = _context_window(lines, idx, radius=8) if idx >= 0 else m.group(0)
        _add(test_name, message, context)

    # 6. Tcl [exception] stack traces
    for m in _TCL_EXCEPTION_RE.finditer(cleaned):
        error_msg = m.group(1).strip()
        idx = _find_line_index(lines, m.group(0).strip())
        test_file = ""
        test_name_from_stack = ""
        if idx >= 0:
            stack_window = lines[idx:min(idx + 60, len(lines))]
            stack_text = "\n".join(stack_window)

            # Look for file reference
            file_match = _TCL_FILE_REF_RE.search(stack_text)
            if file_match:
                test_file = file_match.group(1)

            # Look for test name in the stack trace.
            # Strategy: find the procedure call line just before the
            # (file "...") reference — that has the resolved test name.
            # e.g. 'test_slave_buffers {slave buffer are counted correctly} 1000000 10 0 1'
            #       ^^^ procedure        ^^^ test name in braces
            # Skip Tcl control structures (foreach, if, while, etc.)
            _TCL_CONTROL = {"foreach", "if", "while", "for", "switch", "start_server", "cluster_setup"}
            if test_file:
                # Find the line index of the file reference within the window
                for wi, wline in enumerate(stack_window):
                    if test_file in wline and '(file' in wline:
                        # Look backwards from here for a quoted procedure call with {name}
                        for bi in range(wi - 1, -1, -1):
                            bline = stack_window[bi].strip().strip('"')
                            proc_m = re.match(r'(\w+)\s+\{([^}]+)\}', bline)
                            if proc_m:
                                proc_name = proc_m.group(1)
                                candidate = proc_m.group(2).strip()
                                if proc_name not in _TCL_CONTROL and "$" not in candidate and len(candidate) > 2:
                                    test_name_from_stack = candidate
                                    break
                        break

            # Fallback: try 'test "name" {' — even with $variables, it groups well
            if not test_name_from_stack:
                test_match = _TCL_STACK_TEST_RE.search(stack_text)
                if test_match:
                    test_name_from_stack = test_match.group(1).strip()

            context = _context_window(lines, idx, radius=10)
        else:
            context = m.group(0)

        # Normalize: strip dynamic values from error messages (ports, IPs)
        normalized_error = re.sub(r"\d+\.\d+\.\d+\.\d+:\d+", "<host:port>", error_msg)

        # Strip trailing Tcl $variable suffixes like "- $type"
        if test_name_from_stack:
            display_name = re.sub(r"\s*-\s*\$\w+\s*$", "", test_name_from_stack).strip()
        else:
            display_name = ""

        if display_name and test_file:
            test_name = f"{display_name} in {test_file}"
        elif display_name:
            test_name = f"{display_name} (exception)"
        elif test_file:
            test_name = f"Exception in {test_file}"
        else:
            test_name = f"Exception: {normalized_error[:80]}"
        _add(test_name, error_msg, context)

    # 7. GitHub Actions ##[error] — fallback when no other patterns matched
    if not failures:
        for m in _GHA_ERROR_RE.finditer(cleaned):
            error_msg = m.group(1).strip()
            # Skip generic "Process completed with exit code" if we already have failures
            idx = _find_line_index(lines, "##[error]")
            context = _context_window(lines, idx, radius=15) if idx >= 0 else m.group(0)
            _add(f"Process error: {error_msg}", error_msg, context)

    return failures
