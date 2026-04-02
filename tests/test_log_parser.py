"""Basic unit tests for the Valkey CI log parser.

Tests are based on real log patterns observed in valkey-io/valkey CI runs.
"""

from __future__ import annotations

from valkey_oncall.log_parser import TestFailure, parse_job_log


class TestParseJobLogEmpty:
    """Parser returns empty list for logs with no failures."""

    def test_empty_string(self) -> None:
        assert parse_job_log("") == []

    def test_whitespace_only(self) -> None:
        assert parse_job_log("   \n\n  ") == []

    def test_clean_log(self) -> None:
        log = (
            "[ok]: Some passing test in tests/unit/basic.tcl (42 ms)\n"
            "[ok]: Another passing test in tests/unit/basic.tcl (10 ms)\n"
            "Passed 2, Failed 0\n"
        )
        assert parse_job_log(log) == []


class TestTclErrPattern:
    """Parser extracts failures from Tcl [err] markers."""

    def test_single_err(self) -> None:
        log = (
            "[ok]: Some passing test in tests/unit/basic.tcl (10 ms)\n"
            "[err]: MULTI-BULK buffer overflow in tests/unit/protocol.tcl\n"
            "Expected 'OK' but got error: ERR Protocol error\n"
            "[ok]: Another test in tests/unit/basic.tcl (5 ms)\n"
        )
        failures = parse_job_log(log)
        assert len(failures) == 1
        assert failures[0].test_name == "MULTI-BULK buffer overflow in tests/unit/protocol.tcl"
        assert "Expected 'OK'" in failures[0].error_summary
        assert "[err]" in failures[0].log_lines

    def test_multiple_errs(self) -> None:
        log = (
            "[err]: Test A in tests/unit/a.tcl\n"
            "Error detail A\n"
            "[ok]: passing in tests/unit/b.tcl\n"
            "[err]: Test B in tests/unit/b.tcl\n"
            "Error detail B\n"
        )
        failures = parse_job_log(log)
        names = {f.test_name for f in failures}
        assert "Test A in tests/unit/a.tcl" in names
        assert "Test B in tests/unit/b.tcl" in names

    def test_real_cluster_failure(self) -> None:
        """Pattern from real issue: slot-migration.tcl failures."""
        log = (
            "[err]: Empty-shard migration target is auto-updated after failover in target shard in tests/unit/cluster/slot-migration.tcl\n"
            "incorrect slot state on R 0: expected [609->-65022b]; got [609->-1721381f]\n"
            "[err]: Empty-shard migration source is auto-updated after failover in source shard in tests/unit/cluster/slot-migration.tcl\n"
            "incorrect slot state on R 0: expected [609->-65022b]; got [609->-1721381f]\n"
        )
        failures = parse_job_log(log)
        assert len(failures) == 2
        assert "slot-migration.tcl" in failures[0].test_name
        assert "incorrect slot state" in failures[0].error_summary


class TestSummaryErrPattern:
    """Parser extracts failures from *** [err] summary lines."""

    def test_summary_line(self) -> None:
        log = (
            "\n                   The End\n\n"
            "!!! WARNING The following tests failed:\n\n"
            "*** [err]: Primaries will not time out in tests/unit/cluster/failover2.tcl\n"
            "expected message found in log file: *Failover attempt expired*\n"
        )
        failures = parse_job_log(log)
        assert len(failures) == 1
        assert "Primaries will not time out" in failures[0].test_name
        assert "failover2.tcl" in failures[0].test_name

    def test_summary_deduplicates_with_inline(self) -> None:
        """If both inline [err] and summary *** [err] exist, don't duplicate."""
        log = (
            "[err]: Test X in tests/unit/x.tcl\n"
            "some error\n"
            "\n"
            "*** [err]: Test X in tests/unit/x.tcl\n"
        )
        failures = parse_job_log(log)
        assert len(failures) == 1
        assert failures[0].test_name == "Test X in tests/unit/x.tcl"


class TestTimeoutPattern:
    """Parser extracts failures from [TIMEOUT] markers."""

    def test_timeout(self) -> None:
        log = (
            "[TIMEOUT]: clients state report follows.\n"
            "5 => (IN PROGRESS) Test slow thing in tests/unit/slow.tcl\n"
        )
        failures = parse_job_log(log)
        assert len(failures) >= 1
        assert any("TIMEOUT" in f.error_summary for f in failures)


class TestGtestPattern:
    """Parser extracts failures from Google Test output."""

    def test_gtest_summary(self) -> None:
        log = (
            "[==========] 150 tests from 20 test suites ran.\n"
            "[  PASSED  ] 148 tests.\n"
            "[  FAILED  ] 2 tests, listed below:\n"
            "[  FAILED  ] SdsTest.IncrLen\n"
            "[  FAILED  ] ZiplistTest.Stress\n"
            "\n"
            " 2 FAILED TESTS\n"
        )
        failures = parse_job_log(log)
        names = {f.test_name for f in failures}
        assert "SdsTest.IncrLen" in names
        assert "ZiplistTest.Stress" in names

    def test_gtest_inline_when_no_summary(self) -> None:
        log = (
            "[  RUN      ] SdsTest.IncrLen\n"
            "src/unit/test_sds.cpp:42: Failure\n"
            "Expected equality of these values:\n"
            "  sdslen(s)\n"
            "    Which is: 5\n"
            "  10\n"
            "[  FAILED  ] SdsTest.IncrLen (0 ms)\n"
        )
        failures = parse_job_log(log)
        assert len(failures) == 1
        assert failures[0].test_name == "SdsTest.IncrLen"


class TestSentinelFailedPattern:
    """Parser extracts failures from sentinel FAILED: lines."""

    def test_sentinel_failure(self) -> None:
        log = (
            "00:36:47>Master reboot in very short time: FAILED: At least one Sentinel did not receive failover info\n"
            "(Jumping to next unit after error)\n"
            "FAILED: caught an error in the test\n"
            "assertion:At least one Sentinel did not receive failover info\n"
        )
        failures = parse_job_log(log)
        assert len(failures) >= 1
        # Should capture the sentinel test name
        found_sentinel = any("Master reboot" in f.test_name for f in failures)
        assert found_sentinel


class TestMixedLog:
    """Parser handles logs with multiple failure types."""

    def test_mixed_tcl_and_gtest(self) -> None:
        log = (
            "[err]: ACL setuser test in tests/unit/auth.tcl\n"
            "Expected 'OK' but got ERR\n"
            "\n"
            "[==========] 50 tests from 5 test suites ran.\n"
            "[  PASSED  ] 49 tests.\n"
            "[  FAILED  ] 1 test, listed below:\n"
            "[  FAILED  ] SdsTest.Basic\n"
        )
        failures = parse_job_log(log)
        names = {f.test_name for f in failures}
        assert "ACL setuser test in tests/unit/auth.tcl" in names
        assert "SdsTest.Basic" in names


# ---------------------------------------------------------------------------
# Property-based tests (hypothesis)
# ---------------------------------------------------------------------------

from hypothesis import given, settings, assume
import hypothesis.strategies as st


# -- Strategies for generating valid failure patterns --

# Alphabet for test names: printable ASCII without newlines, and avoiding
# the substring " in tests/" to prevent regex ambiguity in the Tcl pattern.
_SAFE_ALPHA = st.sampled_from(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789 _-.()"
)

_test_name_st = st.text(_SAFE_ALPHA, min_size=1, max_size=60).map(
    lambda s: s.strip()
).filter(
    lambda s: len(s) >= 1 and " in tests/" not in s and "\n" not in s
)

_tcl_path_segment = st.text(
    st.sampled_from("abcdefghijklmnopqrstuvwxyz_-"),
    min_size=1,
    max_size=20,
)

_tcl_path_st = _tcl_path_segment.map(lambda seg: f"tests/unit/{seg}.tcl")

# GTest names: SuiteName.TestName (alphanumeric + underscore, no spaces)
_gtest_ident = st.text(
    st.sampled_from("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"),
    min_size=1,
    max_size=30,
).filter(lambda s: s[0].isalpha())

_gtest_name_st = st.tuples(_gtest_ident, _gtest_ident).map(
    lambda t: f"{t[0]}.{t[1]}"
)

# Sentinel prefix (no colons or "FAILED" to avoid nested matches)
_sentinel_prefix_st = st.text(
    st.sampled_from("abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"),
    min_size=1,
    max_size=40,
).filter(lambda s: s.strip() and "FAILED" not in s and ":" not in s and "\n" not in s)

_sentinel_msg_st = st.text(
    st.sampled_from("abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-."),
    min_size=1,
    max_size=40,
).filter(lambda s: s.strip() and "\n" not in s)


# -- Log builders --

def _build_tcl_log(test_name: str, tcl_path: str) -> str:
    return (
        "[ok]: Some passing test in tests/unit/basic.tcl (10 ms)\n"
        f"[err]: {test_name} in {tcl_path}\n"
        "Expected 'OK' but got error\n"
    )


def _build_gtest_log(gtest_name: str) -> str:
    return (
        "[==========] 10 tests from 2 test suites ran.\n"
        "[  PASSED  ] 9 tests.\n"
        "[  FAILED  ] 1 test, listed below:\n"
        f"[  FAILED  ] {gtest_name}\n"
        "\n"
        " 1 FAILED TEST\n"
    )


def _build_sentinel_log(prefix: str, message: str) -> str:
    return (
        f"{prefix}: FAILED: {message}\n"
        "(Jumping to next unit after error)\n"
    )


class TestPropertyParserExtractsTestNames:
    """Property 7: Parser extracts test names from logs containing failure patterns.

    **Validates: Requirements 4.2**
    """

    @settings(max_examples=100)
    @given(test_name=_test_name_st, tcl_path=_tcl_path_st)
    def test_tcl_err_pattern_extracts_name(self, test_name: str, tcl_path: str) -> None:
        """Tcl [err] pattern: parser returns at least one failure whose test_name
        is a substring of the original log."""
        log = _build_tcl_log(test_name, tcl_path)
        failures = parse_job_log(log)

        assert len(failures) >= 1, "Parser should find at least one failure"
        for f in failures:
            assert f.test_name in log, (
                f"test_name {f.test_name!r} not found in log"
            )

    @settings(max_examples=100)
    @given(gtest_name=_gtest_name_st)
    def test_gtest_pattern_extracts_name(self, gtest_name: str) -> None:
        """GTest pattern: parser returns at least one failure whose test_name
        is a substring of the original log."""
        log = _build_gtest_log(gtest_name)
        failures = parse_job_log(log)

        assert len(failures) >= 1, "Parser should find at least one failure"
        for f in failures:
            assert f.test_name in log, (
                f"test_name {f.test_name!r} not found in log"
            )

    @settings(max_examples=100)
    @given(prefix=_sentinel_prefix_st, message=_sentinel_msg_st)
    def test_sentinel_pattern_extracts_name(self, prefix: str, message: str) -> None:
        """Sentinel FAILED pattern: parser returns at least one failure whose
        test_name is a substring of the original log."""
        log = _build_sentinel_log(prefix, message)
        failures = parse_job_log(log)

        assert len(failures) >= 1, "Parser should find at least one failure"
        for f in failures:
            assert f.test_name in log, (
                f"test_name {f.test_name!r} not found in log"
            )
