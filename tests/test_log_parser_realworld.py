"""Real-world regression tests for the Valkey CI log parser.

Each test uses a log snippet extracted from actual valkey-io/valkey CI job
logs.  As new failure patterns are discovered, add a new test here with the
raw snippet so the parser never regresses.

Naming convention: test_<pattern>_<source_description>
"""

from __future__ import annotations

from valkey_oncall.log_parser import TestFailure, parse_job_log


# ---------------------------------------------------------------------------
# 1. Tcl [err] with GitHub Actions timestamps
# Source: job 69272507658, run 23722657826 (unstable daily, 2026-03-30)
# ---------------------------------------------------------------------------

_TCL_ERR_GHA_TIMESTAMP = """\
2026-03-31T00:28:17.2110000Z 38348:M 31 Mar 2026 00:28:17.211 * Discarding previously cached primary state.
2026-03-31T00:28:17.2110001Z 38348:M 31 Mar 2026 00:28:17.211 * Setting secondary replication ID to 6ac59a34.
2026-03-31T00:28:17.2110002Z [err]: The best replica can initiate an election immediately in an automatic failover in tests/unit/cluster/faster-failover.tcl
2026-03-31T00:28:17.2110003Z log message of '"*Successful partial resynchronization with primary*"' not found in ./tests/tmp/server.10822.103/stdout after line: 0 till line: 155
2026-03-31T00:28:17.2110004Z 
2026-03-31T00:28:17.2110005Z ===== Start of server log (pid 38545) =====
"""


class TestTclErrWithGHATimestamp:
    """[err] lines prefixed with GitHub Actions timestamps."""

    def test_faster_failover(self) -> None:
        failures = parse_job_log(_TCL_ERR_GHA_TIMESTAMP)
        assert len(failures) >= 1
        f = failures[0]
        assert "The best replica can initiate an election immediately" in f.test_name
        assert "faster-failover.tcl" in f.test_name
        assert "log message" in f.error_summary

    def test_timestamp_stripping_does_not_lose_content(self) -> None:
        """The error detail after [err] should still be captured."""
        failures = parse_job_log(_TCL_ERR_GHA_TIMESTAMP)
        assert any("Successful partial resynchronization" in f.error_summary
                    or "log message" in f.error_summary for f in failures)


# ---------------------------------------------------------------------------
# 2. Tcl [err] with GHA timestamps — replication test
# Source: job from run 23803833154 (fix-rdma-io-threads, 2026-03-31)
# ---------------------------------------------------------------------------

_TCL_ERR_REPLICATION = """\
2026-03-31T15:02:19.5643261Z ===== End of server stderr log (pid 54604) =====
2026-03-31T15:02:19.5643608Z 
2026-03-31T15:02:19.5644045Z [err]: diskless no replicas drop during rdb pipe in tests/integration/replication.tcl
2026-03-31T15:02:19.5644773Z rdb child didn't terminate
2026-03-31T15:02:19.7217411Z === () Starting server on 127.0.0.1:21815 ok
"""


class TestTclErrReplication:
    """[err] from a replication test with GHA timestamps."""

    def test_diskless_replication(self) -> None:
        failures = parse_job_log(_TCL_ERR_REPLICATION)
        assert len(failures) == 1
        assert "diskless no replicas drop during rdb pipe" in failures[0].test_name
        assert "replication.tcl" in failures[0].test_name
        assert "rdb child" in failures[0].error_summary


# ---------------------------------------------------------------------------
# 3. Tcl [exception] — maxmemory slave buffer crash
# Source: job 69432215377, run 23820775003 (flaky-failover, 2026-03-31)
# The test name is in a procedure call: test_slave_buffers {name} args
# ---------------------------------------------------------------------------

_TCL_EXCEPTION_MAXMEMORY = """\
===== End of server stderr log (pid 14153) =====

[exception]: Executing test client: I/O error reading reply.
I/O error reading reply
    while executing
"$rd_master setrange key:0 0 [string repeat A $payload_len]"
    ("uplevel" body line 53)
    invoked from within
"uplevel 1 $code"
    (procedure "test" line 63)
    invoked from within
"test "$test_name" {
            set slave [srv 0 client]
            set slave_host [srv 0 host]
            set slave_port [srv 0 port]
            s..."
    ("uplevel" body line 3)
    invoked from within
"uplevel 1 $code "
    (procedure "start_server" line 2)
    invoked from within
"start_server {} {
        set slave_pid [s process_id]
        test "$test_name" {
            set slave [srv 0 client]
            set slave_host [sr..."
    ("uplevel" body line 2)
    invoked from within
"uplevel 1 $code "
    (procedure "start_server" line 2)
    invoked from within
"start_server {tags {"maxmemory external:skip"}} {
        start_server {} {
        set slave_pid [s process_id]
        test "$test_name" {
         ..."
    (procedure "test_slave_buffers" line 2)
    invoked from within
"test_slave_buffers {slave buffer are counted correctly} 1000000 10 0 1"
    (file "tests/unit/maxmemory.tcl" line 392)
    invoked from within
"source $path"
    (procedure "execute_test_file" line 6)
"""


class TestTclExceptionMaxmemory:
    """[exception] with stack trace containing test name in procedure call."""

    def test_extracts_test_name_from_proc_call(self) -> None:
        failures = parse_job_log(_TCL_EXCEPTION_MAXMEMORY)
        assert len(failures) == 1
        f = failures[0]
        assert "slave buffer are counted correctly" in f.test_name
        assert "maxmemory.tcl" in f.test_name

    def test_error_summary_has_io_error(self) -> None:
        failures = parse_job_log(_TCL_EXCEPTION_MAXMEMORY)
        assert "I/O error reading reply" in failures[0].error_summary


# ---------------------------------------------------------------------------
# 4. Tcl [exception] — MOVED error in cluster test
# Source: job from run 23820775003 (cluster failover test)
# The test name has an unresolved $type variable.
# ---------------------------------------------------------------------------

_TCL_EXCEPTION_MOVED = """\
===== End of server stderr log (pid 31489) =====

[exception]: Executing test client: MOVED 1 127.0.0.1:24196.
MOVED 1 127.0.0.1:24196
    while executing
"wait_for_condition 1000 50 {
            [R 4 get key_991803] == 1024 &&
            [R 7 get key_991803] == 1024
        } else {
            puts "R..."
    ("uplevel" body line 63)
    invoked from within
"uplevel 1 $code"
    (procedure "test" line 63)
    invoked from within
"test "New non-empty replica reports zero repl offset and rank, and fails to win election - $type" {
        # Write some data to primary 0, slot 1, ma..."
    (procedure "test_nonempty_replica" line 2)
    invoked from within
"test_nonempty_replica "sigstop""
    ("uplevel" body line 2)
    invoked from within
"uplevel 1 $code"
    (procedure "cluster_setup" line 53)
"""


class TestTclExceptionMoved:
    """[exception] with MOVED error — test name has $type variable."""

    def test_extracts_test_name_stripping_type_variable(self) -> None:
        failures = parse_job_log(_TCL_EXCEPTION_MOVED)
        assert len(failures) == 1
        f = failures[0]
        # Should have the test name with $type stripped
        assert "New non-empty replica reports zero repl offset" in f.test_name
        # $type should be stripped from the display name
        assert "$type" not in f.test_name

    def test_error_summary_has_moved(self) -> None:
        failures = parse_job_log(_TCL_EXCEPTION_MOVED)
        assert "MOVED" in failures[0].error_summary


# ---------------------------------------------------------------------------
# 5. Tcl [exception] — memefficiency with foreach parameterization
# Source: job from run 23825749975 (unstable daily, 2026-04-01)
# The test name has $size_range — a foreach loop variable.
# ---------------------------------------------------------------------------

_TCL_EXCEPTION_MEMEFFICIENCY = """\
===== End of server stderr log (pid 13697) =====

[exception]: Executing test client: error writing "sock556845bd7480": connection timed out.
error writing "sock556845bd7480": connection timed out
    while executing
"$rd set $key $val"
    (procedure "test_memory_efficiency" line 9)
    invoked from within
"test_memory_efficiency $size_range"
    ("uplevel" body line 2)
    invoked from within
"uplevel 1 $code"
    (procedure "test" line 63)
    invoked from within
"test "Memory efficiency with values in range $size_range" {
            set efficiency [test_memory_efficiency $size_range]
            assert {$effic..."
    ("foreach" body line 2)
    invoked from within
"foreach {size_range expected_min_efficiency} {
        32    0.15
        64    0.25
        128   0.35
        1024  0.75
        16384 0.82
    } {
..."
    ("uplevel" body line 2)
    invoked from within
"uplevel 1 $code "
    (procedure "start_server" line 2)
    invoked from within
"start_server {tags {"memefficiency external:skip"}} {
    foreach {size_range expected_min_efficiency} {
        32    0.15
        64    0.25
       ..."
    (file "tests/unit/memefficiency.tcl" line 24)
    invoked from within
"source $path"
    (procedure "execute_test_file" line 6)
"""


class TestTclExceptionMemefficiency:
    """[exception] with foreach-parameterized test name."""

    def test_extracts_parameterized_test_name(self) -> None:
        failures = parse_job_log(_TCL_EXCEPTION_MEMEFFICIENCY)
        assert len(failures) == 1
        f = failures[0]
        # Should use the test "..." name since proc call is foreach
        assert "Memory efficiency" in f.test_name
        assert "memefficiency.tcl" in f.test_name

    def test_error_summary_has_connection_timeout(self) -> None:
        failures = parse_job_log(_TCL_EXCEPTION_MEMEFFICIENCY)
        assert "connection timed out" in failures[0].error_summary


# ---------------------------------------------------------------------------
# 6. GitHub Actions ##[error] fallback — process exit with no other pattern
# Source: synthetic based on real pattern (job exits with code 1, no Tcl markers)
# ---------------------------------------------------------------------------

_GHA_ERROR_ONLY = """\
2026-04-01T01:05:06.5070748Z HGETALL _hash
2026-04-01T01:05:06.5070912Z HEXISTS _hash -519618290
2026-04-01T01:05:06.5071107Z HDEL _hash -301167168329
2026-04-01T01:05:06.5089664Z ##[error]Process completed with exit code 1.
2026-04-01T01:05:06.5223518Z Post job cleanup.
"""


class TestGHAErrorFallback:
    """##[error] fallback when no Tcl/GTest/sentinel patterns match."""

    def test_captures_process_error(self) -> None:
        failures = parse_job_log(_GHA_ERROR_ONLY)
        assert len(failures) == 1
        assert "Process completed with exit code 1" in failures[0].test_name

    def test_not_used_when_other_patterns_match(self) -> None:
        """##[error] should NOT produce a failure when [err] already matched."""
        log = (
            "2026-04-01T00:00:00.0000000Z [err]: Some test in tests/unit/foo.tcl\n"
            "2026-04-01T00:00:00.0000001Z some error detail\n"
            "2026-04-01T00:00:01.0000000Z ##[error]Process completed with exit code 1.\n"
        )
        failures = parse_job_log(log)
        # Should only have the [err] failure, not the ##[error]
        assert len(failures) == 1
        assert "Some test" in failures[0].test_name


# ---------------------------------------------------------------------------
# 7. Mixed: [err] + [exception] in same log should not duplicate
# ---------------------------------------------------------------------------

_MIXED_ERR_AND_EXCEPTION = """\
2026-04-01T00:00:00.0000000Z [err]: Test A in tests/unit/a.tcl
2026-04-01T00:00:00.0000001Z assertion failed
2026-04-01T00:00:01.0000000Z [exception]: Executing test client: timeout.
2026-04-01T00:00:01.0000001Z timeout
2026-04-01T00:00:01.0000002Z     (procedure "test" line 63)
2026-04-01T00:00:01.0000003Z     invoked from within
2026-04-01T00:00:01.0000004Z "test "Test B" {"
2026-04-01T00:00:01.0000005Z     (file "tests/unit/b.tcl" line 10)
"""


class TestMixedPatterns:
    """Log with both [err] and [exception] should capture both."""

    def test_captures_both_err_and_exception(self) -> None:
        failures = parse_job_log(_MIXED_ERR_AND_EXCEPTION)
        names = {f.test_name for f in failures}
        assert any("Test A" in n for n in names)
        assert any("Test B" in n for n in names)
        assert len(failures) == 2


# ---------------------------------------------------------------------------
# 8. Logs with only passing tests — should return empty
# ---------------------------------------------------------------------------

_ALL_PASSING_WITH_TIMESTAMPS = """\
2026-04-01T00:00:00.0000000Z [ok]: Test 1 in tests/unit/basic.tcl (10 ms)
2026-04-01T00:00:00.0000001Z [ok]: Test 2 in tests/unit/basic.tcl (5 ms)
2026-04-01T00:00:00.0000002Z Passed 2, Failed 0
"""


class TestAllPassingWithTimestamps:
    """Logs with only [ok] markers and GHA timestamps should return empty."""

    def test_no_failures(self) -> None:
        assert parse_job_log(_ALL_PASSING_WITH_TIMESTAMPS) == []


# ---------------------------------------------------------------------------
# 9. Summary *** [err] with GHA timestamps
# ---------------------------------------------------------------------------

_SUMMARY_ERR_WITH_TIMESTAMPS = """\
2026-04-01T00:00:00.0000000Z 
2026-04-01T00:00:00.0000001Z                    The End
2026-04-01T00:00:00.0000002Z 
2026-04-01T00:00:00.0000003Z !!! WARNING The following tests failed:
2026-04-01T00:00:00.0000004Z 
2026-04-01T00:00:00.0000005Z *** [err]: Primaries will not time out in tests/unit/cluster/failover2.tcl
2026-04-01T00:00:00.0000006Z expected message found in log file: *Failover attempt expired*
"""


class TestSummaryErrWithTimestamps:
    """*** [err] summary lines with GHA timestamps."""

    def test_summary_err(self) -> None:
        failures = parse_job_log(_SUMMARY_ERR_WITH_TIMESTAMPS)
        assert len(failures) == 1
        assert "Primaries will not time out" in failures[0].test_name
        assert "failover2.tcl" in failures[0].test_name
