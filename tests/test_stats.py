"""Tests for the prior-aware confidence statistics (valkey_oncall.stats)."""

from valkey_oncall.stats import (
    CONF_HIGH_P,
    CONF_MED_P,
    beta_binomial_pmf,
    beta_binomial_upper_tail,
    learn_prior,
    posterior_mean,
    regression_confidence,
)

_SEVERITY = {"low": 0, "medium": 1, "high": 2, "unknown": -1}


class TestBetaMath:
    def test_pmf_sums_to_one(self):
        a, b = learn_prior(10, 200)
        total = sum(beta_binomial_pmf(j, 5, a, b) for j in range(6))
        assert abs(total - 1.0) < 1e-9, total

    def test_posterior_mean_learns_baseline(self):
        a, b = learn_prior(10, 200)
        assert abs(posterior_mean(a, b) - 10.5 / 201) < 1e-9

    def test_upper_tail_monotone_in_k(self):
        a, b = learn_prior(10, 200)
        assert (
            beta_binomial_upper_tail(3, 5, a, b)
            < beta_binomial_upper_tail(2, 5, a, b)
            < beta_binomial_upper_tail(1, 5, a, b)
        )

    def test_upper_tail_zero_runs(self):
        a, b = learn_prior(0, 100)
        assert beta_binomial_upper_tail(0, 0, a, b) == 1.0


class TestRegressionConfidence:
    def test_clean_test_fresh_failure_is_high(self):
        # Pristine test (0 fails in 400 days) failing 3/3 since onset.
        label, burst_p, p0 = regression_confidence(0, 400, 3, 3)
        assert label == "high", (label, burst_p)
        assert burst_p <= CONF_HIGH_P
        assert p0 < 0.01

    def test_known_flake_small_burst_is_low(self):
        # ~5% flake (20/400) failing 1 of 3 since onset -> expected noise.
        label, burst_p, _ = regression_confidence(20, 400, 1, 3)
        assert label == "low", (label, burst_p)
        assert burst_p > CONF_MED_P

    def test_known_flake_big_burst_is_high(self):
        # ~5% flake failing 5/5 -> a genuine burst even for a flake.
        label, burst_p, _ = regression_confidence(20, 400, 5, 5)
        assert label == "high", (label, burst_p)

    def test_no_pre_history_is_unknown(self):
        # Already failing at window start: no clean baseline to learn from.
        assert regression_confidence(0, 0, 1, 1) == ("unknown", None, None)
        assert regression_confidence(5, 10, 1, 0) == ("unknown", None, None)

    def test_confidence_escalates_with_burst_size(self):
        # For a fixed flaky history, more post-onset fails -> non-decreasing
        # severity (low -> medium -> high) as the burst gets more surprising.
        sev = [_SEVERITY[regression_confidence(20, 400, k, 5)[0]] for k in range(1, 6)]
        assert sev == sorted(sev), sev
        assert sev[0] <= sev[-1] and sev[-1] == _SEVERITY["high"], sev

    def test_cleaner_baseline_is_more_sensitive(self):
        # Same post-onset burst is more surprising against a cleaner history.
        _, burst_clean, _ = regression_confidence(1, 400, 2, 4)
        _, burst_flaky, _ = regression_confidence(40, 400, 2, 4)
        assert burst_clean < burst_flaky
