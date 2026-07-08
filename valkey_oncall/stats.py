"""Prior-aware confidence for regressions (Beta-Bernoulli statistics).

A test at a commit fails with a hidden probability ``p`` (one coin flip per
run -- a Bernoulli trial). To judge whether a fresh burst of failures is a
real regression or just the test's usual flakiness, we:

1. Learn the test's baseline rate from its own pre-onset history with a Beta
   posterior (the conjugate prior for a Bernoulli rate).
2. Ask how surprising the post-onset failures are under that posterior, via
   the Beta-Binomial upper tail (the posterior-predictive probability of
   seeing at least that many failures). A tiny probability = surprising =
   high confidence the rate genuinely changed.

This makes confidence *per-test* and *sample-size aware*: a historically
clean test is damning after one fresh failure, while a known flake needs a
much bigger burst before we believe it regressed.

References:
  * Bayesian inference        https://en.wikipedia.org/wiki/Bayesian_inference
  * Beta distribution         https://en.wikipedia.org/wiki/Beta_distribution
  * Conjugate prior           https://en.wikipedia.org/wiki/Conjugate_prior
  * Beta-binomial distribution https://en.wikipedia.org/wiki/Beta-binomial_distribution
  * Posterior predictive      https://en.wikipedia.org/wiki/Posterior_predictive_distribution
  * Jeffreys prior            https://en.wikipedia.org/wiki/Jeffreys_prior
"""

from __future__ import annotations

import math
from typing import Optional, Tuple

# Default prior pseudocounts. Jeffreys prior Beta(0.5, 0.5) -- weakly
# informative, standard for a Bernoulli rate, and avoids the degenerate
# behaviour of a flat or zero prior at the boundaries.
PRIOR_A = 0.5
PRIOR_B = 0.5

# Burst-probability cutoffs mapping surprise -> confidence tier. A burst that
# would occur at most CONF_HIGH_P of the time under the learned baseline is
# "high" confidence; up to CONF_MED_P is "medium"; anything more likely is
# "low" (plausibly just baseline flakiness).
CONF_HIGH_P = 0.01
CONF_MED_P = 0.10


def _logbeta(a: float, b: float) -> float:
    return math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)


def learn_prior(
    hist_fails: int, hist_total: int, a0: float = PRIOR_A, b0: float = PRIOR_B
) -> Tuple[float, float]:
    """Beta posterior over the baseline fail rate from history counts."""
    return a0 + hist_fails, b0 + (hist_total - hist_fails)


def posterior_mean(a: float, b: float) -> float:
    """Posterior mean rate of a Beta(a, b)."""
    return a / (a + b)


def beta_binomial_pmf(j: int, m: int, a: float, b: float) -> float:
    """P(exactly j failures in m future runs) under Beta(a, b)."""
    log_c = math.lgamma(m + 1) - math.lgamma(j + 1) - math.lgamma(m - j + 1)
    return math.exp(log_c + _logbeta(a + j, b + m - j) - _logbeta(a, b))


def beta_binomial_upper_tail(k: int, m: int, a: float, b: float) -> float:
    """P(K >= k failures in m runs) under Beta(a, b). Small -> surprising."""
    if m <= 0:
        return 1.0
    k = max(k, 0)
    return sum(beta_binomial_pmf(j, m, a, b) for j in range(k, m + 1))


def regression_confidence(
    pre_fails: int,
    pre_total: int,
    post_fails: int,
    post_total: int,
    a0: float = PRIOR_A,
    b0: float = PRIOR_B,
) -> Tuple[str, Optional[float], Optional[float]]:
    """Prior-aware confidence that a regression is real, not baseline noise.

    ``pre_*`` are the test's failure/day counts BEFORE onset (its history),
    ``post_*`` the counts since onset. Returns ``(label, burst_p, p0_hat)``:

      label   -- "high" / "medium" / "low", or "unknown" when there is no
                 clean pre-onset history to learn a baseline from.
      burst_p -- probability of >= post_fails failures in post_total runs
                 under the learned baseline (smaller = more surprising).
      p0_hat  -- learned baseline fail rate (posterior mean).
    """
    if pre_total <= 0 or post_total <= 0:
        return "unknown", None, None
    a, b = learn_prior(pre_fails, pre_total, a0, b0)
    p0_hat = posterior_mean(a, b)
    burst_p = beta_binomial_upper_tail(post_fails, post_total, a, b)
    if burst_p <= CONF_HIGH_P:
        label = "high"
    elif burst_p <= CONF_MED_P:
        label = "medium"
    else:
        label = "low"
    return label, burst_p, p0_hat
