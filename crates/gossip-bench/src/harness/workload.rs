//! Offered-load trace. Each key has its own arrival process on its own
//! random stream, so the trace (request times, keys, and load-balancer
//! picks) is a pure function of the regime and the seed and is identical
//! across solutions. Requests arrive continuously in virtual time; nothing
//! is batched to ticks.

use super::node::KeyKind;
use super::plan::{NS_PER_MS, NS_PER_S};
use super::regime::Regime;
use super::rng::{Rng, Stream};

enum Gaps {
    /// `period * U(1 - jitter, 1 + jitter)`, first arrival `U(0, period)`.
    Jittered { period_ns: f64, jitter: f64 },
    /// Poisson process.
    Poisson { mean_ns: f64 },
}

pub struct KeyProcess {
    rng: Rng,
    gaps: Gaps,
    from_ns: u64,
    until_ns: u64,
}

impl KeyProcess {
    pub fn for_regime(regime: &Regime, kinds: &[KeyKind], seed: u64) -> Vec<Self> {
        let sustainable_rps = regime.rule.limit as f64 * 1000.0 / regime.rule.window_ms as f64;
        let mut hot = regime.traffic.hot_keys.iter();
        let mut bursts = regime.traffic.bursts.iter();
        kinds
            .iter()
            .enumerate()
            .map(|(key, kind)| {
                let rng = Rng::stream(seed, Stream::Traffic, key as u64);
                let (gaps, from_ns, until_ns) = match kind {
                    KeyKind::Site => (
                        Gaps::Jittered {
                            period_ns: (regime.traffic.site_period_ms * NS_PER_MS) as f64,
                            jitter: regime.traffic.site_jitter,
                        },
                        0,
                        u64::MAX,
                    ),
                    KeyKind::Hot => {
                        let multiple = hot.next().map_or(1.0, |h| h.limit_multiple);
                        (
                            Gaps::Poisson {
                                mean_ns: NS_PER_S as f64 / (multiple * sustainable_rps),
                            },
                            0,
                            u64::MAX,
                        )
                    }
                    KeyKind::Burst => {
                        let burst = bursts.next().copied();
                        let rate = burst.map_or(1.0, |b| b.rate_rps);
                        let from = burst.map_or(0, |b| b.start_s * NS_PER_S);
                        let until = burst.map_or(0, |b| (b.start_s + b.duration_s) * NS_PER_S);
                        (
                            Gaps::Poisson {
                                mean_ns: NS_PER_S as f64 / rate,
                            },
                            from,
                            until,
                        )
                    }
                };
                Self {
                    rng,
                    gaps,
                    from_ns,
                    until_ns,
                }
            })
            .collect()
    }

    pub fn first(&mut self) -> Option<u64> {
        let offset = match self.gaps {
            Gaps::Jittered { period_ns, .. } => self.rng.unit() * period_ns,
            Gaps::Poisson { mean_ns } => self.rng.exp(mean_ns),
        };
        self.within(self.from_ns as f64 + offset)
    }

    pub fn next(&mut self, now_ns: u64) -> Option<u64> {
        let gap = match self.gaps {
            Gaps::Jittered { period_ns, jitter } => {
                period_ns * (1.0 - jitter + 2.0 * jitter * self.rng.unit())
            }
            Gaps::Poisson { mean_ns } => self.rng.exp(mean_ns),
        };
        // A gap of at least 1 ns keeps arrivals strictly ordered per key.
        self.within(now_ns as f64 + gap.max(1.0))
    }

    /// Pick the node index (into the running list) this request lands on.
    pub fn pick(&mut self, running: usize) -> usize {
        self.rng.below(running as u64) as usize
    }

    fn within(&self, at_ns: f64) -> Option<u64> {
        let at = at_ns as u64;
        (at < self.until_ns).then_some(at)
    }
}
