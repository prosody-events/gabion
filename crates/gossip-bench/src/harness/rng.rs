//! Deterministic random streams.
//!
//! Every source of randomness in a study run draws from its own SplitMix64
//! stream, derived from the run seed and a [`Stream`] tag. Separating the
//! streams keeps the offered-load trace and the churn schedule identical
//! across solutions: a solution that sends more datagrams consumes more of
//! the network stream, but never shifts a request arrival or a pod start.

/// Which concern a stream belongs to. The discriminant is mixed into the
/// seed, so two streams never share a sequence.
#[derive(Clone, Copy, Debug)]
pub enum Stream {
    /// Per-key request arrival processes and load-balancer picks.
    Traffic = 1,
    /// Datagram latency jitter and loss draws.
    Network = 2,
    /// Pod start offsets, discovery delivery jitter, crash victims.
    Churn = 3,
    /// Measurement choices (which keys the oracle tracks).
    Oracle = 4,
    /// Per-node gossip RNG seeds handed to the runtime.
    Gossip = 5,
}

/// SplitMix64 (Steele, Lea, Flood 2014). Small, fast, and good enough for
/// simulation draws; no cryptographic property is needed.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    /// Independent stream `index` of concern `stream` under run seed `seed`.
    pub fn stream(seed: u64, stream: Stream, index: u64) -> Self {
        Self(mix64(
            mix64(seed ^ (stream as u64).wrapping_mul(0xA076_1D64_78BD_642F)) ^ index,
        ))
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix64(self.0)
    }

    /// Uniform in `[0, 1)` with 53 bits of precision.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1_u64 << 53) as f64)
    }

    /// Uniform integer in `[0, n)`; `n` must be non-zero. Lemire's
    /// multiply-shift, whose bias is below 2^-64 per draw for the `n`
    /// used here.
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0);
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// Uniform integer in `[0, n]`.
    pub fn up_to(&mut self, n: u64) -> u64 {
        if n == u64::MAX {
            self.next_u64()
        } else {
            self.below(n + 1)
        }
    }

    /// Bernoulli trial with success probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && self.unit() < p
    }

    /// Exponential variate with the given mean, in the same unit.
    pub fn exp(&mut self, mean: f64) -> f64 {
        -mean * (1.0 - self.unit()).ln()
    }
}

/// SplitMix64 finalizer: a bijective avalanche over `u64`.
pub fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
