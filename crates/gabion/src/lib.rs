pub mod aggregate;
pub mod crdt;
pub mod defaults;
pub mod discovery;
pub mod gossip;
#[cfg(feature = "transport-udp")]
pub mod identity;
pub mod rules;
pub mod settings;
pub mod throttle;
pub mod window;
pub mod wire;

#[cfg(test)]
mod mock;

/// Writes a 128-bit id or hash as `0x` and 32 hex digits.
#[cfg(feature = "serde")]
fn hex<S: serde::Serializer>(value: &u128, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&format_args!("{value:#034x}"))
}
