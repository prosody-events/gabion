//! Serde glue for the 128-bit identifiers that cross the JS boundary.
//!
//! Rule fingerprints and key hashes are `u128`. JavaScript numbers are IEEE
//! doubles and cannot hold 128 bits without loss, and `serde_wasm_bindgen`
//! has no lossless `u128` representation, so every `u128` is rendered as a
//! fixed-width lowercase hex string (`"0123…cdef"`, 32 nibbles). The Rust
//! types keep `u128` fields; only the wire form is textual.

pub mod u128_hex {
    use serde::Serializer;

    pub fn serialize<S: Serializer>(value: &u128, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&format!("{value:032x}"))
    }
}

pub mod option_u128_hex {
    use serde::Serializer;

    pub fn serialize<S: Serializer>(value: &Option<u128>, ser: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(v) => ser.serialize_some(&format!("{v:032x}")),
            None => ser.serialize_none(),
        }
    }
}
