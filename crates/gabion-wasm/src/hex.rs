//! Serde glue for the 128-bit identifiers that cross the JS boundary.
//!
//! Rule fingerprints and key hashes are `u128`. JavaScript numbers are IEEE
//! doubles and cannot hold 128 bits without loss, and `serde_wasm_bindgen`
//! has no lossless `u128` representation, so every `u128` is rendered as a
//! fixed-width lowercase hex string (`"0123…cdef"`, 32 nibbles) and parsed
//! back the same way. The Rust types keep `u128` fields; only the wire form
//! is textual.

pub mod u128_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &u128, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&format!("{value:032x}"))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<u128, D::Error> {
        let raw = String::deserialize(de)?;
        let trimmed = raw.strip_prefix("0x").unwrap_or(&raw);
        u128::from_str_radix(trimmed, 16).map_err(serde::de::Error::custom)
    }
}

pub mod option_u128_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &Option<u128>, ser: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(v) => ser.serialize_some(&format!("{v:032x}")),
            None => ser.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Option<u128>, D::Error> {
        let raw = Option::<String>::deserialize(de)?;
        raw.map(|s| {
            let trimmed = s.strip_prefix("0x").unwrap_or(&s).to_owned();
            u128::from_str_radix(&trimmed, 16).map_err(serde::de::Error::custom)
        })
        .transpose()
    }
}
