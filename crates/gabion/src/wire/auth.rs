//! HMAC-SHA256 tag for authenticated frames.

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use super::DecodeError;

pub(crate) const AUTH_TAG_LEN: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HmacKey(pub [u8; 32]);

pub(crate) fn sign(key: &HmacKey, payload: &[u8]) -> [u8; AUTH_TAG_LEN] {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(&key.0).expect("HMAC-SHA256 accepts any 32-byte key");
    mac.update(payload);
    mac.finalize().into_bytes().into()
}

pub(crate) fn verify(key: &HmacKey, payload: &[u8], tag: &[u8]) -> Result<(), DecodeError> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(&key.0).map_err(|_| DecodeError::AuthenticationFailed)?;
    mac.update(payload);
    mac.verify_slice(tag)
        .map_err(|_| DecodeError::AuthenticationFailed)
}

#[cfg(test)]
mod tests;
