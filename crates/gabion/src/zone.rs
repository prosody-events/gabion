//! Zone names and the 4-byte hash every gossip header carries for its
//! sender's zone. Nodes compare zones by hash only, so two names that hash
//! alike are one zone: that costs cross-zone bytes, never counts.

use std::fmt;
use std::num::NonZeroU32;

use crate::rules::stable_hasher;

/// A zone name: 1 to 63 of `[A-Za-z0-9._-]`, starting and ending with a
/// letter or digit, like a Kubernetes label value. Hashed once, here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZoneName {
    name: Box<str>,
    hash: ZoneHash,
}

impl ZoneName {
    /// The only constructor; an empty name is an error, not "unset".
    pub fn parse(text: &str) -> Result<Self, ZoneError> {
        Ok(Self {
            hash: ZoneHash::parse(text)?,
            name: text.into(),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.name
    }

    pub fn hash(&self) -> ZoneHash {
        self.hash
    }
}

/// The low 32 bits of a zone name's stable 128-bit hash; 0 means "no zone"
/// on the wire, so it never occurs.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ZoneHash(NonZeroU32);

impl ZoneHash {
    /// A placeholder for tables sized before any zone is known.
    pub(crate) const MIN: Self = Self(NonZeroU32::MIN);

    /// The hash of zone name `text`, validated as [`ZoneName::parse`]
    /// validates it, without keeping the name.
    pub(crate) fn parse(text: &str) -> Result<Self, ZoneError> {
        let len = text.chars().count();
        let edge = |c: Option<char>| c.filter(|c| !c.is_ascii_alphanumeric());
        if len == 0 {
            return Err(ZoneError::Empty);
        }
        if len > 63 {
            return Err(ZoneError::TooLong { len });
        }
        if let Some(char) = text
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
        {
            return Err(ZoneError::BadChar { char });
        }
        if let Some(char) = edge(text.chars().next()).or(edge(text.chars().last())) {
            return Err(ZoneError::BadEdge { char });
        }
        let mut hasher = stable_hasher();
        hasher.write(text.as_bytes());
        Ok(Self(from_low32(hasher.finish_128() as u32)))
    }

    /// A header's zone field: 0 is no zone.
    pub(crate) fn from_wire(raw: u32) -> Option<Self> {
        NonZeroU32::new(raw).map(Self)
    }

    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// `0x` and eight hex digits, as logs name a remote zone.
impl fmt::Display for ZoneHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#010x}", self.0)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for ZoneHash {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// A hash of 0 becomes 1.
fn from_low32(low: u32) -> NonZeroU32 {
    NonZeroU32::new(low).unwrap_or(NonZeroU32::MIN)
}

/// Why a zone name is invalid; each text completes "it is not a valid zone
/// name: …".
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ZoneError {
    #[error("it is empty")]
    Empty,
    #[error("it is {len} characters long")]
    TooLong { len: usize },
    #[error("it contains `{char}`")]
    BadChar { char: char },
    #[error("it starts or ends with `{char}`")]
    BadEdge { char: char },
}

#[cfg(test)]
mod tests;
