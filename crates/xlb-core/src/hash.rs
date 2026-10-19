//! [`BlakeHash`], the content identity of an xlb asset.

/// A string that is not a 64-char lowercase/uppercase hex BLAKE3 digest.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid hash: {0}")]
pub struct InvalidHash(pub String);

/// A BLAKE3 content-hash — the stable identity of an xlb asset.
///
/// All arithmetic in xlb is over these hashes; no integer IDs or paths are
/// canonical.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlakeHash([u8; 32]);

impl BlakeHash {
    /// Compute the BLAKE3 hash of `data`.
    pub fn hash(data: &[u8]) -> Self {
        Self(*blake3::hash(data).as_bytes())
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_hex(s: &str) -> Result<Self, InvalidHash> {
        let raw = hex::decode(s).map_err(|_| InvalidHash(s.to_string()))?;
        let arr: [u8; 32] = raw
            .try_into()
            .map_err(|_| InvalidHash(format!("expected 32 bytes: {s}")))?;
        Ok(Self(arr))
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns `true` when `data` hashes to this value.
    pub fn verify(&self, data: &[u8]) -> bool {
        *self == Self::hash(data)
    }
}

impl std::fmt::Debug for BlakeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlakeHash({}…)", &self.to_hex()[..8])
    }
}

impl std::fmt::Display for BlakeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl std::str::FromStr for BlakeHash {
    type Err = InvalidHash;
    fn from_str(s: &str) -> Result<Self, InvalidHash> {
        Self::from_hex(s)
    }
}
