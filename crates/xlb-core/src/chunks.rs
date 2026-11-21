//! Byte layout of an object backed by fixed-size CAS chunks.

use serde::{Deserialize, Serialize};

use crate::BlakeHash;

pub const CAS_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
pub const MODEL_INDEX_PREFIX: &str = "model-indexes/blake3/";
pub const CAS_INDEX_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChunkIndex {
    pub chunks: Vec<BlakeHash>,
    pub size_bytes: u64,
}

impl ChunkIndex {
    pub fn is_complete(&self) -> bool {
        self.chunks.len() as u64 == self.size_bytes.div_ceil(CAS_CHUNK_BYTES)
    }

    /// Chunk hash and stored size at the given chunk ordinal.
    pub fn chunk(&self, ordinal: u64) -> Option<(BlakeHash, u64)> {
        let offset = ordinal.checked_mul(CAS_CHUNK_BYTES)?;
        if offset >= self.size_bytes {
            return None;
        }
        let hash = *self.chunks.get(usize::try_from(ordinal).ok()?)?;
        Some((hash, (self.size_bytes - offset).min(CAS_CHUNK_BYTES)))
    }
}
