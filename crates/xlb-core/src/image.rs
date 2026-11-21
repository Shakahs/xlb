//! Shared schema for OCI blobs assembled from XLB origin chunks.

use serde::{Deserialize, Serialize};

use crate::BlakeHash;

pub const IMAGE_CHUNK_BYTES: usize = crate::chunks::CAS_CHUNK_BYTES as usize;
/// Index objects are keyed by the blob's BLAKE3 OCI digest (`blake3:<hex>`).
pub const IMAGE_INDEX_PREFIX: &str = "images/blake3/";
pub const IMAGE_INDEX_LIMIT: usize = 16 * 1024 * 1024;
/// The OCI digest algorithm prefix of every image blob.
pub const IMAGE_DIGEST_PREFIX: &str = "blake3:";

/// The lowercase hex of a `blake3:<hex>` OCI digest.
pub fn blake3_digest_hex(digest: &str) -> Option<&str> {
    let hex = digest.strip_prefix(IMAGE_DIGEST_PREFIX)?;
    is_digest_hex(hex).then_some(hex)
}

/// Whether `hex` is a 64-character lowercase hex BLAKE3 hash.
pub fn is_digest_hex(hex: &str) -> bool {
    hex.len() == 64 && hex.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageBlobIndex {
    pub chunks: Vec<BlakeHash>,
    pub size_bytes: u64,
    pub media_type: String,
}

impl ImageBlobIndex {
    pub fn has_complete_chunk_list(&self) -> bool {
        self.chunks.len() as u64 == self.size_bytes.div_ceil(IMAGE_CHUNK_BYTES as u64)
    }
}
