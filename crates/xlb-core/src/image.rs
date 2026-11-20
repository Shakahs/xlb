//! Shared schema for OCI blobs assembled from XLB origin chunks.

use serde::{Deserialize, Serialize};

use crate::BlakeHash;

pub const IMAGE_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub const IMAGE_INDEX_PREFIX: &str = "images/sha256/";
pub const IMAGE_INDEX_LIMIT: usize = 16 * 1024 * 1024;

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
