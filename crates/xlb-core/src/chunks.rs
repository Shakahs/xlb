//! Byte layout of an object backed by fixed-size CAS chunks.

use serde::{Deserialize, Serialize};
use blake3::hazmat::{ChainingValue, HasherExt, Mode, merge_subtrees_non_root, merge_subtrees_root};

use crate::BlakeHash;

pub const CAS_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
pub const MODEL_INDEX_PREFIX: &str = "model-indexes/blake3/";
pub const CAS_INDEX_LIMIT: usize = 16 * 1024 * 1024;
// Each model range has two quoted hexadecimal values with separators.
pub const MODEL_INDEX_MAX_CHUNKS: u64 = (CAS_INDEX_LIMIT as u64 - 128) / 134;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
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

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelIndex {
    #[serde(flatten)]
    pub layout: ChunkIndex,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chaining_values: Vec<BlakeHash>,
}

impl ModelIndex {
    /// Validate metadata. A legacy multi-chunk layout still requires a full content check.
    pub fn matches_root(&self, root: BlakeHash) -> bool {
        self.layout.is_complete() && matches_content_root(&self.layout.chunks, &self.chaining_values, root)
    }
}

pub(crate) fn matches_content_root(chunks: &[BlakeHash], values: &[BlakeHash], root: BlakeHash) -> bool {
    match chunks {
        [] => values.is_empty() && root == BlakeHash::hash(b""),
        [chunk] => values.is_empty() && *chunk == root,
        _ => values.is_empty() || (values.len() == chunks.len() && model_root(values) == Some(root)),
    }
}

pub fn positioned_value(offset: u64, bytes: &[u8]) -> BlakeHash {
    BlakeHash::from_bytes(blake3::Hasher::new().set_input_offset(offset).update(bytes).finalize_non_root())
}

pub fn model_root(values: &[BlakeHash]) -> Option<BlakeHash> {
    if values.len() < 2 {
        return None;
    }
    let (left, right) = split_subtrees(values);
    Some(BlakeHash::from_bytes(*merge_subtrees_root(&subtree(left), &subtree(right), Mode::Hash).as_bytes()))
}

fn split_subtrees(values: &[BlakeHash]) -> (&[BlakeHash], &[BlakeHash]) {
    // The left subtree holds the largest power of two strictly below the range count.
    values.split_at(1 << (values.len() - 1).ilog2())
}

fn subtree(values: &[BlakeHash]) -> ChainingValue {
    if let [value] = values {
        return *value.as_bytes();
    }
    let (left, right) = split_subtrees(values);
    merge_subtrees_non_root(&subtree(left), &subtree(right), Mode::Hash)
}
