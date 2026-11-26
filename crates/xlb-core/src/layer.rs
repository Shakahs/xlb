//! OCI layer filesystem metadata authenticated by the image manifest.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{BlakeHash, chunks::ChunkIndex};

pub const LAYER_INDEX_PREFIX: &str = "layer-indexes/blake3/";
pub const LAYER_INDEX_LABEL: &str = "containerd.io/snapshot/xlb.layer-index";
pub const LAYER_INDEX_LIMIT: usize = 64 * 1024 * 1024;

#[derive(Debug, Deserialize, Serialize)]
pub struct LayerIndex {
    pub archive: ChunkIndex,
    pub diff_id: BlakeHash,
    pub entries: Vec<LayerEntry>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LayerEntry {
    /// Relative Unix path. The root directory has an empty path.
    #[serde(with = "unix_path")]
    pub path: Vec<u8>,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub modified: u64,
    pub xattrs: BTreeMap<String, Vec<u8>>,
    pub kind: LayerEntryKind,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum LayerEntryKind {
    File { offset: u64, size: u64 },
    Directory { opaque: bool },
    Symlink(#[serde(with = "unix_path")] Vec<u8>),
    Hardlink(#[serde(with = "unix_path")] Vec<u8>),
    Fifo,
    Device { block: bool, major: u32, minor: u32 },
    Whiteout,
}

/// A Unix path or link target as a string when it is UTF-8 and as a byte
/// array otherwise, so every tar path round-trips while a runfiles path costs
/// one byte per character instead of a decimal number per byte.
mod unix_path {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        match std::str::from_utf8(bytes) {
            Ok(text) => serializer.serialize_str(text),
            Err(_) => serializer.collect_seq(bytes),
        }
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum EncodedPath {
        Text(String),
        Bytes(Vec<u8>),
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        Ok(match EncodedPath::deserialize(deserializer)? {
            EncodedPath::Text(text) => text.into_bytes(),
            EncodedPath::Bytes(bytes) => bytes,
        })
    }
}
