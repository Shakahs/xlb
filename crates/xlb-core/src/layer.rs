//! OCI layer filesystem metadata authenticated by the image manifest.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{BlakeHash, chunks::ChunkIndex};

pub const LAYER_INDEX_PREFIX: &str = "layer-indexes/blake3/";
pub const LAYER_INDEX_LABEL: &str = "containerd.io/snapshot/xlb.layer-index";

#[derive(Debug, Deserialize, Serialize)]
pub struct LayerIndex {
    pub archive: ChunkIndex,
    pub diff_id: BlakeHash,
    pub entries: Vec<LayerEntry>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LayerEntry {
    /// Relative Unix path. The root directory has an empty path.
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
    Symlink(Vec<u8>),
    Hardlink(Vec<u8>),
    Fifo,
    Device { block: bool, major: u32, minor: u32 },
    Whiteout,
}
