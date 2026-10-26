//! Pinned trees — durable named pin sets on a disk-backed class (R1003).
//!
//! A *pinned tree* is the IPFS "pin this tree" idea over xlb's flat CAS: a
//! name plus the set of blake3 hashes it covers. Declared once, it is written
//! to `<cache_dir>/pins/<name>.json`, reloaded by [`crate::AssetClass::register`]
//! and declared on the retention index **before** the cache replays its stored
//! entries, so the rebuild after a restart can never evict a pinned blob. Its
//! members are never evicted and sit outside the byte budget, like any pin.
//!
//! This is distinct from the manifest-derived pin sets of R002-F2, which are
//! re-derived on every manifest refresh and deliberately not persisted. The
//! two share the retention index's one set namespace; a tree name that
//! collides with a manifest `pinned_sets` key would be replaced by the next
//! refresh, so name trees for what they hold (`keys-pack`), not generically.
//!
//! In-memory classes (`cache_dir = None`) accept trees too, but nothing
//! persists — [`PinnedTreeInfo::durable`] says which one you have.
//!
//! The `pins/` subdirectory is invisible to the blob store: `DiskBlobStore`
//! only admits regular files named by a 64-char hex hash.

use std::{
    io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::BlakeHash;

/// Subdirectory of a class's `cache_dir` holding one JSON file per tree.
pub(crate) const PINS_DIR: &str = "pins";

/// What one tree covers and how much of it the node holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedTreeInfo {
    pub name: String,
    /// Hashes the tree covers.
    pub members: u64,
    /// Of `members`, how many blobs are in the cache now.
    pub present: u64,
    pub present_bytes: u64,
    /// `true` when the tree is written under the class's `cache_dir` and
    /// survives a restart; `false` on an in-memory class.
    pub durable: bool,
}

/// Outcome of seeding a tree's bytes from local directories.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedReport {
    /// Blobs read from disk, verified, and written into the cache.
    pub ingested: u64,
    /// Members that were already cached; not re-read.
    pub already_cached: u64,
    /// Members no seed directory held (still pinned; arrive by fetch later).
    pub missing: Vec<BlakeHash>,
    /// Files named as a member whose bytes hash to something else. Skipped —
    /// a corrupt checkout must not become the node's truth.
    pub mismatched: Vec<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum PinError {
    #[error("invalid tree name {0:?}: use 1-128 of [A-Za-z0-9._-], not starting with '.'")]
    InvalidName(String),
    #[error("no pinned tree named {0:?}")]
    NotFound(String),
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("pins file {path}: {message}")]
    Corrupt { path: PathBuf, message: String },
}

/// Tree names become file names, so they are restricted to a path-safe set.
pub fn validate_tree_name(name: &str) -> Result<(), PinError> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(PinError::InvalidName(name.to_string()))
    }
}

/// On-disk shape of one tree. `schema` lets a future change fail loudly.
#[derive(Debug, Serialize, Deserialize)]
struct TreeFile {
    schema: u32,
    name: String,
    hashes: Vec<BlakeHash>,
}

const SCHEMA: u32 = 1;

/// The `pins/` directory of one class.
#[derive(Debug, Clone)]
pub(crate) struct TreeStore {
    dir: PathBuf,
}

impl TreeStore {
    pub(crate) fn under(cache_dir: &Path) -> Self {
        Self {
            dir: cache_dir.join(PINS_DIR),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.json"))
    }

    /// Every persisted tree. Called once at register, before the cache opens,
    /// so it is synchronous. A corrupt file fails the load rather than being
    /// skipped: silently dropping a pin would let the replay evict its blobs.
    pub(crate) fn load_all(&self) -> Result<Vec<(String, Vec<BlakeHash>)>, PinError> {
        let rd = match std::fs::read_dir(&self.dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut trees = Vec::new();
        for entry in rd {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let corrupt = |message: String| PinError::Corrupt {
                path: path.clone(),
                message,
            };
            let raw = std::fs::read(&path)?;
            let file: TreeFile = serde_json::from_slice(&raw).map_err(|e| corrupt(e.to_string()))?;
            if file.schema != SCHEMA {
                return Err(corrupt(format!("unsupported schema {}", file.schema)));
            }
            validate_tree_name(&file.name).map_err(|e| corrupt(e.to_string()))?;
            trees.push((file.name, file.hashes));
        }
        trees.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(trees)
    }

    /// Write (or replace) a tree atomically: temp file + rename.
    pub(crate) async fn save(&self, name: &str, hashes: &[BlakeHash]) -> Result<(), PinError> {
        tokio::fs::create_dir_all(&self.dir).await?;
        let body = serde_json::to_vec_pretty(&TreeFile {
            schema: SCHEMA,
            name: name.to_string(),
            hashes: hashes.to_vec(),
        })
        .map_err(io::Error::other)?;
        let final_path = self.path(name);
        let tmp = self
            .dir
            .join(format!(".{name}.json.tmp.{}", std::process::id()));
        tokio::fs::write(&tmp, body).await?;
        tokio::fs::rename(&tmp, &final_path).await?;
        Ok(())
    }

    /// Delete a tree's file. Returns whether it existed.
    pub(crate) async fn remove(&self, name: &str) -> Result<bool, PinError> {
        match tokio::fs::remove_file(self.path(name)).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

/// Every regular file in `dir` named by a 64-char blake3 hex — the shape of a
/// CAS checkout such as noisetable's `packs/<pack>/assets/`. Names only; the
/// bytes are verified when they are seeded.
pub fn hashes_in_dir(dir: &Path) -> io::Result<Vec<BlakeHash>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Ok(hash) = BlakeHash::from_hex(&name) else {
            continue;
        };
        if entry.file_type()?.is_file() {
            out.push(hash);
        }
    }
    out.sort_by_key(|h| h.to_hex());
    Ok(out)
}

/// The members a pin request names: the explicit `hashes` (hex) when given,
/// else every hash-named file across `dirs`. Shared by every pin surface
/// (`xlb serve`'s socket, yah desktop's embedded node) so they agree.
pub fn resolve_members(hashes: &[String], dirs: &[PathBuf]) -> Result<Vec<BlakeHash>, String> {
    if !hashes.is_empty() {
        return hashes
            .iter()
            .map(|h| BlakeHash::from_hex(h.trim()))
            .collect::<Result<_, _>>()
            .map_err(|e| format!("bad hash: {e}"));
    }
    if dirs.is_empty() {
        return Err("pin_tree needs hashes, from_dirs, or both".into());
    }
    let mut all = Vec::new();
    for d in dirs {
        all.extend(hashes_in_dir(d).map_err(|e| format!("reading {}: {e}", d.display()))?);
    }
    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "xlb-pinned-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn names_are_path_safe() {
        for good in ["keys-pack", "a", "v1.2_x"] {
            validate_tree_name(good).unwrap();
        }
        for bad in ["", ".hidden", "a/b", "../x", "sp ace", &"x".repeat(129)] {
            assert!(validate_tree_name(bad).is_err(), "{bad:?} accepted");
        }
    }

    #[tokio::test]
    async fn trees_round_trip_and_remove() {
        let dir = tmpdir("rt");
        let s = TreeStore::under(&dir);
        assert!(s.load_all().unwrap().is_empty(), "no pins dir yet");
        let hs = vec![BlakeHash::hash(b"a"), BlakeHash::hash(b"b")];
        s.save("keys", &hs).await.unwrap();
        s.save("fonts", &hs[..1]).await.unwrap();
        let loaded = s.load_all().unwrap();
        assert_eq!(loaded, vec![("fonts".into(), hs[..1].to_vec()), ("keys".into(), hs.clone())]);
        assert!(s.remove("fonts").await.unwrap());
        assert!(!s.remove("fonts").await.unwrap());
        assert_eq!(s.load_all().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_corrupt_pins_file_fails_the_load_instead_of_dropping_the_pin() {
        let dir = tmpdir("corrupt");
        std::fs::create_dir_all(dir.join(PINS_DIR)).unwrap();
        std::fs::write(dir.join(PINS_DIR).join("x.json"), b"{not json").unwrap();
        assert!(matches!(
            TreeStore::under(&dir).load_all(),
            Err(PinError::Corrupt { .. })
        ));
    }

    #[test]
    fn hashes_in_dir_takes_only_hex_named_files() {
        let dir = tmpdir("scan");
        let h = BlakeHash::hash(b"blob");
        std::fs::write(dir.join(h.to_hex()), b"blob").unwrap();
        std::fs::write(dir.join("README.md"), b"x").unwrap();
        std::fs::create_dir(dir.join(BlakeHash::hash(b"d").to_hex())).unwrap();
        assert_eq!(hashes_in_dir(&dir).unwrap(), vec![h]);
    }
}
