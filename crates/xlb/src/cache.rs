//! The plain-file cache of one class: `<cache_dir>/<blake3-hex>` per complete
//! blob, under a [`RetentionIndex`].
//!
//! The index is the authority on what is complete locally. A blob enters it
//! only after the blob store has verified the content, and leaves it by LRU
//! eviction, which unlinks the plain file; the store forgets the entry at its
//! next garbage collection (see [`crate::store`]).
//!
//! Pinned blobs are never evicted and sit outside the byte budget. A blob a
//! fetch is returning is held and skipped by eviction until the fetch
//! returns. The index lock is a sync mutex that is never held across an
//! `.await`.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use xlb_core::{PinSetUsage, RetentionIndex};

use crate::BlakeHash;

/// Occupancy of one class's cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    /// Complete blobs held, pinned or not.
    pub entries: u64,
    /// Bytes of all complete blobs held.
    pub bytes: u64,
    /// Of `entries`, how many are pinned and outside the budget.
    pub pinned_entries: u64,
    pub pinned_bytes: u64,
    /// The LRU budget for unpinned blobs.
    pub budget_bytes: u64,
}

pub(crate) struct Cache {
    dir: PathBuf,
    index: Mutex<RetentionIndex>,
}

/// Keeps one blob out of eviction for as long as a fetch is returning it.
pub(crate) struct Hold {
    cache: Arc<Cache>,
    hash: BlakeHash,
}

impl Drop for Hold {
    fn drop(&mut self) {
        let evicted = self.cache.index().release(&self.hash);
        self.cache.unlink(&evicted);
    }
}

impl Cache {
    pub(crate) fn new(dir: &Path, index: RetentionIndex) -> Self {
        Self {
            dir: dir.to_path_buf(),
            index: Mutex::new(index),
        }
    }

    fn index(&self) -> MutexGuard<'_, RetentionIndex> {
        self.index.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// Where the plain file of `hash` lives.
    pub(crate) fn path_for(&self, hash: &BlakeHash) -> PathBuf {
        self.dir.join(hash.to_hex())
    }

    pub(crate) fn hold(self: &Arc<Self>, hash: BlakeHash) -> Hold {
        self.index().hold(hash);
        Hold {
            cache: self.clone(),
            hash,
        }
    }

    /// The path and size of `hash` if it is complete locally; marks it most
    /// recently used.
    pub(crate) fn lookup(&self, hash: &BlakeHash) -> Option<(PathBuf, u64)> {
        let mut index = self.index();
        let size = index.size_of(hash)?;
        index.touch(hash);
        Some((self.path_for(hash), size))
    }

    pub(crate) fn contains(&self, hash: &BlakeHash) -> bool {
        self.index().contains(hash)
    }

    /// Every complete blob.
    pub(crate) fn hashes(&self) -> Vec<BlakeHash> {
        self.index().hashes().copied().collect()
    }

    /// Whether a blob of `size` bytes can be cached at all; `Err` carries the
    /// budget it exceeds.
    pub(crate) fn admits(&self, hash: &BlakeHash, size: u64) -> Result<(), u64> {
        let index = self.index();
        if index.admits(hash, size) {
            Ok(())
        } else {
            Err(index.policy().budget_bytes)
        }
    }

    /// Evict ahead of an incoming blob of `size` bytes, so the download does
    /// not need the budget plus its own size on disk.
    pub(crate) fn make_room(&self, hash: &BlakeHash, size: u64) {
        let evicted = {
            let mut index = self.index();
            if index.is_pinned(hash) {
                Vec::new()
            } else {
                index.make_room(size)
            }
        };
        self.unlink(&evicted);
    }

    /// Record a verified blob whose plain file is in place.
    pub(crate) fn insert(&self, hash: BlakeHash, size: u64) {
        let evicted = self.index().insert(hash, size);
        self.unlink(&evicted);
    }

    /// Drop a blob whose plain file has vanished.
    pub(crate) fn forget(&self, hash: &BlakeHash) {
        self.index().remove(hash);
    }

    fn unlink(&self, evicted: &[BlakeHash]) {
        for hash in evicted {
            let path = self.path_for(hash);
            match std::fs::remove_file(&path) {
                Ok(()) => tracing::debug!(%hash, "evicted"),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!(%hash, "eviction of {} failed: {e}", path.display()),
            }
        }
    }

    pub(crate) fn stats(&self) -> CacheStats {
        let index = self.index();
        let usage = index.usage();
        CacheStats {
            entries: usage.entries,
            bytes: usage.bytes,
            pinned_entries: usage.pinned_entries,
            pinned_bytes: usage.pinned_bytes,
            budget_bytes: index.policy().budget_bytes,
        }
    }

    // ─── Pins ────────────────────────────────────────────────────────────

    pub(crate) fn pin(&self, hash: BlakeHash) {
        self.index().pin(hash);
    }

    pub(crate) fn unpin(&self, hash: &BlakeHash) {
        let evicted = self.index().unpin(hash);
        self.unlink(&evicted);
    }

    pub(crate) fn is_pinned(&self, hash: &BlakeHash) -> bool {
        self.index().is_pinned(hash)
    }

    pub(crate) fn pin_set(&self, name: &str, hashes: impl IntoIterator<Item = BlakeHash>) {
        self.index().pin_set(name, hashes);
    }

    pub(crate) fn unpin_set(&self, name: &str) {
        let evicted = self.index().unpin_set(name);
        self.unlink(&evicted);
    }

    pub(crate) fn pin_set_members(&self, name: &str) -> Vec<BlakeHash> {
        let index = self.index();
        let mut members: Vec<BlakeHash> = index
            .pin_set_members(name)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        members.sort_by_key(BlakeHash::to_hex);
        members
    }

    pub(crate) fn pin_set_usage(&self, name: &str) -> Option<PinSetUsage> {
        self.index().pin_set_usage(name)
    }
}

#[cfg(test)]
mod tests {
    use xlb_core::RetentionPolicy;

    use super::*;

    fn cache(dir: &Path, budget: u64) -> Arc<Cache> {
        Arc::new(Cache::new(
            dir,
            RetentionIndex::new(RetentionPolicy::new(budget, 0)),
        ))
    }

    fn put(cache: &Cache, data: &[u8]) -> BlakeHash {
        let hash = BlakeHash::hash(data);
        std::fs::write(cache.path_for(&hash), data).unwrap();
        cache.insert(hash, data.len() as u64);
        hash
    }

    #[test]
    fn eviction_unlinks_the_least_recently_used_plain_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 10);
        let a = put(&cache, b"aaaaaa");
        let b = put(&cache, b"bbbbbb");
        assert!(!cache.contains(&a));
        assert!(!cache.path_for(&a).exists());
        assert!(cache.path_for(&b).exists());
        assert_eq!(cache.stats().bytes, 6);
    }

    #[test]
    fn a_held_blob_survives_until_the_hold_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 10);
        let a = put(&cache, b"aaaaaa");
        let hold = cache.hold(a);
        let b = put(&cache, b"bbbbbb");
        assert!(cache.path_for(&a).exists(), "held blob is not evicted");
        drop(hold);
        assert!(!cache.path_for(&a).exists());
        assert!(cache.path_for(&b).exists());
    }

    #[test]
    fn a_blob_larger_than_the_budget_is_refused_unless_pinned() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 10);
        let hash = BlakeHash::hash(b"big");
        assert_eq!(cache.admits(&hash, 11), Err(10));
        cache.pin(hash);
        assert_eq!(cache.admits(&hash, 11), Ok(()));
    }

    #[test]
    fn make_room_evicts_before_the_incoming_blob_lands() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 10);
        let a = put(&cache, b"aaaaaa");
        cache.make_room(&BlakeHash::hash(b"incoming"), 6);
        assert!(!cache.path_for(&a).exists());
    }
}
