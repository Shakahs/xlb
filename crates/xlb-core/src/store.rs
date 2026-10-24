//! The storage seam for caches that are not the native disk/memory ones:
//! a minimal async [`BlobStore`] plus [`RetainingCache`], which drives a
//! [`RetentionIndex`] over any store and deletes what the index evicts.
//!
//! Store futures are `Send` on native targets (callers hold caches across
//! tokio tasks) and unconstrained on wasm32, where the browser APIs are
//! single-threaded and `JsFuture` is `!Send`. [`MaybeSend`] carries that
//! split; an `async fn` impl satisfies it on both.
//!
//! @yah:ticket(R002-B7, "RetainingCache holds the cache lock across blob I/O, serializing concurrent gets (T5 regression)")
//! @yah:status(review)
//! @yah:at(2026-10-18T18:23:01Z)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:parent(R002)
//! @yah:severity(medium)
//! @yah:next("Tier: Warrior")
//! @yah:next("At cf749c97 old DiskCache took state.lock() only around index ops, releasing it during tokio::fs I/O; R002-T5 made Cache one Mutex<RetainingCache<NativeStore>> held across blob I/O. Move the lock inside RetainingCache: std::sync::Mutex<RetentionIndex>, never held across .await; store I/O and evicted-blob deletes outside the lock; drop Cache's outer Mutex and the retention() guard.")
//! @yah:depends_on(R002-T5)
//! @yah:handoff("RetainingCache<S> now takes &self everywhere; index is std::sync::Mutex<RetentionIndex> (private index() helper), never held across .await. Store I/O and evicted-blob deletes run outside the lock via delete_evicted, which skips any hash re-inserted since eviction (a residual window between that check and store.delete remains; closing it needs a per-hash in-flight guard).")
//! @yah:handoff("xlb: Cache dropped its outer tokio Mutex and holds RetainingCache<NativeStore> directly; retention() returns &RetainingCache (no await); lib.rs and cache.rs call sites updated; cache.rs Locking header rewritten. Public RetainingCache method names unchanged; crates/xlb-web untouched (it only calls RetainingCache::open).")
//! @yah:handoff("Tests added in store.rs: concurrent_gets_of_different_blobs_overlap_store_io (Barrier(2) store, 1s timeout) and evicted_hash_reinserted_before_delete_is_kept. xlb-core gained a tokio dev-dependency. Suggested: git add crates/xlb-core crates/xlb/src && git commit -m 'xlb: RetainingCache index lock off the blob I/O path (R002-B7)'")
//! @yah:verify("cargo test --workspace EXIT=0, 135 passed vs baseline 133 (+2 new tests) (/tmp/r002b7-4.log)")
//! @yah:verify("cargo check -p xlb-core --target wasm32-unknown-unknown EXIT=0 (/tmp/r002b7-5.log); -p xlb-web EXIT=0 (/tmp/r002b7-6.log)")
//! @yah:gotcha("Leader-reviewed residual race (delete_evicted check vs store.delete racing a concurrent put of the same hash) is self-healing: RetainingCache::get (store.rs:187-195) treats a missing/corrupt store blob as a miss and drops it from the index, so the worst case is one refetch, not a stuck phantom entry. Accepted; a per-hash in-flight guard is only worth adding if refetch cost becomes measurable.")

use std::{
    collections::HashMap,
    future::Future,
    sync::{Mutex, MutexGuard},
};

use crate::{
    hash::BlakeHash,
    retention::{RetentionIndex, RetentionPolicy, RetentionUsage},
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// The backing store failed (I/O, quota, JS exception text).
    #[error("store: {0}")]
    Backend(String),
    /// An unpinned blob larger than the whole budget.
    #[error("{size} bytes exceeds the {budget}-byte budget")]
    TooLarge { size: u64, budget: u64 },
}

/// `Send` on native targets, nothing on wasm32.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}

/// `Send` on native targets, nothing on wasm32.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

/// Flat content-addressed byte storage. No retention logic lives here.
///
/// Implement with plain `async fn`; the signatures below are the desugared
/// form so the returned futures can carry [`MaybeSend`].
pub trait BlobStore {
    fn get(
        &self,
        hash: &BlakeHash,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, StoreError>> + MaybeSend;
    fn put(
        &self,
        hash: &BlakeHash,
        data: &[u8],
    ) -> impl Future<Output = Result<(), StoreError>> + MaybeSend;
    /// Deleting an absent blob is not an error.
    fn delete(&self, hash: &BlakeHash) -> impl Future<Output = Result<(), StoreError>> + MaybeSend;
    /// Every stored blob with its size in bytes, least-recently-used first
    /// where the store can tell (it seeds the LRU order on reopen).
    fn list(&self) -> impl Future<Output = Result<Vec<(BlakeHash, u64)>, StoreError>> + MaybeSend;
}

/// A [`BlobStore`] in a `HashMap`; for tests and as a reference impl.
#[derive(Debug, Default)]
pub struct MemBlobStore {
    blobs: Mutex<HashMap<BlakeHash, Vec<u8>>>,
}

impl MemBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.blobs.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.blobs.lock().unwrap().is_empty()
    }
}

impl BlobStore for MemBlobStore {
    async fn get(&self, hash: &BlakeHash) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.blobs.lock().unwrap().get(hash).cloned())
    }

    async fn put(&self, hash: &BlakeHash, data: &[u8]) -> Result<(), StoreError> {
        self.blobs.lock().unwrap().insert(*hash, data.to_vec());
        Ok(())
    }

    async fn delete(&self, hash: &BlakeHash) -> Result<(), StoreError> {
        self.blobs.lock().unwrap().remove(hash);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<(BlakeHash, u64)>, StoreError> {
        Ok(self
            .blobs
            .lock()
            .unwrap()
            .iter()
            .map(|(h, b)| (*h, b.len() as u64))
            .collect())
    }
}

/// A [`BlobStore`] under pin/release/LRU-budget retention. Reads are
/// BLAKE3-verified: a blob that no longer matches its name is deleted and
/// reported as a miss.
///
/// Every method takes `&self`. The index sits behind a `std::sync::Mutex`
/// that is only ever held for in-memory bookkeeping, never across an
/// `.await`, so concurrent gets/puts of different blobs overlap their store
/// I/O. Evicted blobs are deleted from the store after the lock is released;
/// a hash that was re-inserted in the meantime is left alone.
#[derive(Debug)]
pub struct RetainingCache<S: BlobStore> {
    store: S,
    index: Mutex<RetentionIndex>,
}

impl<S: BlobStore> RetainingCache<S> {
    /// Replay whatever `store` already holds into `index`, evicting unpinned
    /// entries over budget immediately. Declare persisted pins on `index`
    /// *before* calling this: an entry pinned only after the replay may
    /// already have been evicted by it (R1003).
    pub async fn open(store: S, mut index: RetentionIndex) -> Result<Self, StoreError> {
        let mut evicted = Vec::new();
        for (hash, size) in store.list().await? {
            evicted.extend(index.insert(hash, size));
        }
        let cache = Self {
            store,
            index: Mutex::new(index),
        };
        cache.delete_evicted(evicted).await?;
        Ok(cache)
    }

    fn index(&self) -> MutexGuard<'_, RetentionIndex> {
        self.index.lock().unwrap()
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn policy(&self) -> RetentionPolicy {
        self.index().policy()
    }

    pub fn usage(&self) -> RetentionUsage {
        self.index().usage()
    }

    pub fn contains(&self, hash: &BlakeHash) -> bool {
        self.index().contains(hash)
    }

    pub fn is_pinned(&self, hash: &BlakeHash) -> bool {
        self.index().is_pinned(hash)
    }

    /// Fetch and verify. A hash mismatch drops the blob and returns `None`.
    pub async fn get(&self, hash: &BlakeHash) -> Result<Option<Vec<u8>>, StoreError> {
        if !self.index().contains(hash) {
            return Ok(None);
        }
        match self.store.get(hash).await? {
            Some(data) if hash.verify(&data) => {
                self.index().touch(hash);
                Ok(Some(data))
            }
            _ => {
                self.index().remove(hash);
                self.delete_evicted(vec![*hash]).await?;
                Ok(None)
            }
        }
    }

    /// Store `data` under its own hash, evicting unpinned LRU entries to fit.
    pub async fn put(&self, data: &[u8]) -> Result<BlakeHash, StoreError> {
        let hash = BlakeHash::hash(data);
        let size = data.len() as u64;
        {
            let index = self.index();
            if !index.admits(&hash, size) {
                return Err(StoreError::TooLarge {
                    size,
                    budget: index.policy().budget_bytes,
                });
            }
        }
        self.store.put(&hash, data).await?;
        let evicted = self.index().insert(hash, size);
        self.delete_evicted(evicted).await?;
        Ok(hash)
    }

    /// Drop a blob regardless of pins.
    pub async fn remove(&self, hash: &BlakeHash) -> Result<(), StoreError> {
        self.index().remove(hash);
        self.delete_evicted(vec![*hash]).await
    }

    pub fn pin(&self, hash: BlakeHash) {
        self.index().pin(hash);
    }

    pub async fn unpin(&self, hash: &BlakeHash) -> Result<(), StoreError> {
        let evicted = self.index().unpin(hash);
        self.delete_evicted(evicted).await
    }

    pub fn pin_set(&self, name: impl Into<String>, hashes: impl IntoIterator<Item = BlakeHash>) {
        self.index().pin_set(name, hashes);
    }

    pub async fn unpin_set(&self, name: &str) -> Result<(), StoreError> {
        let evicted = self.index().unpin_set(name);
        self.delete_evicted(evicted).await
    }

    pub async fn record_release(
        &self,
        release: impl Into<String>,
        hashes: impl IntoIterator<Item = BlakeHash>,
    ) -> Result<(), StoreError> {
        let evicted = self.index().record_release(release, hashes);
        self.delete_evicted(evicted).await
    }

    /// Delete evicted blobs from the store, outside the index lock. A hash
    /// that a concurrent `put` re-inserted since eviction is skipped, so the
    /// index never claims a blob the store has dropped.
    async fn delete_evicted(&self, hashes: Vec<BlakeHash>) -> Result<(), StoreError> {
        for h in hashes {
            if self.index().contains(&h) {
                continue;
            }
            self.store.delete(&h).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;

    fn blob(n: u8, len: usize) -> Vec<u8> {
        vec![n; len]
    }

    async fn cache(budget: u64, keep: usize) -> RetainingCache<MemBlobStore> {
        RetainingCache::open(
            MemBlobStore::new(),
            RetentionIndex::new(RetentionPolicy::new(budget, keep)),
        )
            .await
            .unwrap()
    }

    #[test]
    fn put_get_roundtrip() {
        block_on(async {
            let c = cache(100, 1).await;
            let h = c.put(b"hello").await.unwrap();
            assert_eq!(c.get(&h).await.unwrap().unwrap(), b"hello");
            assert_eq!(c.get(&BlakeHash::hash(b"nope")).await.unwrap(), None);
        });
    }

    #[test]
    fn budget_evicts_lru_unpinned_and_deletes_from_store() {
        block_on(async {
            let c = cache(20, 1).await;
            let a = c.put(&blob(1, 10)).await.unwrap();
            let b = c.put(&blob(2, 10)).await.unwrap();
            c.get(&a).await.unwrap(); // a is now MRU
            let d = c.put(&blob(3, 10)).await.unwrap();
            assert!(c.contains(&a) && c.contains(&d) && !c.contains(&b));
            assert_eq!(c.store().len(), 2);
        });
    }

    #[test]
    fn pinned_never_evicted_and_outside_budget() {
        block_on(async {
            let c = cache(10, 1).await;
            let p = BlakeHash::hash(&blob(9, 50));
            c.pin(p);
            c.put(&blob(9, 50)).await.unwrap(); // pinned: admitted past budget
            let a = c.put(&blob(1, 10)).await.unwrap();
            let b = c.put(&blob(2, 10)).await.unwrap();
            assert!(c.contains(&p) && c.is_pinned(&p));
            assert!(!c.contains(&a) && c.contains(&b));
            assert_eq!(c.usage().pinned_bytes, 50);
            // unpinning makes it subject to the budget again
            c.unpin(&p).await.unwrap();
            assert!(!c.contains(&p));
            assert_eq!(c.store().len(), 1);
        });
    }

    #[test]
    fn unpinned_over_budget_refused() {
        block_on(async {
            let c = cache(10, 1).await;
            assert!(matches!(
                c.put(&blob(1, 11)).await,
                Err(StoreError::TooLarge {
                    size: 11,
                    budget: 10
                })
            ));
            assert!(c.store().is_empty());
        });
    }

    #[test]
    fn release_ring_keeps_current_plus_previous() {
        block_on(async {
            let c = cache(0, 1).await;
            let hs: Vec<BlakeHash> = (1..=3).map(|n| BlakeHash::hash(&blob(n, 4))).collect();
            for (i, h) in hs.iter().enumerate() {
                c.record_release(format!("r{i}"), [*h]).await.unwrap();
                c.put(&blob(i as u8 + 1, 4)).await.unwrap();
            }
            // r0 rotated out when r2 was recorded; r1 (previous) and r2 stay.
            c.record_release("r2", [hs[2]]).await.unwrap();
            c.record_release("r3", []).await.unwrap(); // now r2 prev, r1 out
            assert!(!c.contains(&hs[0]) && !c.contains(&hs[1]));
            assert!(c.contains(&hs[2]));
        });
    }

    #[test]
    fn open_rebuilds_index_and_enforces_budget() {
        block_on(async {
            let store = MemBlobStore::new();
            for n in 1..=3u8 {
                let b = blob(n, 10);
                store.put(&BlakeHash::hash(&b), &b).await.unwrap();
            }
            let c = RetainingCache::open(store, RetentionIndex::new(RetentionPolicy::new(20, 1)))
                .await
                .unwrap();
            assert_eq!(c.usage().entries, 2);
            assert_eq!(c.store().len(), 2);
        });
    }

    #[test]
    fn pins_declared_before_open_survive_an_over_budget_replay() {
        // R1003: a reopened cache holding more pinned bytes than its budget
        // must not evict them during the rebuild — which it did when pins
        // could only be declared after `open` returned.
        block_on(async {
            let store = MemBlobStore::new();
            let mut hs = Vec::new();
            for n in 1..=3u8 {
                let b = blob(n, 10);
                hs.push(BlakeHash::hash(&b));
                store.put(&hs[hs.len() - 1], &b).await.unwrap();
            }
            let mut index = RetentionIndex::new(RetentionPolicy::new(5, 1));
            index.pin_set("tree", hs.iter().copied());
            let c = RetainingCache::open(store, index).await.unwrap();
            assert_eq!(c.usage().entries, 3);
            assert_eq!(c.usage().pinned_bytes, 30);
            assert_eq!(c.store().len(), 3);
        });
    }

    #[test]
    fn corrupt_blob_is_dropped_on_read() {
        block_on(async {
            let c = cache(100, 1).await;
            let h = c.put(b"good").await.unwrap();
            c.store().put(&h, b"evil").await.unwrap();
            assert_eq!(c.get(&h).await.unwrap(), None);
            assert!(!c.contains(&h) && c.store().is_empty());
        });
    }

    /// Gets park on a shared `Barrier(2)`; they only finish if two are in
    /// flight in the store at once, i.e. the cache lock is not held across I/O.
    struct BarrierStore {
        inner: MemBlobStore,
        barrier: std::sync::Arc<tokio::sync::Barrier>,
    }

    impl BlobStore for BarrierStore {
        async fn get(&self, hash: &BlakeHash) -> Result<Option<Vec<u8>>, StoreError> {
            self.barrier.wait().await;
            self.inner.get(hash).await
        }
        async fn put(&self, hash: &BlakeHash, data: &[u8]) -> Result<(), StoreError> {
            self.inner.put(hash, data).await
        }
        async fn delete(&self, hash: &BlakeHash) -> Result<(), StoreError> {
            self.inner.delete(hash).await
        }
        async fn list(&self) -> Result<Vec<(BlakeHash, u64)>, StoreError> {
            self.inner.list().await
        }
    }

    #[tokio::test]
    async fn concurrent_gets_of_different_blobs_overlap_store_io() {
        let store = BarrierStore {
            inner: MemBlobStore::new(),
            barrier: std::sync::Arc::new(tokio::sync::Barrier::new(2)),
        };
        let c = RetainingCache::open(store, RetentionIndex::new(RetentionPolicy::new(100, 1)))
            .await
            .unwrap();
        let a = c.put(b"aaaa").await.unwrap();
        let b = c.put(b"bbbb").await.unwrap();
        let both = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            futures::future::join(c.get(&a), c.get(&b)),
        )
        .await
        .expect("gets serialized: second get never reached the store");
        assert_eq!(both.0.unwrap().unwrap(), b"aaaa");
        assert_eq!(both.1.unwrap().unwrap(), b"bbbb");
    }

    #[test]
    fn evicted_hash_reinserted_before_delete_is_kept() {
        block_on(async {
            let c = cache(20, 1).await;
            let a = c.put(&blob(1, 10)).await.unwrap();
            let _b = c.put(&blob(2, 10)).await.unwrap();
            // `a` is evicted by the index, then re-put before the delete runs.
            let evicted = c.index().remove(&a);
            assert!(evicted);
            c.put(&blob(1, 10)).await.unwrap();
            c.delete_evicted(vec![a]).await.unwrap();
            assert!(c.contains(&a));
            assert_eq!(c.get(&a).await.unwrap().unwrap(), blob(1, 10));
        });
    }
}
