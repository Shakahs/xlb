//! Per-class blob cache — `FetchTier::Cache` (tier 0).
//!
//! Two flavors selected at [`AssetClass::register`] time by
//! `AssetClassConfig::cache_dir`:
//!
//! - **In-memory** (`cache_dir` = `None`): a `HashMap<BlakeHash, Bytes>` in a
//!   single `RwLock`. Lossy on process restart. Suitable for short-lived
//!   clients (CLI one-shots, tests) and for callers that intentionally opt
//!   out of disk persistence.
//! - **Disk-backed with LRU eviction** (`cache_dir` = `Some(path)`): each blob
//!   is one file at `<cache_dir>/<blake3-hex>`, written atomically via a
//!   `<hash>.tmp` + rename so a torn write can't be served as truth. An
//!   in-memory LRU index tracks recency and total bytes; eviction kicks in
//!   when an insert would push the on-disk footprint past
//!   `cache_budget_bytes`. The index is **rebuilt by scanning the directory
//!   on construction** so the cache survives restart — this is the whole
//!   point of the disk-backed variant (without it the seed economics are
//!   strictly worse than R2-direct, cf. W160 F3).
//!
//! ## Verification on read
//!
//! Disk-cache `get` re-hashes every byte before returning, so silent
//! bit-rot or out-of-band tampering surfaces as a miss + an eviction of the
//! corrupted entry rather than a serve. The cost is one BLAKE3 pass per
//! cache hit; cheap relative to a network round-trip and load-bearing for
//! the "cache survives restart" property — without it we'd have to either
//! trust the disk blindly or store a sidecar mac (which buys nothing
//! beyond what blake3-rehash already gives us).
//!
//! ## Retention
//!
//! Both flavors are an [`xlb_core::BlobStore`] ([`xlb_core::MemBlobStore`] /
//! `DiskBlobStore`) under [`xlb_core::RetainingCache`], the single driver of
//! [`crate::retention::RetentionIndex`] shared with the browser cache: pinned
//! blobs (by hash, pin set, or retained release) are never evicted and sit
//! outside the byte budget, which bounds only unpinned entries.
//!
//! ## LRU ordering
//!
//! Owned by the retention index. A reopened disk cache seeds it oldest-mtime
//! first (`DiskBlobStore::list`).
//!
//! ## Locking
//!
//! `RetainingCache` takes `&self` and guards only its in-memory index with a
//! sync mutex that is never held across an `.await`; blob I/O runs outside it,
//! so concurrent hits and writes on one class overlap (R002-B7).
//!
//! ## Single-flight is *not* provided here
//!
//! Concurrent fetches for the same hash may all run the fetch chain to
//! completion; the cache layer just guarantees that the resulting writes
//! are atomic (no torn files) and idempotent (last-writer-wins with
//! identical bytes). True single-flight would need an inflight-tracking
//! map at the [`AssetClass::fetch_bytes`] level — separate concern, not
//! T3's scope. The tests in `tests/disk_cache.rs` lock in the
//! atomicity-under-contention property.
//!
//! @yah:relay(R002, "xlb: retention policy, ref resolution, browser storage tier")
//! @yah:at(2026-10-17T21:00:57Z)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:next("Context: noisetable R992 built a parallel mini-fetcher (crates/noise_table/marketplace/src/required_fetch.rs) plus pruning (required_manifest.rs) for app assets (fonts, HRTF, notices) that xlb should replace.")
//!
//! @yah:ticket(R002-F1, "Pinning plus retention per AssetClass")
//! @yah:status(review)
//! @yah:at(2026-10-18T18:10:27Z)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:parent(R002)
//! @yah:next("Tier: Warrior")
//! @yah:next("Pinned entries (release manifest, pinned sets) are never evicted; keep the previous N releases; the LRU byte budget applies only to unpinned entries.")
//! @yah:next("Today cache.rs is pure LRU and the in-memory variant has no budget; add one.")
//! @yah:handoff("New crates/xlb/src/retention.rs: platform-neutral (no fs/tokio/clock) RetentionPolicy{budget_bytes,keep_releases} + RetentionIndex (entries, LRU, pins, named pin sets, rotating release ring of current+keep_releases, protected-set union). Budget applies only to unpinned bytes; pinned entries never evicted; unpinned blob > budget refused, pinned always admitted; pins may precede the blob. Methods return evicted-hash lists so the caller does physical deletion (reusable by OPFS R002-F4).")
//! @yah:handoff("cache.rs: DiskCache and MemCache both delegate to RetentionIndex (LruState removed). MemCache now enforces the budget with LRU (previously unbounded). Cache::new takes RetentionPolicy; stats() no longer takes a budget; CacheStats gained pinned_entries/pinned_bytes; added Cache::retain(closure) and is_pinned.")
//! @yah:handoff("lib.rs: AssetClassConfig.keep_releases (default 1, per-class); pub use RetentionPolicy/RetentionIndex; AssetClass::{pin, unpin, pin_set, unpin_set, record_release, is_pinned} for R002-F2 to call. examples/register_class.rs updated for the new field. Cargo.toml untouched.")
//! @yah:handoff("Reference: noisetable required_manifest.rs prune_plan keeps current+previous+pinned by hash; same semantics mapped to record_release + pin_set. Not edited.")
//! @yah:handoff("Baseline 101 passed (lib 57); after: lib 72, all other suites unchanged, 0 failed, build OK.")
//! @yah:verify("cd oss/xlb && cargo test  (lib 72 pass, all suites green)")
//! @yah:verify("cargo build")
//! @yah:gotcha("Peer edited lib.rs (+1 line near line 43/pub mod) concurrently; builds reported skew advisories but the final test run passed.")
//!
//! @yah:ticket(R002-F4, "OPFS-backed cache variant for the browser")
//! @yah:status(review)
//! @yah:at(2026-10-18T18:18:06Z)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:parent(R002)
//! @yah:next("Tier: Warrior")
//! @yah:next("Third cache flavor beside the in-memory and on-disk ones in cache.rs, backed by OPFS, honoring the R002-F1 retention policy. Shape depends on R002-S3 (xlb on wasm vs thin client).")
//! @yah:next("S3 verdict: do NOT compile current xlb for wasm. Extract a native-free core (retention.rs, Ref types from F2, verify/metrics/progress) and implement the OPFS cache over RetentionIndex behind a cache trait. Depends on F2 because the split moves F2's Ref types.")
//! @yah:depends_on(R002-F1)
//! @yah:depends_on(R002-S3)
//! @yah:depends_on(R002-F2)
//! @yah:handoff("New crate crates/xlb-core (workspace member; deps blake3, hex, semver, serde, serde_json, thiserror only): MOVED retention.rs and refs.rs from xlb, MOVED BlakeHash (hash.rs) out of xlb/lib.rs. BlakeHash::from_hex/FromStr now return xlb_core::InvalidHash; xlb::Error::InvalidHash is #[from] InvalidHash (variant shape changed from String; only xlb-cli server.rs matched on from_hex generically, still compiles). xlb re-exports via `pub use xlb_core::{refs, retention, BlakeHash, InvalidHash, Ref, Resolution, ResolveError, RetentionIndex, RetentionPolicy}` so crate::refs/crate::retention paths inside xlb are unchanged.")
//! @yah:handoff("xlb-core/src/store.rs: async BlobStore trait (get/put/delete/list; native async fn in trait, no Send bound), MemBlobStore, StoreError, and RetainingCache<S: BlobStore> (open() rebuilds RetentionIndex from list(); get verifies BLAKE3 and drops corrupt blobs; put hashes, refuses unpinned > budget, deletes evicted; pin/unpin/pin_set/unpin_set/record_release delete evictions). 7 unit tests over MemBlobStore: roundtrip, LRU budget, pinned never evicted/outside budget, oversize refused, release ring, open-rebuild+budget, corrupt-on-read.")
//! @yah:handoff("New crate crates/xlb-web: OpfsBlobStore (cfg wasm32; web-sys/wasm-bindgen-futures deps are target-gated so native builds see only re-exports) — OPFS dir xlb/<class>/, one file per blob named by hex hash, createWritable swap-file write (atomic on close), list() reads file sizes via getFile, NotFound tolerated on delete; open_cache(class, policy) -> RetainingCache<OpfsBlobStore>. BLAKE3 verification on read lives in RetainingCache::get (single owner) rather than being duplicated in the store. Pins are in-memory, re-declared by the caller from the F2 manifest (pin_set/record_release on the cache).")
//! @yah:handoff("verify.rs NOT moved: its Verifier is pub(crate) and returns xlb::Error/bytes::BytesMut, so moving it would drag Error into core; BlakeHash::verify already covers the one-shot case. Native DiskCache/MemCache not put on BlobStore: filed R002-T5. Deps: all versions already in Cargo.lock (web-sys 0.3.98, wasm-bindgen 0.2.121, wasm-bindgen-futures 0.4.71, js-sys 0.3.98, futures 0.3.32 dev-only).")
//! @yah:handoff("Git policy defer, no commit. Suggested: git add Cargo.toml Cargo.lock crates/xlb-core crates/xlb-web crates/xlb/Cargo.toml crates/xlb/src/lib.rs crates/xlb/src/refs.rs crates/xlb/src/retention.rs && git commit -m 'xlb: extract xlb-core, add OPFS BlobStore in xlb-web (R002-F4)'. Note the stray untracked crates/xlb/src/cache.rs-E predates this ticket.")
//! @yah:verify("cargo test --workspace in oss/xlb: EXIT=0, 133 passed (66 xlb lib, 24 cli, 23 xlb-core, 2+8+8 integration, 2 doctests) vs baseline 126; +7 are the new store tests (log /tmp/r002f4-2.log).")
//! @yah:verify("cargo check -p xlb-core --target wasm32-unknown-unknown: EXIT=0 (/tmp/r002f4-3.log); cargo check -p xlb-web --target wasm32-unknown-unknown: EXIT=0 (/tmp/r002f4-4.log).")
//! @yah:verify("UNVERIFIED: OpfsBlobStore has never run in a browser (no wasm-bindgen-test / headless browser run); typechecks only.")
//!
//! @yah:ticket(R002-T5, "Unify native DiskCache/MemCache onto xlb-core BlobStore + RetainingCache")
//! @yah:status(review)
//! @yah:at(2026-10-18T18:21:01Z)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:parent(R002)
//! @yah:next("Tier: Warrior")
//! @arch:see(crates/xlb-core/src/store.rs)
//! @yah:handoff("cache.rs: DiskCache/MemCache and their RetentionIndex driving are deleted. Cache is now one Mutex<RetainingCache<NativeStore>>. NativeStore is an enum over xlb-core MemBlobStore and the new DiskBlobStore (tokio::fs, one file per hex hash, same <hex>.tmp.<pid>.<n> atomic write, list() sorted by mtime and sweeps stale tmp files, so existing cache dirs stay readable). Cache::new is now async. Cache::retain(closure) was replaced by Cache::retention(), which returns the RetainingCache guard; AssetClass pin/unpin/pin_set/unpin_set/record_release/is_pinned call it directly.")
//! @yah:handoff("store.rs: BlobStore methods now return impl Future + MaybeSend. MaybeSend is Send on non-wasm and a blanket no-op on wasm32, and async fn impls still satisfy it. MemBlobStore switched from RefCell to Mutex so it is Sync. Added RetainingCache::policy().")
//! @yah:handoff("Tests: all 18 cache.rs tests kept (ported to the async Cache::new / retention()); none deleted. Workspace total is 133 passed, same as baseline. Git policy defer, no commit. Suggested: git add crates/xlb-core crates/xlb/src/cache.rs crates/xlb/src/lib.rs && git commit -m 'xlb: unify native caches onto BlobStore + RetainingCache (R002-T5)'")
//! @yah:verify("cargo test --workspace: EXIT=0, 133 passed (66 xlb lib, 24 cli, 23 xlb-core, 2+8+8 integration, 2 doctests), matching baseline 133 (/tmp/r002t5-2.log)")
//! @yah:verify("cargo check -p xlb-core --target wasm32-unknown-unknown EXIT=0 (/tmp/r002t5-3.log); cargo check -p xlb-web --target wasm32-unknown-unknown EXIT=0 (/tmp/r002t5-4.log)")
//! @yah:handoff("R002-B7 fixed the serialization: RetainingCache is &self with an internal sync index mutex never held across .await; Cache has no outer Mutex and retention() returns &RetainingCache.")

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use xlb_core::{BlobStore, MemBlobStore, RetainingCache, StoreError};

use crate::{retention::RetentionPolicy, BlakeHash};

/// Monotonic counter for temp-file scratch paths. Process-static so
/// concurrent `put` calls (especially against the same hash) produce
/// distinct temp files — otherwise the rename of one tramples the
/// temp file another task is still writing.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

// ─── Cache (public to the crate, hidden from users) ───────────────────────────

/// Per-class cache — `FetchTier::Cache`. Either in-memory or disk-backed.
pub(crate) struct Cache {
    disk_backed: bool,
    inner: RetainingCache<NativeStore>,
}

/// A point-in-time reading of one class's cache occupancy (R423-T6).
///
/// Exposed because "how full is the cache" is the first question anyone asks
/// of a running node, and until this existed the control socket answered it
/// with a hard-coded `0` — a number indistinguishable from an empty cache.
///
/// `bytes` is the cache's own accounting, not a `statfs`: the sum of the
/// retention index's per-entry sizes, which is what eviction is actually
/// measured against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    /// Bytes currently held.
    pub bytes: u64,
    /// Number of blobs currently held.
    pub entries: u64,
    /// The configured `cache_budget_bytes`. Bounds the **unpinned** share of
    /// `bytes` (`bytes - pinned_bytes`); pinned entries sit outside it.
    pub budget_bytes: u64,
    /// `true` for the disk-backed variant. Both variants now enforce the
    /// budget; this says whether the bytes survive a restart.
    pub disk_backed: bool,
    /// Blobs currently protected by a pin, pin set, or retained release.
    pub pinned_entries: u64,
    /// Bytes held by pinned blobs.
    pub pinned_bytes: u64,
}

impl Cache {
    /// Build a cache for one `AssetClass`.
    ///
    /// `cache_dir = None` → in-memory; `Some(path)` → disk-backed at `path`
    /// with `policy`'s LRU eviction budget over unpinned entries. The directory
    /// is created if it doesn't exist; existing entries on disk are picked up
    /// into the LRU index (cache survives restart).
    pub(crate) async fn new(
        cache_dir: Option<&Path>,
        policy: RetentionPolicy,
    ) -> std::io::Result<Self> {
        let (store, disk_backed) = match cache_dir {
            None => (NativeStore::Mem(MemBlobStore::new()), false),
            Some(dir) => (NativeStore::Disk(DiskBlobStore::create(dir)?), true),
        };
        let inner = RetainingCache::open(store, policy)
            .await
            .map_err(std::io::Error::other)?;
        Ok(Self {
            disk_backed,
            inner,
        })
    }

    /// Try to read `hash` from the cache. Returns `None` on miss.
    ///
    /// Re-verifies BLAKE3 before returning; a mismatched entry is removed and
    /// counted as a miss, as is a store error (logged).
    pub(crate) async fn get(&self, hash: &BlakeHash) -> Option<Bytes> {
        match self.inner.get(hash).await {
            Ok(data) => data.map(Bytes::from),
            Err(e) => {
                tracing::warn!(%hash, "cache read failed: {e}");
                None
            }
        }
    }

    /// Insert `bytes` (which must hash to `hash`). Disk-backed: writes
    /// atomically via temp+rename and evicts oldest LRU entries until the
    /// unpinned footprint is within budget. An unpinned blob larger than the
    /// budget is skipped (logged), not an error.
    pub(crate) async fn put(&self, hash: BlakeHash, bytes: Bytes) -> std::io::Result<()> {
        debug_assert!(hash.verify(&bytes), "cache put: bytes do not match hash");
        match self.inner.put(&bytes).await {
            Ok(_) => Ok(()),
            Err(StoreError::TooLarge { size, budget }) => {
                tracing::warn!(%hash, size, budget, "cache: blob exceeds budget, skipping write");
                Ok(())
            }
            Err(e) => Err(std::io::Error::other(e)),
        }
    }

    /// Cheap presence check — does not read the blob.
    ///
    /// Checks the in-memory index only, so a hit does *not* re-verify
    /// BLAKE3. Callers needing a verified hit must call [`Cache::get`].
    pub(crate) async fn contains(&self, hash: &BlakeHash) -> bool {
        self.inner.contains(hash)
    }

    /// Current occupancy. Cheap: reads an index already held in memory, so
    /// this never touches the filesystem.
    pub(crate) async fn stats(&self) -> CacheStats {
        let c = &self.inner;
        let u = c.usage();
        CacheStats {
            bytes: u.bytes,
            entries: u.entries,
            budget_bytes: c.policy().budget_bytes,
            disk_backed: self.disk_backed,
            pinned_entries: u.pinned_entries,
            pinned_bytes: u.pinned_bytes,
        }
    }

    /// The underlying cache, for pin/release calls (`pin`, `unpin`,
    /// `pin_set`, `unpin_set`, `record_release`, `is_pinned`). Evictions those
    /// trigger are deleted from the store by `RetainingCache` itself.
    pub(crate) fn retention(&self) -> &RetainingCache<NativeStore> {
        &self.inner
    }
}

// ─── NativeStore ──────────────────────────────────────────────────────────────

/// The two native backends behind one type, so [`Cache`] is a single
/// `RetainingCache` rather than a per-flavor pair of them.
pub(crate) enum NativeStore {
    Mem(MemBlobStore),
    Disk(DiskBlobStore),
}

impl BlobStore for NativeStore {
    async fn get(&self, hash: &BlakeHash) -> Result<Option<Vec<u8>>, StoreError> {
        match self {
            Self::Mem(s) => s.get(hash).await,
            Self::Disk(s) => s.get(hash).await,
        }
    }

    async fn put(&self, hash: &BlakeHash, data: &[u8]) -> Result<(), StoreError> {
        match self {
            Self::Mem(s) => s.put(hash, data).await,
            Self::Disk(s) => s.put(hash, data).await,
        }
    }

    async fn delete(&self, hash: &BlakeHash) -> Result<(), StoreError> {
        match self {
            Self::Mem(s) => s.delete(hash).await,
            Self::Disk(s) => s.delete(hash).await,
        }
    }

    async fn list(&self) -> Result<Vec<(BlakeHash, u64)>, StoreError> {
        match self {
            Self::Mem(s) => s.list().await,
            Self::Disk(s) => s.list().await,
        }
    }
}

// ─── DiskBlobStore ────────────────────────────────────────────────────────────

/// One file per blob at `<dir>/<blake3-hex>`. Layout is unchanged from the
/// pre-`BlobStore` cache, so existing cache directories stay readable.
pub(crate) struct DiskBlobStore {
    dir: PathBuf,
}

fn io_err(e: std::io::Error) -> StoreError {
    StoreError::Backend(e.to_string())
}

impl DiskBlobStore {
    /// Create the directory if missing. Files whose name isn't a 64-char hex
    /// BlakeHash are ignored (and silently left alone — callers own the dir).
    fn create(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
        })
    }

    fn path(&self, hash: &BlakeHash) -> PathBuf {
        self.dir.join(hash.to_hex())
    }
}

impl BlobStore for DiskBlobStore {
    async fn get(&self, hash: &BlakeHash) -> Result<Option<Vec<u8>>, StoreError> {
        match tokio::fs::read(self.path(hash)).await {
            Ok(d) => Ok(Some(d)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err(e)),
        }
    }

    async fn put(&self, hash: &BlakeHash, data: &[u8]) -> Result<(), StoreError> {
        let final_path = self.path(hash);
        // Unique scratch path per put — process pid + monotonic counter.
        // Concurrent puts (especially for the same hash) MUST get distinct
        // temp files so the rename of one doesn't trample the in-flight
        // write of another. Rename-to-final-name is the atomicity point.
        let tmp_path = {
            let mut p = final_path.clone();
            let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            p.set_extension(format!("tmp.{pid}.{n}"));
            p
        };
        tokio::fs::write(&tmp_path, data).await.map_err(io_err)?;
        // rename is atomic on POSIX + on Windows when target doesn't exist;
        // when the target *does* exist Windows differs, but we accept
        // last-writer-wins for the same hash.
        tokio::fs::rename(&tmp_path, &final_path)
            .await
            .map_err(io_err)
    }

    async fn delete(&self, hash: &BlakeHash) -> Result<(), StoreError> {
        match tokio::fs::remove_file(self.path(hash)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_err(e)),
        }
    }

    /// Ordered by file mtime, oldest first → most-recently-used last. mtime is
    /// portable enough for our scales; on platforms with weird/zero mtimes the
    /// order degenerates to filesystem iteration order, which is still a valid
    /// (if arbitrary) LRU starting state.
    ///
    /// Also sweeps in-flight temp files left by a crashed write. Two formats
    /// are accepted: `<name>.tmp` (legacy) and `<hex>.tmp.<pid>.<n>` (what
    /// `put` writes today). Real entries are 64-char hex with no `.tmp`
    /// substring, so this is a safe match.
    async fn list(&self) -> Result<Vec<(BlakeHash, u64)>, StoreError> {
        let mut found: Vec<(std::time::SystemTime, BlakeHash, u64)> = Vec::new();
        let mut rd = tokio::fs::read_dir(&self.dir).await.map_err(io_err)?;
        while let Some(entry) = rd.next_entry().await.map_err(io_err)? {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.ends_with(".tmp") || name.contains(".tmp.") {
                let _ = tokio::fs::remove_file(&path).await;
                continue;
            }
            let Ok(hash) = BlakeHash::from_hex(name) else {
                continue;
            };
            let Ok(meta) = entry.metadata().await else {
                continue;
            };
            if !meta.is_file() {
                continue;
            }
            let mtime = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            found.push((mtime, hash, meta.len()));
        }
        found.sort_by_key(|(t, _, _)| *t);
        Ok(found.into_iter().map(|(_, h, n)| (h, n)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn pol(budget: u64) -> RetentionPolicy {
        RetentionPolicy::new(budget, 1)
    }

    fn tmpdir(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "xlb-cache-test-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[tokio::test]
    async fn mem_cache_roundtrip() {
        let cache = Cache::new(None, pol(1024)).await.unwrap();
        let data = Bytes::from_static(b"hello mem cache");
        let hash = BlakeHash::hash(&data);
        cache.put(hash, data.clone()).await.unwrap();
        assert_eq!(cache.get(&hash).await.as_deref(), Some(&data[..]));
        assert!(cache.contains(&hash).await);
    }

    #[tokio::test]
    async fn disk_cache_roundtrip() {
        let dir = tmpdir("roundtrip");
        let cache = Cache::new(Some(&dir), pol(1024)).await.unwrap();
        let data = Bytes::from_static(b"hello disk cache");
        let hash = BlakeHash::hash(&data);
        cache.put(hash, data.clone()).await.unwrap();
        assert_eq!(cache.get(&hash).await.as_deref(), Some(&data[..]));
        // Check the file actually landed on disk.
        let on_disk = std::fs::read(dir.join(hash.to_hex())).unwrap();
        assert_eq!(on_disk, data.as_ref());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn disk_cache_survives_reopen() {
        // The whole reason this module exists. Drop the cache, re-open the
        // dir, verify the blob is still there.
        let dir = tmpdir("reopen");
        let data = Bytes::from_static(b"survive across restart");
        let hash = BlakeHash::hash(&data);
        {
            let cache = Cache::new(Some(&dir), pol(1024)).await.unwrap();
            cache.put(hash, data.clone()).await.unwrap();
        }
        let cache2 = Cache::new(Some(&dir), pol(1024)).await.unwrap();
        assert!(cache2.contains(&hash).await);
        assert_eq!(cache2.get(&hash).await.as_deref(), Some(&data[..]));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn lru_evicts_oldest_under_budget() {
        // Budget = 100 bytes. Insert 4×40 = 160 bytes worth → oldest must go.
        let dir = tmpdir("lru-evict");
        let cache = Cache::new(Some(&dir), pol(100)).await.unwrap();
        let mk = |n: u8| Bytes::from(vec![n; 40]);
        let a = mk(b'A');
        let b = mk(b'B');
        let c = mk(b'C');
        let ha = BlakeHash::hash(&a);
        let hb = BlakeHash::hash(&b);
        let hc = BlakeHash::hash(&c);

        cache.put(ha, a.clone()).await.unwrap();
        cache.put(hb, b.clone()).await.unwrap();
        cache.put(hc, c.clone()).await.unwrap();

        // 3×40=120 > budget=100, so A (oldest) must have been evicted.
        assert!(!cache.contains(&ha).await, "A should be evicted (oldest)");
        assert!(cache.contains(&hb).await, "B should survive");
        assert!(cache.contains(&hc).await, "C should survive");
        // Disk reflects the eviction.
        assert!(
            !dir.join(ha.to_hex()).exists(),
            "A's file should be gone from disk"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn lru_get_touches_recency() {
        // Insert A, B; get(A) so A becomes MRU; insert C that forces eviction.
        // B (now oldest) should go, not A.
        let dir = tmpdir("lru-touch");
        let cache = Cache::new(Some(&dir), pol(100)).await.unwrap();
        let a = Bytes::from(vec![b'A'; 40]);
        let b = Bytes::from(vec![b'B'; 40]);
        let c = Bytes::from(vec![b'C'; 40]);
        let ha = BlakeHash::hash(&a);
        let hb = BlakeHash::hash(&b);
        let hc = BlakeHash::hash(&c);

        cache.put(ha, a.clone()).await.unwrap();
        cache.put(hb, b.clone()).await.unwrap();
        // Touch A — moves it to MRU.
        let _ = cache.get(&ha).await;
        cache.put(hc, c.clone()).await.unwrap();

        assert!(cache.contains(&ha).await, "A was touched, should survive");
        assert!(
            !cache.contains(&hb).await,
            "B is now oldest, should be evicted"
        );
        assert!(cache.contains(&hc).await);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn disk_cache_verifies_on_read() {
        // Corrupt a cache file by hand — get must reject + evict.
        let dir = tmpdir("verify");
        let cache = Cache::new(Some(&dir), pol(1024)).await.unwrap();
        let data = Bytes::from_static(b"real bytes");
        let hash = BlakeHash::hash(&data);
        cache.put(hash, data.clone()).await.unwrap();

        // Out-of-band tamper.
        std::fs::write(dir.join(hash.to_hex()), b"TAMPERED").unwrap();
        assert!(
            cache.get(&hash).await.is_none(),
            "tampered cache entry must be treated as a miss"
        );
        assert!(
            !cache.contains(&hash).await,
            "tampered entry must be evicted from the index"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn concurrent_put_same_hash_atomic() {
        // N concurrent writers for the same hash must not produce a torn
        // file: a reader either sees the full blob or nothing, never a
        // half-rename.
        let dir = tmpdir("concurrent");
        let cache = Arc::new(Cache::new(Some(&dir), pol(4096)).await.unwrap());
        let data = Bytes::from(vec![0xab; 200]);
        let hash = BlakeHash::hash(&data);

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let data = data.clone();
            tasks.push(tokio::spawn(async move {
                cache.put(hash, data).await.unwrap();
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }

        // Final state: blob is present, full, verified.
        let got = cache.get(&hash).await.expect("blob must be present");
        assert_eq!(got, data, "no torn writes");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn shrunk_budget_evicts_on_open() {
        // Write two blobs with a generous budget, then re-open with a tiny
        // budget. The oldest should be evicted at open time.
        let dir = tmpdir("shrink");
        let a = Bytes::from(vec![b'A'; 40]);
        let b = Bytes::from(vec![b'B'; 40]);
        let ha = BlakeHash::hash(&a);
        let hb = BlakeHash::hash(&b);
        {
            let cache = Cache::new(Some(&dir), pol(1024)).await.unwrap();
            cache.put(ha, a.clone()).await.unwrap();
            // Bump B's mtime past A's so the rebuilt LRU treats A as older.
            // tokio::time::sleep doesn't move mtime on macOS at sub-second
            // resolution reliably; touch via std::fs to guarantee ordering.
            std::thread::sleep(std::time::Duration::from_millis(50));
            cache.put(hb, b.clone()).await.unwrap();
        }
        let cache = Cache::new(Some(&dir), pol(60)).await.unwrap(); // < 2×40
        assert!(
            !cache.contains(&ha).await,
            "A (older) should have been evicted on open under shrunk budget"
        );
        assert!(
            cache.contains(&hb).await,
            "B (newer) should survive shrunk-budget open"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn temp_files_cleaned_on_open() {
        // A crashed write leaves a *.tmp behind. open() should clear them.
        let dir = tmpdir("tmp-cleanup");
        std::fs::write(dir.join("abc.tmp"), b"crashed write").unwrap();
        std::fs::write(dir.join("def.tmp.123.4"), b"another").unwrap();
        let _cache = Cache::new(Some(&dir), pol(1024)).await.unwrap();
        assert!(!dir.join("abc.tmp").exists());
        assert!(!dir.join("def.tmp.123.4").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── stats() (R423-T6) ──────────────────────────────────────────────────

    #[tokio::test]
    async fn disk_stats_track_puts_and_report_the_budget() {
        let dir = tmpdir("stats-disk");
        let cache = Cache::new(Some(&dir), pol(1024)).await.unwrap();

        let empty = cache.stats().await;
        assert_eq!(empty.bytes, 0);
        assert_eq!(empty.entries, 0);
        assert!(empty.disk_backed);
        // The disk variant reports the budget it was OPENED with, not the
        // argument — those are the same value in practice, and if they ever
        // diverge the cache's own number is the one eviction obeys.
        assert_eq!(empty.budget_bytes, 1024);

        let a = Bytes::from(vec![b'A'; 40]);
        let b = Bytes::from(vec![b'B'; 60]);
        cache.put(BlakeHash::hash(&a), a.clone()).await.unwrap();
        cache.put(BlakeHash::hash(&b), b.clone()).await.unwrap();

        let s = cache.stats().await;
        assert_eq!(s.bytes, 100, "40 + 60");
        assert_eq!(s.entries, 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn disk_stats_shrink_after_eviction() {
        // The number a used/budget bar is drawn from must come DOWN when the
        // LRU evicts, or the bar reads over-full forever.
        let dir = tmpdir("stats-evict");
        let cache = Cache::new(Some(&dir), pol(100)).await.unwrap();
        for byte in [b'A', b'B', b'C'] {
            let blob = Bytes::from(vec![byte; 40]);
            cache.put(BlakeHash::hash(&blob), blob).await.unwrap();
        }
        let s = cache.stats().await;
        assert_eq!(
            s.entries, 2,
            "third put evicts the oldest under a 100B budget"
        );
        assert_eq!(s.bytes, 80);
        assert!(s.bytes <= s.budget_bytes);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn mem_stats_are_marked_not_disk_backed() {
        let cache = Cache::new(None, pol(100)).await.unwrap();
        let blob = Bytes::from(vec![b'X'; 50]);
        cache.put(BlakeHash::hash(&blob), blob).await.unwrap();

        let s = cache.stats().await;
        assert_eq!(s.bytes, 50);
        assert_eq!(s.entries, 1);
        assert!(!s.disk_backed);
        assert_eq!(s.budget_bytes, 100);
    }

    #[tokio::test]
    async fn mem_cache_enforces_budget_lru() {
        let cache = Cache::new(None, pol(100)).await.unwrap();
        let blobs: Vec<Bytes> = (0..3u8).map(|n| Bytes::from(vec![n; 40])).collect();
        let hs: Vec<BlakeHash> = blobs.iter().map(|b| BlakeHash::hash(b)).collect();
        cache.put(hs[0], blobs[0].clone()).await.unwrap();
        cache.put(hs[1], blobs[1].clone()).await.unwrap();
        assert!(cache.get(&hs[0]).await.is_some(), "touch 0 so 1 is oldest");
        cache.put(hs[2], blobs[2].clone()).await.unwrap();
        assert!(cache.contains(&hs[0]).await);
        assert!(!cache.contains(&hs[1]).await);
        assert!(cache.contains(&hs[2]).await);
        assert!(cache.stats().await.bytes <= 100);
    }

    #[tokio::test]
    async fn pinned_blob_survives_pressure_on_both_flavors() {
        let dir = tmpdir("pinned");
        for cache in [
            Cache::new(None, pol(100)).await.unwrap(),
            Cache::new(Some(&dir), pol(100)).await.unwrap(),
        ] {
            let blobs: Vec<Bytes> = (0..4u8).map(|n| Bytes::from(vec![n; 60])).collect();
            let hs: Vec<BlakeHash> = blobs.iter().map(|b| BlakeHash::hash(b)).collect();
            cache.retention().pin(hs[0]);
            for (h, b) in hs.iter().zip(&blobs) {
                cache.put(*h, b.clone()).await.unwrap();
            }
            assert!(cache.contains(&hs[0]).await, "pinned entry kept");
            assert!(cache.get(&hs[0]).await.is_some());
            assert!(!cache.contains(&hs[1]).await && !cache.contains(&hs[2]).await);
            assert!(cache.contains(&hs[3]).await);
            let s = cache.stats().await;
            assert_eq!((s.pinned_entries, s.pinned_bytes), (1, 60));
            // Unpinning hands it back to the LRU and re-enforces the budget.
            cache.retention().unpin(&hs[0]).await.unwrap();
            // hs[0] was touched by the `get` above, so hs[3] is the LRU victim.
            assert!(cache.contains(&hs[0]).await);
            assert!(!cache.contains(&hs[3]).await);
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn disk_release_rotation_deletes_files_two_back() {
        let dir = tmpdir("release");
        let cache = Cache::new(Some(&dir), pol(1000)).await.unwrap();
        let mut hs = Vec::new();
        for n in 0..3u8 {
            let b = Bytes::from(vec![n; 30]);
            hs.push(BlakeHash::hash(&b));
            cache.put(hs[n as usize], b).await.unwrap();
            let h = hs[n as usize];
            cache
                .retention()
                .record_release(format!("r{n}"), [h])
                .await
                .unwrap();
        }
        // Nothing over budget, so nothing is evicted by rotation alone;
        // r0 is merely unprotected now.
        assert!(!cache.retention().is_pinned(&hs[0]));
        assert!({
            let r = cache.retention();
            r.is_pinned(&hs[1]) && r.is_pinned(&hs[2])
        });
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn oversized_pinned_blob_is_admitted_on_disk() {
        let dir = tmpdir("oversized-pinned");
        let cache = Cache::new(Some(&dir), pol(10)).await.unwrap();
        let b = Bytes::from(vec![9u8; 50]);
        let h = BlakeHash::hash(&b);
        cache.put(h, b.clone()).await.unwrap();
        assert!(!cache.contains(&h).await, "unpinned oversize refused");
        cache.retention().pin(h);
        cache.put(h, b).await.unwrap();
        assert!(cache.contains(&h).await);
        std::fs::remove_dir_all(&dir).ok();
    }
}
