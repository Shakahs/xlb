//! The file-backed iroh-blobs store of one class, rooted at
//! `<cache_dir>/.store`.
//!
//! The store keeps the BLAKE3 outboards, the partial downloads and its
//! metadata there. A complete blob's data lives as the plain file
//! `<cache_dir>/<blake3-hex>`, which the store references as an external
//! file: [`BlobStore::export_reference`] moves a store-owned data file there
//! after a peer download, and [`BlobStore::import_reference`] adopts a file
//! the origin tier wrote there.
//!
//! iroh-blobs deletes entries only through garbage collection, so the store
//! runs it on a short interval. The class's cache index and the fetches in
//! flight are the live set; everything else is collected, which is how an
//! evicted blob and an abandoned partial download leave the store.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use iroh_blobs::{
    api::blobs::{AddPathOptions, BlobStatus, ExportMode, ExportOptions, ImportMode},
    store::{
        fs::{options::Options, FsStore},
        GcConfig, ProtectCb, ProtectOutcome,
    },
    BlobFormat, Hash as IrohHash,
};

use crate::{cache::Cache, BlakeHash};

/// Subdirectory of a class's `cache_dir` that the store owns.
pub(crate) const STORE_DIR: &str = ".store";

pub(crate) fn to_iroh(hash: &BlakeHash) -> IrohHash {
    IrohHash::from_bytes(*hash.as_bytes())
}

pub(crate) fn from_iroh(hash: &IrohHash) -> BlakeHash {
    BlakeHash::from_bytes(*hash.as_bytes())
}

/// What garbage collection must keep.
pub(crate) struct LiveSet {
    /// False until the startup reconciliation has filled the cache index;
    /// collection is skipped until then.
    ready: AtomicBool,
    cache: Arc<Cache>,
    /// Hashes with a transfer in flight, with the number of transfers.
    in_flight: Mutex<HashMap<BlakeHash, u32>>,
}

impl LiveSet {
    fn fill(&self, live: &mut HashSet<IrohHash>) -> ProtectOutcome {
        if !self.ready.load(Ordering::Acquire) {
            return ProtectOutcome::Abort;
        }
        live.extend(self.cache.hashes().iter().map(to_iroh));
        let in_flight = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        live.extend(in_flight.keys().map(to_iroh));
        ProtectOutcome::Continue
    }
}

/// Keeps a hash in the live set while a transfer for it is in flight.
pub(crate) struct InFlight {
    live: Arc<LiveSet>,
    hash: BlakeHash,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        let mut in_flight = self.live.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = in_flight.get_mut(&self.hash) {
            *count -= 1;
            if *count == 0 {
                in_flight.remove(&self.hash);
            }
        }
    }
}

type ProtectFuture<'a> = Pin<Box<dyn Future<Output = ProtectOutcome> + Send + Sync + 'a>>;

/// Pins the closure to the higher-ranked signature [`ProtectCb`] needs.
fn protect_callback<F>(callback: F) -> ProtectCb
where
    F: for<'a> Fn(&'a mut HashSet<IrohHash>) -> ProtectFuture<'a> + Send + Sync + 'static,
{
    Arc::new(callback)
}

pub(crate) struct BlobStore {
    store: FsStore,
    live: Arc<LiveSet>,
    gc_interval: Duration,
}

impl BlobStore {
    /// Open or create the store under `cache_dir`. Garbage collection stays
    /// off until [`Self::mark_ready`].
    // [impl -> dsn~xlb-fork~1]
    pub(crate) async fn open(
        cache_dir: &Path,
        cache: Arc<Cache>,
        gc_interval: Duration,
    ) -> Result<Self, String> {
        let root = cache_dir.join(STORE_DIR);
        let live = Arc::new(LiveSet {
            ready: AtomicBool::new(false),
            cache,
            in_flight: Mutex::new(HashMap::new()),
        });
        let mut options = Options::new(&root);
        let protect = live.clone();
        options.gc = Some(GcConfig {
            interval: gc_interval,
            add_protected: Some(protect_callback(move |hashes| {
                let outcome = protect.fill(hashes);
                Box::pin(std::future::ready(outcome))
            })),
        });
        let store = FsStore::load_with_opts(root.join("blobs.db"), options)
            .await
            .map_err(|e| format!("opening blob store at {}: {e}", root.display()))?;
        Ok(Self {
            store,
            live,
            gc_interval,
        })
    }

    pub(crate) fn api(&self) -> &iroh_blobs::api::Store {
        &self.store
    }

    /// Allow garbage collection: the cache index now names every blob to keep.
    pub(crate) fn mark_ready(&self) {
        self.live.ready.store(true, Ordering::Release);
    }

    pub(crate) fn in_flight(&self, hash: BlakeHash) -> InFlight {
        let mut in_flight = self.live.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        *in_flight.entry(hash).or_insert(0) += 1;
        InFlight {
            live: self.live.clone(),
            hash,
        }
    }

    /// The size of `hash` when the store holds it completely.
    pub(crate) async fn complete_size(&self, hash: &BlakeHash) -> Result<Option<u64>, String> {
        match self.store.blobs().status(to_iroh(hash)).await {
            Ok(BlobStatus::Complete { size }) => Ok(Some(size)),
            Ok(_) => Ok(None),
            Err(e) => Err(format!("blob status: {e}")),
        }
    }

    /// Adopt the file at `path` in place: the store computes its outboard and
    /// serves it from there. Returns the BLAKE3 root of the file's content
    /// and a tag that keeps the entry alive until it is dropped.
    pub(crate) async fn import_reference(
        &self,
        path: PathBuf,
    ) -> Result<(BlakeHash, iroh_blobs::api::TempTag), String> {
        let tag = self
            .store
            .blobs()
            .add_path_with_opts(AddPathOptions {
                path,
                format: BlobFormat::Raw,
                mode: ImportMode::TryReference,
            })
            .temp_tag()
            .await
            .map_err(|e| format!("importing file: {e}"))?;
        Ok((from_iroh(&tag.hash()), tag))
    }

    /// Move the complete blob `hash` to the plain file `target` and keep
    /// serving it from there. Returns its size.
    // [impl -> dsn~xlb-fork~1]
    pub(crate) async fn export_reference(
        &self,
        hash: &BlakeHash,
        target: PathBuf,
    ) -> Result<u64, String> {
        self.store
            .blobs()
            .export_with_opts(ExportOptions {
                hash: to_iroh(hash),
                mode: ExportMode::TryReference,
                target,
            })
            .finish()
            .await
            .map_err(|e| format!("exporting blob: {e}"))
    }

    /// Wait for garbage collection to drop a complete entry whose plain file
    /// is gone (an evicted blob). Returns whether the store is free of it.
    pub(crate) async fn wait_forgotten(&self, hash: &BlakeHash) -> bool {
        let deadline = tokio::time::Instant::now() + self.gc_interval * 3 + Duration::from_secs(1);
        loop {
            match self.complete_size(hash).await {
                Ok(None) => return true,
                Ok(Some(_)) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(self.gc_interval / 4).await;
                }
                _ => return false,
            }
        }
    }

    pub(crate) async fn shutdown(&self) {
        if let Err(e) = self.store.shutdown().await {
            tracing::warn!("blob store shutdown: {e}");
        }
    }
}
