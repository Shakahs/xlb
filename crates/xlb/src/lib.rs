//! # xlb — BLAKE3-verified blob distribution to plain files
//!
//! An [`AssetClass`] is a namespace of content-addressed blobs with one cache
//! directory. [`Asset::fetch_to_file`] returns the path of a plain file
//! `<cache_dir>/<blake3-hex>` holding the verified blob. A blob already in
//! the local cache is returned at once ([`FetchTier::Cache`]). Otherwise the
//! blob is cut into segments and each segment is read from the peers that
//! hold it ([`FetchTier::Peer`]) or, when no peer does, from the HTTP origin
//! ([`FetchTier::Origin`]); nodes fetching the same blob divide the origin
//! reads between them (see [`swarm`]).
//!
//! No path holds a blob in memory. Peer and origin data alike are verified
//! chunk group by chunk group against the BLAKE3 tree as they arrive and
//! stored at once, so a blob is served to the admitted peers while it is
//! still downloading, and from its plain file once it is complete.
//!
//! This is a fork of `yah-ai/xlb` 0.8.51. It drops the in-memory store, the
//! `Bytes` fetch API, manifest resolution, the bandwidth governor and the
//! `mshr` endpoint wrapper.

pub(crate) mod cache;
pub mod metrics;
pub mod origin;
pub mod peer;
pub mod pinned;
pub mod sigv4;
pub(crate) mod source;
pub(crate) mod store;
pub(crate) mod swarm;

pub use cache::CacheStats;
pub use metrics::{FetchObserver, FetchReport};
pub use origin::{OriginConfig, OriginSource};
pub use peer::{NodeId, PeerAddr, PeerConfig, RelaySetting, RelayUrl};
pub use pinned::{PinError, PinnedTreeInfo, SeedReport};
pub use sigv4::Credentials;
pub use source::{FetchProgress, FetchTier, ProgressSink};
pub use xlb_core::retention;
pub use xlb_core::{BlakeHash, InvalidHash, RetentionIndex, RetentionPolicy};

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::watch;

use crate::{
    cache::Cache,
    origin::Origin,
    peer::PeerTransport,
    store::BlobStore,
    swarm::{SwarmFailure, SwarmProgress, SwarmSources},
};

/// Subdirectory of a class's `cache_dir` for files that are not verified yet.
const INCOMING_DIR: &str = ".incoming";

/// Default interval of the blob store's garbage collection.
pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(5);

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("invalid hash: {0}")]
    InvalidHash(#[from] InvalidHash),

    /// No tier produced the blob. `detail` says what each tier answered.
    #[error("all sources exhausted for {hash}: {detail}")]
    FetchFailed { hash: BlakeHash, detail: String },

    /// A file's content has a different BLAKE3 root than its name.
    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch {
        expected: BlakeHash,
        actual: BlakeHash,
    },

    /// The origin served a range or an outboard that does not verify
    /// against the blob's hash, or the store could not take the range.
    #[error("origin content for {hash} was not accepted: {detail}")]
    OriginRejected { hash: BlakeHash, detail: String },

    /// The blob is larger than the class's whole cache budget.
    #[error("{hash} is {size} bytes, larger than the cache budget of {budget_bytes} bytes")]
    ExceedsBudget {
        hash: BlakeHash,
        size: u64,
        budget_bytes: u64,
    },

    #[error("configuration: {0}")]
    Config(String),

    #[error("blob store: {0}")]
    Store(String),

    #[error("io: {0}")]
    Io(Arc<std::io::Error>),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(Arc::new(e))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// ─── AssetClassConfig ─────────────────────────────────────────────────────────

/// Configuration for an [`AssetClass`].
pub struct AssetClassConfig {
    /// Stable name for this class; a log and metric identifier.
    pub name: &'static str,
    /// Directory holding one plain file per complete blob, named by its
    /// BLAKE3 hex, and the blob store under `.store`.
    pub cache_dir: PathBuf,
    /// LRU eviction budget for unpinned blobs, in bytes.
    pub cache_budget_bytes: u64,
    /// The peer tier. `None` = this class neither serves nor reads peers.
    pub peer: Option<PeerConfig>,
    /// The origin tier. `None` = no origin.
    pub origin: Option<OriginConfig>,
    /// How often the blob store drops entries the cache no longer holds.
    pub gc_interval: Duration,
    /// Called once per completed fetch with a [`FetchReport`].
    pub observer: Option<FetchObserver>,
}

impl AssetClassConfig {
    /// A class with a 1 GiB budget and neither peers nor an origin.
    pub fn new(name: &'static str, cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            name,
            cache_dir: cache_dir.into(),
            cache_budget_bytes: 1024 * 1024 * 1024,
            peer: None,
            origin: None,
            gc_interval: DEFAULT_GC_INTERVAL,
            observer: None,
        }
    }
}

// ─── AssetClass ───────────────────────────────────────────────────────────────

/// What a finished transfer produced.
#[derive(Debug, Clone)]
struct Fetched {
    path: PathBuf,
    size: u64,
    tier: FetchTier,
    peer_bytes: u64,
    origin_bytes: u64,
}

impl Fetched {
    fn cached(path: PathBuf, size: u64) -> Self {
        Self {
            path,
            size,
            tier: FetchTier::Cache,
            peer_bytes: 0,
            origin_bytes: 0,
        }
    }
}

/// The shared view of one transfer in flight.
#[derive(Debug, Clone, Default)]
struct FlightState {
    progress: Option<FetchProgress>,
    outcome: Option<Result<Fetched>>,
}

type FlightSender = Arc<watch::Sender<FlightState>>;

struct ClassInner {
    name: &'static str,
    observer: Option<FetchObserver>,
    cache: Arc<Cache>,
    store: BlobStore,
    peers: Option<PeerTransport>,
    origin: Option<Origin>,
    trees: pinned::TreeStore,
    /// Names of the declared pinned trees.
    tree_names: Mutex<std::collections::BTreeSet<String>>,
    /// Transfers in flight by hash, so concurrent fetches share one.
    flights: Mutex<HashMap<BlakeHash, watch::Receiver<FlightState>>>,
}

/// A registered asset class — a namespace for content-addressed blobs.
///
/// `AssetClass` is `Clone + Send + Sync`; clone it freely to share across
/// tasks. Call [`AssetClass::shutdown`] before dropping the last clone.
#[derive(Clone)]
pub struct AssetClass(Arc<ClassInner>);

impl AssetClass {
    /// Register a class: open its blob store, reconcile the plain files
    /// already in the cache directory, and bind the peer endpoint if
    /// configured.
    ///
    /// A plain file the store does not know is re-imported in place and
    /// kept only when its content hashes to its name.
    pub async fn register(config: AssetClassConfig) -> Result<Self> {
        let cache_dir = std::path::absolute(&config.cache_dir)?;
        tokio::fs::create_dir_all(&cache_dir).await?;
        let incoming = cache_dir.join(INCOMING_DIR);
        match tokio::fs::remove_dir_all(&incoming).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        tokio::fs::create_dir_all(&incoming).await?;

        // Pinned trees are declared on the index first, so replaying what is
        // already on disk cannot evict them.
        let trees = pinned::TreeStore::under(&cache_dir);
        let mut index = RetentionIndex::new(RetentionPolicy::new(config.cache_budget_bytes, 0));
        let mut tree_names = std::collections::BTreeSet::new();
        for (name, hashes) in trees.load_all().map_err(std::io::Error::other)? {
            tree_names.insert(name.clone());
            index.pin_set(name, hashes);
        }
        let cache = Arc::new(Cache::new(&cache_dir, index));

        let store = BlobStore::open(&cache_dir, cache.clone(), config.gc_interval)
            .await
            .map_err(Error::Store)?;
        reconcile(&cache, &store).await?;
        store.mark_ready();

        let peers = match config.peer {
            Some(peer) => Some(
                PeerTransport::bind(peer, store.api())
                    .await
                    .map_err(Error::Config)?,
            ),
            None => None,
        };
        let origin = match config.origin {
            Some(origin) => Some(Origin::new(origin).map_err(|e| Error::Config(e.to_string()))?),
            None => None,
        };

        Ok(Self(Arc::new(ClassInner {
            name: config.name,
            observer: config.observer,
            cache,
            store,
            peers,
            origin,
            trees,
            tree_names: Mutex::new(tree_names),
            flights: Mutex::new(HashMap::new()),
        })))
    }

    /// Stop serving peers, close the endpoint and shut the blob store down.
    pub async fn shutdown(&self) {
        match &self.0.peers {
            // The router shuts the protocol handler down, which shuts the store.
            Some(peers) => peers.shutdown().await,
            None => self.0.store.shutdown().await,
        }
    }

    /// Return a handle to a specific asset within this class.
    pub fn asset(&self, hash: BlakeHash) -> Asset {
        Asset {
            class: self.clone(),
            hash,
        }
    }

    /// The configured name of this class.
    pub fn name(&self) -> &str {
        self.0.name
    }

    /// The absolute cache directory; a complete blob is the plain file
    /// `<cache_dir>/<blake3-hex>`.
    pub fn cache_dir(&self) -> &Path {
        self.0.cache.dir()
    }

    /// Current cache occupancy. Reads an in-memory index.
    pub fn cache_stats(&self) -> CacheStats {
        self.0.cache.stats()
    }

    // ─── Peers ─────────────────────────────────────────────────────────────

    /// This node's ID on the peer tier, if the class has one.
    pub fn node_id(&self) -> Option<NodeId> {
        self.0.peers.as_ref().map(PeerTransport::node_id)
    }

    /// The UDP addresses the peer endpoint is bound to.
    pub fn bound_addrs(&self) -> Vec<SocketAddr> {
        self.0
            .peers
            .as_ref()
            .map(PeerTransport::bound_addrs)
            .unwrap_or_default()
    }

    /// Replace the peer set. Later fetches read only from these peers and
    /// only these peers are served. A no-op on a class without a peer tier.
    pub fn set_peers(&self, peers: Vec<PeerAddr>) {
        let admit = peers.iter().map(|peer| peer.node_id).collect();
        self.set_peer_sets(peers, admit);
    }

    /// Replace the peer set and, separately, the nodes this node serves.
    /// Later fetches dial, observe and read only `peers`; get and observe
    /// requests are answered only for the node IDs in `admit`, which need
    /// not be in `peers`. A no-op on a class without a peer tier.
    // [impl -> dsn~xlb-peer-reach~1]
    pub fn set_peer_sets(&self, peers: Vec<PeerAddr>, admit: Vec<NodeId>) {
        if let Some(transport) = &self.0.peers {
            transport.set_peer_sets(peers, admit);
        }
    }

    /// The relay the peer endpoint is reachable through, once it has
    /// connected to one.
    pub fn relay_url(&self) -> Option<RelayUrl> {
        self.0.peers.as_ref().and_then(PeerTransport::relay_url)
    }

    /// The current peer set.
    pub fn peers(&self) -> Vec<PeerAddr> {
        self.0
            .peers
            .as_ref()
            .map(PeerTransport::peers)
            .unwrap_or_default()
    }

    // ─── Pins ──────────────────────────────────────────────────────────────

    /// Pin one blob: never evicted, outside the byte budget. May be called
    /// before the blob has been fetched.
    pub fn pin(&self, hash: BlakeHash) {
        self.0.cache.pin(hash);
    }

    /// Drop a hash pin; the blob rejoins the LRU (and may be evicted at once
    /// if the unpinned footprint is over budget).
    pub fn unpin(&self, hash: &BlakeHash) {
        self.0.cache.unpin(hash);
    }

    /// Whether `hash` is currently protected from eviction.
    pub fn is_pinned(&self, hash: &BlakeHash) -> bool {
        self.0.cache.is_pinned(hash)
    }

    fn tree_names(&self) -> std::sync::MutexGuard<'_, std::collections::BTreeSet<String>> {
        self.0.tree_names.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Declare (or replace) the pinned tree `name` covering `hashes`. It is
    /// persisted first, then pinned, so it survives a restart and its members
    /// are never evicted. The bytes need not be present: seed them with
    /// [`Self::seed_tree_from_dirs`] or let fetches fill them in.
    pub async fn pin_tree(
        &self,
        name: &str,
        hashes: impl IntoIterator<Item = BlakeHash>,
    ) -> std::result::Result<PinnedTreeInfo, PinError> {
        pinned::validate_tree_name(name)?;
        let mut hashes: Vec<BlakeHash> = hashes.into_iter().collect();
        hashes.sort_by_key(BlakeHash::to_hex);
        hashes.dedup();
        self.0.trees.save(name, &hashes).await?;
        // Replacing a tree can drop members; unpin first so the evictions
        // that frees are deleted, then pin the new membership.
        if self.tree_names().contains(name) {
            self.0.cache.unpin_set(name);
        }
        self.0.cache.pin_set(name, hashes);
        self.tree_names().insert(name.to_string());
        self.tree_info(name)
            .ok_or_else(|| PinError::NotFound(name.to_string()))
    }

    /// Remove the pinned tree `name`; its members rejoin the LRU and may be
    /// evicted at once if the unpinned footprint is over budget.
    pub async fn unpin_tree(&self, name: &str) -> std::result::Result<(), PinError> {
        if !self.tree_names().remove(name) {
            return Err(PinError::NotFound(name.to_string()));
        }
        self.0.trees.remove(name).await?;
        self.0.cache.unpin_set(name);
        Ok(())
    }

    /// Every pinned tree on this class, by name.
    pub fn pinned_trees(&self) -> Vec<PinnedTreeInfo> {
        let names: Vec<String> = self.tree_names().iter().cloned().collect();
        names.iter().filter_map(|n| self.tree_info(n)).collect()
    }

    /// The members of pinned tree `name`, or `None` if there is no such tree.
    pub fn tree_members(&self, name: &str) -> Option<Vec<BlakeHash>> {
        if !self.tree_names().contains(name) {
            return None;
        }
        Some(self.0.cache.pin_set_members(name))
    }

    fn tree_info(&self, name: &str) -> Option<PinnedTreeInfo> {
        let usage = self.0.cache.pin_set_usage(name)?;
        Some(PinnedTreeInfo {
            name: name.to_string(),
            members: usage.members,
            present: usage.present,
            present_bytes: usage.present_bytes,
            durable: true,
        })
    }

    /// Fill pinned tree `name` from local directories holding blobs at
    /// `<dir>/<blake3-hex>`. Each file is copied into the cache directory and
    /// admitted only when its content hashes to its name; members already
    /// cached are not re-read. No network.
    pub async fn seed_tree_from_dirs(
        &self,
        name: &str,
        dirs: &[PathBuf],
    ) -> std::result::Result<SeedReport, PinError> {
        let members = self
            .tree_members(name)
            .ok_or_else(|| PinError::NotFound(name.to_string()))?;
        let mut report = SeedReport::default();
        'member: for hash in members {
            if self.0.cache.contains(&hash) {
                report.already_cached += 1;
                continue;
            }
            for dir in dirs {
                let source = dir.join(hash.to_hex());
                let staged = self.staging_path(&hash);
                match tokio::fs::copy(&source, &staged).await {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                }
                match self.adopt(hash, &staged).await {
                    Ok(_) => {
                        report.ingested += 1;
                        continue 'member;
                    }
                    Err(Error::HashMismatch { .. }) => report.mismatched.push(source),
                    Err(e) => return Err(std::io::Error::other(e).into()),
                }
            }
            report.missing.push(hash);
        }
        Ok(report)
    }

    // ─── Fetch ─────────────────────────────────────────────────────────────

    fn staging_path(&self, hash: &BlakeHash) -> PathBuf {
        self.0.cache.dir().join(INCOMING_DIR).join(hash.to_hex())
    }

    /// The plain file of `hash` if it is complete locally.
    fn local(&self, hash: &BlakeHash) -> Option<(PathBuf, u64)> {
        let (path, size) = self.0.cache.lookup(hash)?;
        if path.is_file() {
            Some((path, size))
        } else {
            tracing::warn!(%hash, "cached file vanished from {}", path.display());
            self.0.cache.forget(hash);
            None
        }
    }

    /// Move the staged file to `<cache_dir>/<hex>`, have the store adopt it
    /// in place, and admit it only when its content hashes to `hash`.
    /// Returns its size. The file is gone again on any error.
    async fn adopt(&self, hash: BlakeHash, staged: &Path) -> Result<u64> {
        let path = self.0.cache.path_for(&hash);
        let adopted: Result<u64> = async {
            tokio::fs::rename(staged, &path).await?;
            let size = tokio::fs::metadata(&path).await?.len();
            let (actual, tag) = self
                .0
                .store
                .import_reference(path.clone())
                .await
                .map_err(Error::Store)?;
            if actual != hash {
                return Err(Error::HashMismatch {
                    expected: hash,
                    actual,
                });
            }
            // The index entry keeps the blob alive from here; the import's
            // temporary tag is dropped after it.
            self.0.cache.insert(hash, size);
            drop(tag);
            Ok(size)
        }
        .await;
        if adopted.is_err() {
            for leftover in [staged, path.as_path()] {
                if let Err(e) = tokio::fs::remove_file(leftover).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!(%hash, "removing {}: {e}", leftover.display());
                    }
                }
            }
        }
        adopted
    }

    /// Join the transfer in flight for `hash`, or start one.
    fn join_flight(&self, hash: BlakeHash) -> watch::Receiver<FlightState> {
        let mut flights = self.0.flights.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(flight) = flights.get(&hash) {
            // A closed channel is a transfer task that died; replace it.
            if flight.has_changed().is_ok() {
                return flight.clone();
            }
        }
        let (tx, rx) = watch::channel(FlightState::default());
        flights.insert(hash, rx.clone());
        let class = self.clone();
        tokio::spawn(async move {
            let tx: FlightSender = Arc::new(tx);
            let outcome = class.transfer(hash, &tx).await;
            // Unregister before publishing, so a fetch that arrives after the
            // outcome starts from the cache rather than from this flight.
            class
                .0
                .flights
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&hash);
            tx.send_modify(|state| state.outcome = Some(outcome));
        });
        rx
    }

    /// Obtain `hash` from the local cache, or else from the peers and the
    /// origin together.
    // [impl -> dsn~xlb-fork~1]
    // [impl -> dsn~xlb-swarm-fetch~1]
    async fn transfer(&self, hash: BlakeHash, flight: &FlightSender) -> Result<Fetched> {
        if let Some((path, size)) = self.local(&hash) {
            return Ok(Fetched::cached(path, size));
        }
        let inner = &self.0;

        // The store can still hold a complete entry for a blob the cache
        // evicted. The fetch would see it as done, so wait for the store to
        // collect it first. This must precede the in-flight guard, which
        // would keep the entry alive.
        if inner.store.complete_size(&hash).await.map_err(Error::Store)?.is_some()
            && !inner.store.wait_forgotten(&hash).await
        {
            return Err(Error::FetchFailed {
                hash,
                detail: "the store still holds an evicted entry".into(),
            });
        }
        let _in_flight = inner.store.in_flight(hash);

        let incoming_dir = inner.cache.dir().join(INCOMING_DIR);
        let sources = SwarmSources {
            hash,
            store: inner.store.api(),
            peers: inner.peers.as_ref(),
            origin: inner.origin.as_ref(),
            incoming_dir: &incoming_dir,
        };
        let admit = |size: u64| self.reserve(&hash, size);
        let report = flight.clone();
        let progress = move |received: SwarmProgress| {
            report.send_modify(|state| {
                state.progress = Some(FetchProgress {
                    bytes_so_far: (received.peer_bytes + received.origin_bytes).min(received.size),
                    total: Some(received.size),
                    tier: if received.peer_bytes >= received.origin_bytes {
                        FetchTier::Peer
                    } else {
                        FetchTier::Origin
                    },
                })
            });
        };
        let received = match swarm::fetch(sources, admit, progress).await {
            Ok(received) => received,
            Err(SwarmFailure::Fatal(e)) => return Err(e),
            Err(SwarmFailure::Miss(detail)) => {
                tracing::debug!(%hash, "fetch missed: {detail}");
                return Err(Error::FetchFailed { hash, detail });
            }
        };

        // The complete blob leaves the store for its plain file and is
        // served from there.
        let path = inner.cache.path_for(&hash);
        inner
            .store
            .export_reference(&hash, path.clone())
            .await
            .map_err(Error::Store)?;
        inner.cache.insert(hash, received.size);
        let tier = match (received.peer_bytes, received.origin_bytes) {
            (0, 0) => FetchTier::Cache,
            (peer, origin) if peer > origin => FetchTier::Peer,
            _ => FetchTier::Origin,
        };
        Ok(Fetched {
            path,
            size: received.size,
            tier,
            peer_bytes: received.peer_bytes,
            origin_bytes: received.origin_bytes,
        })
    }

    /// Refuse a blob larger than the whole budget, else evict ahead of it.
    fn reserve(&self, hash: &BlakeHash, size: u64) -> Result<()> {
        self.0
            .cache
            .admits(hash, size)
            .map_err(|budget_bytes| Error::ExceedsBudget {
                hash: *hash,
                size,
                budget_bytes,
            })?;
        self.0.cache.make_room(hash, size);
        Ok(())
    }

    /// Fetch `hash` to its plain file and report which tier served it. The
    /// class observer, if any, has been called when this returns.
    pub(crate) async fn fetch_reported(
        &self,
        hash: BlakeHash,
        sink: Option<ProgressSink>,
    ) -> (Result<PathBuf>, FetchReport) {
        let started = std::time::Instant::now();
        // Held until this call returns, so the blob it is about to hand out
        // cannot be evicted by a concurrent fetch.
        let _hold = self.0.cache.hold(hash);

        let outcome = match self.local(&hash) {
            Some((path, size)) => Ok(Fetched::cached(path, size)),
            None => {
                let mut flight = self.join_flight(hash);
                loop {
                    let (progress, outcome) = {
                        let state = flight.borrow_and_update();
                        (state.progress, state.outcome.clone())
                    };
                    if let Some(outcome) = outcome {
                        break outcome;
                    }
                    if let (Some(progress), Some(sink)) = (progress, &sink) {
                        sink(progress);
                    }
                    if flight.changed().await.is_err() {
                        break Err(Error::FetchFailed {
                            hash,
                            detail: "the transfer task ended without an outcome".into(),
                        });
                    }
                }
            }
        };

        let (tier, bytes_served, peer_bytes, origin_bytes) = match &outcome {
            Ok(fetched) => (
                Some(fetched.tier),
                fetched.size,
                fetched.peer_bytes,
                fetched.origin_bytes,
            ),
            Err(_) => (None, 0, 0, 0),
        };
        let report = FetchReport {
            class: self.0.name,
            hash,
            tier,
            peer_id: None,
            bytes_served,
            peer_bytes,
            origin_bytes,
            duration_ms: started.elapsed().as_millis() as u64,
        };
        if let Some(observer) = &self.0.observer {
            observer(&report);
        }
        (outcome.map(|fetched| fetched.path), report)
    }
}

/// Rebuild the cache index from the plain files in the cache directory,
/// oldest first so the LRU order survives a restart.
async fn reconcile(cache: &Arc<Cache>, store: &BlobStore) -> Result<()> {
    let mut found: Vec<(std::time::SystemTime, BlakeHash, PathBuf, u64)> = Vec::new();
    let mut entries = tokio::fs::read_dir(cache.dir()).await?;
    while let Some(entry) = entries.next_entry().await? {
        let Some(hash) = entry
            .file_name()
            .to_str()
            .and_then(|name| BlakeHash::from_hex(name).ok())
        else {
            continue;
        };
        let metadata = entry.metadata().await?;
        if !metadata.is_file() {
            continue;
        }
        let modified = metadata.modified().unwrap_or(std::time::UNIX_EPOCH);
        found.push((modified, hash, entry.path(), metadata.len()));
    }
    found.sort_by_key(|(modified, hash, _, _)| (*modified, hash.to_hex()));

    for (_, hash, path, len) in found {
        if store.complete_size(&hash).await.map_err(Error::Store)? == Some(len) {
            cache.insert(hash, len);
            continue;
        }
        // Unknown to the store: verify by importing it in place.
        match store.import_reference(path.clone()).await {
            Ok((actual, tag)) if actual == hash => {
                cache.insert(hash, len);
                drop(tag);
            }
            Ok((actual, _)) => {
                tracing::warn!(%hash, %actual, "removing cache file whose content does not match its name");
                tokio::fs::remove_file(&path).await?;
            }
            Err(e) => {
                tracing::warn!(%hash, "removing cache file the store cannot import: {e}");
                tokio::fs::remove_file(&path).await?;
            }
        }
    }
    Ok(())
}

// ─── Asset ────────────────────────────────────────────────────────────────────

/// A handle to a specific content-addressed blob within an [`AssetClass`].
#[derive(Clone)]
pub struct Asset {
    class: AssetClass,
    hash: BlakeHash,
}

impl Asset {
    /// Fetch the blob and return the path of its plain file,
    /// `<cache_dir>/<blake3-hex>`, trying the local cache, then the peer
    /// set, then the origin. The content is BLAKE3-verified.
    ///
    /// Concurrent calls for the same hash share one transfer.
    // [impl -> dsn~xlb-fork~1]
    pub async fn fetch_to_file(&self) -> Result<PathBuf> {
        self.class.fetch_reported(self.hash, None).await.0
    }

    /// As [`fetch_to_file`](Self::fetch_to_file), reporting cumulative bytes
    /// and the serving tier to `sink`. A cache hit never invokes `sink`.
    pub async fn fetch_to_file_with_progress(&self, sink: ProgressSink) -> Result<PathBuf> {
        self.class.fetch_reported(self.hash, Some(sink)).await.0
    }

    /// As [`fetch_to_file`](Self::fetch_to_file), returning the
    /// [`FetchReport`] alongside. The report is produced on a miss too, with
    /// `tier: None`.
    pub async fn fetch_to_file_reported(
        &self,
        sink: Option<ProgressSink>,
    ) -> (Result<PathBuf>, FetchReport) {
        self.class.fetch_reported(self.hash, sink).await
    }

    /// Whether this blob is complete in the local cache.
    pub fn is_cached(&self) -> bool {
        self.class.0.cache.contains(&self.hash)
    }

    pub fn hash(&self) -> BlakeHash {
        self.hash
    }
}
