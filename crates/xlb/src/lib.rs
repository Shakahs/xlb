//! BLAKE3-verified chunks for the model mount, OCI mirror and snapshotter.

pub mod allocator;
mod cache_checkpoint;
pub mod cas_cache;
mod chunk_admission;
mod inventory_publication;
pub mod metrics;
pub mod origin;
mod panda_chunks;
mod panda_inventory;
mod panda_network;
mod panda_peers;
pub mod peer;
mod request_limits;
mod source;
mod transfer_policy;

#[cfg(test)]
#[path = "swarm_tests.rs"]
mod swarm_tests;

pub use chunk_admission::{ChunkAdmission, TransferPriority};
pub use metrics::{FetchObserver, FetchReport};
pub use origin::{Credentials, OriginConfig, S3RequestEvent, S3RequestStats};
pub use peer::{NodeId, PeerAddr, PeerConfig, RelaySetting, RelayUrl};
pub use request_limits::{RequestLimits, RequestStats};
pub use source::{FetchProgress, FetchTier, ProgressSink};
pub use xlb_core::{BlakeHash, FetchSources, InvalidHash, NodeTopology};

use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};

use bytes::Bytes;
use tokio::sync::{Notify, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{cache_checkpoint::CacheCheckpoint, cas_cache::{CasCache, VerifiedChunk}, origin::Origin, panda_peers::PandaPeers};

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("invalid hash: {0}")]
    InvalidHash(#[from] InvalidHash),
    #[error("all sources exhausted for {hash}: {detail}")]
    FetchFailed { hash: BlakeHash, detail: String },
    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: BlakeHash, actual: BlakeHash },
    #[error("origin content for {hash} was not accepted: {detail}")]
    OriginRejected { hash: BlakeHash, detail: String },
    #[error("configuration: {0}")]
    Config(String),
    #[error("CAS storage: {0}")]
    Store(String),
    #[error("io: {0}")]
    Io(Arc<std::io::Error>),
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(Arc::new(error))
    }
}

impl Error {
    fn failure_kind(&self) -> &'static str {
        match self {
            Self::FetchFailed { detail, .. } if detail.contains("timed out") => "timeout",
            Self::FetchFailed { detail, .. } if detail.contains("wrong chunk length") => "invalid_length",
            Self::FetchFailed { detail, .. } if detail.contains("does not hold") || detail.contains("no eligible peer") => "unavailable",
            Self::FetchFailed { detail, .. } if detail.starts_with("origin:") => "origin",
            Self::FetchFailed { .. } => "peer_or_source",
            Self::OriginRejected { .. } | Self::HashMismatch { .. } => "integrity",
            Self::Store(_) | Self::Io(_) => "storage",
            Self::Config(_) | Self::InvalidHash(_) => "configuration",
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Configuration for one indexed CAS namespace.
pub struct AssetClassConfig {
    pub name: &'static str,
    pub cache_dir: PathBuf,
    pub cache_budget_bytes: u64,
    pub cas_memory_bytes: usize,
    pub peer: Option<PeerConfig>,
    pub origin: Option<OriginConfig>,
    pub request_limits: Option<RequestLimits>,
    pub chunk_limit: Option<ChunkAdmission>,
    pub observer: Option<FetchObserver>,
}

impl AssetClassConfig {
    pub fn new(name: &'static str, cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            name,
            cache_dir: cache_dir.into(),
            cache_budget_bytes: 1024 * 1024 * 1024,
            cas_memory_bytes: 512 * 1024 * 1024,
            peer: None,
            origin: None,
            request_limits: None,
            chunk_limit: None,
            observer: None,
        }
    }
}

pub struct CacheStats {
    pub entries: u64,
    pub bytes: u64,
    pub budget_bytes: u64,
}

#[derive(Debug, Clone)]
struct Fetched {
    content: Bytes,
    size: u64,
    tier: FetchTier,
    peer_bytes: u64,
    origin_bytes: u64,
    peers: BTreeMap<String, u64>,
}

impl Fetched {
    fn cached(content: Bytes) -> Self {
        Self {
            size: content.len() as u64,
            content,
            tier: FetchTier::Cache,
            peer_bytes: 0,
            origin_bytes: 0,
            peers: BTreeMap::new(),
        }
    }
}

type Flight = watch::Receiver<Option<Result<Fetched>>>;

/// A transfer in progress, its size and the signal that promotes it to demand.
type FlightEntry = (u64, Flight, Arc<Notify>);

struct ClassInner {
    name: &'static str,
    observer: Option<FetchObserver>,
    cache_dir: PathBuf,
    cache_budget_bytes: u64,
    cache: Arc<CasCache>,
    checkpoint: CacheCheckpoint,
    peers: Option<PandaPeers>,
    origin: Option<Origin>,
    flights: Mutex<HashMap<BlakeHash, FlightEntry>>,
    transfers: TaskTracker,
    chunk_limit: Option<ChunkAdmission>,
    shutdown: CancellationToken,
}

/// A namespace of verified indexed chunks with local storage and optional peers.
#[derive(Clone)]
pub struct AssetClass(Arc<ClassInner>);

impl AssetClass {
    pub async fn register(config: AssetClassConfig) -> Result<Self> {
        let limits = match config.request_limits {
            Some(limits) => limits,
            None => {
                let origin = config.origin.as_ref().map(|origin| origin.concurrency);
                let peer = config.peer.as_ref().map(|peer| peer.concurrency);
                let total = origin.or(peer).unwrap_or(NonZeroUsize::MIN);
                RequestLimits::new(total, origin.unwrap_or(total), peer.unwrap_or(total))?
            }
        };
        let origin = config.origin.map(|origin| Origin::new(origin, limits.clone())).transpose()?;
        let cache_dir = std::path::absolute(&config.cache_dir)?;
        let disk = usize::try_from(config.cache_budget_bytes)
            .map_err(|_| Error::Config("CAS disk budget exceeds the platform limit".into()))?;
        let cache = Arc::new(CasCache::open(&cache_dir, config.cas_memory_bytes, disk).await?);
        let peers = match config.peer {
            Some(peer) => Some(PandaPeers::start(peer, cache.clone(), limits, config.name).await?),
            None => None,
        };
        let checkpoint = CacheCheckpoint::start(cache.clone()).await?;
        Ok(Self(Arc::new(ClassInner {
            name: config.name,
            observer: config.observer,
            cache_dir,
            cache_budget_bytes: config.cache_budget_bytes,
            cache,
            checkpoint,
            peers,
            origin,
            flights: Mutex::default(),
            transfers: TaskTracker::new(),
            chunk_limit: config.chunk_limit,
            shutdown: CancellationToken::new(),
        })))
    }

    pub async fn shutdown(&self) -> Result<()> {
        {
            let _flights = self.0.flights.lock().unwrap();
            self.0.shutdown.cancel();
            self.0.transfers.close();
        }
        if let Some(peers) = &self.0.peers {
            peers.shutdown().await;
        }
        self.0.transfers.wait().await;
        self.0.checkpoint.stop().await;
        self.0.cache.close().await
    }

    pub async fn watch_cache(&self) -> Result<()> {
        self.0.checkpoint.stopped().await;
        Err(Error::Store("XLB CAS checkpoint actor stopped".into()))
    }

    pub fn indexed_chunk(&self, hash: BlakeHash, size: u64) -> Asset {
        Asset { class: self.clone(), hash, size }
    }

    pub fn name(&self) -> &'static str {
        self.0.name
    }

    pub fn cache_dir(&self) -> &Path {
        &self.0.cache_dir
    }

    pub fn cache_stats(&self) -> CacheStats {
        let (entries, bytes) = self.0.cache.occupancy();
        CacheStats { entries, bytes, budget_bytes: self.0.cache_budget_bytes }
    }

    pub fn s3_request_stats(&self) -> Option<S3RequestStats> {
        self.0.origin.as_ref().map(Origin::s3_request_stats)
    }

    pub fn subscribe_s3_requests(&self, capacity: NonZeroUsize) -> Option<tokio::sync::mpsc::Receiver<S3RequestEvent>> {
        self.0.origin.as_ref()?.subscribe_s3_requests(capacity)
    }

    /// Read a bounded index or manifest from the CAS bucket.
    // [impl -> dsn~xlb-fork~1]
    pub async fn read_origin_object(&self, key: &str, limit_bytes: usize) -> Result<Option<Vec<u8>>> {
        let origin = self.0.origin.as_ref()
            .ok_or_else(|| Error::Config(format!("class {} has no origin", self.0.name)))?;
        origin.read_object(key, limit_bytes).await
            .map_err(|error| Error::Store(format!("origin object {key}: {error}")))
    }

    pub fn node_id(&self) -> Option<NodeId> {
        self.0.peers.as_ref().map(PandaPeers::node_id)
    }

    pub fn bound_addrs(&self) -> Vec<SocketAddr> {
        self.0.peers.as_ref().map(PandaPeers::bound_addrs).unwrap_or_default()
    }

    pub fn set_peers(&self, peers: Vec<PeerAddr>) {
        let admit = peers.iter().map(|peer| peer.node_id).collect();
        self.set_peer_sets(peers, admit);
    }

    // [impl -> dsn~xlb-peer-reach~1]
    pub fn set_peer_sets(&self, peers: Vec<PeerAddr>, admit: Vec<NodeId>) {
        if let Some(transport) = &self.0.peers {
            transport.set_peers(peers, admit);
        }
    }

    pub fn set_node_topology(&self, topology: Option<NodeTopology>) {
        if let Some(transport) = &self.0.peers {
            transport.set_topology(topology);
        }
    }

    pub fn relay_url(&self) -> Option<RelayUrl> {
        self.0.peers.as_ref().and_then(PandaPeers::relay_url)
    }

    pub fn peers(&self) -> Vec<PeerAddr> {
        self.0.peers.as_ref().map(PandaPeers::peers).unwrap_or_default()
    }

    async fn cached_chunk(&self, hash: BlakeHash, size: u64) -> Result<Option<Fetched>> {
        match self.0.cache.get(hash).await {
            Ok(Some(chunk)) if chunk.body().len() as u64 == size => Ok(Some(Fetched::cached(chunk.body().clone()))),
            Ok(Some(_)) => Err(Error::Store(format!("CAS chunk {hash} has an unexpected length"))),
            Ok(None) => Ok(None),
            Err(error) => {
                tracing::warn!(%hash, %error, "refetch unreadable XLB chunk");
                Ok(None)
            }
        }
    }

    // A demand read that joins a transfer still waiting for admission
    // promotes it.
    // [impl -> dsn~xlb-fuse-snapshots~1]
    fn join_flight(&self, hash: BlakeHash, size: u64, priority: TransferPriority) -> Result<(u64, Flight)> {
        let mut flights = self.0.flights.lock().unwrap();
        if self.0.shutdown.is_cancelled() {
            return Err(Error::FetchFailed { hash, detail: "XLB is shutting down".into() });
        }
        if let Some((size, flight, promotion)) = flights.get(&hash) {
            if flight.has_changed().is_ok() {
                if priority == TransferPriority::Demand {
                    promotion.notify_one();
                }
                return Ok((*size, flight.clone()));
            }
        }
        let (tx, rx) = watch::channel(None);
        let promotion = Arc::new(Notify::new());
        flights.insert(hash, (size, rx.clone(), promotion.clone()));
        let class = self.clone();
        use tracing::Instrument;
        self.0.transfers.spawn(async move {
            let admission = class.0.chunk_limit.clone();
            let outcome = tokio::select! {
                result = async {
                    let _slot = match &admission {
                        Some(admission) => Some(admission.admit(priority, &promotion).await),
                        None => None,
                    };
                    class.transfer_chunk(hash, size).await
                } => result,
                () = class.0.shutdown.cancelled() => Err(Error::FetchFailed {
                    hash, detail: "XLB is shutting down".into(),
                }),
            };
            class.0.flights.lock().unwrap().remove(&hash);
            tx.send_replace(Some(outcome));
        }.instrument(tracing::Span::current()));
        Ok((size, rx))
    }

    // [impl -> dsn~xlb-supervised-lifecycle~1]
    async fn transfer_chunk(&self, hash: BlakeHash, size: u64) -> Result<Fetched> {
        use transfer_policy::{ChunkAction, ChunkEvent, ChunkTransfer, Sources};

        if size == 0 || size > xlb_core::chunks::CAS_CHUNK_BYTES {
            return Err(Error::Store(format!("invalid CAS chunk length {size}")));
        }
        let hit = self.cached_chunk(hash, size).await?;
        let peers = self.0.peers.as_ref();
        let mut candidates = None;
        let mut next_sources = || Sources {
            peer: candidates.get_or_insert_with(|| {
                peers.map_or_else(Vec::new, |peers| peers.candidates(hash)).into_iter()
            }).next(),
            origin_available: self.0.origin.is_some(),
        };
        let mut verified = None;
        let mut tier = FetchTier::Origin;
        let mut details = std::collections::BTreeMap::new();
        let mut failure = Error::FetchFailed { hash, detail: "no eligible peer or backing store".into() };
        let mut state = rust_fsm::StateMachine::<ChunkTransfer>::new();
        let mut event = if hit.is_some() {
            ChunkEvent::CacheHit
        } else {
            ChunkEvent::CacheMiss(next_sources())
        };
        loop {
            let action = state.consume(&event).expect("valid transfer transition").expect("transfer action");
            event = match action {
                ChunkAction::ReadPeer(node) => {
                    match peers.expect("peer directory").fetch(node, hash, size as usize).await {
                        Ok(Some(chunk)) => {
                            tier = FetchTier::Peer;
                            details.insert(node.to_string(), size);
                            verified = Some(chunk);
                            ChunkEvent::Verified
                        }
                        Ok(None) => ChunkEvent::PeerSkipped(next_sources()),
                        Err(error) => {
                            failure = Error::FetchFailed { hash, detail: format!("peer {node}: {error}") };
                            ChunkEvent::PeerFailed(next_sources())
                        }
                    }
                }
                ChunkAction::ReadOrigin => {
                    let origin = self.0.origin.as_ref().expect("selected backing store");
                    match origin.read_chunk(hash, size as usize).await {
                        Ok(body) => match VerifiedChunk::verify(hash, size, body).await {
                            Ok(chunk) => {
                                verified = Some(chunk);
                                ChunkEvent::Verified
                            }
                            Err(error) => {
                                failure = Error::OriginRejected { hash, detail: error.to_string() };
                                ChunkEvent::OriginFailed
                            }
                        },
                        Err(error) => {
                            failure = Error::FetchFailed { hash, detail: format!("origin: {error}") };
                            ChunkEvent::OriginFailed
                        }
                    }
                }
                ChunkAction::Store => {
                    self.0.cache.insert_verified(verified.as_ref().expect("verified chunk").clone());
                    ChunkEvent::Stored
                }
                ChunkAction::Announce => {
                    if let Some(peers) = peers {
                        peers.announce(hash);
                    }
                    ChunkEvent::Announced
                }
                ChunkAction::Return => return hit.or_else(|| verified.map(|chunk| Fetched {
                        content: chunk.body().clone(),
                        size,
                        tier,
                        peer_bytes: if tier == FetchTier::Peer { size } else { 0 },
                        origin_bytes: if tier == FetchTier::Origin { size } else { 0 },
                        peers: details,
                    })).ok_or(failure),
            };
        }
    }

    async fn fetch(&self, hash: BlakeHash, size: u64, priority: TransferPriority) -> Result<Fetched> {
        if let Some(hit) = self.cached_chunk(hash, size).await? {
            return Ok(hit);
        }
        loop {
            let (in_flight_size, mut flight) = self.join_flight(hash, size, priority)?;
            let outcome = flight.wait_for(Option::is_some).await.map_err(|_| Error::FetchFailed {
                hash, detail: "the transfer task ended without an outcome".into(),
            })?.clone().expect("completed transfer");
            if let Ok(fetched) = &outcome {
                if fetched.size != size {
                    return Err(Error::Store(format!("CAS chunk {hash} has an unexpected length")));
                }
                return outcome;
            }
            if in_flight_size == size {
                return outcome;
            }
        }
    }
}

/// A chunk named by a validated model or image index.
#[derive(Clone)]
pub struct Asset {
    class: AssetClass,
    hash: BlakeHash,
    size: u64,
}

impl Asset {
    pub async fn read_chunk(&self) -> Result<Bytes> {
        self.read_chunk_reported().await.0
    }

    /// Read the chunk for a waiting reader.
    pub async fn read_chunk_reported(&self) -> (Result<Bytes>, FetchReport) {
        self.read_chunk_with(TransferPriority::Demand).await
    }

    #[tracing::instrument(name = "xlb.chunk.fetch", skip_all, fields(class = self.class.0.name, hash = %self.hash, ?priority, source = tracing::field::Empty, peer = tracing::field::Empty, bytes = tracing::field::Empty, success = tracing::field::Empty, error = tracing::field::Empty))]
    pub async fn read_chunk_with(&self, priority: TransferPriority) -> (Result<Bytes>, FetchReport) {
        let started = Instant::now();
        let outcome = self.class.fetch(self.hash, self.size, priority).await;
        let fetched = outcome.as_ref().ok();
        let report = FetchReport {
            class: self.class.0.name,
            hash: self.hash,
            tier: fetched.map(|chunk| chunk.tier),
            peer_id: fetched.and_then(|chunk| chunk.peers.keys().next().cloned()),
            bytes_served: fetched.map_or(0, |chunk| chunk.size),
            peer_bytes: fetched.map_or(0, |chunk| chunk.peer_bytes),
            origin_bytes: fetched.map_or(0, |chunk| chunk.origin_bytes),
            peers: fetched.map(|chunk| chunk.peers.clone()).unwrap_or_default(),
            duration_ms: started.elapsed().as_millis() as u64,
            failure_kind: outcome.as_ref().err().map(Error::failure_kind),
        };
        let span = tracing::Span::current();
        span.record("source", report.tier_label());
        span.record("peer", report.peer_id.as_deref().unwrap_or(""));
        span.record("bytes", report.bytes_served);
        span.record("success", outcome.is_ok());
        if let Err(error) = &outcome {
            span.record("error", tracing::field::display(error));
            tracing::warn!(class = self.class.0.name, hash = %self.hash, cause = %error, kind = error.failure_kind(), "XLB chunk fetch failed");
        }
        if let Some(observer) = &self.class.0.observer {
            observer(&report);
        }
        (outcome.map(|chunk| chunk.content), report)
    }

    pub fn is_cached(&self) -> bool {
        self.class.0.cache.may_hold(self.hash)
    }
}
