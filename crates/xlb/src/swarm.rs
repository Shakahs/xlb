//! Load missing CAS ranges from one source per range. Complete providers
//! come from the local smol-kv directory. Tokio runs the disjoint reads,
//! and Tower regulates when requests start.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::Duration,
};

use futures::{StreamExt, TryStreamExt, stream};
use iroh_blobs::{
    Hash as IrohHash,
    api::{Store, proto::Bitfield},
    protocol::{ChunkRanges, ChunkRangesExt},
};
use tokio::sync::{OnceCell, watch};

use crate::{
    BlakeHash, Error,
    discovery::{ProviderEvent, is_fresh},
    origin::{CHUNK_GROUP_BYTES, OUTBOARD_KEY_SUFFIX, Origin, OriginError, OriginOutboard},
    peer::{NodeId, PeerTransport, ProviderChoice},
    rate::RequestRate,
    store::to_iroh,
};

#[cfg(test)]
#[path = "swarm_tests.rs"]
mod tests;

pub(crate) struct SwarmSources {
    pub(crate) hash: BlakeHash,
    pub(crate) store: Store,
    pub(crate) peers: Option<Arc<PeerTransport>>,
    pub(crate) origin: Option<Origin>,
    pub(crate) incoming_dir: PathBuf,
    pub(crate) indexed_size: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SwarmProgress {
    pub(crate) size: u64,
    pub(crate) peer_bytes: u64,
    pub(crate) origin_bytes: u64,
}

pub(crate) enum SwarmFailure {
    Miss(String),
    Fatal(Error),
}

type ProviderList = Option<Arc<[ProviderChoice]>>;

// [impl -> dsn~xlb-chunk-discovery~1]
async fn follow_providers(peers: &PeerTransport, hash: IrohHash, updates: &watch::Sender<ProviderList>) {
    let mut available = HashMap::<NodeId, ProviderChoice>::new();
    loop {
        let records = peers.providers(hash);
        tokio::pin!(records);
        let mut current_done = false;
        while let Some(record) = records.next().await {
            match record {
                ProviderEvent::Available { address, location, timestamp } => {
                    if is_fresh(timestamp) && available.get(&address.id).is_none_or(|entry| timestamp >= entry.timestamp) {
                        peers.learn_provider(&address);
                        available.insert(address.id, ProviderChoice {
                            node: address.id, same_location: peers.shares_location(&location), timestamp,
                        });
                    }
                }
                ProviderEvent::Expired { node, timestamp } => {
                    if available.get(&node).is_some_and(|entry| entry.timestamp == timestamp) {
                        available.remove(&node);
                    }
                }
                ProviderEvent::CurrentDone => current_done = true,
            }
            if current_done {
                available.retain(|_, provider| is_fresh(provider.timestamp));
                updates.send_replace(Some(available.values().copied().collect()));
            }
        }
        updates.send_if_modified(|providers| {
            if providers.is_some() {
                return false;
            }
            *providers = Some(Arc::new([]));
            true
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

struct SwarmFetch {
    sources: SwarmSources,
    hash: IrohHash,
    providers: watch::Receiver<ProviderList>,
    outboard: OnceCell<OriginOutboard>,
    origin_rate: RequestRate,
    peer_rate: RequestRate,
    origin_bytes: AtomicU64,
    peer_bytes: AtomicU64,
}

fn origin_failure(hash: BlakeHash, error: OriginError) -> SwarmFailure {
    match error {
        OriginError::Rejected(detail) => SwarmFailure::Fatal(Error::OriginRejected { hash, detail }),
        other => SwarmFailure::Miss(format!("origin: {other}")),
    }
}

fn missing_ranges(held: &ChunkRanges, (start, end): (u64, u64)) -> Vec<(u64, u64)> {
    let missing = &ChunkRanges::bytes(start..end) - held;
    missing.boundaries().as_chunks::<2>().0.iter()
        .map(|[start, stop]| (start.to_bytes(), stop.to_bytes().min(end)))
        .collect()
}

impl SwarmFetch {
    async fn local_bitfield(&self) -> Result<Bitfield, SwarmFailure> {
        self.sources.store.blobs().observe(self.hash).await
            .map_err(|err| SwarmFailure::Fatal(Error::Store(err.to_string())))
    }

    fn eligible_providers(&self, peers: &PeerTransport) -> Vec<ProviderChoice> {
        // The KV subscription supplies a snapshot and subsequent changes.
        self.providers.borrow().as_deref().unwrap_or_default().iter().copied()
            .filter(|entry| is_fresh(entry.timestamp) && peers.is_eligible(&entry.node))
            .collect()
    }

    async fn wait_for_providers(&self, peers: &PeerTransport) -> Vec<ProviderChoice> {
        if !peers.has_sources() {
            return Vec::new();
        }
        let mut providers = self.providers.clone();
        let wait = providers.wait_for(|list| list.as_deref().is_some_and(|list| {
            list.iter().any(|entry| is_fresh(entry.timestamp) && peers.is_eligible(&entry.node))
        }));
        let _ = tokio::time::timeout(peers.config().stall_timeout, wait).await;
        self.eligible_providers(peers)
    }

    /// Establish the verified length with reads from one source at a time.
    // [impl -> dsn~xlb-swarm-fetch~1]
    async fn establish_size(
        &self,
        admit: &(impl Fn(u64) -> Result<(), Error> + Sync),
    ) -> Result<u64, SwarmFailure> {
        if let Some(size) = self.local_bitfield().await?.validated_size() {
            admit(size).map_err(SwarmFailure::Fatal)?;
            return Ok(size);
        }
        let mut peer_attempted = false;
        if let Some(peers) = self.sources.peers.as_deref() {
            let providers = if self.sources.origin.is_some() {
                self.eligible_providers(peers)
            } else {
                self.wait_for_providers(peers).await
            };
            if !providers.is_empty() {
                peer_attempted = true;
                self.read_peer(peers, ChunkRanges::last_chunk(), providers).await.ok();
                if let Some(size) = self.local_bitfield().await?.validated_size() {
                    admit(size).map_err(SwarmFailure::Fatal)?;
                    return Ok(size);
                }
            }
        }
        let Some(origin) = self.sources.origin.as_ref() else {
            return Err(SwarmFailure::Miss("peers: no provider proved the blob size".into()));
        };
        let origin_result = self.establish_origin_size(origin, admit).await;
        if matches!(origin_result, Err(SwarmFailure::Miss(_))) && !peer_attempted {
            if let Some(peers) = self.sources.peers.as_deref() {
                let providers = self.wait_for_providers(peers).await;
                if !providers.is_empty() {
                    self.read_peer(peers, ChunkRanges::last_chunk(), providers).await.ok();
                    if let Some(size) = self.local_bitfield().await?.validated_size() {
                        admit(size).map_err(SwarmFailure::Fatal)?;
                        return Ok(size);
                    }
                }
            }
        }
        origin_result
    }

    async fn establish_origin_size(
        &self,
        origin: &Origin,
        admit: &(impl Fn(u64) -> Result<(), Error> + Sync),
    ) -> Result<u64, SwarmFailure> {
        let hash = self.sources.hash;
        let size = origin.object_size(&hash).await.map_err(|err| origin_failure(hash, err))?;
        if size == 0 {
            return Err(SwarmFailure::Miss("origin: the object is empty".into()));
        }
        admit(size).map_err(SwarmFailure::Fatal)?;
        let start = (size - 1) / CHUNK_GROUP_BYTES * CHUNK_GROUP_BYTES;
        self.read_origin(origin, size, (start, size)).await?;
        match self.local_bitfield().await?.validated_size() {
            Some(proven) if proven == size => Ok(size),
            _ => Err(SwarmFailure::Miss("origin: the last chunk group did not prove the size".into())),
        }
    }

    async fn read_origin(&self, origin: &Origin, size: u64, range: (u64, u64)) -> Result<(), SwarmFailure> {
        let hash = self.sources.hash;
        let outboard = self.outboard.get_or_try_init(|| async {
            let path = self.sources.incoming_dir.join(format!("{}{OUTBOARD_KEY_SUFFIX}", hash.to_hex()));
            origin.fetch_outboard(&hash, size, path).await
        }).await.map_err(|err| origin_failure(hash, err))?;
        origin.import_range(&self.sources.store, hash, outboard, range, &self.origin_bytes, &self.origin_rate)
            .await.map_err(|err| origin_failure(hash, err))
    }

    async fn read_peer(
        &self,
        peers: &PeerTransport,
        ranges: ChunkRanges,
        providers: Vec<ProviderChoice>,
    ) -> Result<(), SwarmFailure> {
        let mut reported = 0;
        peers.fetch_ranges(self.hash, ranges, providers, &self.peer_rate, |received| {
            self.peer_bytes.fetch_add(received - reported, Ordering::Relaxed);
            reported = received;
        }).await.map_err(|err| SwarmFailure::Miss(format!("peers: {err}")))
    }

    async fn read_range(&self, size: u64, range: (u64, u64)) -> Result<(), SwarmFailure> {
        let mut peer_attempted = false;
        if let Some(peers) = self.sources.peers.as_deref() {
            let mut providers = self.eligible_providers(peers);
            if providers.is_empty() && self.sources.origin.is_none() {
                providers = self.wait_for_providers(peers).await;
            }
            if !providers.is_empty() {
                peer_attempted = true;
                let result = self.read_peer(peers, ChunkRanges::bytes(range.0..range.1), providers).await;
                if result.is_ok() || self.sources.origin.is_none() {
                    return result;
                }
            }
        }
        if let Some(origin) = self.sources.origin.as_ref() {
            // A failed peer attempt may have verified part of this range.
            if peer_attempted {
                let held = self.local_bitfield().await?;
                for remaining in missing_ranges(&held.ranges, range) {
                    self.read_origin(origin, size, remaining).await?;
                }
                return Ok(());
            }
            let result = self.read_origin(origin, size, range).await;
            if matches!(result, Err(SwarmFailure::Miss(_))) {
                if let Some(peers) = self.sources.peers.as_deref() {
                    let providers = self.wait_for_providers(peers).await;
                    if !providers.is_empty() {
                        return self.read_peer(peers, ChunkRanges::bytes(range.0..range.1), providers).await;
                    }
                }
            }
            return result;
        }
        Err(SwarmFailure::Miss("peers: no eligible holder for the range".into()))
    }

    fn progress(&self, size: u64) -> SwarmProgress {
        SwarmProgress {
            size,
            peer_bytes: self.peer_bytes.load(Ordering::Relaxed).min(size),
            origin_bytes: self.origin_bytes.load(Ordering::Relaxed).min(size),
        }
    }

    async fn transfer(&self, size: u64, progress: impl Fn(SwarmProgress) + Send) -> Result<SwarmProgress, SwarmFailure> {
        let origin_bytes = self.sources.origin.as_ref().map_or(size, Origin::range_bytes);
        let peer_bytes = self.sources.peers.as_ref().map_or(size, |peers| peers.config().segment_bytes);
        let range_bytes = if self.sources.indexed_size.is_some() {
            size
        } else {
            origin_bytes.min(peer_bytes).max(CHUNK_GROUP_BYTES) / CHUNK_GROUP_BYTES * CHUNK_GROUP_BYTES
        };
        let held = self.local_bitfield().await?;
        let concurrency = self.sources.origin.as_ref().map(Origin::concurrency)
            .or_else(|| self.sources.peers.as_ref().map(|peer| peer.config().concurrency))
            .unwrap_or(1);
        let gather = stream::iter((0..size).step_by(range_bytes as usize))
            .flat_map(|start| {
                let end = start.saturating_add(range_bytes).min(size);
                stream::iter(missing_ranges(&held.ranges, (start, end)))
            })
            .map(|range| self.read_range(size, range))
            .buffer_unordered(concurrency.max(1))
            .try_for_each(|()| std::future::ready(Ok(())));
        let report = async {
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            let mut previous = None;
            loop {
                interval.tick().await;
                let current = self.progress(size);
                if previous != Some(current) {
                    progress(current);
                    previous = Some(current);
                }
            }
        };
        tokio::select! {
            outcome = gather => outcome?,
            () = report => unreachable!("progress reports run until the reads finish"),
        }
        let stored = self.local_bitfield().await?;
        if stored.is_complete() && stored.validated_size() == Some(size) {
            Ok(self.progress(size))
        } else {
            Err(SwarmFailure::Miss("stored blob does not match the expected size".into()))
        }
    }
}

/// Read disjoint ranges from the best eligible current source.
// [impl -> dsn~xlb-swarm-fetch~1]
pub(crate) async fn fetch(
    sources: SwarmSources,
    admit: impl Fn(u64) -> Result<(), Error> + Send + Sync,
    progress: impl Fn(SwarmProgress) + Send,
) -> Result<SwarmProgress, SwarmFailure> {
    let hash = to_iroh(&sources.hash);
    let peers = sources.peers.clone();
    let (updates, providers) = watch::channel(None);
    let fetch = async {
        let mut providers = providers;
        if peers.is_some() {
            let _ = providers.wait_for(Option::is_some).await;
        }
        let swarm = SwarmFetch {
            hash,
            providers,
            origin_rate: RequestRate::new(sources.origin.as_ref().map_or(1, Origin::concurrency)),
            peer_rate: RequestRate::new(peers.as_ref().map_or(1, |peer| peer.config().concurrency)),
            sources,
            outboard: OnceCell::new(),
            origin_bytes: AtomicU64::new(0),
            peer_bytes: AtomicU64::new(0),
        };
        let size = match swarm.sources.indexed_size {
            Some(0) => return Err(SwarmFailure::Miss("the index names an empty chunk".into())),
            Some(size) => {
                admit(size).map_err(SwarmFailure::Fatal)?;
                size
            }
            None => swarm.establish_size(&admit).await?,
        };
        swarm.transfer(size, progress).await
    };
    if let Some(transport) = peers.as_deref() {
        tokio::select! {
            outcome = fetch => outcome,
            () = follow_providers(transport, hash, &updates) => unreachable!("provider lookup runs until the fetch finishes"),
        }
    } else {
        fetch.await
    }
}
