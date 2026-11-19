//! Swarm fetch: nodes fetching the same blob share what each has verified so
//! far and divide the origin reads between them.
//!
//! Every range lands in the blob store as it is verified, from a peer or
//! from the origin, so a partial blob is served to peers while it downloads.
//!
//! 1. **Size.** The fetch obtains the blob's last chunk group, from a peer
//!    whose bitfield shows it has it, else from the origin. That proves the
//!    size and creates the partial entry other nodes observe.
//! 2. **Fetchers.** The fetch observes the blob on every peer for as long as
//!    it runs. A peer with a non-empty, incomplete bitfield is a fellow
//!    fetcher; a complete one is a seed. A peer that refuses or fails the
//!    observe request is neither, so peer relations may be one-directional
//!    and two nodes may see different fetchers.
//! 3. **Segments.** The blob is cut into segments of
//!    [`PeerConfig::segment_bytes`](crate::PeerConfig::segment_bytes). A
//!    segment some peer holds completely is read from the peers that hold
//!    it, starting at a different one per segment. A segment nobody holds is
//!    read from the origin by its origin reader: the fetcher with the
//!    highest rendezvous weight for (blob hash, segment index, node ID).
//!    This node reads its own segments from the origin in ascending order,
//!    as [`OriginConfig::range_bytes`](crate::OriginConfig::range_bytes)
//!    ranges.
//! 4. **Takeover.** A segment nobody holds that saw no progress on any peer
//!    for [`PeerConfig::stall_timeout`](crate::PeerConfig::stall_timeout) is
//!    read from the origin by the node that needs it, last segment first so
//!    it meets its assigned reader as late as possible. A reader that leaves
//!    the fetcher set drops out of the rendezvous at once.
//!
//! The outboard is fetched from the origin when the first origin range is
//! needed. With no peers every segment is this node's own and the blob comes
//! from the origin with the origin's concurrency.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    path::Path,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use futures::{stream::FuturesUnordered, StreamExt};
use iroh_blobs::{
    api::{proto::Bitfield, Store},
    protocol::{ChunkRanges, ChunkRangesExt},
    Hash as IrohHash,
};
use tokio::{sync::Semaphore, task::JoinSet, time::Instant};

use crate::{
    origin::{Origin, OriginError, OriginOutboard, CHUNK_GROUP_BYTES, OUTBOARD_KEY_SUFFIX},
    peer::{BitfieldWatch, NodeId, PeerTransport},
    store::to_iroh,
    BlakeHash, Error,
};

/// How often the fetch looks at the peers' bitfields and its free slots, and
/// publishes progress. Bitfields change far more often than a segment
/// finishes; reading them on a tick keeps their churn off the transfer.
const SCHEDULE_INTERVAL: Duration = Duration::from_millis(50);
/// Pause before a failed observe request to a peer is sent again.
const OBSERVE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
/// BLAKE3 chunk size, the unit of a bitfield.
const CHUNK_BYTES: u64 = 1024;

/// Where one fetch may read from.
pub(crate) struct SwarmSources<'a> {
    pub(crate) hash: BlakeHash,
    pub(crate) store: &'a Store,
    pub(crate) peers: Option<&'a PeerTransport>,
    pub(crate) origin: Option<&'a Origin>,
    /// Directory for the outboard file while the fetch runs.
    pub(crate) incoming_dir: &'a Path,
}

/// Bytes a fetch has received so far, by where they came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SwarmProgress {
    pub(crate) size: u64,
    pub(crate) peer_bytes: u64,
    pub(crate) origin_bytes: u64,
}

pub(crate) enum SwarmFailure {
    /// No source produced the blob; the fetch is a miss.
    Miss(String),
    /// The fetch must fail with this error.
    Fatal(Error),
}

/// The fixed segments of one blob.
#[derive(Debug, Clone, Copy)]
struct SegmentLayout {
    size: u64,
    segment_bytes: u64,
}

impl SegmentLayout {
    fn count(&self) -> usize {
        usize::try_from(self.size.div_ceil(self.segment_bytes)).unwrap_or(usize::MAX)
    }

    /// The half-open byte range of segment `index`.
    fn bounds(&self, index: usize) -> (u64, u64) {
        let start = index as u64 * self.segment_bytes;
        (start, (start + self.segment_bytes).min(self.size))
    }

    /// The segments that chunks `[first, end)` touch; `end` of `None` is the
    /// end of the blob.
    fn touched(&self, first: u64, end: Option<u64>) -> std::ops::Range<usize> {
        let start = first.saturating_mul(CHUNK_BYTES);
        let end = end
            .map_or(self.size, |end| end.saturating_mul(CHUNK_BYTES))
            .min(self.size);
        if start >= end {
            return 0..0;
        }
        (start / self.segment_bytes) as usize..(end.div_ceil(self.segment_bytes)) as usize
    }
}

#[derive(Default)]
struct PeerView {
    /// The latest bitfield of every peer whose observe request is answered.
    bitfields: HashMap<NodeId, Bitfield>,
    /// Peers whose first observe request was answered or failed.
    settled: HashSet<NodeId>,
    layout: Option<SegmentLayout>,
    /// Per segment, when a peer's bitfield last grew inside it.
    progressed: Vec<Instant>,
}

/// What the peers hold of the blob, written by one observer task per peer.
#[derive(Default)]
struct SharedView(Mutex<PeerView>);

impl SharedView {
    fn lock(&self) -> MutexGuard<'_, PeerView> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn record(&self, node: NodeId, held: &Bitfield, added: &ChunkRanges) {
        let mut view = self.lock();
        view.bitfields.insert(node, held.clone());
        view.settled.insert(node);
        let Some(layout) = view.layout else { return };
        let now = Instant::now();
        // Boundaries alternate between the start and the end of a range; a
        // last range without an end runs to the end of the blob.
        for range in added.boundaries().chunks(2) {
            let end = range.get(1).map(|end| end.0);
            for index in layout.touched(range[0].0, end) {
                if let Some(progressed) = view.progressed.get_mut(index) {
                    *progressed = now;
                }
            }
        }
    }

    /// The peer answers no observe request: it holds nothing for this node.
    fn forget(&self, node: NodeId) {
        let mut view = self.lock();
        view.bitfields.remove(&node);
        view.settled.insert(node);
    }

    fn remove(&self, node: NodeId) {
        let mut view = self.lock();
        view.bitfields.remove(&node);
        view.settled.remove(&node);
    }
}

/// Observe `hash` on `node` for as long as the fetch runs.
// [impl -> dsn~xlb-swarm-fetch~1]
async fn observe_peer(watch: BitfieldWatch, node: NodeId, hash: IrohHash, view: Arc<SharedView>) {
    loop {
        let ended = watch
            .follow(node, hash, |held, added| view.record(node, held, added))
            .await;
        tracing::debug!(%node, %hash, "peer bitfield unavailable: {ended}");
        view.forget(node);
        tokio::time::sleep(OBSERVE_RETRY_INTERVAL).await;
    }
}

/// One observer task per member of the peer set.
struct PeerObservers<'a> {
    transport: Option<&'a PeerTransport>,
    hash: IrohHash,
    view: Arc<SharedView>,
    tasks: JoinSet<()>,
    watched: HashMap<NodeId, tokio::task::AbortHandle>,
}

impl PeerObservers<'_> {
    /// Follow the current peer set: observe new members, drop removed ones.
    fn follow_peer_set(&mut self) {
        let Some(transport) = self.transport else {
            return;
        };
        let current: HashSet<NodeId> = transport.peer_ids().into_iter().collect();
        self.watched.retain(|node, task| {
            let member = current.contains(node);
            if !member {
                task.abort();
                self.view.remove(*node);
            }
            member
        });
        for node in current {
            if !self.watched.contains_key(&node) {
                let observer =
                    observe_peer(transport.bitfield_watch(), node, self.hash, self.view.clone());
                self.watched.insert(node, self.tasks.spawn(observer));
            }
        }
    }
}

/// The fetcher that reads segment `index` of `hash` from the origin: the one
/// with the highest rendezvous weight. Every node that sees the same
/// fetchers picks the same reader, and a fetcher that joins or leaves moves
/// only the segments it gains or loses.
// [impl -> dsn~xlb-swarm-fetch~1]
fn origin_reader(hash: &BlakeHash, index: usize, fetchers: &[[u8; 32]]) -> Option<[u8; 32]> {
    fetchers.iter().copied().max_by_key(|fetcher| {
        let mut input = [0u8; 72];
        input[..32].copy_from_slice(hash.as_bytes());
        input[32..40].copy_from_slice(&(index as u64).to_le_bytes());
        input[40..].copy_from_slice(fetcher);
        *BlakeHash::hash(&input).as_bytes()
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SegmentState {
    Pending,
    Running,
    Done,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SegmentSource {
    Peer,
    Origin,
}

type SegmentRead<'a> =
    Pin<Box<dyn Future<Output = (usize, SegmentSource, Result<(), SwarmFailure>)> + Send + 'a>>;

/// The parts of a running fetch its segment reads share.
struct SwarmFetch<'a> {
    sources: SwarmSources<'a>,
    hash: IrohHash,
    view: Arc<SharedView>,
    outboard: tokio::sync::OnceCell<Arc<OriginOutboard>>,
    /// Limits the ranged GETs in flight to the origin's concurrency.
    origin_requests: Arc<Semaphore>,
    peer_bytes: AtomicU64,
    origin_bytes: Arc<AtomicU64>,
}

fn origin_failure(hash: BlakeHash, error: OriginError) -> SwarmFailure {
    match error {
        OriginError::Rejected(detail) => SwarmFailure::Fatal(Error::OriginRejected { hash, detail }),
        other => SwarmFailure::Miss(format!("origin: {other}")),
    }
}

impl SwarmFetch<'_> {
    async fn local_bitfield(&self) -> Result<Bitfield, SwarmFailure> {
        self.sources
            .store
            .blobs()
            .observe(self.hash)
            .await
            .map_err(|e| SwarmFailure::Fatal(Error::Store(e.to_string())))
    }

    fn progress(&self, size: u64) -> SwarmProgress {
        SwarmProgress {
            size,
            peer_bytes: self.peer_bytes.load(Ordering::Relaxed).min(size),
            origin_bytes: self.origin_bytes.load(Ordering::Relaxed).min(size),
        }
    }

    /// The blob's outboard, fetched from the origin on first use.
    // [impl -> dsn~xlb-swarm-fetch~1]
    async fn outboard(&self, origin: &Origin, size: u64) -> Result<Arc<OriginOutboard>, SwarmFailure> {
        let hash = self.sources.hash;
        self.outboard
            .get_or_try_init(|| async {
                let path = self
                    .sources
                    .incoming_dir
                    .join(format!("{}{OUTBOARD_KEY_SUFFIX}", hash.to_hex()));
                origin.fetch_outboard(&hash, size, path).await.map(Arc::new)
            })
            .await
            .cloned()
            .map_err(|e| origin_failure(hash, e))
    }

    /// Wait until every peer has answered its first observe request or
    /// failed it, at most for the connect timeout.
    async fn await_first_answers(&self, peers: &PeerTransport) {
        let expected = peers.peer_ids();
        let deadline = Instant::now() + peers.config().connect_timeout;
        while Instant::now() < deadline {
            let settled = {
                let view = self.view.lock();
                expected.iter().all(|node| view.settled.contains(node))
            };
            if settled {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Obtain the blob's last chunk group, which proves the size and makes
    /// this node's partial entry visible to its peers. Returns the size.
    // [impl -> dsn~xlb-swarm-fetch~1]
    async fn establish_size(
        &self,
        admit: &(impl Fn(u64) -> Result<(), Error> + Sync),
    ) -> Result<u64, SwarmFailure> {
        if let Some(size) = self.local_bitfield().await?.validated_size() {
            admit(size).map_err(SwarmFailure::Fatal)?;
            return Ok(size);
        }
        let mut misses: Vec<String> = Vec::new();
        if let Some(peers) = self.sources.peers {
            self.await_first_answers(peers).await;
            let holders: Vec<NodeId> = {
                let view = self.view.lock();
                let mut holders: Vec<NodeId> = view
                    .bitfields
                    .iter()
                    .filter(|(_, held)| held.is_validated())
                    .map(|(node, _)| *node)
                    .collect();
                holders.sort();
                holders
            };
            if holders.is_empty() {
                misses.push("peers: no peer holds the blob".into());
            } else {
                let probe = peers
                    .fetch_ranges(self.hash, ChunkRanges::last_chunk(), holders, |_| {})
                    .await;
                match (probe, self.local_bitfield().await?.validated_size()) {
                    (Ok(()), Some(size)) => {
                        admit(size).map_err(SwarmFailure::Fatal)?;
                        return Ok(size);
                    }
                    (Ok(()), None) => misses.push("peers: did not prove the blob size".into()),
                    (Err(e), _) => misses.push(format!("peers: {e}")),
                }
            }
        }
        let Some(origin) = self.sources.origin else {
            if misses.is_empty() {
                misses.push("no peer tier and no origin configured".into());
            }
            return Err(SwarmFailure::Miss(misses.join("; ")));
        };
        let hash = self.sources.hash;
        let miss = |error: OriginError, misses: &[String]| match origin_failure(hash, error) {
            SwarmFailure::Miss(detail) => {
                let mut all = misses.to_vec();
                all.push(detail);
                SwarmFailure::Miss(all.join("; "))
            }
            fatal => fatal,
        };
        let size = origin
            .object_size(&hash)
            .await
            .map_err(|e| miss(e, &misses))?;
        if size == 0 {
            return Err(SwarmFailure::Miss("origin: the object is empty".into()));
        }
        admit(size).map_err(SwarmFailure::Fatal)?;
        let outboard = self.outboard(origin, size).await?;
        let last_group = (size - 1) / CHUNK_GROUP_BYTES * CHUNK_GROUP_BYTES;
        origin
            .import_range(
                self.sources.store,
                hash,
                &outboard,
                (last_group, size),
                &self.origin_bytes,
            )
            .await
            .map_err(|e| miss(e, &misses))?;
        match self.local_bitfield().await?.validated_size() {
            Some(proven) if proven == size => Ok(size),
            _ => Err(SwarmFailure::Miss(
                "origin: the last chunk group did not prove the size".into(),
            )),
        }
    }

    /// Read bytes `[start, end)` from the origin as concurrent ranges. The
    /// blob's last chunk group is already stored and is not read again.
    // [impl -> dsn~xlb-swarm-fetch~1]
    async fn read_origin_segment(
        &self,
        origin: &Origin,
        size: u64,
        (start, end): (u64, u64),
    ) -> Result<(), SwarmFailure> {
        let hash = self.sources.hash;
        let outboard = self.outboard(origin, size).await?;
        let last_group = (size - 1) / CHUNK_GROUP_BYTES * CHUNK_GROUP_BYTES;
        let end = end.min(last_group);
        let range_bytes = origin.range_bytes();
        // Dropping the set on an early return aborts the other ranges.
        let mut ranges = JoinSet::new();
        let mut range_start = start;
        while range_start < end {
            let range = (range_start, (range_start + range_bytes).min(end));
            range_start = range.1;
            let (origin, store) = (origin.clone(), self.sources.store.clone());
            let (outboard, received) = (outboard.clone(), self.origin_bytes.clone());
            let requests = self.origin_requests.clone();
            // Spawned so ranges are hashed on all the runtime's threads.
            ranges.spawn(async move {
                let _slot = requests.acquire_owned().await;
                origin
                    .import_range(&store, hash, &outboard, range, &received)
                    .await
            });
        }
        while let Some(joined) = ranges.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(origin_failure(hash, e)),
                Err(e) => return Err(SwarmFailure::Miss(format!("origin: range task: {e}"))),
            }
        }
        Ok(())
    }

    /// Read `ranges` from `holders`, counting the bytes as peer bytes.
    async fn read_peer_segment(
        &self,
        peers: &PeerTransport,
        ranges: ChunkRanges,
        holders: Vec<NodeId>,
    ) -> Result<(), SwarmFailure> {
        // The downloader reports the cumulative bytes of the request and
        // starts over when it moves on to the next peer.
        let mut reported = 0u64;
        let count = |bytes: u64| {
            if bytes >= reported {
                self.peer_bytes.fetch_add(bytes - reported, Ordering::Relaxed);
            } else {
                self.peer_bytes.fetch_sub(reported - bytes, Ordering::Relaxed);
            }
            reported = bytes;
        };
        peers
            .fetch_ranges(self.hash, ranges, holders, count)
            .await
            .map_err(|e| SwarmFailure::Miss(format!("peers: {e}")))
    }
}

/// Fetch the blob into the store from the peers and the origin.
///
/// `admit` sees the size before anything but the last chunk group is
/// transferred. `progress` is called from the fetch's own task between
/// scheduling passes, never from a transfer. Returns the final byte counts
/// once the store holds the blob completely.
// [impl -> dsn~xlb-swarm-fetch~1]
pub(crate) async fn fetch(
    sources: SwarmSources<'_>,
    admit: impl Fn(u64) -> Result<(), Error> + Send + Sync,
    progress: impl Fn(SwarmProgress) + Send,
) -> Result<SwarmProgress, SwarmFailure> {
    let hash = to_iroh(&sources.hash);
    let view = Arc::new(SharedView::default());
    let mut observers = PeerObservers {
        transport: sources.peers,
        hash,
        view: view.clone(),
        tasks: JoinSet::new(),
        watched: HashMap::new(),
    };
    observers.follow_peer_set();
    let (peers, origin) = (sources.peers, sources.origin);
    let own_id: [u8; 32] = peers.map_or([0; 32], |peers| *peers.node_id().as_bytes());
    let swarm = SwarmFetch {
        origin_requests: Arc::new(Semaphore::new(origin.map_or(1, Origin::concurrency))),
        sources,
        hash,
        view,
        outboard: tokio::sync::OnceCell::new(),
        peer_bytes: AtomicU64::new(0),
        origin_bytes: Arc::new(AtomicU64::new(0)),
    };

    let size = swarm.establish_size(&admit).await?;

    // Without a peer tier a segment is one origin range.
    let range_bytes = origin.map(Origin::range_bytes);
    let segment_bytes = match peers {
        Some(peers) => (peers.config().segment_bytes / CHUNK_GROUP_BYTES).max(1) * CHUNK_GROUP_BYTES,
        None => range_bytes.unwrap_or(CHUNK_GROUP_BYTES),
    };
    let layout = SegmentLayout {
        size,
        segment_bytes,
    };
    let peer_limit = peers.map_or(0, |peers| peers.config().concurrency.max(1));
    // As many origin segments in flight as keep the origin's concurrency of
    // ranges busy.
    let origin_limit = match (origin, range_bytes) {
        (Some(origin), Some(range_bytes)) => {
            let segment_ranges = usize::try_from(segment_bytes.div_ceil(range_bytes)).unwrap_or(1);
            origin.concurrency().div_ceil(segment_ranges).max(1)
        }
        _ => 0,
    };
    let stall_timeout = peers.map(|peers| peers.config().stall_timeout);

    let held = swarm.local_bitfield().await?;
    let mut states: Vec<SegmentState> = (0..layout.count())
        .map(|index| {
            let (start, end) = layout.bounds(index);
            if held.ranges.is_superset(&ChunkRanges::bytes(start..end)) {
                SegmentState::Done
            } else {
                SegmentState::Pending
            }
        })
        .collect();
    // Segments whose peers failed to deliver what their bitfields promised.
    let mut peer_failed = vec![false; states.len()];
    {
        let mut view = swarm.view.lock();
        view.layout = Some(layout);
        view.progressed = vec![Instant::now(); states.len()];
    }

    let mut running: FuturesUnordered<SegmentRead<'_>> = FuturesUnordered::new();
    let (mut peer_reads, mut origin_reads) = (0usize, 0usize);
    let mut last_event = Instant::now();
    let mut reported = None;
    let mut ticker = tokio::time::interval(SCHEDULE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick is immediate. When this node's partial entry has only
    // just appeared, fellow fetchers that started at the same moment show up
    // in the bitfields within the next one.
    ticker.tick().await;
    if peers.is_some_and(|peers| !peers.peer_ids().is_empty()) {
        ticker.tick().await;
    }

    loop {
        observers.follow_peer_set();

        // ── Start what can be started ──────────────────────────────────────
        let mut holders_exist = false;
        if states.contains(&SegmentState::Pending)
            && (peer_reads < peer_limit || origin_reads < origin_limit)
        {
            let (bitfields, progressed) = {
                let view = swarm.view.lock();
                let mut bitfields: Vec<(NodeId, Bitfield)> = view
                    .bitfields
                    .iter()
                    .filter(|(_, held)| !held.ranges.is_empty())
                    .map(|(node, held)| (*node, held.clone()))
                    .collect();
                bitfields.sort_by_key(|(node, _)| *node);
                (bitfields, view.progressed.clone())
            };
            holders_exist = !bitfields.is_empty();
            let mut fetchers: Vec<[u8; 32]> = vec![own_id];
            fetchers.extend(
                bitfields
                    .iter()
                    .filter(|(_, held)| !held.is_complete())
                    .map(|(node, _)| *node.as_bytes()),
            );
            let now = Instant::now();
            let mut stalled: Vec<usize> = Vec::new();
            let mut starts: Vec<(usize, Option<Vec<NodeId>>)> = Vec::new();
            let (mut peer_free, mut origin_free) = (
                peer_limit.saturating_sub(peer_reads),
                origin_limit.saturating_sub(origin_reads),
            );
            for index in 0..states.len() {
                if peer_free == 0 && origin_free == 0 {
                    break;
                }
                if states[index] != SegmentState::Pending {
                    continue;
                }
                let (start, end) = layout.bounds(index);
                let wanted = ChunkRanges::bytes(start..end);
                let mut holders: Vec<NodeId> = if peer_failed[index] {
                    Vec::new()
                } else {
                    bitfields
                        .iter()
                        .filter(|(_, held)| held.ranges.is_superset(&wanted))
                        .map(|(node, _)| *node)
                        .collect()
                };
                if !holders.is_empty() {
                    if peer_free > 0 {
                        // Start each segment at a different holder so the
                        // segments in flight spread over them.
                        let first = index % holders.len();
                        holders.rotate_left(first);
                        starts.push((index, Some(holders)));
                        peer_free -= 1;
                    }
                    continue;
                }
                if origin_free == 0 {
                    continue;
                }
                let own = peer_failed[index]
                    || origin_reader(&swarm.sources.hash, index, &fetchers) == Some(own_id);
                if own {
                    starts.push((index, None));
                    origin_free -= 1;
                } else if stall_timeout
                    .is_some_and(|timeout| now.duration_since(progressed[index]) >= timeout)
                {
                    stalled.push(index);
                }
            }
            // Take over stalled segments from the end of the blob, away from
            // where their readers work.
            for index in stalled.into_iter().rev().take(origin_free) {
                tracing::debug!(%hash, index, "reading a stalled segment from the origin");
                starts.push((index, None));
            }
            for (index, holders) in starts {
                states[index] = SegmentState::Running;
                let bounds = layout.bounds(index);
                let swarm = &swarm;
                match (holders, peers, origin) {
                    (Some(holders), Some(peers), _) => {
                        peer_reads += 1;
                        let wanted = ChunkRanges::bytes(bounds.0..bounds.1);
                        running.push(Box::pin(async move {
                            let read = swarm.read_peer_segment(peers, wanted, holders).await;
                            (index, SegmentSource::Peer, read)
                        }));
                    }
                    (None, _, Some(origin)) => {
                        origin_reads += 1;
                        running.push(Box::pin(async move {
                            let read = swarm.read_origin_segment(origin, size, bounds).await;
                            (index, SegmentSource::Origin, read)
                        }));
                    }
                    _ => states[index] = SegmentState::Pending,
                }
            }
        }

        // ── Publish progress ───────────────────────────────────────────────
        let current = swarm.progress(size);
        if reported != Some(current) && current.peer_bytes + current.origin_bytes > 0 {
            progress(current);
            reported = Some(current);
        }

        if states.iter().all(|state| *state == SegmentState::Done) {
            break;
        }

        // ── Without an origin, nothing may ever supply what is missing ─────
        if running.is_empty() && origin.is_none() {
            let latest = swarm.view.lock().progressed.iter().copied().max();
            let quiet = Instant::now().duration_since(latest.map_or(last_event, |at| at.max(last_event)));
            if !holders_exist {
                return Err(SwarmFailure::Miss(
                    "peers: no peer holds the rest of the blob".into(),
                ));
            }
            if stall_timeout.is_some_and(|timeout| quiet >= timeout) {
                return Err(SwarmFailure::Miss(
                    "peers: the fetchers holding part of the blob stalled".into(),
                ));
            }
        }

        // ── Wait for a segment or the next look at the peers ───────────────
        tokio::select! {
            Some((index, source, read)) = running.next(), if !running.is_empty() => {
                last_event = Instant::now();
                match source {
                    SegmentSource::Peer => peer_reads -= 1,
                    SegmentSource::Origin => origin_reads -= 1,
                }
                match (source, read) {
                    (_, Ok(())) => states[index] = SegmentState::Done,
                    // The origin can still supply what the peers did not.
                    (SegmentSource::Peer, Err(SwarmFailure::Miss(detail))) if origin.is_some() => {
                        tracing::debug!(%hash, index, "peer segment failed: {detail}");
                        peer_failed[index] = true;
                        states[index] = SegmentState::Pending;
                    }
                    (_, Err(failure)) => return Err(failure),
                }
            }
            _ = ticker.tick() => {}
        }
    }

    match swarm.sources.store.blobs().has(hash).await {
        Ok(true) => Ok(swarm.progress(size)),
        Ok(false) => Err(SwarmFailure::Miss(
            "segments finished but the blob is incomplete".into(),
        )),
        Err(e) => Err(SwarmFailure::Fatal(Error::Store(e.to_string()))),
    }
}
