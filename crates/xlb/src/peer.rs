//! Peer tier: an iroh endpoint that serves the class's blob store to the
//! runtime peer set and reads blobs from it.
//!
//! The endpoint has an ephemeral key and no address lookup service other than
//! an in-memory one that holds exactly the addresses given to
//! [`PeerTransport::set_peer_sets`]. Relays, when configured, are for address
//! discovery and hole punching only: a request is sent and served only on a
//! connection whose selected path is a direct IP path, and a connection
//! whose data would move onto a relay path is closed.
//!
//! ## Serving
//!
//! The stock iroh-blobs protocol handler serves every blob of the store,
//! complete or partial: a get request is answered with `export_bao`, which
//! encodes whatever verified ranges the entry holds, and an observe request
//! with the entry's bitfield and its later growth. Its provider events are
//! intercepted: a connection whose remote node ID is outside the current
//! peer set is refused, and each get and observe request on an accepted
//! connection is checked again, so removing a peer stops serving it.
//! Admission looks only at the peer set, never at how much of a blob is held.
//!
//! ## Fetching
//!
//! [`PeerTransport::fetch_ranges`] sends cancellable iroh-blobs range
//! requests to providers advertised by smol-kv and skips recent failures.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::SocketAddr,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use futures::StreamExt;
pub use iroh::RelayUrl;
use iroh::{
    address_lookup::memory::MemoryLookup,
    endpoint::{presets, Connection, QuicTransportConfig, VarInt},
    protocol::{AcceptError, ProtocolHandler, Router},
    Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey, TransportAddr,
};
use iroh_blobs::{
    api::{
        remote::GetProgressItem,
        Store,
    },
    protocol::{ChunkRanges, GetRequest},
    get::StreamPair,
    provider::events::{
        AbortReason, ConnectMode, EventMask, EventSender, ObserveMode,
        ProviderMessage, RequestMode, ThrottleMode,
    },
    util::connection_pool::ConnectionPool,
    BlobsProtocol, Hash as IrohHash,
};
use rand::seq::SliceRandom;
use tokio::time::Instant;

use crate::{NodeTopology, RequestLimits, discovery::{Discovery, ProviderEvent, is_fresh}, stream_bytes::QuicBytes};
use xlb_core::image::IMAGE_CHUNK_BYTES;

/// The node ID of an xlb node: the public key of its iroh endpoint.
pub type NodeId = EndpointId;

/// Default UDP port of the peer endpoint.
pub const DEFAULT_PEER_PORT: u16 = 4433;
/// Default size of one peer segment.
pub const DEFAULT_PEER_SEGMENT_BYTES: u64 = 32 * 1024 * 1024;
/// Default number of peer segments in flight for one blob.
pub const DEFAULT_PEER_CONCURRENCY: usize = 16;
/// Default time allowed for connecting to one peer.
pub const DEFAULT_PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Default time a segment may see no progress on any peer before a fetcher
/// other than its assigned origin reader reads it from the origin.
pub const DEFAULT_PEER_STALL_TIMEOUT: Duration = Duration::from_secs(10);
const PEER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_EXCLUSION: Duration = Duration::from_secs(60);
// At 100 ms RTT, 64 MiB covers a 5 Gbit/s bandwidth-delay product.
const PEER_CONNECTION_WINDOW: u32 = 64 * 1024 * 1024;

#[derive(Clone, Copy)]
pub(crate) struct ProviderChoice {
    pub(crate) node: NodeId,
    pub(crate) same_location: bool,
    pub(crate) timestamp: u64,
}

#[derive(Default)]
struct FailedPeers(Mutex<HashMap<NodeId, Instant>>);

impl FailedPeers {
    fn insert(&self, node: NodeId, now: Instant) {
        let mut peers = self.0.lock().unwrap_or_else(|err| err.into_inner());
        peers.retain(|_, until| *until > now);
        peers.insert(node, now + PEER_EXCLUSION);
    }

    fn contains(&self, node: &NodeId, now: Instant) -> bool {
        self.0.lock().unwrap_or_else(|err| err.into_inner())
            .get(node).is_some_and(|until| *until > now)
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;

    // [utest -> dsn~xlb-chunk-discovery~1]
    #[test]
    fn a_failure_excludes_only_that_holder_for_sixty_seconds() {
        let failures = FailedPeers::default();
        let failed = SecretKey::generate().public();
        let healthy = SecretKey::generate().public();
        let now = Instant::now();
        failures.insert(failed, now);
        assert!(failures.contains(&failed, now + Duration::from_secs(59)));
        assert!(!failures.contains(&failed, now + Duration::from_secs(60)));
        assert!(!failures.contains(&healthy, now));
    }
}

/// One member of the peer set: a node this node reads from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAddr {
    pub node_id: NodeId,
    /// UDP socket addresses the peer's endpoint may be reachable at, private
    /// and public. May be empty when `relay_url` is set.
    pub addrs: Vec<SocketAddr>,
    /// The relay the peer is reachable through, if it reported one.
    pub relay_url: Option<RelayUrl>,
}

/// Which relay servers the peer endpoint uses to discover its addresses and
/// to punch holes. Blob requests never travel through a relay (see
/// [`path_verdict`]).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RelaySetting {
    /// iroh's default relay map.
    #[default]
    Default,
    /// No relays: peers are reached at their socket addresses only.
    Disabled,
    /// These relay URLs.
    Custom(Vec<String>),
}

impl RelaySetting {
    fn relay_mode(&self) -> Result<RelayMode, String> {
        match self {
            RelaySetting::Default => Ok(RelayMode::Default),
            RelaySetting::Disabled => Ok(RelayMode::Disabled),
            RelaySetting::Custom(urls) => urls
                .iter()
                .map(|url| url.parse::<RelayUrl>().map_err(|e| format!("relay URL {url:?}: {e}")))
                .collect::<Result<Vec<_>, String>>()
                .map(RelayMode::custom),
        }
    }
}

/// What the paths of a connection allow it to be used for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathVerdict {
    /// Application data travels on a direct IP path: requests may be sent
    /// and served.
    Direct,
    /// Application data travels through a relay: no request may be sent or
    /// served.
    Relayed,
    /// No path is selected yet: wait, and refuse if none is in time.
    Unselected,
}

/// Judge a connection by its open paths, each given as (selected for
/// application data, direct IP path). Only the selected path counts: a
/// direct path that is open but not selected carries no data.
// [impl -> dsn~xlb-peer-reach~1]
pub(crate) fn path_verdict(paths: impl IntoIterator<Item = (bool, bool)>) -> PathVerdict {
    match paths.into_iter().find(|(selected, _)| *selected) {
        Some((_, true)) => PathVerdict::Direct,
        Some((_, false)) => PathVerdict::Relayed,
        None => PathVerdict::Unselected,
    }
}

fn connection_verdict(connection: &Connection) -> PathVerdict {
    let paths = connection.paths();
    path_verdict(paths.iter().map(|path| (path.is_selected(), path.is_ip())))
}

/// Wait up to `limit` for `connection` to carry its data on a direct path.
// [impl -> dsn~xlb-peer-reach~1]
async fn await_direct_path(connection: &Connection, limit: Duration) -> bool {
    let direct = async {
        let mut snapshots = connection.paths_stream();
        while let Some(paths) = snapshots.next().await {
            let paths = paths.iter().map(|path| (path.is_selected(), path.is_ip()));
            if path_verdict(paths) == PathVerdict::Direct {
                return true;
            }
        }
        false
    };
    tokio::time::timeout(limit, direct).await.unwrap_or(false)
}

/// Close `connection` as soon as its data, having travelled on a direct
/// path, would travel through a relay. Ends when the connection closes.
// [impl -> dsn~xlb-peer-reach~1]
async fn close_when_relayed(connection: Connection, mut was_direct: bool) {
    let mut snapshots = connection.paths_stream();
    while let Some(paths) = snapshots.next().await {
        let paths = paths.iter().map(|path| (path.is_selected(), path.is_ip()));
        match path_verdict(paths) {
            PathVerdict::Direct => was_direct = true,
            PathVerdict::Relayed if was_direct => {
                tracing::warn!(remote = %connection.remote_id(), "closing a blob connection that lost its direct path");
                connection.close(VarInt::from_u32(0), b"no direct path");
                return;
            }
            PathVerdict::Relayed | PathVerdict::Unselected => {}
        }
    }
}

/// The incoming blob connections by connection ID, for the admission task.
type ServedConnections = Arc<Mutex<HashMap<u64, Connection>>>;

/// The iroh-blobs protocol handler, with every accepted connection
/// registered for path checks and closed when it loses its direct path.
#[derive(Debug, Clone)]
struct DirectPathBlobs {
    blobs: BlobsProtocol,
    served: ServedConnections,
}

// [impl -> dsn~xlb-peer-reach~1]
impl ProtocolHandler for DirectPathBlobs {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        // The provider names a connection by this ID in its events.
        let connection_id = connection.stable_id() as u64;
        self.served
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(connection_id, connection.clone());
        let guard = tokio::spawn(close_when_relayed(connection.clone(), false));
        let accepted = self.blobs.accept(connection).await;
        guard.abort();
        self.served
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&connection_id);
        accepted
    }

    async fn shutdown(&self) {
        self.blobs.shutdown().await;
    }
}

/// Configuration of the peer tier of one class.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// UDP address the endpoint binds.
    pub bind_addr: SocketAddr,
    /// The relay servers the endpoint uses.
    pub relay: RelaySetting,
    /// Size of one segment of a blob; rounded down to whole chunk groups.
    pub segment_bytes: u64,
    /// Segments in flight for one blob.
    pub concurrency: usize,
    /// Time allowed for connecting to one peer before the next is tried.
    pub connect_timeout: Duration,
    /// How long an active peer-only load waits for a replacement supplier.
    pub stall_timeout: Duration,
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], DEFAULT_PEER_PORT)),
            relay: RelaySetting::Default,
            segment_bytes: DEFAULT_PEER_SEGMENT_BYTES,
            concurrency: DEFAULT_PEER_CONCURRENCY,
            connect_timeout: DEFAULT_PEER_CONNECT_TIMEOUT,
            stall_timeout: DEFAULT_PEER_STALL_TIMEOUT,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PeerError {
    #[error("peer download: {0}")]
    Download(String),
}

type PoolOptions = iroh_blobs::util::connection_pool::Options;

/// Pool options under which a connection is handed out only once its data
/// travels on a direct path, and is closed when that path is lost.
/// `connect_timeout` covers connecting and the wait for the direct path.
/// `PoolOptions` is non-exhaustive, so it can only be changed field by field.
// [impl -> dsn~xlb-peer-reach~1]
fn direct_path_pool(connect_timeout: Duration) -> PoolOptions {
    let mut options = PoolOptions::default().with_on_connected(move |_endpoint, connection| async move {
        if !await_direct_path(&connection, connect_timeout).await {
            connection.close(VarInt::from_u32(0), b"no direct path");
            return Err(std::io::Error::other("no direct path to the peer"));
        }
        tokio::spawn(close_when_relayed(connection, true));
        Ok(())
    });
    options.connect_timeout = connect_timeout;
    options
}

type PeerSet = Arc<RwLock<BTreeMap<EndpointId, PeerAddr>>>;
type AdmitSet = Arc<RwLock<BTreeSet<EndpointId>>>;

pub(crate) struct PeerTransport {
    config: PeerConfig,
    request_limits: RequestLimits,
    endpoint: Endpoint,
    router: Router,
    lookup: MemoryLookup,
    /// The nodes this node reads from.
    peers: PeerSet,
    /// The nodes this node serves.
    admitted: AdmitSet,
    failed: FailedPeers,
    pool: ConnectionPool,
    discovery: Discovery,
}

impl PeerTransport {
    /// Bind the endpoint and start serving `store` to the (empty) admit set.
    // [impl -> dsn~xlb-fork~1]
    pub(crate) async fn bind(
        config: PeerConfig,
        store: &Store,
        complete_hashes: impl Fn() -> Vec<IrohHash> + Send + 'static,
        request_limits: RequestLimits,
    ) -> Result<Self, String> {
        let lookup = MemoryLookup::new();
        // The only IP transport is the configured address, so the UDP port
        // is the configured one. The in-memory lookup is the only address
        // lookup service. Relays, when enabled, serve address discovery and
        // hole punching; requests are sent and served on direct paths only.
        let endpoint = Endpoint::builder(presets::Minimal)
            .transport_config(QuicTransportConfig::builder()
                .stream_receive_window(VarInt::from_u32(IMAGE_CHUNK_BYTES as u32))
                .receive_window(VarInt::from_u32(PEER_CONNECTION_WINDOW))
                .send_window(u64::from(PEER_CONNECTION_WINDOW))
                .build())
            .secret_key(SecretKey::generate())
            .clear_ip_transports()
            .bind_addr(config.bind_addr)
            .map_err(|e| format!("peer bind address {}: {e}", config.bind_addr))?
            .relay_mode(config.relay.relay_mode()?)
            .address_lookup(lookup.clone())
            .bind()
            .await
            .map_err(|e| format!("binding peer endpoint on {}: {e}", config.bind_addr))?;

        let peers: PeerSet = Arc::default();
        let admitted: AdmitSet = Arc::default();
        let mask = EventMask {
            connected: ConnectMode::Intercept,
            get: RequestMode::Intercept,
            get_many: RequestMode::Intercept,
            push: RequestMode::Disabled,
            observe: ObserveMode::Intercept,
            throttle: ThrottleMode::None,
        };
        let (events, messages) = EventSender::channel(64, mask);
        let served: ServedConnections = Arc::default();
        tokio::spawn(admit_peers(
            messages,
            admitted.clone(),
            served.clone(),
            config.connect_timeout,
        ));

        let blobs = DirectPathBlobs {
            blobs: BlobsProtocol::new(store, Some(events)),
            served,
        };
        let gossip = iroh_gossip::Gossip::builder().spawn(endpoint.clone());
        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs)
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();
        let discovery = Discovery::join(&gossip, endpoint.clone(), complete_hashes).await?;
        let pool = ConnectionPool::new(endpoint.clone(), iroh_blobs::ALPN, direct_path_pool(config.connect_timeout));

        Ok(Self {
            config,
            request_limits,
            endpoint,
            router,
            lookup,
            peers,
            admitted,
            failed: FailedPeers::default(),
            pool,
            discovery,
        })
    }

    pub(crate) fn node_id(&self) -> NodeId {
        self.endpoint.id()
    }

    pub(crate) fn config(&self) -> &PeerConfig {
        &self.config
    }

    pub(crate) async fn announce(&self, hash: IrohHash) {
        self.discovery.announce(hash).await;
    }

    pub(crate) fn set_node_topology(&self, topology: Option<NodeTopology>) {
        self.discovery.set_node_topology(topology);
    }

    pub(crate) fn providers(&self, hash: IrohHash) -> impl futures::Stream<Item = ProviderEvent> + Send + 'static {
        self.discovery.providers(hash)
    }

    pub(crate) fn learn_provider(&self, address: &EndpointAddr) -> bool {
        let peers = self.read_peers();
        if !peers.contains_key(&address.id) {
            return false;
        }
        self.lookup.add_endpoint_info(address.clone());
        true
    }

    pub(crate) fn is_eligible(&self, node: &NodeId) -> bool {
        self.read_peers().contains_key(node) && !self.failed.contains(node, Instant::now())
    }

    pub(crate) fn has_sources(&self) -> bool {
        !self.read_peers().is_empty()
    }

    pub(crate) fn shares_location(&self, location: &str) -> bool {
        self.discovery.shares_location(location)
    }

    /// The relay this endpoint is reachable through, once it has connected
    /// to one. Always `None` with relays disabled.
    pub(crate) fn relay_url(&self) -> Option<RelayUrl> {
        self.endpoint.addr().relay_urls().next().cloned()
    }

    /// The UDP addresses the endpoint is bound to.
    pub(crate) fn bound_addrs(&self) -> Vec<SocketAddr> {
        self.endpoint.bound_sockets()
    }

    pub(crate) fn peers(&self) -> Vec<PeerAddr> {
        self.read_peers().values().cloned().collect()
    }

    fn read_peers(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<EndpointId, PeerAddr>> {
        self.peers.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Replace both peer sets. Later fetches dial, observe and read only
    /// `peers`; only the node IDs in `admit` are served, and they need no
    /// address because they dial this node. This node's own ID is ignored.
    ///
    /// iroh is given every address of a peer: its relay, through which it
    /// can punch a hole when no socket address is reachable as given, and
    /// all its socket addresses as direct addresses.
    // [impl -> dsn~xlb-peer-reach~1]
    // [impl -> dsn~xlb-fork~1]
    pub(crate) fn set_peer_sets(&self, peers: Vec<PeerAddr>, admit: Vec<NodeId>) {
        let own = self.endpoint.id();
        let next: BTreeMap<EndpointId, PeerAddr> = peers
            .into_iter()
            .filter(|peer| peer.node_id != own)
            .map(|peer| (peer.node_id, peer))
            .collect();
        let bootstrap = {
            let mut current = self.peers.write().unwrap_or_else(|e| e.into_inner());
            for gone in current.keys().filter(|id| !next.contains_key(*id)) {
                self.lookup.remove_endpoint_info(*gone);
            }
            let mut bootstrap = Vec::new();
            for (id, peer) in &next {
                if current.get(id) == Some(peer) {
                    continue;
                }
                let relay = peer.relay_url.iter().cloned().map(TransportAddr::Relay);
                let direct = peer.addrs.iter().copied().map(TransportAddr::Ip);
                self.lookup
                    .set_endpoint_info(EndpointAddr::from_parts(*id, relay.chain(direct)));
                bootstrap.push(*id);
            }
            *current = next;
            bootstrap
        };
        let admit: BTreeSet<EndpointId> = admit.into_iter().filter(|id| *id != own).collect();
        *self.admitted.write().unwrap_or_else(|e| e.into_inner()) = admit;
        self.discovery.bootstrap(bootstrap);
    }

    /// Request the missing verified ranges from one eligible provider at a time.
    // [impl -> dsn~xlb-chunk-discovery~1]
    pub(crate) async fn fetch_ranges(
        &self,
        store: &Store,
        hash: IrohHash,
        ranges: ChunkRanges,
        mut providers: Vec<ProviderChoice>,
        mut on_bytes: impl FnMut(NodeId, u64),
    ) -> Result<(), PeerError> {
        providers.shuffle(&mut rand::rng());
        providers.sort_by_key(|provider| !provider.same_location);
        let request = Arc::new(GetRequest::blob_ranges(hash, ranges));
        let remote = store.remote();
        for ProviderChoice { node: provider, timestamp, .. } in providers {
            if !is_fresh(timestamp) || !self.is_eligible(&provider) {
                continue;
            }
            let local = remote.local_for_request(request.clone()).await
                .map_err(|err| PeerError::Download(err.to_string()))?;
            if local.is_complete() {
                return Ok(());
            }
            let slot = self.request_limits.peer().await;
            if !is_fresh(timestamp) || !self.is_eligible(&provider) {
                continue;
            }
            let attempt = async {
                let mut received = 0;
                let connection = self.pool.get_or_connect(provider).await
                    .map_err(|err| err.to_string())?;
                if connection_verdict(&connection) != PathVerdict::Direct {
                    return Err("the connection has no direct path".into());
                }
                let (send, recv) = connection.open_bi().await.map_err(|err| err.to_string())?;
                let stream = StreamPair::new(connection.stable_id() as u64, QuicBytes(recv), send);
                let items = remote.execute_get(stream, local.missing()).stream();
                tokio::pin!(items);
                while let Some(item) = items.next().await {
                    match item {
                        GetProgressItem::Progress(bytes) => {
                            if bytes > received {
                                on_bytes(provider, bytes - received);
                                received = bytes;
                            }
                        }
                        GetProgressItem::Done(stats) => {
                            if stats.payload_bytes_read > received {
                                on_bytes(provider, stats.payload_bytes_read - received);
                            }
                            return Ok(());
                        }
                        GetProgressItem::Error(err) => return Err(err.to_string()),
                    }
                }
                Err("transfer ended without a result".into())
            };
            let failure = match tokio::time::timeout(PEER_ATTEMPT_TIMEOUT, attempt).await {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(err)) => err,
                Err(_) => "transfer attempt timed out".into(),
            };
            drop(slot);
            self.failed.insert(provider, Instant::now());
            tracing::debug!(%provider, %hash, %failure, "excluding failed XLB provider");
        }
        Err(PeerError::Download("no eligible provider completed the range".into()))
    }

    /// Stop accepting, shut the store down and close the endpoint.
    pub(crate) async fn shutdown(&self) {
        self.discovery.shutdown().await;
        if let Err(e) = self.router.shutdown().await {
            tracing::warn!("peer transport shutdown: {e}");
        }
    }
}

/// Answer the provider's intercepted events from the peer set.
// [impl -> dsn~xlb-peer-reach~1]
async fn admit_peers(
    mut messages: tokio::sync::mpsc::Receiver<ProviderMessage>,
    admitted: AdmitSet,
    served: ServedConnections,
    path_wait: Duration,
) {
    // Connection ID to the remote node ID, for accepted connections.
    let mut connections: HashMap<u64, EndpointId> = HashMap::new();
    let is_member = |id: &EndpointId| admitted.read().unwrap_or_else(|e| e.into_inner()).contains(id);
    // A request is served to an admitted node, and only on a connection
    // whose data travels on a direct path. The path may still be settling
    // when the first request arrives, so the answer is given off this task.
    let request_verdict = |connections: &HashMap<u64, EndpointId>, connection_id: u64| {
        let member = connections.get(&connection_id).is_some_and(&is_member);
        let connection = served
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&connection_id)
            .cloned();
        async move {
            match connection {
                Some(connection) if member => {
                    if await_direct_path(&connection, path_wait).await {
                        Ok(())
                    } else {
                        tracing::warn!(remote = %connection.remote_id(), "refusing a blob request on a connection without a direct path");
                        Err(AbortReason::Permission)
                    }
                }
                _ => Err(AbortReason::Permission),
            }
        }
    };
    while let Some(message) = messages.recv().await {
        match message {
            ProviderMessage::ClientConnected(msg) => {
                let verdict = match msg.inner.endpoint_id {
                    Some(id) if is_member(&id) => {
                        connections.insert(msg.inner.connection_id, id);
                        Ok(())
                    }
                    other => {
                        tracing::warn!(remote = ?other, "refusing blob connection from outside the admit set");
                        Err(AbortReason::Permission)
                    }
                };
                msg.tx.send(verdict).await.ok();
            }
            ProviderMessage::ConnectionClosed(msg) => {
                connections.remove(&msg.inner.connection_id);
            }
            ProviderMessage::GetRequestReceived(msg) => {
                let verdict = request_verdict(&connections, msg.inner.connection_id);
                tokio::spawn(async move { msg.tx.send(verdict.await).await.ok() });
            }
            ProviderMessage::GetManyRequestReceived(msg) => {
                let verdict = request_verdict(&connections, msg.inner.connection_id);
                tokio::spawn(async move { msg.tx.send(verdict.await).await.ok() });
            }
            ProviderMessage::ObserveRequestReceived(msg) => {
                let verdict = request_verdict(&connections, msg.inner.connection_id);
                tokio::spawn(async move { msg.tx.send(verdict.await).await.ok() });
            }
            // Push is disabled and nothing else is intercepted; dropping an
            // unexpected intercept message refuses it.
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Paths are (selected for application data, direct IP path).
    // [utest -> dsn~xlb-peer-reach~1]
    #[test]
    fn requests_are_allowed_only_while_data_travels_on_a_direct_path() {
        assert_eq!(path_verdict([(true, true)]), PathVerdict::Direct);
        assert_eq!(path_verdict([(false, false), (true, true)]), PathVerdict::Direct);
        // Only a relay path, or a direct path that is open but not the one
        // carrying data: refused.
        assert_eq!(path_verdict([(true, false)]), PathVerdict::Relayed);
        assert_eq!(path_verdict([(true, false), (false, true)]), PathVerdict::Relayed);
        // Nothing selected yet: wait, then refuse.
        assert_eq!(path_verdict([]), PathVerdict::Unselected);
        assert_eq!(path_verdict([(false, true)]), PathVerdict::Unselected);
    }
}
