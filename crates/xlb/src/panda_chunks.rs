//! Whole-chunk transfers over p2panda's iroh endpoint.

use std::{fmt, sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures::{StreamExt, stream};
use iroh::{
    endpoint::{Connection, RecvStream, SendStream, VarInt},
    protocol::{AcceptError, ProtocolHandler},
};
use p2panda_net::Endpoint;
use tokio::sync::OnceCell;
use tokio_util::task::AbortOnDropHandle;
use opentelemetry::{KeyValue, global, metrics::{Counter, UpDownCounter}};
use tracing::Instrument;

use crate::{
    BlakeHash, NodeId,
    cas_cache::CasCache,
    peer::{await_direct_path, close_when_relayed, connection_verdict, PathVerdict},
};

pub(crate) const CHUNK_PROTOCOL: &[u8] = b"xlb/cas-chunk/1";
const PEER_SETUP_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const PEER_READ_IDLE: Duration = Duration::from_secs(5);

// [impl -> dsn~xlb-connection-transfer-metrics~1]
fn record_peer_bytes(bytes: usize, direction: &'static str) {
    static TRANSFERRED: std::sync::OnceLock<Counter<u64>> = std::sync::OnceLock::new();
    TRANSFERRED.get_or_init(|| global::meter("imgcloud.xlb").u64_counter("xlb.transfer.bytes").with_unit("By").build())
        .add(bytes as u64, &[KeyValue::new("source", "peer"), KeyValue::new("direction", direction)]);
}

struct PeerConnectionMetric(&'static str);

impl PeerConnectionMetric {
    fn new(direction: &'static str) -> Self {
        peer_connections().add(1, &[KeyValue::new("direction", direction)]);
        Self(direction)
    }
}

impl Drop for PeerConnectionMetric {
    fn drop(&mut self) { peer_connections().add(-1, &[KeyValue::new("direction", self.0)]); }
}

fn peer_connections() -> &'static UpDownCounter<i64> {
    static CONNECTIONS: std::sync::OnceLock<UpDownCounter<i64>> = std::sync::OnceLock::new();
    CONNECTIONS.get_or_init(|| global::meter("imgcloud.xlb").i64_up_down_counter("xlb.peer.connections.active").build())
}

fn record_upload_failure(cause: &'static str) {
    static FAILED: std::sync::OnceLock<Counter<u64>> = std::sync::OnceLock::new();
    FAILED.get_or_init(|| global::meter("imgcloud.xlb").u64_counter("xlb.peer.upload.failures").build())
        .add(1, &[KeyValue::new("failure.kind", cause)]);
}

#[derive(Clone)]
pub(crate) struct ChunkProvider {
    pub(crate) cache: Arc<CasCache>,
    pub(crate) admitted: Arc<dyn Fn(NodeId) -> bool + Send + Sync>,
    pub(crate) concurrency: usize,
}

impl fmt::Debug for ChunkProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ChunkProvider").finish_non_exhaustive()
    }
}

// [impl -> dsn~xlb-peer-reach~1]
impl ProtocolHandler for ChunkProvider {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id();
        if !(self.admitted)(peer) || !await_direct_path(&connection, PEER_SETUP_TIMEOUT).await {
            connection.close(VarInt::from_u32(0), b"peer unavailable");
            return Ok(());
        }
        let _connection_metric = PeerConnectionMetric::new("incoming");
        let _paths = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(close_when_relayed(connection.clone(), true)));
        stream::unfold(connection, |connection| async move {
            let streams = connection.accept_bi().await.ok()?;
            Some(((connection.clone(), streams), connection))
        })
        .for_each_concurrent(self.concurrency, |(connection, (mut send, receive))| async move {
            if (self.admitted)(peer) && connection_verdict(&connection) == PathVerdict::Direct {
                let span = tracing::info_span!("xlb.peer.upload", %peer, hash = tracing::field::Empty, error = tracing::field::Empty);
                async {
                    match self.serve(&mut send, receive).await {
                        Ok(()) => {}
                        Err(error) => {
                            let _ = send.reset(VarInt::from_u32(1));
                            tracing::Span::current().record("error", error.as_str());
                            record_upload_failure("transfer_error");
                            tracing::warn!(%peer, %error, "XLB peer upload failed");
                        }
                    }
                }.instrument(span).await;
            } else {
                let _ = send.reset(VarInt::from_u32(1));
            }
        })
        .await;
        Ok(())
    }
}

impl ChunkProvider {
    async fn serve(&self, send: &mut SendStream, mut receive: RecvStream) -> Result<(), String> {
        let digest = tokio::time::timeout(PEER_SETUP_TIMEOUT, async {
            let mut digest = [0; 32];
            receive.read_exact(&mut digest).await.map_err(|error| error.to_string())?;
            if receive.read_chunk(1).await.map_err(|error| error.to_string())?.is_some() {
                return Err("CAS request has trailing bytes".to_owned());
            }
            Ok(digest)
        }).await.map_err(|_| "peer request timed out".to_owned())??;
        tracing::Span::current().record("hash", tracing::field::display(BlakeHash::from_bytes(digest)));
        if let Some(chunk) = self.cache.get(BlakeHash::from_bytes(digest)).await.map_err(|error| error.to_string())? {
            let bytes = chunk.body().len() as u64;
            send.write_chunk(chunk.body().clone()).await.map_err(|error| error.to_string())?;
            record_peer_bytes(bytes as usize, "upload");
            send.finish().map_err(|error| error.to_string())?;
            return Ok(());
        }
        send.finish().map_err(|error| error.to_string())
    }
}

#[derive(Default)]
pub(crate) struct ChunkConnections {
    peers: DashMap<NodeId, Arc<OnceCell<DirectConnection>>>,
}

struct DirectConnection {
    connection: Connection,
    _monitor: AbortOnDropHandle<()>,
}

impl Drop for DirectConnection {
    fn drop(&mut self) {
        self.connection.close(VarInt::from_u32(0), b"peer connection released");
    }
}

impl ChunkConnections {
    pub(crate) fn retain(&self, mut retained: impl FnMut(&NodeId) -> bool) {
        self.peers.retain(|node, _| retained(node));
    }

    pub(crate) async fn fetch(
        &self, endpoint: &Endpoint, node: NodeId, digest: BlakeHash, size: usize,
    ) -> Result<Bytes, String> {
        let cell = self.peers.entry(node).or_default().clone();
        if cell.get().is_some_and(|peer| peer.connection.close_reason().is_some()) {
            self.peers.remove_if(&node, |_, current| Arc::ptr_eq(current, &cell));
        }
        let cell = self.peers.entry(node).or_default().clone();
        let (peer, mut receive) = tokio::time::timeout(PEER_SETUP_TIMEOUT, async {
            let peer = cell.get_or_try_init(|| connect(endpoint, node)).await?;
            let receive = request_chunk(&peer.connection, digest).await?;
            Ok::<_, String>((peer, receive))
        })
        .await
        .map_err(|_| "peer setup timed out before the CAS request was sent".to_owned())??;

        read_chunk(&mut receive, size).await.map_err(|error| {
            let paths = peer.connection.paths();
            if let Some(path) = paths.iter().find(|path| path.is_selected()) {
                let stats = path.stats();
                format!("{error} (path RTT: {:?}, cumulative packet losses: {})", stats.rtt, stats.lost_packets)
            } else {
                error
            }
        })
    }
}

async fn connect(endpoint: &Endpoint, node: NodeId) -> Result<DirectConnection, String> {
    let node = p2panda_core::VerifyingKey::from_bytes(node.as_bytes()).map_err(|error| error.to_string())?;
    let connection = endpoint.connect(node, CHUNK_PROTOCOL).await.map_err(|error| error.to_string())?;
    if !await_direct_path(&connection, PEER_SETUP_TIMEOUT).await {
        connection.close(VarInt::from_u32(0), b"no direct path");
        return Err("no direct path".into());
    }
    let monitored = connection.clone();
    let monitor = AbortOnDropHandle::new(tokio::spawn(async move {
        let _connection_metric = PeerConnectionMetric::new("outgoing");
        close_when_relayed(monitored, true).await;
    }));
    Ok(DirectConnection { connection, _monitor: monitor })
}

async fn request_chunk(connection: &Connection, digest: BlakeHash) -> Result<RecvStream, String> {
    if connection_verdict(connection) != PathVerdict::Direct {
        return Err("no direct path".into());
    }
    let (mut send, receive) = connection.open_bi().await.map_err(|error| error.to_string())?;
    send.write_all(digest.as_bytes()).await.map_err(|error| error.to_string())?;
    send.finish().map_err(|error| error.to_string())?;
    Ok(receive)
}

async fn read_chunk(receive: &mut RecvStream, size: usize) -> Result<Bytes, String> {
    let first = read_with_idle_timeout(receive, size + 1, 0, size).await?
        .ok_or_else(|| "peer does not hold the chunk".to_owned())?;
    if first.len() > size {
        return Err("peer exceeded the indexed chunk length".into());
    }
    let data = if first.len() == size {
        first
    } else {
        let mut data = BytesMut::with_capacity(size);
        data.extend_from_slice(&first);
        while data.len() < size {
            let available = size - data.len();
            let Some(part) = read_with_idle_timeout(receive, available, data.len(), size).await? else {
                break;
            };
            data.extend_from_slice(&part);
        }
        data.freeze()
    };
    if data.len() != size {
        return Err(format!("peer sent the wrong chunk length: received {} bytes, expected {size}", data.len()));
    }
    if read_with_idle_timeout(receive, 1, data.len(), size).await?.is_some() {
        return Err(format!("peer sent the wrong chunk length: more than {size} bytes"));
    }
    Ok(data)
}

async fn read_with_idle_timeout(
    receive: &mut RecvStream, max_read: usize, received: usize, expected: usize,
) -> Result<Option<Bytes>, String> {
    let chunk = tokio::time::timeout(PEER_READ_IDLE, receive.read_chunk(max_read))
        .await
        .map_err(|_| format!("peer response timed out after {PEER_READ_IDLE:?} without progress ({received}/{expected} bytes received)"))?
        .map_err(|error| format!("peer response interrupted ({received}/{expected} bytes received): {error}"))?;
    if let Some(bytes) = &chunk { record_peer_bytes(bytes.len(), "download"); }
    Ok(chunk)
}
