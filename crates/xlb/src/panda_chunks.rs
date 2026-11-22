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

use crate::{
    BlakeHash, NodeId,
    cas_cache::CasCache,
    peer::{await_direct_path, close_when_relayed, connection_verdict, PathVerdict},
};

pub(crate) const CHUNK_PROTOCOL: &[u8] = b"xlb/cas-chunk/1";
pub(crate) const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

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
        if !(self.admitted)(peer) || !await_direct_path(&connection, ATTEMPT_TIMEOUT).await {
            connection.close(VarInt::from_u32(0), b"peer unavailable");
            return Ok(());
        }
        let _paths = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(close_when_relayed(connection.clone(), true)));
        stream::unfold(connection, |connection| async move {
            let streams = connection.accept_bi().await.ok()?;
            Some(((connection.clone(), streams), connection))
        })
        .for_each_concurrent(self.concurrency, |(connection, (send, receive))| async move {
            if (self.admitted)(peer) && connection_verdict(&connection) == PathVerdict::Direct {
                let _ = tokio::time::timeout(ATTEMPT_TIMEOUT, self.serve(send, receive)).await;
            }
        })
        .await;
        Ok(())
    }
}

impl ChunkProvider {
    async fn serve(&self, mut send: SendStream, mut receive: RecvStream) -> Result<(), String> {
        let mut digest = [0; 32];
        receive.read_exact(&mut digest).await.map_err(|error| error.to_string())?;
        if receive.read_chunk(1).await.map_err(|error| error.to_string())?.is_some() {
            return Err("CAS request has trailing bytes".into());
        }
        if let Some(chunk) = self.cache.get(BlakeHash::from_bytes(digest)).await.map_err(|error| error.to_string())? {
            send.write_chunk(chunk.body().clone()).await.map_err(|error| error.to_string())?;
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
        let peer = cell.get_or_try_init(|| connect(endpoint, node)).await?;
        read_chunk(&peer.connection, digest, size).await
    }
}

async fn connect(endpoint: &Endpoint, node: NodeId) -> Result<DirectConnection, String> {
    let node = p2panda_core::VerifyingKey::from_bytes(node.as_bytes()).map_err(|error| error.to_string())?;
    let connection = endpoint.connect(node, CHUNK_PROTOCOL).await.map_err(|error| error.to_string())?;
    if !await_direct_path(&connection, ATTEMPT_TIMEOUT).await {
        connection.close(VarInt::from_u32(0), b"no direct path");
        return Err("no direct path".into());
    }
    let monitor = AbortOnDropHandle::new(tokio::spawn(close_when_relayed(connection.clone(), true)));
    Ok(DirectConnection { connection, _monitor: monitor })
}

async fn read_chunk(connection: &Connection, digest: BlakeHash, size: usize) -> Result<Bytes, String> {
    if connection_verdict(connection) != PathVerdict::Direct {
        return Err("no direct path".into());
    }
    let (mut send, mut receive) = connection.open_bi().await.map_err(|error| error.to_string())?;
    send.write_all(digest.as_bytes()).await.map_err(|error| error.to_string())?;
    send.finish().map_err(|error| error.to_string())?;

    let first = receive.read_chunk(size + 1).await.map_err(|error| error.to_string())?
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
            let Some(part) = receive.read_chunk(available).await.map_err(|error| error.to_string())? else {
                break;
            };
            data.extend_from_slice(&part);
        }
        data.freeze()
    };
    if data.len() != size || receive.read_chunk(1).await.map_err(|error| error.to_string())?.is_some() {
        return Err("peer sent the wrong chunk length".into());
    }
    Ok(data)
}
