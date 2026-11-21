//! Serves complete Foyer chunks and verified ranges from active transfers.

use std::{io, sync::Arc};

use bao_tree::{
    BaoTree,
    io::{EncodeError, mixed::{self, EncodedItem}, outboard::PreOrderMemOutboard},
};
use futures::StreamExt;
use iroh_blobs::{
    Hash,
    api::{Store, proto::{Bitfield, Command, ExportBaoMsg, ObserveMsg}},
    store::{IROH_BLOCK_SIZE, mem::MemStore},
};
use irpc::channel::{SendError, mpsc::Sender};
use tokio::{sync::{OwnedSemaphorePermit, mpsc}, task::JoinSet};
use xlb_core::chunks::CAS_CHUNK_BYTES;

use crate::{BlakeHash, Flights, cas_cache::CasCache, store::from_iroh};

// The staged bytes and the assembled output can coexist during verification.
pub(crate) const STAGING_BYTES: usize = 2 * CAS_CHUNK_BYTES as usize + 32_704;

#[derive(Debug)]
pub(crate) struct StagedChunk {
    pub(crate) store: Store,
    pub(crate) finished: tokio_util::sync::CancellationToken,
    _memory: OwnedSemaphorePermit,
}

impl StagedChunk {
    pub(crate) fn new(memory: OwnedSemaphorePermit) -> Self {
        Self {
            store: MemStore::new().into(),
            finished: tokio_util::sync::CancellationToken::new(),
            _memory: memory,
        }
    }
}

pub(crate) fn provider(cache: Arc<CasCache>, flights: Flights) -> Store {
    let (commands, mut requests) = mpsc::channel::<Command>(32);
    tokio::spawn(async move {
        let mut readers = JoinSet::new();
        loop {
            tokio::select! {
                command = requests.recv() => match command {
                    Some(Command::ExportBao(request)) => {
                        let flight = stage(&flights, &request.inner.hash);
                        readers.spawn(export(cache.clone(), request, flight));
                    }
                    Some(Command::Observe(request)) => {
                        let flight = stage(&flights, &request.inner.hash);
                        readers.spawn(observe(cache.clone(), request, flight));
                    }
                    Some(Command::Shutdown(command)) => {
                        readers.shutdown().await;
                        let _ = command.tx.send(()).await;
                        break;
                    }
                    Some(_) => {}
                    None => break,
                },
                _ = readers.join_next(), if !readers.is_empty() => {},
            }
        }
    });
    Store::from_client(commands)
}

fn stage(flights: &Flights, hash: &Hash) -> Option<Arc<StagedChunk>> {
    let flights = flights.lock().unwrap_or_else(|error| error.into_inner());
    let stage = flights.get(&from_iroh(hash))?.borrow().stage.clone();
    stage
}

struct BaoSender(Sender<EncodedItem>);

impl mixed::Sender for BaoSender {
    type Error = SendError;

    async fn send(&mut self, item: EncodedItem) -> Result<(), Self::Error> {
        self.0.send(item).await
    }
}

async fn export(cache: Arc<CasCache>, command: ExportBaoMsg, stage: Option<Arc<StagedChunk>>) {
    let ExportBaoMsg { inner, tx, .. } = command;
    if let Ok(Some(chunk)) = cache.get(from_iroh(&inner.hash)).await {
        let outboard = PreOrderMemOutboard {
            root: inner.hash.into(),
            tree: BaoTree::new(chunk.body().len() as u64, IROH_BLOCK_SIZE),
            data: chunk.outboard().as_ref(),
        };
        let mut sender = BaoSender(tx);
        let _ = mixed::traverse_ranges_validated(chunk.body(), outboard, &inner.ranges, &mut sender).await;
    } else if let Some(stage) = stage {
        let stream = stage.store.blobs().export_bao(inner.hash, inner.ranges).stream();
        tokio::pin!(stream);
        while let Some(item) = stream.next().await {
            if tx.send(item).await.is_err() {
                break;
            }
        }
    } else {
        let _ = tx.send(EncodedItem::Error(EncodeError::Io(io::Error::new(
            io::ErrorKind::NotFound, "chunk is not in the local CAS",
        )))).await;
    }
}

async fn observe(cache: Arc<CasCache>, command: ObserveMsg, stage: Option<Arc<StagedChunk>>) {
    let ObserveMsg { inner, tx, .. } = command;
    let hash: BlakeHash = from_iroh(&inner.hash);
    if let Ok(Some(chunk)) = cache.get(hash).await {
        let size = chunk.body().len() as u64;
        drop(chunk);
        let _ = tx.send(Bitfield::complete(size)).await;
    } else if let Some(stage) = stage {
        if let Ok(stream) = stage.store.blobs().observe(inner.hash).stream().await {
            tokio::pin!(stream);
            while let Some(item) = tokio::select! {
                update = stream.next() => update,
                _ = stage.finished.cancelled() => {
                    if let Ok(Some(chunk)) = cache.get(hash).await {
                        let _ = tx.send(Bitfield::complete(chunk.body().len() as u64)).await;
                    }
                    None
                }
                _ = tx.closed() => return,
            } {
                let complete = item.is_complete();
                if tx.send(item).await.is_err() || complete {
                    break;
                }
            }
        }
    } else {
        let _ = tx.send(Bitfield::empty()).await;
    }
    tx.closed().await;
}

#[cfg(test)]
mod tests {
    use bao_tree::ChunkRanges;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use iroh_blobs::protocol::ChunkRangesExt;
    use tokio::sync::{Semaphore, watch};

    use super::*;
    use crate::FlightState;

    // [utest -> dsn~xlb-foyer-cas~1]
    #[tokio::test]
    async fn staged_verified_range_is_servable_before_foyer_admission() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(CasCache::open(dir.path(), 160 << 20, 128 << 20).await.unwrap());
        let source = MemStore::new();
        let part_len = 16 * 1024;
        let body = Bytes::from(vec![23; 2 * part_len + 7]);
        let tag = source.blobs().add_slice(&body).temp_tag().await.unwrap();
        let hash = tag.hash();
        let digest = from_iroh(&hash);
        let range = ChunkRanges::bytes(0..part_len as u64);
        let proof: Vec<Bytes> = source.blobs().export_bao(hash, range.clone())
            .into_byte_stream().try_collect().await.unwrap();
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        let stage = Arc::new(StagedChunk::new(permit));
        stage.store.blobs().import_bao_bytes(hash, range.clone(), proof.concat()).await.unwrap();
        let flights = Flights::default();
        let (flight, state) = watch::channel(FlightState {
            stage: Some(stage),
            ..Default::default()
        });
        flights.lock().unwrap().insert(digest, state);
        let peer = provider(cache.clone(), flights);
        let served = peer.blobs().export_bao(hash, range).data_to_bytes().await.unwrap();
        assert_eq!(served, body.slice(..part_len));
        assert!(cache.get(digest).await.unwrap().is_none());

        cache.insert(digest, body.len() as u64, body.clone()).await.unwrap();
        assert_eq!(peer.blobs().get_bytes(hash).await.unwrap(), body);
        peer.shutdown().await.unwrap();
        drop(flight);
        cache.close().await.unwrap();
    }
}
