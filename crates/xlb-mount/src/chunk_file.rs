use std::sync::Arc;
use std::io::{Error, ErrorKind};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures::future::try_join_all;
use tokio::sync::OnceCell;
use xlb::{AssetClass, BlakeHash, FetchReport, TransferPriority};
use xlb_core::chunks::{CAS_CHUNK_BYTES, ChunkIndex, positioned_value};

#[derive(Clone)]
pub(crate) struct ChunkFile {
    pub(crate) index: Arc<ChunkIndex>,
    class: AssetClass,
    checks: Arc<Vec<(BlakeHash, OnceCell<()>)>>,
}

impl ChunkFile {
    pub(crate) fn new(index: Arc<ChunkIndex>, class: AssetClass) -> Result<Self> {
        ensure!(index.is_complete(), "CAS index has the wrong number of chunks");
        Ok(Self { index, class, checks: Arc::default() })
    }

    pub(crate) fn with_model_values(mut self, values: Vec<BlakeHash>) -> Self {
        self.checks = Arc::new(values.into_iter().map(|value| (value, OnceCell::new())).collect());
        self
    }

    pub(crate) async fn read_chunk(&self, ordinal: u64) -> Result<(Bytes, FetchReport)> {
        self.read_chunk_with(ordinal, TransferPriority::Demand).await
    }

    pub(crate) async fn read_chunk_with(&self, ordinal: u64, priority: TransferPriority) -> Result<(Bytes, FetchReport)> {
        self.read_chunk_serving(ordinal, priority, None).await
    }

    /// Read a chunk for a caller that serves `served` of its bytes, or all of them.
    async fn read_chunk_serving(&self, ordinal: u64, priority: TransferPriority, served: Option<u64>) -> Result<(Bytes, FetchReport)> {
        let (hash, length) = self.index.chunk(ordinal).context("CAS index is truncated")?;
        let (body, report) = self.class.indexed_chunk(hash, length)
            .read_chunk_serving(priority, served.unwrap_or(length)).await;
        let body = body?;
        ensure!(body.len() as u64 == length, "CAS chunk {hash} has an unexpected length");
        if let Some((expected, check)) = self.checks.get(ordinal as usize) {
            check.get_or_try_init(|| async {
                let payload = body.clone();
                let actual = tokio::task::spawn_blocking(move || {
                    positioned_value(ordinal * CAS_CHUNK_BYTES, &payload)
                }).await?;
                if actual != *expected {
                    return Err(Error::new(ErrorKind::InvalidData,
                        format!("CAS chunk {hash} differs from the model at ordinal {ordinal}")).into());
                }
                Ok::<_, anyhow::Error>(())
            }).await?;
        }
        Ok((body, report))
    }

    // [impl -> dsn~xlb-fuse-snapshots~1]
    pub(crate) async fn read(&self, offset: u64, size: u32) -> Result<Bytes> {
        let end = offset.saturating_add(u64::from(size)).min(self.index.size_bytes);
        if end <= offset {
            return Ok(Bytes::new());
        }
        let first = offset / CAS_CHUNK_BYTES;
        let last = (end - 1) / CAS_CHUNK_BYTES;
        let reads = (first..=last).map(|ordinal| async move {
            let start = ordinal * CAS_CHUNK_BYTES;
            let served = end.min(start + CAS_CHUNK_BYTES) - offset.max(start);
            let (body, _) = self.read_chunk_serving(ordinal, TransferPriority::Demand, Some(served)).await?;
            let range = offset.saturating_sub(start) as usize..(end - start).min(body.len() as u64) as usize;
            Ok::<_, anyhow::Error>(body.slice(range))
        });
        let mut blocks = try_join_all(reads).await?;
        // A read within one chunk replies from the cached chunk's buffer.
        if blocks.len() == 1 {
            return Ok(blocks.pop().expect("one block"));
        }
        let mut bytes = Vec::with_capacity((end - offset) as usize);
        for block in blocks {
            bytes.extend_from_slice(&block);
        }
        Ok(bytes.into())
    }
}
