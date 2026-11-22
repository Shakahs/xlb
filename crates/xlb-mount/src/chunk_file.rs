use std::sync::Arc;
use std::io::{Error, ErrorKind};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures::future::try_join_all;
use tokio::sync::OnceCell;
use xlb::{AssetClass, BlakeHash, FetchReport};
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
        let (hash, length) = self.index.chunk(ordinal).context("CAS index is truncated")?;
        let (body, report) = self.class.indexed_chunk(hash, length).read_chunk_reported().await;
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
    pub(crate) async fn read(&self, offset: u64, size: u32) -> Result<Vec<u8>> {
        let end = offset.saturating_add(u64::from(size)).min(self.index.size_bytes);
        if end <= offset {
            return Ok(Vec::new());
        }
        let first = offset / CAS_CHUNK_BYTES;
        let last = (end - 1) / CAS_CHUNK_BYTES;
        let reads = (first..=last).map(|ordinal| async move {
            let start = ordinal * CAS_CHUNK_BYTES;
            let (body, _) = self.read_chunk(ordinal).await?;
            let range = offset.saturating_sub(start) as usize..(end - start).min(body.len() as u64) as usize;
            Ok::<_, anyhow::Error>(body.slice(range))
        });
        let mut bytes = Vec::with_capacity((end - offset) as usize);
        for block in try_join_all(reads).await? {
            bytes.extend_from_slice(&block);
        }
        Ok(bytes)
    }
}
