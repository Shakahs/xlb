use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use futures::future::try_join_all;
use xlb::AssetClass;
use xlb_core::chunks::{CAS_CHUNK_BYTES, ChunkIndex};


#[derive(Clone)]
pub(crate) struct ChunkFile {
    pub(crate) index: Arc<ChunkIndex>,
    class: AssetClass,
}

impl ChunkFile {
    pub(crate) fn new(index: Arc<ChunkIndex>, class: AssetClass) -> Result<Self> {
        ensure!(index.is_complete(), "CAS index has the wrong number of chunks");
        Ok(Self { index, class })
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
            let (hash, length) = self.index.chunk(ordinal).context("CAS index is truncated")?;
            let start = ordinal * CAS_CHUNK_BYTES;
            let body = self.class.indexed_chunk(hash, length).read_chunk().await?;
            let range = offset.saturating_sub(start) as usize..(end - start).min(length) as usize;
            Ok::<_, anyhow::Error>(body.slice(range))
        });
        let mut bytes = Vec::with_capacity((end - offset) as usize);
        for block in try_join_all(reads).await? {
            bytes.extend_from_slice(&block);
        }
        Ok(bytes)
    }
}
