use anyhow::{Context, Result, ensure};
use bao_tree::{BaoTree, BlockSize, io::outboard::PreOrderMemOutboard};
use bytes::Bytes;
use futures::{TryStreamExt, future::BoxFuture, stream};
use opendal::Operator;
use tokio::io::{AsyncRead, AsyncReadExt};
use xlb_core::{
    BlakeHash,
    chunks::{CAS_CHUNK_BYTES, CAS_INDEX_LIMIT, ChunkIndex, MODEL_INDEX_PREFIX},
    image::is_digest_hex,
};

pub const ORIGIN_OBJECT_PREFIX: &str = "models/blake3/";
pub const ORIGIN_OUTBOARD_SUFFIX: &str = ".obao";
pub const OUTBOARD_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);

pub fn origin_object_key(blake3: &str) -> Result<String> {
    ensure!(is_digest_hex(blake3), "CAS origin requires a lowercase BLAKE3 content hash");
    Ok(format!("{ORIGIN_OBJECT_PREFIX}{blake3}"))
}

pub fn origin_outboard_key(blake3: &str) -> Result<String> {
    Ok(format!("{}{ORIGIN_OUTBOARD_SUFFIX}", origin_object_key(blake3)?))
}

pub fn origin_outboard_bytes(size: u64) -> u64 {
    BaoTree::new(size, OUTBOARD_BLOCK_SIZE).outboard_size()
}

pub async fn object_stored(origin: &Operator, key: &str, size: u64) -> Result<bool> {
    match origin.stat(key).await {
        Ok(metadata) => Ok(metadata.is_file() && metadata.content_length() == size),
        Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) async fn cas_chunk_stored(origin: &Operator, hash: BlakeHash, size: u64) -> Result<bool> {
    let hash = hash.to_hex();
    let data_key = origin_object_key(&hash)?;
    let outboard_key = origin_outboard_key(&hash)?;
    let (data, outboard) = tokio::try_join!(
        object_stored(origin, &data_key, size),
        object_stored(origin, &outboard_key, origin_outboard_bytes(size)),
    )?;
    Ok(data && outboard)
}

pub(crate) async fn publish_cas_chunk(origin: &Operator, bytes: Bytes) -> Result<BlakeHash> {
    let (bytes, hashes) = tokio::task::spawn_blocking(move || {
        let hashes = PreOrderMemOutboard::create(&bytes, OUTBOARD_BLOCK_SIZE);
        (bytes, hashes)
    }).await?;
    let hash = BlakeHash::from_bytes(*hashes.root.as_bytes());
    let encoded = hash.to_hex();
    tokio::try_join!(
        store_cas_object(origin, origin_object_key(&encoded)?, bytes),
        store_cas_object(origin, origin_outboard_key(&encoded)?, hashes.data.into()),
    )?;
    Ok(hash)
}

async fn store_cas_object(origin: &Operator, key: String, bytes: Bytes) -> Result<()> {
    if !object_stored(origin, &key, bytes.len() as u64).await? {
        origin.write(&key, bytes).await?;
    }
    Ok(())
}

// [impl -> dsn~xlb-fuse-snapshots~1]
pub fn publish_model_index(
    origin: &Operator, reader: impl AsyncRead + Unpin + Send + 'static, digest: &str, size: u64, concurrency: usize,
) -> BoxFuture<'static, Result<()>> {
    let origin = origin.clone();
    let digest = digest.to_owned();
    Box::pin(publish_index(origin, reader, digest, size, concurrency))
}

async fn publish_index(
    origin: Operator, reader: impl AsyncRead + Unpin + Send, digest: String, size: u64, concurrency: usize,
) -> Result<()> {
    ensure!(is_digest_hex(&digest), "model index requires a BLAKE3 digest");
    ensure!(concurrency > 0, "CAS upload concurrency must be positive");
    let state = (reader, size, blake3::Hasher::new());
    let expected = digest.clone();
    let chunks = stream::try_unfold(state, move |(mut reader, remaining, mut hash)| {
        let expected = expected.clone();
        async move {
        if remaining == 0 {
            ensure!(reader.read(&mut [0]).await? == 0, "model exceeds its indexed size");
            ensure!(hash.finalize().to_hex().as_str() == expected, "origin model differs from its BLAKE3 digest");
            return Ok::<_, anyhow::Error>(None);
        }
        let size = remaining.min(CAS_CHUNK_BYTES) as usize;
        let mut chunk = vec![0; size];
        reader.read_exact(&mut chunk).await.context("read model CAS chunk")?;
        hash.update(&chunk);
        Ok(Some((Bytes::from(chunk), (reader, remaining - size as u64, hash))))
        }
    }).map_ok(|bytes| {
        let origin = origin.clone();
        async move { publish_cas_chunk(&origin, bytes).await }
    })
        .try_buffered(concurrency).try_collect().await?;
    let index = serde_json::to_vec(&ChunkIndex { chunks, size_bytes: size })?;
    ensure!(index.len() <= CAS_INDEX_LIMIT, "model CAS index exceeds its read limit");
    let key = format!("{MODEL_INDEX_PREFIX}{digest}");
    if !object_stored(&origin, &key, index.len() as u64).await? {
        origin.write(&key, index).await?;
    }
    Ok(())
}
