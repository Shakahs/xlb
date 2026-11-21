use anyhow::{Context, Result, ensure};
use bao_tree::{BaoTree, BlockSize, io::outboard::PreOrderMemOutboard};
use bytes::Bytes;
use futures::{TryStreamExt, future::BoxFuture, stream};
use opendal::{Error, ErrorKind, Operator};
use tokio::io::{AsyncRead, AsyncReadExt};
use xlb_core::{
    BlakeHash,
    chunks::{CAS_CHUNK_BYTES, CAS_INDEX_LIMIT, ChunkIndex, MODEL_INDEX_PREFIX, ModelIndex, positioned_value},
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

pub async fn publish_cas_chunk(origin: &Operator, bytes: Bytes) -> Result<BlakeHash> {
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
    if matching_cas_object(origin, &key, &bytes).await? {
        return Ok(());
    }
    if origin.info().capability().write_with_if_not_exists {
        match origin.write_with(&key, bytes.clone()).if_not_exists(true).await {
            Ok(_) => {}
            Err(error) if error.kind() == opendal::ErrorKind::ConditionNotMatch => {
                if !matching_cas_object(origin, &key, &bytes).await? { return Err(error.into()); }
            }
            Err(error) => return Err(error.into()),
        }
    } else {
        origin.write(&key, bytes).await?;
    }
    Ok(())
}

async fn matching_cas_object(origin: &Operator, key: &str, expected: &Bytes) -> Result<bool> {
    let size = expected.len() as u64;
    if !object_stored(origin, key, size).await? {
        return Ok(false);
    }
    if size > 0 {
        let existing = origin.read_with(key).range(..size).await?;
        ensure!(existing.to_bytes() == *expected, "{key} differs from its verified CAS bytes");
    }
    Ok(true)
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
    let ranges: Vec<_> = stream::try_unfold((reader, size), move |(mut reader, remaining)| {
        async move {
            if remaining == 0 {
                ensure!(reader.read(&mut [0]).await? == 0, "model exceeds its indexed size");
                return Ok::<_, anyhow::Error>(None);
            }
            let length = remaining.min(CAS_CHUNK_BYTES) as usize;
            let mut chunk = vec![0; length];
            reader.read_exact(&mut chunk).await.context("read model CAS chunk")?;
            Ok(Some(((size - remaining, Bytes::from(chunk)), (reader, remaining - length as u64))))
        }
    }).map_ok(|(offset, bytes)| {
        let origin = origin.clone();
        async move {
            let value = if size > CAS_CHUNK_BYTES {
                let payload = bytes.clone();
                Some(tokio::task::spawn_blocking(move || positioned_value(offset, &payload)).await?)
            } else { None };
            let hash = publish_cas_chunk(&origin, bytes).await?;
            Ok((hash, value))
        }
    })
        .try_buffered(concurrency).try_collect().await?;
    let (chunks, values): (Vec<_>, Vec<_>) = ranges.into_iter().unzip();
    let layout = ChunkIndex { chunks, size_bytes: size };
    let index = ModelIndex { layout, chaining_values: values.into_iter().flatten().collect() };
    store_model_index(&origin, &digest, &index).await
}

pub async fn store_model_index(origin: &Operator, digest: &str, index: &ModelIndex) -> Result<()> {
    ensure!(is_digest_hex(digest), "model index requires a BLAKE3 digest");
    ensure!(index.matches_root(BlakeHash::from_hex(digest)?), "origin model differs from its BLAKE3 digest or size");
    ensure!(index.layout.chunks.len() < 2 || !index.chaining_values.is_empty(), "new multi-chunk indexes require BLAKE3 subtree values");
    let encoded = serde_json::to_vec(index)?;
    ensure!(encoded.len() <= CAS_INDEX_LIMIT, "model CAS index exceeds its read limit");
    let key = format!("{MODEL_INDEX_PREFIX}{digest}");
    if matching_model_index(origin, &key, index).await? { return Ok(()); }
    if origin.info().capability().write_with_if_not_exists {
        match origin.write_with(&key, encoded).if_not_exists(true).await {
            Ok(_) => {},
            Err(error) if error.kind() == ErrorKind::ConditionNotMatch => {
                if !matching_model_index(origin, &key, index).await? { return Err(error.into()); }
            }
            Err(error) => return Err(error.into()),
        }
    } else {
        origin.write(&key, encoded).await?;
    }
    Ok(())
}

async fn matching_model_index(origin: &Operator, key: &str, expected: &ModelIndex) -> Result<bool> {
    let metadata = match origin.stat(key).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let conflict = || Error::new(ErrorKind::ConditionNotMatch, "existing model CAS index differs from verified chunks")
        .with_context("key", key);
    let size = metadata.content_length();
    if size == 0 || size > CAS_INDEX_LIMIT as u64 || !metadata.is_file() { return Err(conflict().into()); }
    let bytes = origin.read_with(key).range(..size).await?;
    if serde_json::from_slice::<ModelIndex>(&bytes.to_bytes()).is_ok_and(|index| {
        index.layout == expected.layout
            && (index.chaining_values.is_empty() || index.chaining_values == expected.chaining_values)
    }) {
        Ok(true)
    } else {
        Err(conflict().into())
    }
}
