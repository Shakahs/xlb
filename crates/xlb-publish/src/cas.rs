use anyhow::{Context, Result, ensure};
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

rust_fsm::state_machine! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    chunk_publication(Checking)

    Checking => {
        Found => Complete [Finish],
        Missing => Writing [Write],
    },
    Writing => {
        Stored => Complete [Finish],
        Conflict => Rechecking [Check],
    },
    Rechecking => {
        Found => Complete [Finish],
        Missing => Retry [RetryWrite],
    },
}

pub fn origin_object_key(blake3: &str) -> Result<String> {
    ensure!(is_digest_hex(blake3), "CAS origin requires a lowercase BLAKE3 content hash");
    Ok(format!("{ORIGIN_OBJECT_PREFIX}{blake3}"))
}

pub async fn object_stored(origin: &Operator, key: &str, size: u64) -> Result<bool> {
    match origin.stat(key).await {
        Ok(metadata) => Ok(metadata.is_file() && metadata.content_length() == size),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) async fn cas_chunk_stored(origin: &Operator, hash: BlakeHash, size: u64) -> Result<bool> {
    object_stored(origin, &origin_object_key(&hash.to_hex())?, size).await
}

pub async fn publish_cas_chunk(origin: &Operator, bytes: Bytes) -> Result<BlakeHash> {
    let (bytes, hash) = tokio::task::spawn_blocking(move || {
        let hash = BlakeHash::hash(&bytes);
        (bytes, hash)
    }).await?;
    store_cas_object(origin, origin_object_key(&hash.to_hex())?, bytes).await?;
    Ok(hash)
}

// [impl -> dsn~xlb-model-origin~1]
async fn store_cas_object(origin: &Operator, key: String, bytes: Bytes) -> Result<()> {
    let size = bytes.len() as u64;
    let check_object = || async {
        match origin.stat(&key).await {
            Ok(metadata) if metadata.is_file() && metadata.content_length() == size => Ok(true),
            Ok(_) => Err(Error::new(ErrorKind::ConditionNotMatch, "existing CAS object has an unexpected length")
                .with_context("key", &key)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    };
    use chunk_publication::{Input, Output, StateMachine};
    let mut machine = StateMachine::new();
    let mut payload = Some(bytes);
    let mut conflict: Option<Error> = None;
    let mut event = if check_object().await? { Input::Found } else { Input::Missing };
    loop {
        event = match machine.consume(&event).expect("valid CAS publication transition") {
            Some(Output::Finish) => return Ok(()),
            Some(Output::Check) => if check_object().await? { Input::Found } else { Input::Missing },
            Some(Output::RetryWrite) => return Err(conflict.expect("conditional write conflict").set_temporary().into()),
            Some(Output::Write) => {
                let bytes = payload.take().expect("CAS payload is written once");
                let result = if origin.info().capability().write_with_if_not_exists {
                    origin.write_with(&key, bytes).if_not_exists(true).await.map(|_| ())
                } else { origin.write(&key, bytes).await.map(|_| ()) };
                match result {
                    Ok(()) => Input::Stored,
                    Err(error) if error.kind() == ErrorKind::ConditionNotMatch => { conflict = Some(error); Input::Conflict },
                    Err(error) => return Err(error.into()),
                }
            },
            None => unreachable!("each CAS publication transition has an action"),
        };
    }
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
