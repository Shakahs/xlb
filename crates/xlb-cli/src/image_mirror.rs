//! Read-only OCI Distribution endpoint for the node's containerd.

use std::{collections::HashMap, io::{Error, ErrorKind}, ops::Range, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use rand::seq::SliceRandom;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_util::task::AbortOnDropHandle;
use xlb::{Asset, AssetClass, BlakeHash};
use xlb_core::chunks::positioned_value;
use xlb_core::image::{IMAGE_CHUNK_BYTES, IMAGE_DIGEST_PREFIX, IMAGE_INDEX_LIMIT, IMAGE_INDEX_PREFIX, ImageBlobIndex, blake3_digest_hex, is_digest_hex};

#[derive(Clone)]
struct ImageMirror {
    class: AssetClass,
    index: Arc<RwLock<HashMap<String, Arc<ImageBlobIndex>>>>,
    concurrency: usize,
}

// [impl -> dsn~xlb-image-mirror~1]
pub async fn serve(class: AssetClass, concurrency: usize, cancel: tokio_util::sync::CancellationToken) -> Result<()> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 5477))
        .await
        .context("bind OCI image mirror")?;
    axum::serve(listener, router(class, concurrency))
        .with_graceful_shutdown(cancel.cancelled_owned()).await?;
    Ok(())
}

fn router(class: AssetClass, concurrency: usize) -> Router {
    Router::new()
        .route("/v2/", get(|| async {
            ([("docker-distribution-api-version", "registry/2.0")], StatusCode::OK)
        }))
        .route("/v2/{*path}", get(serve_blob))
        .with_state(ImageMirror {
            class, index: Arc::default(), concurrency,
        })
}

async fn open_slice((asset, range, positioned): (Asset, Range<usize>, Option<(u64, BlakeHash)>)) -> Result<Bytes> {
    let bytes = asset.read_chunk().await?;
    if let Some((offset, value)) = positioned {
        let payload = bytes.clone();
        let actual = tokio::task::spawn_blocking(move || positioned_value(offset, &payload)).await?;
        if actual != value {
            return Err(Error::new(ErrorKind::InvalidData, "OCI chunk differs from its indexed position").into());
        }
    }
    Ok(bytes.slice(range))
}

impl ImageMirror {
    async fn lookup(&self, digest: &str) -> Result<Option<Arc<ImageBlobIndex>>> {
        if let Some(blob) = self.index.read().await.get(digest) {
            return Ok(Some(blob.clone()));
        }
        let key = format!("{IMAGE_INDEX_PREFIX}{digest}");
        let Some(bytes) = self.class.read_origin_object(&key, IMAGE_INDEX_LIMIT).await? else {
            return Ok(None);
        };
        let blob: ImageBlobIndex = serde_json::from_slice(&bytes).context("parse OCI image index")?;
        let hash = BlakeHash::from_hex(digest)?;
        anyhow::ensure!(blob.matches_root(hash), "OCI blob index differs from its BLAKE3 digest");
        HeaderValue::from_str(&blob.media_type).context("invalid indexed OCI media type")?;
        if blob.chunks.len() > 1 && blob.chaining_values.is_empty() {
            let blob = &blob;
            let mut chunks = stream::iter(0..blob.chunks.len()).map(|position| {
                let offset = position as u64 * IMAGE_CHUNK_BYTES as u64;
                let size = (blob.size_bytes - offset).min(IMAGE_CHUNK_BYTES as u64);
                let chunk = self.class.indexed_chunk(blob.chunks[position], size);
                open_slice((chunk, 0..size as usize, None))
            }).buffered(self.concurrency);
            let mut hasher = blake3::Hasher::new();
            while let Some(bytes) = chunks.try_next().await? {
                hasher.update(&bytes);
            }
            anyhow::ensure!(hasher.finalize().as_bytes() == hash.as_bytes(),
                "legacy OCI blob index differs from its BLAKE3 digest");
        }
        let blob = Arc::new(blob);
        Ok(Some(self.index.write().await.entry(digest.to_owned()).or_insert(blob).clone()))
    }
}

async fn serve_blob(
    State(mirror): State<ImageMirror>,
    Path(path): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let Some(digest) = image_digest(&path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match mirror.serve_digest(digest, method, headers).await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%digest, %error, "OCI image pull failed");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}

/// The BLAKE3 hex an OCI blob or manifest request names. Blobs are named by
/// `blake3:<hex>`; manifests also by the tag `<hex>`, which pods reference
/// because the kubelet rejects digest algorithms it does not register.
fn image_digest(path: &str) -> Option<&str> {
    let (resource, reference) = path.rsplit_once('/')?;
    let (repository, kind) = resource.rsplit_once('/')?;
    if repository.is_empty() {
        return None;
    }
    match kind {
        "blobs" => blake3_digest_hex(reference),
        "manifests" => blake3_digest_hex(reference).or_else(|| is_digest_hex(reference).then_some(reference)),
        _ => None,
    }
}

impl ImageMirror {
    async fn serve_digest(&self, digest: &str, method: Method, headers: HeaderMap) -> Result<Response> {
        let Some(blob) = self.lookup(digest).await? else {
            return Ok(StatusCode::NOT_FOUND.into_response());
        };
        let size = blob.size_bytes;
        let mut response = Response::builder()
            .header(header::CONTENT_TYPE, &blob.media_type)
            .header("docker-content-digest", format!("{IMAGE_DIGEST_PREFIX}{digest}"))
            .header(header::ACCEPT_RANGES, "bytes");
        if method == Method::HEAD {
            return Ok(response.header(header::CONTENT_LENGTH, size).body(Body::empty())?);
        }

        let (start, length) = match headers.get(header::RANGE) {
            Some(value) => {
                let Some((start, end)) = value.to_str().ok().and_then(|value| byte_range(value, size)) else {
                    return Ok((StatusCode::RANGE_NOT_SATISFIABLE,
                        [(header::CONTENT_RANGE, format!("bytes */{size}"))]).into_response());
                };
                response = response.status(StatusCode::PARTIAL_CONTENT)
                    .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{size}"));
                (start, end - start + 1)
            }
            None => (0, size),
        };
        let body = self.chunk_body(digest.to_owned(), blob, start, length);
        Ok(response.header(header::CONTENT_LENGTH, length).body(body)?)
    }

    // [impl -> dsn~xlb-chunk-discovery~1]
    fn chunk_body(&self, digest: String, blob: Arc<ImageBlobIndex>, start: u64, length: u64) -> Body {
        let end = start + length;
        let chunk_bytes = IMAGE_CHUNK_BYTES as u64;
        let class = self.class.clone();
        let first = start / chunk_bytes;
        let last = end.div_ceil(chunk_bytes);
        let width = self.concurrency;
        let cached = self.index.clone();
        let resolved = blob.clone();
        let sources = stream::iter((first..last).step_by(width))
            .flat_map(move |window| {
                let mut positions: Vec<_> = (window..(window + width as u64).min(last)).collect();
                positions.shuffle(&mut rand::rng());
                let mut pulls: Vec<_> = positions.into_iter().map(|position| {
                    let offset = position * chunk_bytes;
                    let size = (blob.size_bytes - offset).min(chunk_bytes);
                    let asset = class.indexed_chunk(blob.chunks[position as usize], size);
                    let range = start.saturating_sub(offset) as usize..(end - offset).min(size) as usize;
                    let positioned = blob.chaining_values.get(position as usize).map(|value| (offset, *value));
                    let pull = tokio::spawn(async move {
                        open_slice((asset, range, positioned)).await
                    });
                    (position, AbortOnDropHandle::new(pull))
                }).collect();
                pulls.sort_unstable_by_key(|(position, _)| *position);
                stream::iter(pulls.into_iter().map(|(_, pull)| pull))
            })
            .then(move |pull| {
                let cached = cached.clone();
                let resolved = resolved.clone();
                let digest = digest.clone();
                async move {
                    let result = pull.await?;
                    if let Err(error) = &result {
                        if error.downcast_ref::<Error>().is_some_and(|error| error.kind() == ErrorKind::InvalidData) {
                            let mut index = cached.write().await;
                            if index.get(&digest).is_some_and(|current| Arc::ptr_eq(current, &resolved)) {
                                index.remove(&digest);
                            }
                        }
                    }
                    result
                }
            });
        Body::from_stream(sources)
    }
}

fn byte_range(value: &str, size: u64) -> Option<(u64, u64)> {
    let last = size.checked_sub(1)?;
    let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
    if start.is_empty() {
        let suffix = end.parse::<u64>().ok()?;
        return (suffix > 0).then(|| (size.saturating_sub(suffix), last));
    }
    let start = start.parse::<u64>().ok()?;
    let end = if end.is_empty() { last } else { end.parse::<u64>().ok()?.min(last) };
    (start <= end).then_some((start, end))
}
