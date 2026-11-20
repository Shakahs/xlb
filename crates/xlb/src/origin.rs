//! Origin tier: ranged GETs verified against the blob's published outboard
//! and imported into the blob store as they arrive.
//!
//! The origin is either an S3-compatible bucket read with SigV4-signed,
//! path-style requests, or an unauthenticated URL template. The object is
//! read as `Range` requests of [`OriginConfig::range_bytes`], rounded down to
//! whole 16 KiB chunk groups.
//!
//! ## Outboard
//!
//! Next to the object for a hash, at the object's key followed by
//! [`OUTBOARD_KEY_SUFFIX`], the origin holds the blob's BLAKE3 outboard: a
//! bao-tree pre-order outboard for 16 KiB chunk groups (iroh-blobs'
//! `IROH_BLOCK_SIZE`) with no size prefix. It is the 64-byte hash pair (left
//! child hash, right child hash) of every parent node of the tree, in
//! pre-order, root first, and nothing else; a blob of at most one chunk
//! group has an empty outboard, which is never requested.
//!
//! A range is turned into the bao encoding iroh-blobs exchanges between
//! peers, the size, then parent hash pairs from the outboard and the range's
//! chunk groups from the response body in pre-order, and handed to the
//! store's `import_bao_reader`. The store verifies every hash pair and chunk
//! group against the blob's hash before it writes it, so a range is servable
//! to peers as soon as it is stored and wrong origin content is never
//! stored.

use std::{
    io,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

use bao_tree::{
    iter::{BaoChunk, ResponseIter},
    BaoTree,
};
use bytes::Bytes;
use iroh::endpoint::VarInt;
use iroh_blobs::{
    api::Store,
    protocol::{ChunkRanges, ChunkRangesExt},
    store::IROH_BLOCK_SIZE,
    util::RecvStream,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::{
    rate::RequestRate,
    sigv4::{self, Credentials, SigningInput},
    store::to_iroh,
    stream_bytes::{ByteFragments, receive, receive_exact},
    BlakeHash,
};

/// Default size of one ranged GET.
/// How long an origin connection may stay silent before its request fails.
const ORIGIN_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub const DEFAULT_RANGE_BYTES: u64 = 8 * 1024 * 1024;
/// Default number of ranged GETs in flight for one blob.
pub const DEFAULT_ORIGIN_CONCURRENCY: usize = 32;
/// Default number of retries of one range after its first attempt.
pub const DEFAULT_RANGE_RETRIES: u32 = 4;
/// Default object key layout.
pub const DEFAULT_KEY_TEMPLATE: &str = "models/blake3/{blake3}";
/// Appended to an object's key or URL to name its outboard.
pub const OUTBOARD_KEY_SUFFIX: &str = ".obao";

/// iroh-blobs verifies and stores data in 16 KiB chunk groups. A range or
/// segment boundary inside a group could not be verified on its own.
pub(crate) const CHUNK_GROUP_BYTES: u64 = 16 * 1024;

const HASH_TOKEN: &str = "{blake3}";
/// One hash pair of the outboard.
const HASH_PAIR_BYTES: usize = 64;

/// Where the origin object for a hash lives.
#[derive(Debug, Clone)]
pub enum OriginSource {
    /// An S3-compatible bucket, addressed path-style as
    /// `{endpoint}/{bucket}/{key}` and read with SigV4-signed requests.
    S3 {
        /// e.g. `https://fly.storage.tigris.dev`.
        endpoint: String,
        bucket: String,
        /// SigV4 region; Tigris and R2 use `auto`.
        region: String,
        /// Object key with a `{blake3}` token.
        key_template: String,
        credentials: Credentials,
    },
    /// An unauthenticated URL with a `{blake3}` token. The server must
    /// answer `HEAD` with `Content-Length` and honour `Range`.
    Url { template: String },
}

/// Configuration of the origin tier of one class.
#[derive(Debug, Clone)]
pub struct OriginConfig {
    pub source: OriginSource,
    /// Size of one ranged GET; rounded down to whole chunk groups.
    pub range_bytes: u64,
    /// Ranged GETs in flight for one blob.
    pub concurrency: usize,
    /// Retries of one range after its first attempt.
    pub range_retries: u32,
}

impl OriginConfig {
    /// `source` with the default range size, concurrency and retries.
    pub fn new(source: OriginSource) -> Self {
        Self {
            source,
            range_bytes: DEFAULT_RANGE_BYTES,
            concurrency: DEFAULT_ORIGIN_CONCURRENCY,
            range_retries: DEFAULT_RANGE_RETRIES,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OriginError {
    #[error("origin configuration: {0}")]
    Config(String),
    #[error("origin has no object for this hash")]
    NotFound,
    #[error("origin has no outboard for this hash")]
    NoOutboard,
    #[error("origin outboard: {0}")]
    Outboard(String),
    #[error("origin answered {status} to {method}")]
    Status { method: &'static str, status: u16 },
    #[error("origin response: {0}")]
    Protocol(String),
    #[error("origin request: {0}")]
    Http(#[from] reqwest::Error),
    #[error("origin file: {0}")]
    Io(#[from] std::io::Error),
    /// The store refused a range: it does not verify against the blob's
    /// hash, or the store could not write it.
    #[error("origin range was not accepted: {0}")]
    Rejected(String),
}

impl OriginError {
    /// Whether a further attempt at the same request can succeed.
    fn is_transient(&self) -> bool {
        match self {
            OriginError::Status { status, .. } => *status >= 500 || *status == 429,
            OriginError::Http(_) | OriginError::Protocol(_) => true,
            OriginError::Config(_)
            | OriginError::NotFound
            | OriginError::NoOutboard
            | OriginError::Outboard(_)
            | OriginError::Io(_)
            | OriginError::Rejected(_) => false,
        }
    }
}

/// The resolved request target for one hash.
struct Target {
    url: String,
    /// Host header and unencoded path, present when the request is signed.
    signing: Option<(String, String)>,
}

struct OriginInner {
    config: OriginConfig,
    client: reqwest::Client,
    requests: RequestCounts,
}

/// The type of one S3 request, without credentials or a signed URL.
#[derive(Clone, Copy, Debug)]
pub enum S3RequestKind {
    ObjectSize,
    ObjectRead,
    Outboard,
    DataRange { start: u64, end: u64 },
}

/// One request from client dispatch through release of its response.
#[derive(Clone, Copy, Debug)]
pub struct S3RequestEvent {
    pub kind: S3RequestKind,
    pub status: Option<u16>,
    pub elapsed: Duration,
}

/// S3 request overlap for one asset class since the origin client started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct S3RequestStats {
    pub active: usize,
    pub peak: usize,
    pub active_ranges: usize,
    pub peak_ranges: usize,
    pub dropped_records: u64,
}

#[derive(Default)]
struct RequestCounter {
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl RequestCounter {
    fn start(&self) {
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        if active > self.peak.load(Ordering::Relaxed) {
            self.peak.fetch_max(active, Ordering::Relaxed);
        }
    }
}

#[derive(Default)]
struct RequestCounts {
    all: RequestCounter,
    ranges: RequestCounter,
    events: OnceLock<mpsc::Sender<S3RequestEvent>>,
    dropped_records: AtomicU64,
}

struct RequestGuard<'a> {
    counts: &'a RequestCounts,
    kind: S3RequestKind,
    started: Option<Instant>,
    status: Option<u16>,
}

impl Drop for RequestGuard<'_> {
    fn drop(&mut self) {
        if matches!(self.kind, S3RequestKind::DataRange { .. }) {
            self.counts.ranges.active.fetch_sub(1, Ordering::Relaxed);
        }
        self.counts.all.active.fetch_sub(1, Ordering::Relaxed);
        if let (Some(started), Some(events)) = (self.started, self.counts.events.get()) {
            let event = S3RequestEvent { kind: self.kind, status: self.status, elapsed: started.elapsed() };
            if matches!(events.try_send(event), Err(mpsc::error::TrySendError::Full(_))) {
                self.counts.dropped_records.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

struct TrackedResponse<'a> {
    body: reqwest::Response,
    _request: RequestGuard<'a>,
}

/// The origin tier of one class.
#[derive(Clone)]
pub(crate) struct Origin(Arc<OriginInner>);

/// The outboard of one blob, fetched from the origin into a local file.
pub(crate) struct OriginOutboard {
    tree: BaoTree,
    /// `None` for a blob of one chunk group, whose tree has no parents.
    pairs: Option<Arc<std::fs::File>>,
    path: PathBuf,
}

impl Drop for OriginOutboard {
    fn drop(&mut self) {
        if self.pairs.is_some() {
            if let Err(e) = std::fs::remove_file(&self.path) {
                tracing::warn!("removing {}: {e}", self.path.display());
            }
        }
    }
}

impl OriginOutboard {
    /// The hash pairs of the parent nodes the bao encoding of `ranges`
    /// contains, concatenated in the order the encoding has them.
    async fn parent_pairs(&self, ranges: &ChunkRanges) -> io::Result<Vec<u8>> {
        let mut offsets = Vec::new();
        for chunk in ResponseIter::new(self.tree, ranges.clone()) {
            if let BaoChunk::Parent { node, .. } = chunk {
                let position = self
                    .tree
                    .pre_order_offset(node)
                    .ok_or_else(|| io::Error::other("the outboard has no such parent node"))?;
                offsets.push(position * HASH_PAIR_BYTES as u64);
            }
        }
        let Some(pairs) = self.pairs.clone() else {
            return if offsets.is_empty() {
                Ok(Vec::new())
            } else {
                Err(io::Error::other("the blob has parent nodes but no outboard"))
            };
        };
        // One blocking call per range; the reads are served by the page cache.
        tokio::task::spawn_blocking(move || {
            let mut loaded = vec![0u8; offsets.len() * HASH_PAIR_BYTES];
            let (slots, _) = loaded.as_chunks_mut::<HASH_PAIR_BYTES>();
            for (pair, offset) in slots.iter_mut().zip(offsets) {
                pairs.read_exact_at(pair, offset)?;
            }
            Ok(loaded)
        })
        .await
        .map_err(io::Error::other)?
    }
}

/// The bao encoding of one range of a blob, produced as the store reads it:
/// the size, then for each node of the range's part of the tree in pre-order
/// its hash pair from the outboard or its chunk group from the origin's
/// response body.
struct RangeFeed<'a> {
    /// Size prefix or hash pair not handed out yet.
    pending: Bytes,
    /// The range's hash pairs still to hand out, in encoding order.
    parents: Bytes,
    plan: ResponseIter,
    /// Bytes of the current chunk group still to hand out.
    leaf_remaining: usize,
    /// Body bytes received and not handed out yet.
    body: Bytes,
    response: TrackedResponse<'a>,
    /// Body bytes handed out by every range of the fetch.
    received: &'a AtomicU64,
    /// Why the response body ended early, when it did.
    failure: &'a Mutex<Option<OriginError>>,
}

impl RangeFeed<'_> {
    fn fail(&self, error: OriginError) -> io::Error {
        *self.failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
        io::Error::other("the origin response ended early")
    }

}

impl ByteFragments for RangeFeed<'_> {
    async fn next_bytes(&mut self, limit: usize) -> io::Result<Option<Bytes>> {
        loop {
            if !self.pending.is_empty() {
                return Ok(Some(self.pending.split_to(self.pending.len().min(limit))));
            }
            if self.leaf_remaining != 0 {
                if self.body.is_empty() {
                    self.body = match self.response.body.chunk().await {
                        Ok(Some(chunk)) => chunk,
                        Ok(None) => return Err(self.fail(OriginError::Protocol("origin closed the range short".into()))),
                        Err(err) => return Err(self.fail(err.into())),
                    };
                    continue;
                }
                let count = self.leaf_remaining.min(self.body.len()).min(limit);
                self.leaf_remaining -= count;
                self.received.fetch_add(count as u64, Ordering::Relaxed);
                return Ok(Some(self.body.split_to(count)));
            }
            match self.plan.next() {
                Some(BaoChunk::Parent { .. }) => {
                    if self.parents.len() < HASH_PAIR_BYTES {
                        return Err(io::Error::other("the outboard is short of a hash pair"));
                    }
                    self.pending = self.parents.split_to(HASH_PAIR_BYTES);
                }
                Some(BaoChunk::Leaf { size, .. }) => self.leaf_remaining = size,
                None => return Ok(None),
            }
        }
    }
}

impl RecvStream for RangeFeed<'_> {
    async fn recv_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        receive(self, len).await
    }

    async fn recv_bytes_exact(&mut self, len: usize) -> io::Result<Bytes> {
        receive_exact(self, len).await
    }

    async fn recv_exact(&mut self, mut target: &mut [u8]) -> io::Result<()> {
        while !target.is_empty() {
            let fragment = self.next_bytes(target.len()).await?.ok_or(io::ErrorKind::UnexpectedEof)?;
            let (head, rest) = target.split_at_mut(fragment.len());
            head.copy_from_slice(&fragment);
            target = rest;
        }
        Ok(())
    }

    fn stop(&mut self, _code: VarInt) -> io::Result<()> {
        Ok(())
    }

    fn id(&self) -> u64 {
        0
    }
}

impl Origin {
    pub(crate) fn new(config: OriginConfig) -> Result<Self, OriginError> {
        if config.range_bytes == 0 || config.concurrency == 0 {
            return Err(OriginError::Config(
                "range_bytes and concurrency must be at least 1".into(),
            ));
        }
        let template = match &config.source {
            OriginSource::S3 { key_template, .. } => key_template,
            OriginSource::Url { template } => template,
        };
        if !template.contains(HASH_TOKEN) {
            return Err(OriginError::Config(format!(
                "template {template:?} has no {HASH_TOKEN} token"
            )));
        }
        // Content is verified as it arrives, so a redirect is never needed;
        // refusing them keeps signed requests on the configured host.
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            // A stalled origin connection fails its range instead of
            // holding the fetch; the range's retries then apply.
            .connect_timeout(ORIGIN_IDLE_TIMEOUT)
            .read_timeout(ORIGIN_IDLE_TIMEOUT)
            .build()?;
        Ok(Self(Arc::new(OriginInner { config, client, requests: RequestCounts::default() })))
    }

    pub(crate) fn s3_request_stats(&self) -> Option<S3RequestStats> {
        let counts = &self.0.requests;
        matches!(self.0.config.source, OriginSource::S3 { .. }).then(|| S3RequestStats {
            active: counts.all.active.load(Ordering::Relaxed),
            peak: counts.all.peak.load(Ordering::Relaxed),
            active_ranges: counts.ranges.active.load(Ordering::Relaxed),
            peak_ranges: counts.ranges.peak.load(Ordering::Relaxed),
            dropped_records: counts.dropped_records.load(Ordering::Relaxed),
        })
    }

    pub(crate) fn subscribe_s3_requests(&self, capacity: std::num::NonZeroUsize) -> Option<mpsc::Receiver<S3RequestEvent>> {
        if !matches!(self.0.config.source, OriginSource::S3 { .. }) {
            return None;
        }
        let (sender, receiver) = mpsc::channel(capacity.get());
        self.0.requests.events.set(sender).ok().map(|()| receiver)
    }

    /// Ranged GETs in flight for one blob.
    pub(crate) fn concurrency(&self) -> usize {
        self.0.config.concurrency
    }

    /// The size of one ranged GET, in whole chunk groups.
    pub(crate) fn range_bytes(&self) -> u64 {
        (self.0.config.range_bytes / CHUNK_GROUP_BYTES).max(1) * CHUNK_GROUP_BYTES
    }

    /// The size of the origin object for `hash`.
    pub(crate) async fn object_size(&self, hash: &BlakeHash) -> Result<u64, OriginError> {
        let request = self.0.request(reqwest::Method::HEAD, "HEAD", hash, "", None)?;
        let response = self.0.send(request, S3RequestKind::ObjectSize).await?;
        let status = response.body.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(OriginError::NotFound);
        }
        if !status.is_success() {
            return Err(OriginError::Status {
                method: "HEAD",
                status: status.as_u16(),
            });
        }
        response.body
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| OriginError::Protocol("HEAD carried no Content-Length".into()))
    }

    /// The whole object at `location`, a key of an S3 source's bucket or the
    /// URL of a URL source, or `None` when the origin has none. An object
    /// longer than `limit_bytes` is refused.
    // [impl -> dsn~xlb-fork~1]
    pub(crate) async fn read_object(
        &self,
        location: &str,
        limit_bytes: usize,
    ) -> Result<Option<Vec<u8>>, OriginError> {
        let target = self.0.object_target(location)?;
        let request = self.0.target_request(reqwest::Method::GET, "GET", target, None)?;
        let mut response = self.0.send(request, S3RequestKind::ObjectRead).await?;
        let status = response.body.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if status != reqwest::StatusCode::OK {
            return Err(OriginError::Status {
                method: "GET",
                status: status.as_u16(),
            });
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.body.chunk().await? {
            if body.len() + chunk.len() > limit_bytes {
                return Err(OriginError::Protocol(format!(
                    "{location} is longer than {limit_bytes} bytes"
                )));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Some(body))
    }

    /// Fetch the outboard of the `size`-byte blob `hash` into a new file at
    /// `path`. The file is removed when the outboard is dropped.
    // [impl -> dsn~xlb-swarm-fetch~1]
    pub(crate) async fn fetch_outboard(
        &self,
        hash: &BlakeHash,
        size: u64,
        path: PathBuf,
    ) -> Result<OriginOutboard, OriginError> {
        let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
        let expected = tree.outboard_size();
        if expected == 0 {
            return Ok(OriginOutboard {
                tree,
                pairs: None,
                path,
            });
        }
        let mut failures = 0u32;
        loop {
            match self.0.download_outboard(hash, &path, expected).await {
                Ok(()) => break,
                Err(e) if e.is_transient() && failures < self.0.config.range_retries => {
                    failures += 1;
                    tracing::warn!(%hash, failures, "origin outboard retry: {e}");
                    tokio::time::sleep(retry_delay(failures)).await;
                }
                Err(e) => {
                    if let Err(remove) = tokio::fs::remove_file(&path).await {
                        if remove.kind() != io::ErrorKind::NotFound {
                            tracing::warn!(%hash, "removing {}: {remove}", path.display());
                        }
                    }
                    return Err(e);
                }
            }
        }
        let pairs = tokio::fs::File::open(&path).await?.into_std().await;
        Ok(OriginOutboard {
            tree,
            pairs: Some(Arc::new(pairs)),
            path,
        })
    }

    /// Read bytes `[start, end)` of `hash` from the origin, verify them
    /// against `outboard` and store them. `start` must be a multiple of the
    /// chunk group size, and `end` one too or the blob's size.
    ///
    /// `received` counts the body bytes handed to the store. Retry only
    /// ranges whose chunk groups have yet to be verified and stored.
    // [impl -> dsn~xlb-swarm-fetch~1]
    pub(crate) async fn import_range(
        &self,
        store: &Store,
        hash: BlakeHash,
        outboard: &OriginOutboard,
        (start, end): (u64, u64),
        received: &AtomicU64,
        rate: &RequestRate,
    ) -> Result<(), OriginError> {
        let wanted = ChunkRanges::bytes(start..end);
        let mut missing = wanted.clone();
        let mut failures = 0u32;
        loop {
            let mut outcome = Ok(());
            for boundaries in missing.boundaries().as_chunks::<2>().0 {
                let range = (boundaries[0].to_bytes(), boundaries[1].to_bytes().min(end));
                rate.start().await;
                outcome = self.0.import_attempt(store, &hash, outboard, range, received).await;
                if outcome.is_err() {
                    break;
                }
            }
            match outcome {
                Ok(()) => return Ok(()),
                Err(e) if e.is_transient() && failures < self.0.config.range_retries => {
                    failures += 1;
                    tracing::warn!(%hash, start, end, failures, "origin range retry: {e}");
                    tokio::time::sleep(retry_delay(failures)).await;
                    let held = store.blobs().observe(to_iroh(&hash)).await
                        .map_err(|err| OriginError::Rejected(err.to_string()))?;
                    missing = &wanted - &held.ranges;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

fn retry_delay(failures: u32) -> Duration {
    Duration::from_millis(200u64 << failures.min(6))
}

impl OriginInner {
    async fn send(&self, request: reqwest::RequestBuilder, kind: S3RequestKind) -> Result<TrackedResponse<'_>, reqwest::Error> {
        let counts = &self.requests;
        counts.all.start();
        if matches!(kind, S3RequestKind::DataRange { .. }) {
            counts.ranges.start();
        }
        let mut guard = RequestGuard {
            counts, kind, started: counts.events.get().map(|_| Instant::now()), status: None,
        };
        let body = request.send().await?;
        guard.status = Some(body.status().as_u16());
        Ok(TrackedResponse { body, _request: guard })
    }

    /// The request target of the object for `hash`, or with
    /// [`OUTBOARD_KEY_SUFFIX`] as `suffix` of its outboard.
    fn target(&self, hash: &BlakeHash, suffix: &str) -> Result<Target, OriginError> {
        let template = match &self.config.source {
            OriginSource::Url { template } => template,
            OriginSource::S3 { key_template, .. } => key_template,
        };
        self.object_target(&format!(
            "{}{suffix}",
            template.replace(HASH_TOKEN, &hash.to_hex())
        ))
    }

    /// The request target of the object at `location`: a key of an S3
    /// source's bucket, or the whole URL of a URL source.
    fn object_target(&self, location: &str) -> Result<Target, OriginError> {
        match &self.config.source {
            OriginSource::Url { .. } => Ok(Target {
                url: location.to_string(),
                signing: None,
            }),
            OriginSource::S3 {
                endpoint, bucket, ..
            } => {
                let endpoint = endpoint.trim_end_matches('/');
                let (scheme, rest) = endpoint.split_once("://").ok_or_else(|| {
                    OriginError::Config(format!("endpoint {endpoint:?} has no scheme"))
                })?;
                let (host, base_path) = match rest.split_once('/') {
                    Some((host, path)) => (host, format!("/{path}")),
                    None => (rest, String::new()),
                };
                let path = format!(
                    "{base_path}/{bucket}/{}",
                    location.trim_start_matches('/')
                );
                Ok(Target {
                    url: format!("{scheme}://{host}{}", sigv4::canonical_uri(&path)),
                    signing: Some((host.to_string(), path)),
                })
            }
        }
    }

    /// Build one request, signed when the source has credentials. `range` is
    /// a half-open byte range.
    // [impl -> dsn~xlb-fork~1]
    fn request(
        &self,
        method: reqwest::Method,
        method_name: &'static str,
        hash: &BlakeHash,
        suffix: &str,
        range: Option<(u64, u64)>,
    ) -> Result<reqwest::RequestBuilder, OriginError> {
        self.target_request(method, method_name, self.target(hash, suffix)?, range)
    }

    /// Build one request to `target`, signed when the source has credentials.
    fn target_request(
        &self,
        method: reqwest::Method,
        method_name: &'static str,
        target: Target,
        range: Option<(u64, u64)>,
    ) -> Result<reqwest::RequestBuilder, OriginError> {
        let range_value = range.map(|(start, end)| format!("bytes={start}-{}", end - 1));
        let mut request = self.client.request(method, &target.url);
        if let Some(value) = &range_value {
            request = request.header(reqwest::header::RANGE, value);
        }
        if let (
            Some((host, path)),
            OriginSource::S3 {
                region,
                credentials,
                ..
            },
        ) = (&target.signing, &self.config.source)
        {
            let range_header = range_value.as_deref().map(|value| ("range", value));
            let signed = sigv4::sign(
                credentials,
                &SigningInput {
                    method: method_name,
                    host,
                    path,
                    headers: range_header.as_slice(),
                    payload_sha256: sigv4::EMPTY_PAYLOAD_SHA256,
                    region,
                    unix_secs: sigv4::now_unix(),
                },
            );
            request = request
                .header("x-amz-content-sha256", sigv4::EMPTY_PAYLOAD_SHA256)
                .header("x-amz-date", signed.amz_date)
                .header(reqwest::header::AUTHORIZATION, signed.authorization);
        }
        Ok(request)
    }

    /// One attempt at the whole outboard object, which must be `expected`
    /// bytes long.
    async fn download_outboard(
        &self,
        hash: &BlakeHash,
        path: &Path,
        expected: u64,
    ) -> Result<(), OriginError> {
        let request = self.request(reqwest::Method::GET, "GET", hash, OUTBOARD_KEY_SUFFIX, None)?;
        let mut response = self.send(request, S3RequestKind::Outboard).await?;
        let status = response.body.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(OriginError::NoOutboard);
        }
        if status != reqwest::StatusCode::OK {
            return Err(OriginError::Status {
                method: "GET",
                status: status.as_u16(),
            });
        }
        let mut file = tokio::fs::File::create(path).await?;
        let mut written = 0u64;
        while let Some(chunk) = response.body.chunk().await? {
            written += chunk.len() as u64;
            if written > expected {
                break;
            }
            file.write_all(&chunk).await?;
        }
        // tokio's file completes a write in the background; flush surfaces
        // its error and makes the bytes visible to the readers.
        file.flush().await?;
        if written != expected {
            return Err(OriginError::Outboard(format!(
                "the object is not the {expected} bytes a blob of this size has"
            )));
        }
        Ok(())
    }

    /// One attempt at `[start, end)`: the response body is verified and
    /// stored chunk group by chunk group as it arrives.
    async fn import_attempt(
        &self,
        store: &Store,
        hash: &BlakeHash,
        outboard: &OriginOutboard,
        (start, end): (u64, u64),
        received: &AtomicU64,
    ) -> Result<(), OriginError> {
        // A range that ends inside the blob's last chunk group must cover the
        // group to its end: the encoding of a part of a chunk group has
        // parent nodes below the ones an outboard holds.
        let ranges = if end == outboard.tree.size() {
            ChunkRanges::bytes(start..)
        } else {
            ChunkRanges::bytes(start..end)
        };
        let parents = outboard.parent_pairs(&ranges).await?;
        let request = self.request(reqwest::Method::GET, "GET", hash, "", Some((start, end)))?;
        let response = self.send(request, S3RequestKind::DataRange { start, end }).await?;
        let status = response.body.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(OriginError::NotFound);
        }
        if status != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(OriginError::Status {
                method: "GET",
                status: status.as_u16(),
            });
        }
        let failure = Mutex::new(None);
        let feed = RangeFeed {
            pending: Bytes::copy_from_slice(&outboard.tree.size().to_le_bytes()),
            parents: Bytes::from(parents),
            plan: ResponseIter::new(outboard.tree, ranges.clone()),
            leaf_remaining: 0,
            body: Bytes::new(),
            response,
            received,
            failure: &failure,
        };
        let imported = store
            .blobs()
            .import_bao_reader(to_iroh(hash), ranges, feed)
            .await
            .map(drop);
        match imported {
            Ok(_) => Ok(()),
            Err(e) => {
                let ended_early = failure.into_inner().unwrap_or_else(|e| e.into_inner());
                Err(ended_early
                    .unwrap_or_else(|| OriginError::Rejected(format!("bytes {start}-{end}: {e}"))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3(endpoint: &str) -> OriginInner {
        OriginInner {
            config: OriginConfig::new(OriginSource::S3 {
                endpoint: endpoint.into(),
                bucket: "artifacts".into(),
                region: "auto".into(),
                key_template: DEFAULT_KEY_TEMPLATE.into(),
                credentials: Credentials {
                    access_key_id: "id".into(),
                    secret_access_key: "secret".into(),
                },
            }),
            client: reqwest::Client::new(),
            requests: RequestCounts::default(),
        }
    }

    #[test]
    fn s3_target_is_path_style_under_the_key_template() {
        let hash = BlakeHash::hash(b"x");
        let target = s3("https://fly.storage.tigris.dev/")
            .target(&hash, "")
            .unwrap();
        assert_eq!(
            target.url,
            format!(
                "https://fly.storage.tigris.dev/artifacts/models/blake3/{}",
                hash.to_hex()
            )
        );
        let (host, path) = target.signing.unwrap();
        assert_eq!(host, "fly.storage.tigris.dev");
        assert_eq!(path, format!("/artifacts/models/blake3/{}", hash.to_hex()));
    }

    // [utest -> dsn~xlb-swarm-fetch~1]
    #[test]
    fn the_outboard_is_the_object_key_with_a_suffix() {
        let hash = BlakeHash::hash(b"x");
        let (_, path) = s3("https://fly.storage.tigris.dev")
            .target(&hash, OUTBOARD_KEY_SUFFIX)
            .unwrap()
            .signing
            .unwrap();
        assert_eq!(
            path,
            format!("/artifacts/models/blake3/{}.obao", hash.to_hex())
        );
    }

    #[test]
    fn s3_target_keeps_the_port_in_the_signed_host() {
        let hash = BlakeHash::hash(b"x");
        let (host, _) = s3("http://127.0.0.1:9000")
            .target(&hash, "")
            .unwrap()
            .signing
            .unwrap();
        assert_eq!(host, "127.0.0.1:9000");
    }

    #[test]
    fn a_template_without_the_hash_token_is_rejected() {
        let config = OriginConfig::new(OriginSource::Url {
            template: "http://127.0.0.1/static.bin".into(),
        });
        assert!(matches!(Origin::new(config), Err(OriginError::Config(_))));
    }
}
