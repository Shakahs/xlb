//! OpenDAL reads for the S3-backed chunk cache.

use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock, atomic::{AtomicU64, AtomicUsize, Ordering}},
    time::{Duration, Instant},
};

use backon::{ExponentialBuilder, Retryable};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use opendal::{Buffer, ErrorKind, HttpBody, HttpTransport, HttpTransporter, Operator, layers::TimeoutLayer};
use opendal_http_transport_reqwest::ReqwestTransport;
use tokio::sync::mpsc;

use crate::{BlakeHash, Error, RequestLimits, Result};

const ORIGIN_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const HASH_TOKEN: &str = "{blake3}";
pub const DEFAULT_ORIGIN_CONCURRENCY: usize = 32;
pub const DEFAULT_ORIGIN_RETRIES: u32 = 4;
pub const DEFAULT_KEY_TEMPLATE: &str = "models/blake3/{blake3}";

#[derive(Clone)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

pub struct OriginConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub key_template: String,
    pub credentials: Credentials,
    pub concurrency: NonZeroUsize,
    pub retries: u32,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OriginError {
    #[error(transparent)]
    Storage(#[from] opendal::Error),
    #[error("object {key} exceeds the {limit}-byte read limit")]
    TooLarge { key: String, limit: usize },
}

/// The type of one S3 request, without credentials or a signed URL.
#[derive(Clone, Copy, Debug)]
pub enum S3RequestKind {
    ObjectRead,
    ChunkRead,
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
    pub active_chunks: usize,
    pub peak_chunks: usize,
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
    chunks: RequestCounter,
    events: OnceLock<mpsc::Sender<S3RequestEvent>>,
    dropped_records: AtomicU64,
}

struct RequestGuard {
    counts: Arc<RequestCounts>,
    kind: S3RequestKind,
    started: Option<Instant>,
    status: Option<u16>,
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        if matches!(self.kind, S3RequestKind::ChunkRead) {
            self.counts.chunks.active.fetch_sub(1, Ordering::Relaxed);
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

struct S3Transport {
    inner: ReqwestTransport,
    limits: RequestLimits,
    counts: Arc<RequestCounts>,
    chunk_prefix: String,
}

impl HttpTransport for S3Transport {
    async fn fetch(&self, request: http::Request<Buffer>) -> opendal::Result<http::Response<HttpBody>> {
        let slot = self.limits.origin().await;
        let kind = if request.uri().path().contains(&self.chunk_prefix) {
            S3RequestKind::ChunkRead
        } else {
            S3RequestKind::ObjectRead
        };
        let counts = &self.counts;
        counts.all.start();
        if matches!(kind, S3RequestKind::ChunkRead) {
            counts.chunks.start();
        }
        let mut guard = RequestGuard {
            counts: counts.clone(),
            kind,
            started: counts.events.get().map(|_| Instant::now()),
            status: None,
        };
        let response = self.inner.fetch(request).await?;
        guard.status = Some(response.status().as_u16());
        Ok(response.map(|body| body.map_inner(|stream| {
            let lease = (guard, slot);
            Box::new(stream.inspect(move |_| {
                let _lease = &lease;
            }))
        })))
    }
}

pub(crate) struct Origin {
    operator: Operator,
    key_template: String,
    requests: Arc<RequestCounts>,
    retries: u32,
}

impl Origin {
    pub(crate) fn new(config: OriginConfig, limits: RequestLimits) -> Result<Self> {
        let (chunk_prefix, _) = config.key_template.split_once(HASH_TOKEN)
            .ok_or_else(|| Error::Config("CAS key template must include {blake3}".into()))?;
        let client = reqwest::Client::builder()
            .http1_only()
            .connect_timeout(ORIGIN_IDLE_TIMEOUT)
            .read_timeout(ORIGIN_IDLE_TIMEOUT)
            .build().map_err(|error| Error::Config(format!("build S3 client: {error}")))?;
        let requests = Arc::new(RequestCounts::default());
        let backend = opendal::services::S3::default()
            .endpoint(&config.endpoint)
            .bucket(&config.bucket)
            .region(&config.region)
            .access_key_id(&config.credentials.access_key_id)
            .secret_access_key(&config.credentials.secret_access_key)
            .disable_config_load()
            .disable_ec2_metadata();
        let operator = Operator::new(backend)
            .map_err(|error| Error::Config(format!("configure S3 origin: {error}")))?;
        let context = operator.context().with_http_transport(HttpTransporter::new(S3Transport {
            inner: ReqwestTransport::new(client),
            limits,
            counts: requests.clone(),
            chunk_prefix: chunk_prefix.to_owned(),
        }));
        let operator = operator.with_context(context).layer(TimeoutLayer::new().with_io_timeout(ORIGIN_IDLE_TIMEOUT));
        Ok(Self { operator, key_template: config.key_template, retries: config.retries, requests })
    }

    pub(crate) fn s3_request_stats(&self) -> S3RequestStats {
        let counts = &self.requests;
        S3RequestStats {
            active: counts.all.active.load(Ordering::Relaxed),
            peak: counts.all.peak.load(Ordering::Relaxed),
            active_chunks: counts.chunks.active.load(Ordering::Relaxed),
            peak_chunks: counts.chunks.peak.load(Ordering::Relaxed),
            dropped_records: counts.dropped_records.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn subscribe_s3_requests(&self, capacity: NonZeroUsize) -> Option<mpsc::Receiver<S3RequestEvent>> {
        let (sender, receiver) = mpsc::channel(capacity.get());
        self.requests.events.set(sender).ok().map(|()| receiver)
    }

    pub(crate) async fn read_object(&self, key: &str, limit: usize) -> Result<Option<Vec<u8>>, OriginError> {
        match self.read_whole(key, limit).await {
            Ok(buffer) => Ok(Some(buffer.to_vec())),
            Err(OriginError::Storage(error)) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn read_chunk(&self, hash: BlakeHash, length: usize) -> Result<Bytes, OriginError> {
        self.read_whole(&self.key_template.replace(HASH_TOKEN, &hash.to_hex()), length)
            .await.map(|buffer| buffer.to_bytes())
    }

    // Keep retry outside the reader so an interrupted body starts a fresh GET.
    // [impl -> dsn~xlb-swarm-fetch~1]
    async fn read_whole(&self, key: &str, limit: usize) -> Result<Buffer, OriginError> {
        (|| async {
            let mut stream = self.operator.reader(key).await?.into_stream(..).await?;
            let mut length = 0;
            let mut parts = Vec::new();
            while let Some(buffer) = stream.try_next().await? {
                length += buffer.len();
                if length > limit {
                    return Err(OriginError::TooLarge { key: key.to_owned(), limit });
                }
                parts.extend(buffer);
            }
            Ok(parts.into_iter().collect())
        })
        .retry(ExponentialBuilder::default().with_jitter().with_max_times(self.retries as usize))
        .when(|error| matches!(error, OriginError::Storage(error) if error.is_temporary()))
        .await
    }
}
