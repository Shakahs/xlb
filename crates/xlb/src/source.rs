use std::sync::Arc;

/// Where a fetch got a blob from. A blob that is not in the local cache is
/// read from the peers and the origin together; the fetch is then attributed
/// to whichever of the two delivered more bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FetchTier {
    /// The blob is already complete in the local cache directory.
    Cache = 0,
    /// Peers of the runtime peer set, read concurrently over iroh-blobs.
    Peer = 1,
    /// The HTTP origin, read with concurrent ranged GETs.
    Origin = 2,
}

impl FetchTier {
    pub fn label(self) -> &'static str {
        match self {
            FetchTier::Cache => "cache",
            FetchTier::Peer => "peer",
            FetchTier::Origin => "origin",
        }
    }
}

/// Cumulative download progress for a single in-flight fetch.
///
/// A cache hit never emits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchProgress {
    /// Bytes received so far from the peers and the origin, cumulative.
    pub bytes_so_far: u64,
    /// Total size of the blob.
    pub total: Option<u64>,
    /// The tier that has delivered more of the bytes so far.
    pub tier: FetchTier,
}

/// Callback sink for incremental [`FetchProgress`] during a fetch.
///
/// Invoked on the caller's task, never on the transfer itself, so a slow sink
/// delays only its own caller's view of the progress.
pub type ProgressSink = Arc<dyn Fn(FetchProgress) + Send + Sync>;
