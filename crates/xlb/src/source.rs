use std::sync::Arc;

/// Source of a verified chunk or the dominant source of a model read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FetchTier {
    /// The chunk is already in Foyer.
    Cache = 0,
    /// A peer supplied the chunk.
    Peer = 1,
    /// The S3 backing store supplied the chunk.
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchProgress {
    /// Bytes received so far from the peers and the origin, cumulative.
    pub bytes_so_far: u64,
    /// Total size of the blob.
    pub total: Option<u64>,
    /// The tier that has delivered more of the bytes so far.
    pub tier: FetchTier,
    pub sources: xlb_core::FetchSources,
}

/// Callback sink for incremental [`FetchProgress`] during a fetch.
///
/// Invoked on the caller's task, never on the transfer itself, so a slow sink
/// delays only its own caller's view of the progress.
pub type ProgressSink = Arc<dyn Fn(FetchProgress) + Send + Sync>;
