//! Platform-neutral core of xlb: nothing here touches tokio, the filesystem,
//! iroh or reqwest.
//!
//! The native `xlb` crate re-exports these types from its root.

pub mod hash;
pub mod image;
pub mod peer;
pub mod retention;

pub use hash::{BlakeHash, InvalidHash};
pub use peer::{NodeTopology, PeerReach};
pub use retention::{PinSetUsage, RetentionIndex, RetentionPolicy, RetentionUsage};

/// Payload bytes received during one fetch, grouped by origin and peer ID.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchSources {
    pub origin_bytes: u64,
    pub peers: std::collections::BTreeMap<String, u64>,
}

impl FetchSources {
    pub fn is_empty(&self) -> bool {
        self.origin_bytes == 0 && self.peers.is_empty()
    }
}
