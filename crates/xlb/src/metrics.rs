//! Per-fetch observability — which tier actually served a blob, how many
//! bytes, and how long it took.
//!
//! One report per completed [`crate::Asset::fetch_to_file`] call, hit or miss.
//! **A miss is a report too.** `tier: None` is the "no source had it" case; a
//! hit rate built only from successes has no denominator.
//!
//! `bytes_served` is download-side: bytes this node obtained. Bytes this node
//! served to a peer are not visible from the fetch path.

use std::sync::Arc;

use crate::{BlakeHash, FetchTier};

/// One completed fetch, as a metric.
///
/// Emitted through the [`FetchObserver`] registered on
/// [`crate::AssetClassConfig::observer`], and returned directly by
/// [`crate::Asset::fetch_to_file_reported`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchReport {
    /// The [`crate::AssetClass`] this fetch belonged to.
    pub class: &'static str,
    /// Content address of the blob.
    pub hash: BlakeHash,
    /// Which tier served it: the peers when most bytes came from them, else
    /// the origin. `None` means no source had the blob.
    pub tier: Option<FetchTier>,
    /// Always `None`: the peer tier reads one blob from several peers at once,
    /// so there is no single serving peer to attribute.
    pub peer_id: Option<String>,
    /// Size of the blob obtained. `0` on a miss.
    pub bytes_served: u64,
    /// Bytes this fetch read from peers. `0` on a cache hit and on a miss.
    pub peer_bytes: u64,
    /// Bytes this fetch read from the origin. `0` on a cache hit and on a
    /// miss.
    pub origin_bytes: u64,
    /// Wall time for the whole call, including tiers that missed first.
    pub duration_ms: u64,
}

impl FetchReport {
    /// `true` when some tier served the blob.
    pub fn is_hit(&self) -> bool {
        self.tier.is_some()
    }

    /// `true` when the bytes came from peers rather than from the local cache
    /// or the origin.
    pub fn is_peer_hit(&self) -> bool {
        matches!(self.tier, Some(FetchTier::Peer))
    }

    /// Label of the serving tier, or `"miss"`.
    pub fn tier_label(&self) -> &'static str {
        match self.tier {
            Some(t) => t.label(),
            None => "miss",
        }
    }
}

/// Called once per completed fetch. Must not block: it runs on the fetch
/// caller's task.
pub type FetchObserver = Arc<dyn Fn(&FetchReport) + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    fn report(tier: Option<FetchTier>) -> FetchReport {
        FetchReport {
            class: "test",
            hash: BlakeHash::hash(b"x"),
            tier,
            peer_id: None,
            bytes_served: 1,
            peer_bytes: 0,
            origin_bytes: 0,
            duration_ms: 0,
        }
    }

    #[test]
    fn a_miss_is_not_a_hit_and_labels_as_miss() {
        let r = report(None);
        assert!(!r.is_hit());
        assert!(!r.is_peer_hit());
        assert_eq!(r.tier_label(), "miss");
    }

    #[test]
    fn cache_and_origin_are_hits_but_never_peer_hits() {
        for (tier, label) in [(FetchTier::Cache, "cache"), (FetchTier::Origin, "origin")] {
            let r = report(Some(tier));
            assert!(r.is_hit());
            assert!(!r.is_peer_hit());
            assert_eq!(r.tier_label(), label);
        }
    }

    #[test]
    fn the_peer_tier_is_a_peer_hit() {
        assert!(report(Some(FetchTier::Peer)).is_peer_hit());
    }
}
