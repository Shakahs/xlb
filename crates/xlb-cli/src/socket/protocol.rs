use serde::{Deserialize, Serialize};

/// Commands sent client → server over the control socket.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    Inspect,
    SubscribeEvents,
    ClassStats {
        class: String,
    },
    Fetch {
        class: String,
        hash: String,
        out: Option<String>,
    },
    SetRole {
        class: String,
        role: String,
    },
    SetBandwidth {
        class: String,
        upload_kbps: Option<u32>,
        download_kbps: Option<u32>,
    },
    /// Declare (or replace) pinned tree `name` on `class` (R1003). Members are
    /// `hashes` (blake3 hex); when that is empty, every `<blake3-hex>` file in
    /// `from_dirs`. Either way the members are then seeded from `from_dirs` —
    /// paths on the NODE's filesystem, hash-verified, no R2 — and published to
    /// peers. The tree persists under the class's `cache_dir`.
    PinTree {
        class: String,
        name: String,
        #[serde(default)]
        hashes: Vec<String>,
        #[serde(default)]
        from_dirs: Vec<String>,
    },
    ListTrees {
        class: String,
    },
    UnpinTree {
        class: String,
        name: String,
    },
}

/// Responses and streamed events sent server → client.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    NodeInfo(NodeInfo),
    ClassStats(ClassStats),
    Event(NodeEvent),
    FetchResult(FetchResult),
    Trees { trees: Vec<TreeInfo> },
    TreePinned { tree: TreeInfo, seed: SeedSummary },
    Ok,
    Error { message: String },
}

/// One pinned tree on a class (R1003). Mirrors `xlb::PinnedTreeInfo`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeInfo {
    pub name: String,
    pub members: u64,
    /// Of `members`, blobs the node holds now. Fewer means some bytes were
    /// never seeded and will arrive only by fetch.
    pub present: u64,
    pub present_bytes: u64,
    /// `false` on an in-memory class: the tree is gone on restart.
    pub durable: bool,
}

/// What seeding a just-pinned tree from `from_dirs` did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedSummary {
    pub ingested: u64,
    pub already_cached: u64,
    /// Members no seed dir held (blake3 hex).
    pub missing: Vec<String>,
    /// Files whose bytes do not hash to their name; skipped.
    pub mismatched: Vec<String>,
    /// Blobs now published to peers; `0` when the node runs no transport.
    pub served: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub uptime_secs: u64,
    pub classes: Vec<ClassInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassInfo {
    pub name: String,
    pub role: String,
    pub cdn_fallback: Option<String>,
    pub permanent_seeds: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassStats {
    pub name: String,
    pub role: String,
    /// NOT IMPLEMENTED — always `0`. The node has no peer registry to count;
    /// `AssetClass` exposes no connected-peer set and `BlobTransport` does not
    /// surface one either. Left in the wire shape because the TUI reads it,
    /// but do not build a dashboard on it: a `0` here means "unknown", not
    /// "no peers". `recent_fetches[].peer_id` is the real, if narrower,
    /// answer — it names peers that actually served something.
    pub peer_count: usize,
    /// Bytes currently held in this class's tier-0 cache (R423-T6). Real as
    /// of R423-T6; was hard-coded `0` before.
    pub cache_bytes: u64,
    /// Blobs currently held. `0` with a non-zero `cache_bytes` is impossible.
    #[serde(default)]
    pub cache_entries: u64,
    pub cache_budget_bytes: u64,
    /// `false` when the class runs an in-memory cache, in which case
    /// `cache_budget_bytes` is NOT enforced and a used/budget ratio is
    /// meaningless. See `xlb::CacheStats::disk_backed`.
    #[serde(default)]
    pub cache_disk_backed: bool,
    /// NOT IMPLEMENTED — always `0.0`. No throughput meter is wired; the
    /// `BandwidthGovernor` sets caps but does not measure achieved rate.
    pub upload_kbps: f64,
    /// NOT IMPLEMENTED — always `0.0`. Same reason as `upload_kbps`.
    pub download_kbps: f64,
    pub governor: GovernorState,
    /// Cumulative fetches observed for this class since the node started
    /// (R423-T6). Cumulative, not windowed, so a poller can diff it.
    #[serde(default)]
    pub fetches_total: u64,
    /// Of `fetches_total`, those served by some tier (i.e. not a miss).
    /// `fetches_total - fetches_hit` is the miss count.
    #[serde(default)]
    pub fetches_hit: u64,
    /// Cumulative bytes handed to callers of this class since node start.
    #[serde(default)]
    pub bytes_served_total: u64,
    /// Most-recent-last, capped ring. Bounded by the node's retention window,
    /// so this is a tail and never a complete history — use `fetches_total`
    /// for counts.
    pub recent_fetches: Vec<FetchRecord>,
}

/// Cumulative per-class fetch counters kept in-process since node start
/// (R423-T6). Projected onto [`ClassStats`] by the socket server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassTotals {
    pub fetches: u64,
    /// Fetches served by some tier. `fetches - hits` is the miss count.
    pub hits: u64,
    pub bytes_served: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovernorState {
    pub on_battery: bool,
    pub metered: bool,
    pub is_passive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRecord {
    pub timestamp_secs: u64,
    pub class: String,
    pub hash_short: String,
    pub bytes: u64,
    pub tier: String,
    pub ok: bool,
    pub note: Option<String>,
    /// `NodeId` of the peer that served it, for the tiers that have one
    /// (lan / swarm / seed). Absent for cache, CDN and misses — that absence
    /// is information, not a gap to fill.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum NodeEvent {
    FetchStarted {
        class: String,
        hash: String,
    },
    FetchCompleted {
        class: String,
        hash: String,
        bytes: u64,
        tier: String,
        elapsed_ms: u64,
    },
    FetchFailed {
        class: String,
        hash: String,
        reason: String,
    },
    PeerJoined {
        class: String,
        node_id: String,
    },
    PeerLeft {
        class: String,
        node_id: String,
    },
    GovernorChanged {
        class: String,
        is_passive: bool,
    },
    /// R423-T7 — the per-fetch metric, one per completed fetch, hit or miss.
    ///
    /// Separate from [`NodeEvent::FetchCompleted`] rather than an extension of
    /// it, for two reasons that both bite if they are merged. `FetchCompleted`
    /// fires only for fetches driven over the control socket and only on
    /// success, so it can never carry a hit RATE; and it is an existing wire
    /// shape with consumers. This one fires for every fetch the node performs
    /// through any path, and reports misses.
    FetchMetric(FetchMetric),
}

/// One completed fetch as a metric row — the R423-T7 tuple, plus the reporting
/// node's own identity and cohort.
///
/// This is also exactly the shape written to `node.metrics_path` as NDJSON, so
/// a collector tailing the file and one subscribed to the socket parse the same
/// bytes. Keep the two in lockstep: a metric that means different things on two
/// transports is worse than one transport.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchMetric {
    /// Unix seconds when the fetch completed.
    pub timestamp_secs: u64,
    /// The reporting node's own `NodeId` — not the peer's.
    pub node_id: String,
    /// Reporting node's cohort (`yah-castle` / `home-server` / `desktop` /
    /// `unknown`), from `node.peer_class`. Without it a dashboard cannot
    /// separate volatile desktop seeds from stable ones (R489-F3).
    pub peer_class: String,
    pub class: String,
    /// Full blake3 hex — not shortened. A truncated hash cannot be joined
    /// against a release manifest, which is the main thing anyone will want to
    /// do with these rows.
    pub blake3: String,
    /// `cache` | `lan` | `swarm` | `seed` | `cdn` | `miss`.
    pub tier_source: String,
    pub bytes_served: u64,
    /// `NodeId` of the peer that served it; absent for cache, CDN and misses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResult {
    pub class: String,
    pub hash: String,
    pub bytes: u64,
    pub tier: String,
    pub elapsed_ms: u64,
    pub saved_to: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // These pin the literal bytes on the wire. yah's desktop card
    // (`app/yah/desktop/src/xlb_ctl.rs`) hand-mirrors these types — this crate
    // is `publish = false` with no lib target, so it cannot be depended on —
    // and pins the SAME literals from its side. Changing a tag or a field name
    // here without changing it there turns an operator surface into a lie, so
    // the two test sets are the seam that makes such a change fail loudly.

    #[test]
    fn the_inspect_command_is_one_tagged_field() {
        assert_eq!(
            serde_json::to_string(&Command::Inspect).unwrap(),
            r#"{"cmd":"inspect"}"#
        );
    }

    #[test]
    fn the_class_stats_command_carries_the_class_beside_the_tag() {
        assert_eq!(
            serde_json::to_string(&Command::ClassStats {
                class: "yah-cli".into()
            })
            .unwrap(),
            r#"{"cmd":"class_stats","class":"yah-cli"}"#
        );
    }

    #[test]
    fn a_node_info_response_flattens_beside_its_type_tag() {
        let resp = Response::NodeInfo(NodeInfo {
            node_id: "abc".into(),
            uptime_secs: 1,
            classes: vec![],
        });
        assert_eq!(
            serde_json::to_string(&resp).unwrap(),
            r#"{"type":"node_info","node_id":"abc","uptime_secs":1,"classes":[]}"#
        );
    }

    // R1003: yah desktop's xlb_ctl pins these same literals from its side.
    #[test]
    fn the_tree_commands_are_on_the_wire_under_these_names() {
        assert_eq!(
            serde_json::to_string(&Command::ListTrees { class: "c".into() }).unwrap(),
            r#"{"cmd":"list_trees","class":"c"}"#
        );
        assert_eq!(
            serde_json::to_string(&Command::UnpinTree {
                class: "c".into(),
                name: "t".into()
            })
            .unwrap(),
            r#"{"cmd":"unpin_tree","class":"c","name":"t"}"#
        );
        let pin: Command = serde_json::from_str(
            r#"{"cmd":"pin_tree","class":"c","name":"t","from_dirs":["/a"]}"#,
        )
        .unwrap();
        assert!(matches!(pin, Command::PinTree { hashes, from_dirs, .. }
            if hashes.is_empty() && from_dirs == ["/a"]));
    }

    #[test]
    fn a_trees_response_carries_each_tree_beside_its_type_tag() {
        let resp = Response::Trees {
            trees: vec![TreeInfo {
                name: "t".into(),
                members: 3,
                present: 2,
                present_bytes: 10,
                durable: true,
            }],
        };
        assert_eq!(
            serde_json::to_string(&resp).unwrap(),
            r#"{"type":"trees","trees":[{"name":"t","members":3,"present":2,"present_bytes":10,"durable":true}]}"#
        );
    }

    #[test]
    fn an_error_response_carries_message_beside_its_type_tag() {
        let resp = Response::Error {
            message: "boom".into(),
        };
        assert_eq!(
            serde_json::to_string(&resp).unwrap(),
            r#"{"type":"error","message":"boom"}"#
        );
    }

    #[test]
    fn the_r423_t6_class_stats_fields_are_on_the_wire_under_these_names() {
        let stats = ClassStats {
            name: "yah-cli".into(),
            role: "permanent".into(),
            peer_count: 0,
            cache_bytes: 2048,
            cache_entries: 2,
            cache_budget_bytes: 8192,
            cache_disk_backed: true,
            upload_kbps: 0.0,
            download_kbps: 0.0,
            governor: GovernorState {
                on_battery: false,
                metered: true,
                is_passive: false,
            },
            fetches_total: 5,
            fetches_hit: 4,
            bytes_served_total: 900,
            recent_fetches: vec![],
        };
        let json = serde_json::to_value(&stats).unwrap();
        for key in [
            "cache_bytes",
            "cache_entries",
            "cache_budget_bytes",
            "cache_disk_backed",
            "fetches_total",
            "fetches_hit",
            "bytes_served_total",
            "recent_fetches",
        ] {
            assert!(json.get(key).is_some(), "missing wire field {key}");
        }
        assert_eq!(json["cache_disk_backed"], serde_json::json!(true));
        assert_eq!(json["fetches_hit"], serde_json::json!(4));
    }

    #[test]
    fn a_fetch_record_omits_peer_id_rather_than_sending_null() {
        // Absence is the signal for cache / cdn / miss rows. A `null` would
        // read the same to a careless consumer, but `skip_serializing_if`
        // keeps the tail small on the common case.
        let rec = FetchRecord {
            timestamp_secs: 1,
            class: "yah-cli".into(),
            hash_short: "abcd".into(),
            bytes: 10,
            tier: "cache".into(),
            ok: true,
            note: None,
            peer_id: None,
        };
        let json = serde_json::to_string(&rec).unwrap();
        assert!(!json.contains("peer_id"), "{json}");

        let served = FetchRecord {
            peer_id: Some("p1".into()),
            tier: "seed".into(),
            ..rec
        };
        assert!(serde_json::to_string(&served).unwrap().contains(r#""peer_id":"p1""#));
    }
}
