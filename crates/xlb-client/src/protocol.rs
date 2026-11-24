//! The JSON messages of the daemon's control socket.
//!
//! Every message is one frame (see [`crate::write_frame`]). A [`Command`] is
//! tagged by `cmd`, a [`Response`] by `type`. Each command is answered by one
//! response, except:
//!
//! - `fetch`, answered by zero or more `fetch_progress` frames and then one
//!   `fetch_result` or `error`;
//! - `subscribe_events`, answered by `event` frames until the client hangs up.

use serde::{Deserialize, Serialize};
use xlb_core::{FetchSources, NodeTopology};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    Inspect,
    SubscribeEvents,
    ClassStats {
        class: String,
    },
    /// Prepare an indexed model and return its mounted path.
    Fetch {
        class: String,
        /// BLAKE3 hash, 64 hex characters.
        hash: String,
    },
    /// Replace the peer set.
    SetPeers {
        /// The nodes the daemon may dial, observe and read from.
        peers: Vec<Peer>,
        /// Node IDs the daemon serves get and observe requests of. They
        /// need not be in `peers`. When absent, the daemon serves exactly
        /// the nodes in `peers`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        admit: Option<Vec<String>>,
        /// The class whose peer set to replace; every class with a peer tier
        /// when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        class: Option<String>,
        /// Topology the selected node publishes to the peer directory.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        topology: Option<NodeTopology>,
    },

}

/// One member of a peer set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    /// The peer's node ID, as its `inspect` reports it (hex).
    pub node_id: String,
    /// UDP socket addresses the peer's endpoint may be reachable at, private
    /// and public, e.g. `10.0.3.7:4433`. May be empty when `relay_url` is
    /// set.
    #[serde(default)]
    pub addrs: Vec<String>,
    /// The peer's relay, as its `inspect` reports it. Through it the daemon
    /// finds a direct path to a peer whose socket addresses are not
    /// reachable as given; blob data never travels through the relay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    NodeInfo(NodeInfo),
    ClassStats(ClassStats),
    Event(NodeEvent),
    FetchProgress(FetchProgress),
    FetchResult(FetchResult),
    /// Answer to `set_peers`: how many classes took the new set.
    PeersSet { classes: u64 },
    Error { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node ID of the first class with a peer tier; `null` when none has one.
    pub node_id: Option<String>,
    /// UDP addresses that class's endpoint is bound to. A wildcard address
    /// (`0.0.0.0:4433`) names the port; the caller supplies the node's IP.
    pub udp_addrs: Vec<String>,
    /// The relay that class's endpoint is reachable through, once it has
    /// connected to one; absent before that and with relays disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_url: Option<String>,
    /// Cache directory of that class, or of the first class.
    pub cache_dir: Option<String>,
    pub uptime_secs: u64,
    pub classes: Vec<ClassInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassInfo {
    pub name: String,
    /// `null` when the class has no peer tier.
    pub node_id: Option<String>,
    pub udp_addrs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_url: Option<String>,
    pub cache_dir: String,
    pub cache_budget_bytes: u64,
    /// Whether the class has an origin tier.
    pub origin: bool,
    /// Size of the current peer set.
    pub peer_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassStats {
    pub name: String,
    pub peer_count: u64,
    pub cache_bytes: u64,
    pub cache_entries: u64,
    pub cache_budget_bytes: u64,
    pub fetches_total: u64,
    pub fetches_hit: u64,
    pub bytes_served_total: u64,
    pub recent_fetches: Vec<FetchRecord>,
}

/// Per-class counters since the daemon started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassTotals {
    pub fetches: u64,
    pub hits: u64,
    pub bytes_served: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchRecord {
    pub timestamp_secs: u64,
    pub class: String,
    pub hash_short: String,
    pub bytes: u64,
    /// `cache`, `peer`, `origin` or `miss`.
    pub tier: String,
    pub ok: bool,
}

/// Cumulative progress of the fetch this connection asked for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchProgress {
    pub bytes_so_far: u64,
    /// Size of the blob, once the serving tier has reported it.
    pub total: Option<u64>,
    /// `peer` or `origin`.
    pub tier: Tier,
    #[serde(default, skip_serializing_if = "FetchSources::is_empty")]
    pub sources: FetchSources,
}

/// The tier that served, or is serving, a fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    Cache,
    Peer,
    Origin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchResult {
    pub class: String,
    pub hash: String,
    /// Absolute path of the verified plain file in the daemon's cache
    /// directory.
    pub path: String,
    /// Size of the blob.
    pub bytes: u64,
    pub tier: Tier,
    pub elapsed_ms: u64,
    #[serde(default, skip_serializing_if = "FetchSources::is_empty")]
    pub sources: FetchSources,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// One per fetch, hit or miss.
    FetchMetric(FetchMetric),
}

/// One completed fetch, as emitted to subscribers and the metrics file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchMetric {
    pub timestamp_secs: u64,
    /// The emitting node.
    pub node_id: String,
    /// Deployment cohort of the emitting node, from its configuration.
    pub peer_class: String,
    pub class: String,
    pub blake3: String,
    /// `cache`, `peer`, `origin` or `miss`.
    pub tier_source: String,
    pub bytes_served: u64,
    /// Bytes this fetch received from peers.
    #[serde(default)]
    pub peer_bytes: u64,
    /// Bytes this fetch read from the origin.
    #[serde(default)]
    pub origin_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
    pub duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // These pin the literal bytes on the wire: other programs speak this
    // protocol without linking this crate.

    #[test]
    fn commands_are_tagged_by_cmd() {
        assert_eq!(
            serde_json::to_string(&Command::Inspect).unwrap(),
            r#"{"cmd":"inspect"}"#
        );
        assert_eq!(
            serde_json::to_string(&Command::ClassStats {
                class: "models".into()
            })
            .unwrap(),
            r#"{"cmd":"class_stats","class":"models"}"#
        );
        assert_eq!(
            serde_json::to_string(&Command::Fetch {
                class: "models".into(),
                hash: "ab".into()
            })
            .unwrap(),
            r#"{"cmd":"fetch","class":"models","hash":"ab"}"#
        );
    }

    #[test]
    fn set_peers_carries_node_ids_with_addresses_and_an_optional_class() {
        let cmd = Command::SetPeers {
            peers: vec![Peer {
                node_id: "aa".into(),
                addrs: vec!["10.0.0.1:4433".into()],
                relay_url: None,
            }],
            admit: None,
            class: None,
            topology: None,
        };
        let json = r#"{"cmd":"set_peers","peers":[{"node_id":"aa","addrs":["10.0.0.1:4433"]}]}"#;
        assert_eq!(serde_json::to_string(&cmd).unwrap(), json);
        assert_eq!(serde_json::from_str::<Command>(json).unwrap(), cmd);
    }

    // [utest -> dsn~xlb-peer-reach~2]
    #[test]
    fn set_peers_carries_peer_addresses_admission_and_node_topology() {
        let cmd = Command::SetPeers {
            peers: vec![Peer {
                node_id: "aa".into(),
                addrs: vec!["10.0.0.1:4433".into(), "203.0.113.7:4433".into()],
                relay_url: Some("https://relay.example./".into()),
            }],
            admit: Some(vec!["bb".into()]),
            class: None,
            topology: Some(NodeTopology {
                provider: "nebius".into(),
                location: "nebius/eu-north1".into(),
                region: Some("eu-north1".into()),
                zone: None,
                reach: xlb_core::PeerReach::Global,
            }),
        };
        let json = r#"{"cmd":"set_peers","peers":[{"node_id":"aa","addrs":["10.0.0.1:4433","203.0.113.7:4433"],"relay_url":"https://relay.example./"}],"admit":["bb"],"topology":{"provider":"nebius","location":"nebius/eu-north1","region":"eu-north1","reach":"global"}}"#;
        assert_eq!(serde_json::to_string(&cmd).unwrap(), json);
        assert_eq!(serde_json::from_str::<Command>(json).unwrap(), cmd);
    }

    #[test]
    fn a_fetch_streams_progress_then_a_result_with_the_path() {
        let progress = Response::FetchProgress(FetchProgress {
            bytes_so_far: 5,
            total: Some(10),
            tier: Tier::Peer,
            sources: FetchSources::default(),
        });
        assert_eq!(
            serde_json::to_string(&progress).unwrap(),
            r#"{"type":"fetch_progress","bytes_so_far":5,"total":10,"tier":"peer"}"#
        );
        let result = Response::FetchResult(FetchResult {
            class: "models".into(),
            hash: "ab".into(),
            path: "/cache/ab".into(),
            bytes: 10,
            tier: Tier::Origin,
            elapsed_ms: 3,
            sources: FetchSources::default(),
        });
        assert_eq!(
            serde_json::to_string(&result).unwrap(),
            r#"{"type":"fetch_result","class":"models","hash":"ab","path":"/cache/ab","bytes":10,"tier":"origin","elapsed_ms":3}"#
        );
    }

    #[test]
    fn node_info_flattens_beside_its_type_tag() {
        let resp = Response::NodeInfo(NodeInfo {
            node_id: Some("abc".into()),
            udp_addrs: vec!["0.0.0.0:4433".into()],
            relay_url: None,
            cache_dir: Some("/cache".into()),
            uptime_secs: 1,
            classes: vec![],
        });
        assert_eq!(
            serde_json::to_string(&resp).unwrap(),
            r#"{"type":"node_info","node_id":"abc","udp_addrs":["0.0.0.0:4433"],"cache_dir":"/cache","uptime_secs":1,"classes":[]}"#
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
}
