//! The daemon side of the control socket.

use std::os::unix::fs::PermissionsExt;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use tokio::{
    net::{unix::WriteHalf, UnixListener, UnixStream},
    sync::{broadcast, watch},
};
use xlb::{AssetClass, BlakeHash, FetchTier, NodeId, PeerAddr, ProgressSink};
use xlb_client::{
    read_frame, write_frame, ClassInfo, ClassStats, Command, FetchProgress, FetchRecord,
    FetchResult, NodeEvent, NodeInfo, NodeTopology, Peer, Response, SeedSummary, Tier, TreeInfo,
};

/// How often a running fetch reports progress to its client. Progress is a
/// display value; coalescing keeps it off the transfer's path.
const PROGRESS_INTERVAL: Duration = Duration::from_millis(200);

/// One class the daemon serves.
#[derive(Clone)]
pub struct ClassEntry {
    pub name: String,
    pub class: AssetClass,
    pub has_origin: bool,
    pub(crate) model_mount: Option<xlb_mount::ModelMount>,
}

#[derive(Clone)]
pub struct NodeState {
    pub started_at: Instant,
    /// In configuration order; the first class with a peer tier names the node.
    pub classes: Arc<Vec<ClassEntry>>,
    pub event_tx: broadcast::Sender<NodeEvent>,
    pub metrics: Arc<crate::metrics_sink::MetricsSink>,
}

impl NodeState {
    fn class(&self, name: &str) -> Option<&ClassEntry> {
        self.classes.iter().find(|entry| entry.name == name)
    }

    /// The class whose peer endpoint identifies this node.
    pub fn primary(&self) -> Option<&ClassEntry> {
        self.classes
            .iter()
            .find(|entry| entry.class.node_id().is_some())
            .or_else(|| self.classes.first())
    }

    pub fn node_info(&self) -> NodeInfo {
        let classes: Vec<ClassInfo> = self.classes.iter().map(class_info).collect();
        let primary = self.primary().map(class_info);
        NodeInfo {
            node_id: primary.as_ref().and_then(|info| info.node_id.clone()),
            udp_addrs: primary
                .as_ref()
                .map(|info| info.udp_addrs.clone())
                .unwrap_or_default(),
            relay_url: primary.as_ref().and_then(|info| info.relay_url.clone()),            cache_dir: primary.map(|info| info.cache_dir),
            uptime_secs: self.started_at.elapsed().as_secs(),
            classes,
        }
    }
}

fn class_info(entry: &ClassEntry) -> ClassInfo {
    let class = &entry.class;
    ClassInfo {
        name: entry.name.clone(),
        node_id: class.node_id().map(|id| id.to_string()),
        udp_addrs: class.bound_addrs().iter().map(ToString::to_string).collect(),
        relay_url: class.relay_url().map(|url| url.to_string()),        cache_dir: class.cache_dir().display().to_string(),
        cache_budget_bytes: class.cache_stats().budget_bytes,
        origin: entry.has_origin,
        peer_count: class.peers().len() as u64,
    }
}

/// Bind the control socket with `mode` and serve it until the task is dropped.
pub async fn listen(state: NodeState, socket_path: &str, mode: u32) -> Result<()> {
    if let Some(directory) = std::path::Path::new(socket_path)
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(directory).await?;
    }
    match std::fs::remove_file(socket_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let listener = UnixListener::bind(socket_path)?;
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(mode))?;
    // A socket only its owner may open also checks the peer's user ID, which
    // the kernel supplies and a client cannot forge.
    let owner_only = mode & 0o077 == 0;
    tracing::info!(socket = socket_path, mode = %format!("{mode:04o}"), "xlb control socket listening");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_conn(state, stream, owner_only).await {
                        tracing::debug!("client closed: {e}");
                    }
                });
            }
            Err(e) => tracing::error!("accept: {e}"),
        }
    }
}

fn same_user(stream: &UnixStream) -> bool {
    // SAFETY: getuid has no preconditions and cannot fail.
    let our_uid = unsafe { libc::getuid() };
    match stream.peer_cred() {
        Ok(cred) if cred.uid() == our_uid => true,
        Ok(cred) => {
            tracing::warn!(
                peer_uid = cred.uid(),
                our_uid,
                "rejecting control-socket connection from foreign uid"
            );
            false
        }
        Err(e) => {
            tracing::warn!("rejecting control-socket connection: peer_cred failed: {e}");
            false
        }
    }
}

async fn handle_conn(state: NodeState, mut stream: UnixStream, owner_only: bool) -> Result<()> {
    if owner_only && !same_user(&stream) {
        return Ok(());
    }
    let (mut reader, mut writer) = stream.split();

    loop {
        let Ok(cmd) = read_frame::<_, Command>(&mut reader).await else {
            break;
        };

        match cmd {
            Command::Inspect => {
                write_frame(&mut writer, &Response::NodeInfo(state.node_info())).await?;
            }

            Command::ClassStats { class } => {
                let resp = match state.class(&class) {
                    Some(entry) => Response::ClassStats(build_stats(&state, entry)),
                    None => unknown_class(&class),
                };
                write_frame(&mut writer, &resp).await?;
            }

            Command::SubscribeEvents => {
                let mut rx = state.event_tx.subscribe();
                loop {
                    match rx.recv().await {
                        Ok(ev) => {
                            if write_frame(&mut writer, &Response::Event(ev))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => break,
                    }
                }
                break;
            }

            Command::Fetch { class, hash } => {
                let resp = do_fetch(&state, &class, &hash, &mut writer).await?;
                write_frame(&mut writer, &resp).await?;
            }

            Command::SetPeers {
                peers,
                admit,
                class,
                topology,
            } => {
                let resp = set_peers(&state, &peers, admit.as_deref(), class.as_deref(), topology.as_ref());
                write_frame(&mut writer, &resp).await?;
            }

            Command::PinTree {
                class,
                name,
                hashes,
                from_dirs,
            } => {
                let resp = pin_tree(&state, &class, &name, &hashes, &from_dirs).await;
                write_frame(&mut writer, &resp).await?;
            }

            Command::ListTrees { class } => {
                let resp = match state.class(&class) {
                    Some(entry) => Response::Trees {
                        trees: entry
                            .class
                            .pinned_trees()
                            .into_iter()
                            .map(tree_info)
                            .collect(),
                    },
                    None => unknown_class(&class),
                };
                write_frame(&mut writer, &resp).await?;
            }

            Command::UnpinTree { class, name } => {
                let resp = match state.class(&class) {
                    Some(entry) => match entry.class.unpin_tree(&name).await {
                        Ok(()) => Response::Ok,
                        Err(e) => error(e),
                    },
                    None => unknown_class(&class),
                };
                write_frame(&mut writer, &resp).await?;
            }
        }
    }

    Ok(())
}

/// How many recent fetches a `class_stats` reply carries.
const RECENT_IN_REPLY: usize = 20;

fn build_stats(state: &NodeState, entry: &ClassEntry) -> ClassStats {
    let cache = entry.class.cache_stats();
    let totals = state.metrics.totals_for(&entry.name);
    let recent_fetches = state
        .metrics
        .recent_for(&entry.name, RECENT_IN_REPLY)
        .into_iter()
        .map(|m| FetchRecord {
            timestamp_secs: m.timestamp_secs,
            class: m.class,
            hash_short: m.blake3.chars().take(12).collect(),
            bytes: m.bytes_served,
            ok: m.tier_source != "miss",
            tier: m.tier_source,
        })
        .collect();

    ClassStats {
        name: entry.name.clone(),
        peer_count: entry.class.peers().len() as u64,
        cache_bytes: cache.bytes,
        cache_entries: cache.entries,
        cache_pinned_bytes: cache.pinned_bytes,
        cache_budget_bytes: cache.budget_bytes,
        fetches_total: totals.fetches,
        fetches_hit: totals.hits,
        bytes_served_total: totals.bytes_served,
        recent_fetches,
    }
}

fn wire_tier(tier: FetchTier) -> Tier {
    match tier {
        FetchTier::Cache => Tier::Cache,
        FetchTier::Peer => Tier::Peer,
        FetchTier::Origin => Tier::Origin,
    }
}

/// Run one fetch, writing coalesced progress frames while it runs. Returns
/// the final response; `Err` only when the client is gone.
async fn do_fetch(
    state: &NodeState,
    class_name: &str,
    hash_str: &str,
    writer: &mut WriteHalf<'_>,
) -> Result<Response> {
    let Some(entry) = state.class(class_name) else {
        return Ok(unknown_class(class_name));
    };
    let hash = match BlakeHash::from_hex(hash_str) {
        Ok(h) => h,
        Err(e) => return Ok(error(e)),
    };

    let _ = state.event_tx.send(NodeEvent::FetchStarted {
        class: class_name.to_string(),
        hash: hash_str.to_string(),
    });

    // The sink only overwrites a slot; this task reads the slot on a timer.
    let (latest_tx, mut latest_rx) = watch::channel(None::<xlb::FetchProgress>);
    let sink: ProgressSink = Arc::new(move |progress| {
        latest_tx.send_replace(Some(progress));
    });
    let fetch = async {
        if let Some(mount) = &entry.model_mount {
            let report = mount.prepare(hash, sink).await?;
            Ok::<_, anyhow::Error>((mount.path(hash), report))
        } else {
            let (result, report) = entry.class.asset(hash).fetch_to_file_reported(Some(sink)).await;
            Ok((result?, report))
        }
    };
    tokio::pin!(fetch);
    let mut ticker = tokio::time::interval(PROGRESS_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let result = loop {
        tokio::select! {
            done = &mut fetch => break done,
            _ = ticker.tick() => {
                if latest_rx.has_changed().unwrap_or(false) {
                    let latest = latest_rx.borrow_and_update().clone();
                    if let Some(progress) = latest {
                        let frame = Response::FetchProgress(FetchProgress {
                            bytes_so_far: progress.bytes_so_far,
                            total: progress.total,
                            tier: wire_tier(progress.tier),
                            sources: progress.sources,
                        });
                        write_frame(writer, &frame).await?;
                    }
                }
            }
        }
    };

    match result {
        Ok((path, report)) if report.tier.is_some() => {
            let tier = report.tier.unwrap();
            let _ = state.event_tx.send(NodeEvent::FetchCompleted {
                class: class_name.to_string(),
                hash: hash_str.to_string(),
                bytes: report.bytes_served,
                tier: tier.label().to_string(),
                elapsed_ms: report.duration_ms,
            });
            Ok(Response::FetchResult(FetchResult {
                class: class_name.to_string(),
                hash: hash_str.to_string(),
                path: path.display().to_string(),
                bytes: report.bytes_served,
                tier: wire_tier(tier),
                elapsed_ms: report.duration_ms,
                sources: xlb::FetchSources { origin_bytes: report.origin_bytes, peers: report.peers },
            }))
        }
        Ok(_) => Ok(Response::Error {
            message: "fetch succeeded without a serving tier".into(),
        }),
        Err(e) => {
            let _ = state.event_tx.send(NodeEvent::FetchFailed {
                class: class_name.to_string(),
                hash: hash_str.to_string(),
                reason: e.to_string(),
            });
            Ok(error(e))
        }
    }
}

/// Replace the peer set of `class`, or of every class with a peer tier.
/// `admit` names the nodes to serve; without it they are the peers.
// [impl -> dsn~xlb-peer-reach~1]
fn set_peers(
    state: &NodeState,
    peers: &[Peer],
    admit: Option<&[String]>,
    class: Option<&str>,
    topology: Option<&NodeTopology>,
) -> Response {
    let node_id = |text: &String| -> Result<NodeId, String> {
        text.parse().map_err(|e| format!("node_id {text:?}: {e}"))
    };
    let parsed: Result<Vec<PeerAddr>, String> = peers
        .iter()
        .map(|peer| {
            let addrs = peer
                .addrs
                .iter()
                .map(|addr| addr.parse().map_err(|e| format!("address {addr:?}: {e}")))
                .collect::<Result<Vec<_>, String>>()?;
            let relay_url = peer
                .relay_url
                .as_ref()
                .map(|url| url.parse().map_err(|e| format!("relay_url {url:?}: {e}")))
                .transpose()?;
            Ok(PeerAddr {
                node_id: node_id(&peer.node_id)?,
                addrs,
                relay_url,
            })
        })
        .collect();
    let parsed = match parsed {
        Ok(parsed) => parsed,
        Err(message) => return Response::Error { message },
    };
    let admitted: Result<Vec<NodeId>, String> = match admit {
        Some(admit) => admit.iter().map(node_id).collect(),
        None => Ok(parsed.iter().map(|peer| peer.node_id).collect()),
    };
    let admitted = match admitted {
        Ok(admitted) => admitted,
        Err(message) => return Response::Error { message },
    };
    if let Some(name) = class {
        if state.class(name).is_none() {
            return unknown_class(name);
        }
    }
    let mut classes = 0;
    for entry in state.classes.iter() {
        let selected = class.is_none_or(|name| name == entry.name);
        if selected && entry.class.node_id().is_some() {
            entry.class.set_peer_sets(parsed.clone(), admitted.clone());
            entry.class.set_node_topology(topology.cloned());
            classes += 1;
        }
    }
    Response::PeersSet { classes }
}

fn unknown_class(class: &str) -> Response {
    Response::Error {
        message: format!("unknown class: {class}"),
    }
}

fn error(e: impl std::fmt::Display) -> Response {
    Response::Error {
        message: e.to_string(),
    }
}

fn tree_info(t: xlb::PinnedTreeInfo) -> TreeInfo {
    TreeInfo {
        name: t.name,
        members: t.members,
        present: t.present,
        present_bytes: t.present_bytes,
        durable: t.durable,
    }
}

/// `PinTree`: declare the tree, then seed its bytes from `from_dirs`. A
/// seeded blob is served to peers as soon as it is in the cache.
async fn pin_tree(
    state: &NodeState,
    class: &str,
    name: &str,
    hashes: &[String],
    from_dirs: &[String],
) -> Response {
    let Some(entry) = state.class(class) else {
        return unknown_class(class);
    };
    let dirs: Vec<std::path::PathBuf> = from_dirs
        .iter()
        .map(|d| crate::config::expand_tilde(d).into())
        .collect();
    let members = match xlb::pinned::resolve_members(hashes, &dirs) {
        Ok(v) => v,
        Err(e) => return error(e),
    };
    let tree = match entry.class.pin_tree(name, members).await {
        Ok(tree) => tree,
        Err(e) => return error(e),
    };
    let report = match entry.class.seed_tree_from_dirs(name, &dirs).await {
        Ok(r) => r,
        Err(e) => return error(format!("tree pinned, seeding failed: {e}")),
    };
    // Seeding changed how much of the tree is present.
    let tree = entry
        .class
        .pinned_trees()
        .into_iter()
        .find(|t| t.name == name)
        .unwrap_or(tree);
    Response::TreePinned {
        tree: tree_info(tree),
        seed: SeedSummary {
            ingested: report.ingested,
            already_cached: report.already_cached,
            missing: report.missing.iter().map(BlakeHash::to_hex).collect(),
            mismatched: report
                .mismatched
                .iter()
                .map(|p| p.display().to_string())
                .collect(),
        },
    }
}
