//! Peer endpoint configuration, direct-path policy and cooldowns.

use std::{collections::HashMap, net::SocketAddr, num::NonZeroUsize, sync::Mutex, time::Duration};

use futures::StreamExt;
use iroh::{EndpointId, RelayMode, endpoint::{Connection, VarInt}};
use tokio::time::Instant;

pub use iroh::RelayUrl;
pub type NodeId = EndpointId;
pub const DEFAULT_PEER_PORT: u16 = 4433;
pub const DEFAULT_PEER_CONCURRENCY: usize = 16;
pub(crate) const PEER_EXCLUSION: Duration = Duration::from_secs(60);
pub(crate) const PEER_FAILURES: u8 = 3;

#[derive(Clone, Copy, Debug)]
enum PeerState {
    Ready(u8),
    Excluded(Instant),
}

#[derive(Clone, Copy, Debug)]
enum PeerOutcome {
    Success,
    Failure(Instant),
    Expire,
}

struct PeerHealth;

impl rust_fsm::StateMachineImpl for PeerHealth {
    type State = PeerState;
    type Input = PeerOutcome;
    type Output = ();

    const INITIAL_STATE: PeerState = PeerState::Ready(0);

    fn transition(state: &PeerState, outcome: &PeerOutcome) -> Option<PeerState> {
        match (state, outcome) {
            (PeerState::Ready(_), PeerOutcome::Success) => Some(PeerState::Ready(0)),
            (PeerState::Ready(count), PeerOutcome::Failure(now)) if *count + 1 >= PEER_FAILURES => Some(PeerState::Excluded(*now + PEER_EXCLUSION)),
            (PeerState::Ready(count), PeerOutcome::Failure(_)) => Some(PeerState::Ready(*count + 1)),
            (PeerState::Excluded(_), PeerOutcome::Success | PeerOutcome::Expire) => Some(PeerState::Ready(0)),
            (PeerState::Excluded(until), PeerOutcome::Failure(_)) => Some(PeerState::Excluded(*until)),
            _ => None,
        }
    }

    fn output(_: &PeerState, _: &PeerOutcome) -> Option<()> { None }
}

#[derive(Default)]
pub(crate) struct FailedPeers(Mutex<HashMap<NodeId, rust_fsm::StateMachine<PeerHealth>>>);

impl FailedPeers {
    pub(crate) fn insert(&self, node: NodeId, now: Instant) -> bool {
        let mut peers = self.0.lock().unwrap_or_else(|err| err.into_inner());
        let state = peers.entry(node).or_default();
        if matches!(state.state(), PeerState::Excluded(until) if *until <= now) {
            state.consume(&PeerOutcome::Expire).expect("valid peer expiry");
        }
        let already_excluded = matches!(state.state(), PeerState::Excluded(_));
        state.consume(&PeerOutcome::Failure(now)).expect("valid peer failure");
        !already_excluded && matches!(state.state(), PeerState::Excluded(_))
    }

    pub(crate) fn contains(&self, node: &NodeId, now: Instant) -> bool {
        self.0.lock().unwrap_or_else(|err| err.into_inner()).get(node)
            .is_some_and(|state| matches!(state.state(), PeerState::Excluded(until) if *until > now))
    }

    pub(crate) fn success(&self, node: NodeId) {
        let mut peers = self.0.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(state) = peers.get_mut(&node) {
            state.consume(&PeerOutcome::Success).expect("valid peer success");
            peers.remove(&node);
        }
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;

    // [utest -> dsn~xlb-chunk-discovery~1]
    #[test]
    fn three_consecutive_failures_exclude_only_that_holder_for_sixty_seconds() {
        let failures = FailedPeers::default();
        let failed = iroh::SecretKey::generate().public();
        let healthy = iroh::SecretKey::generate().public();
        let now = Instant::now();
        failures.insert(failed, now);
        failures.insert(failed, now);
        assert!(!failures.contains(&failed, now));
        failures.success(failed);
        failures.insert(failed, now);
        failures.insert(failed, now);
        assert!(!failures.contains(&failed, now));
        failures.insert(failed, now);
        assert!(failures.contains(&failed, now + Duration::from_secs(59)));
        assert!(!failures.contains(&failed, now + Duration::from_secs(60)));
        assert!(!failures.contains(&healthy, now));
    }
}

/// One member of the peer set: a node this node reads from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAddr {
    pub node_id: NodeId,
    /// UDP socket addresses the peer's endpoint may be reachable at, private
    /// and public. May be empty when `relay_url` is set.
    pub addrs: Vec<SocketAddr>,
    /// The relay the peer is reachable through, if it reported one.
    pub relay_url: Option<RelayUrl>,
}

/// Which relay servers the peer endpoint uses to discover its addresses and
/// to punch holes. Blob requests never travel through a relay (see
/// [`path_verdict`]).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RelaySetting {
    /// iroh's default relay map.
    #[default]
    Default,
    /// No relays: peers are reached at their socket addresses only.
    Disabled,
    /// These relay URLs.
    Custom(Vec<String>),
}

impl RelaySetting {
    pub(crate) fn relay_mode(&self) -> Result<RelayMode, String> {
        match self {
            RelaySetting::Default => Ok(RelayMode::Default),
            RelaySetting::Disabled => Ok(RelayMode::Disabled),
            RelaySetting::Custom(urls) => urls
                .iter()
                .map(|url| url.parse::<RelayUrl>().map_err(|e| format!("relay URL {url:?}: {e}")))
                .collect::<Result<Vec<_>, String>>()
                .map(RelayMode::custom),
        }
    }
}

/// What the paths of a connection allow it to be used for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathVerdict {
    /// Application data travels on a direct IP path: requests may be sent
    /// and served.
    Direct,
    /// Application data travels through a relay: no request may be sent or
    /// served.
    Relayed,
    /// No path is selected yet: wait, and refuse if none is in time.
    Unselected,
}

/// Judge a connection by its open paths, each given as (selected for
/// application data, direct IP path). Only the selected path counts: a
/// direct path that is open but not selected carries no data.
// [impl -> dsn~xlb-peer-reach~1]
pub(crate) fn path_verdict(paths: impl IntoIterator<Item = (bool, bool)>) -> PathVerdict {
    match paths.into_iter().find(|(selected, _)| *selected) {
        Some((_, true)) => PathVerdict::Direct,
        Some((_, false)) => PathVerdict::Relayed,
        None => PathVerdict::Unselected,
    }
}

pub(crate) fn connection_verdict(connection: &Connection) -> PathVerdict {
    let paths = connection.paths();
    path_verdict(paths.iter().map(|path| (path.is_selected(), path.is_ip())))
}

/// Wait up to `limit` for `connection` to carry its data on a direct path.
// [impl -> dsn~xlb-peer-reach~1]
pub(crate) async fn await_direct_path(connection: &Connection, limit: Duration) -> bool {
    let direct = async {
        let mut snapshots = connection.paths_stream();
        while let Some(paths) = snapshots.next().await {
            let paths = paths.iter().map(|path| (path.is_selected(), path.is_ip()));
            if path_verdict(paths) == PathVerdict::Direct {
                return true;
            }
        }
        false
    };
    tokio::time::timeout(limit, direct).await.unwrap_or(false)
}

/// Close `connection` as soon as its data, having travelled on a direct
/// path, would travel through a relay. Ends when the connection closes.
// [impl -> dsn~xlb-peer-reach~1]
pub(crate) async fn close_when_relayed(connection: Connection, mut was_direct: bool) {
    let mut snapshots = connection.paths_stream();
    while let Some(paths) = snapshots.next().await {
        let paths = paths.iter().map(|path| (path.is_selected(), path.is_ip()));
        match path_verdict(paths) {
            PathVerdict::Direct => was_direct = true,
            PathVerdict::Relayed if was_direct => {
                tracing::warn!(remote = %connection.remote_id(), "closing a blob connection that lost its direct path");
                connection.close(VarInt::from_u32(0), b"no direct path");
                return;
            }
            PathVerdict::Relayed | PathVerdict::Unselected => {}
        }
    }
}

/// Configuration of the peer tier of one class.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// UDP address the endpoint binds.
    pub bind_addr: SocketAddr,
    /// The relay servers the endpoint uses.
    pub relay: RelaySetting,
    /// Serve verified data without attempting outbound peer reads.
    pub disable_sourcing: bool,
    /// Maximum age of a signed node inventory record.
    pub inventory_expiry: Duration,
    /// Maximum incoming chunk streams per connection.
    pub concurrency: NonZeroUsize,
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], DEFAULT_PEER_PORT)),
            relay: RelaySetting::Default,
            disable_sourcing: false,
            inventory_expiry: Duration::from_secs(90),
            concurrency: NonZeroUsize::new(DEFAULT_PEER_CONCURRENCY).unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Paths are (selected for application data, direct IP path).
    // [utest -> dsn~xlb-peer-reach~1]
    #[test]
    fn requests_are_allowed_only_while_data_travels_on_a_direct_path() {
        assert_eq!(path_verdict([(true, true)]), PathVerdict::Direct);
        assert_eq!(path_verdict([(false, false), (true, true)]), PathVerdict::Direct);
        // Only a relay path, or a direct path that is open but not the one
        // carrying data: refused.
        assert_eq!(path_verdict([(true, false)]), PathVerdict::Relayed);
        assert_eq!(path_verdict([(true, false), (false, true)]), PathVerdict::Relayed);
        // Nothing selected yet: wait, then refuse.
        assert_eq!(path_verdict([]), PathVerdict::Unselected);
        assert_eq!(path_verdict([(false, true)]), PathVerdict::Unselected);
    }
}
