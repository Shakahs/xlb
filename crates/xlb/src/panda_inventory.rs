//! The local view of verified chunks held by live peers.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use iroh::EndpointAddr;
use p2panda_core::{SeqNum, Timestamp, Topic};
use rust_fsm::{StateMachine, StateMachineImpl};
use serde::{Deserialize, Serialize};
use xlb_core::peer::PeerReach;

use crate::{BlakeHash, NodeId, NodeTopology};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct InventoryRecord {
    pub(crate) address: EndpointAddr,
    pub(crate) topology: NodeTopology,
    pub(crate) checkpoint: bool,
    pub(crate) issued_at: Timestamp,
    #[serde(with = "digest_list")]
    pub(crate) added: Vec<BlakeHash>,
    #[serde(with = "digest_list")]
    pub(crate) removed: Vec<BlakeHash>,
}

mod digest_list {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    use crate::BlakeHash;

    pub(super) fn serialize<S: Serializer>(digests: &[BlakeHash], serializer: S) -> Result<S::Ok, S::Error> {
        let mut payload = Vec::with_capacity(digests.len() * 32);
        for digest in digests {
            payload.extend_from_slice(digest.as_bytes());
        }
        serializer.serialize_bytes(&payload)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<BlakeHash>, D::Error> {
        let bytes = bytes::Bytes::deserialize(deserializer)?;
        let (digests, remainder) = bytes.as_chunks::<32>();
        if !remainder.is_empty() {
            return Err(D::Error::custom("inventory payload has a truncated digest"));
        }
        Ok(digests.iter().copied().map(BlakeHash::from_bytes).collect())
    }
}

struct HolderInventory {
    topology: NodeTopology,
    projection: StateMachine<InventoryProjection>,
    renewed: Timestamp,
    digests: HashSet<BlakeHash>,
}

#[derive(Clone, Copy, Debug)]
enum InventoryState {
    Unknown,
    Current(SeqNum),
    MissingUpdates(SeqNum),
}

#[derive(Clone, Copy, Debug)]
enum InventoryEvent {
    Checkpoint(SeqNum),
    Delta(SeqNum),
}

#[derive(Clone, Copy, Debug)]
enum InventoryAction {
    Replace,
    Update,
    Withdraw,
}

struct InventoryProjection;

impl InventoryProjection {
    fn next(state: InventoryState, event: InventoryEvent) -> Option<(InventoryState, InventoryAction)> {
        use InventoryAction as Action;
        use InventoryEvent as Event;
        use InventoryState as State;

        let latest = match state {
            State::Unknown => None,
            State::Current(sequence) | State::MissingUpdates(sequence) => Some(sequence),
        };
        let sequence = match event {
            Event::Checkpoint(sequence) | Event::Delta(sequence) => sequence,
        };
        if latest.is_some_and(|latest| sequence <= latest) {
            return None;
        }
        Some(match (state, event) {
            (_, Event::Checkpoint(sequence)) => (State::Current(sequence), Action::Replace),
            (State::Current(previous), Event::Delta(sequence)) if previous.checked_add(1) == Some(sequence) => {
                (State::Current(sequence), Action::Update)
            }
            (_, Event::Delta(sequence)) => (State::MissingUpdates(sequence), Action::Withdraw),
        })
    }
}

// [impl -> dsn~xlb-supervised-lifecycle~1]
impl StateMachineImpl for InventoryProjection {
    type State = InventoryState;
    type Input = InventoryEvent;
    type Output = InventoryAction;

    const INITIAL_STATE: Self::State = InventoryState::Unknown;

    fn transition(state: &Self::State, event: &Self::Input) -> Option<Self::State> {
        Self::next(*state, *event).map(|(state, _)| state)
    }

    fn output(state: &Self::State, event: &Self::Input) -> Option<Self::Output> {
        Self::next(*state, *event).map(|(_, action)| action)
    }
}

#[derive(Default)]
pub(crate) struct InventoryView {
    holdings: HashMap<(Topic, NodeId), HolderInventory>,
    holders: HashMap<BlakeHash, HashSet<(Topic, NodeId)>>,
}

impl InventoryView {
    fn remove_holder(
        index: &mut HashMap<BlakeHash, HashSet<(Topic, NodeId)>>,
        digest: BlakeHash, owner: &(Topic, NodeId),
    ) {
        if let Some(holders) = index.get_mut(&digest) {
            holders.remove(owner);
            if holders.is_empty() {
                index.remove(&digest);
            }
        }
    }

    /// Checkpoints replace the whole log's current holdings. Deltas apply
    /// only after the previous operation, so a gap cannot revive stale data.
    pub(crate) fn apply(&mut self, topic: Topic, sequence: SeqNum, record: InventoryRecord) {
        let owner = (topic, record.address.id);
        let entry = self.holdings.entry(owner).or_insert_with(|| HolderInventory {
            topology: record.topology.clone(),
            projection: StateMachine::new(),
            renewed: record.issued_at,
            digests: HashSet::new(),
        });
        let event = if record.checkpoint {
            InventoryEvent::Checkpoint(sequence)
        } else {
            InventoryEvent::Delta(sequence)
        };
        let Ok(Some(action)) = entry.projection.consume(&event) else { return };
        if matches!(action, InventoryAction::Replace | InventoryAction::Withdraw) {
            for digest in entry.digests.drain() {
                Self::remove_holder(&mut self.holders, digest, &owner);
            }
        }
        entry.topology = record.topology;
        entry.renewed = record.issued_at;
        if matches!(action, InventoryAction::Withdraw) {
            return;
        }
        for digest in record.removed {
            entry.digests.remove(&digest);
            Self::remove_holder(&mut self.holders, digest, &owner);
        }
        for digest in record.added {
            entry.digests.insert(digest);
            self.holders.entry(digest).or_default().insert(owner);
        }
    }

    pub(crate) fn candidates(
        &self, digest: BlakeHash, locality: &str, expiry: Duration, now: Timestamp,
    ) -> (HashSet<NodeId>, HashSet<NodeId>) {
        let mut local = HashSet::new();
        let mut global = HashSet::new();
        let cutoff = u64::from(now).saturating_sub(expiry.as_micros() as u64);
        for owner in self.holders.get(&digest).into_iter().flatten() {
            let entry = &self.holdings[owner];
            if u64::from(entry.renewed) <= cutoff {
                continue;
            }
            if entry.topology.location == locality {
                local.insert(owner.1);
            } else if entry.topology.reach == PeerReach::Global {
                global.insert(owner.1);
            }
        }
        (local, global)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(node: NodeId, time: u64, checkpoint: bool, added: Vec<BlakeHash>, removed: Vec<BlakeHash>) -> InventoryRecord {
        InventoryRecord {
            address: EndpointAddr::new(node),
            topology: NodeTopology {
                provider: "aws".into(),
                location: "aws/us-west-2a".into(),
                region: Some("us-west-2".into()),
                zone: Some("us-west-2a".into()),
                reach: PeerReach::Local,
            },
            checkpoint,
            issued_at: Timestamp::from(time),
            added,
            removed,
        }
    }

    // [utest -> dsn~xlb-supervised-lifecycle~1]
    #[test]
    fn a_replayed_checkpoint_cannot_restore_an_evicted_chunk() {
        let mut view = InventoryView::default();
        let node = iroh::SecretKey::generate().public();
        let topic = [17; 32].into();
        let hash = BlakeHash::hash(b"one chunk");
        view.apply(topic, 1, record(node, 1_000_000, true, vec![hash], vec![]));
        view.apply(topic, 2, record(node, 2_000_000, false, vec![], vec![hash]));
        view.apply(topic, 1, record(node, 1_000_000, true, vec![hash], vec![]));
        let (local, _) = view.candidates(hash, "aws/us-west-2a", Duration::from_secs(90), Timestamp::from(5_000_000));
        assert!(local.is_empty());
    }

    #[test]
    fn a_sequence_gap_withdraws_holdings_until_a_newer_checkpoint() {
        let mut view = InventoryView::default();
        let node = iroh::SecretKey::generate().public();
        let topic = [17; 32].into();
        let hash = BlakeHash::hash(b"one chunk");
        let holders = |view: &InventoryView| {
            view.candidates(hash, "aws/us-west-2a", Duration::from_secs(90), Timestamp::from(5_000_000)).0
        };
        view.apply(topic, 1, record(node, 1_000_000, true, vec![hash], vec![]));
        view.apply(topic, 3, record(node, 3_000_000, false, vec![], vec![]));
        assert!(holders(&view).is_empty());
        view.apply(topic, 4, record(node, 4_000_000, true, vec![hash], vec![]));
        assert_eq!(holders(&view), HashSet::from([node]));
    }

    #[test]
    fn expiring_one_node_preserves_another_nodes_holdings() {
        let mut view = InventoryView::default();
        let expired = iroh::SecretKey::generate().public();
        let live = iroh::SecretKey::generate().public();
        let topic = [17; 32].into();
        let hash = BlakeHash::hash(b"one chunk");
        view.apply(topic, 1, record(expired, 1_000_000, true, vec![hash], vec![]));
        view.apply(topic, 1, record(live, 9_000_000, true, vec![hash], vec![]));
        let (local, _) = view.candidates(hash, "aws/us-west-2a", Duration::from_secs(5), Timestamp::from(10_000_000));
        assert_eq!(local, HashSet::from([live]));
    }
}
