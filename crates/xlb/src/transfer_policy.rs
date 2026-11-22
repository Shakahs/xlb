//! Transfer decisions independent of cache, network and storage I/O.

use rust_fsm::StateMachineImpl;

use crate::NodeId;

const MAX_PEER_ATTEMPTS: u8 = 5;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Sources {
    pub(crate) peer: Option<NodeId>,
    pub(crate) origin_available: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ChunkEvent {
    CacheHit,
    CacheMiss(Sources),
    PeerSkipped(Sources),
    PeerFailed(Sources),
    OriginFailed,
    Verified,
    Stored,
    Announced,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChunkState {
    LookingUp,
    ReadingPeer { failed: u8 },
    ReadingOrigin,
    Admitting,
    Advertising,
    Complete,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChunkAction {
    ReadPeer(NodeId),
    ReadOrigin,
    Store,
    Announce,
    Return,
}

pub(crate) struct ChunkTransfer;

impl ChunkTransfer {
    fn next(state: ChunkState, event: ChunkEvent) -> Option<(ChunkState, ChunkAction)> {
        use ChunkAction as Action;
        use ChunkEvent as Event;
        use ChunkState as State;

        Some(match (state, event) {
            (State::LookingUp, Event::CacheHit) => (State::Complete, Action::Return),
            (State::LookingUp, Event::CacheMiss(sources)) => Self::select(0, sources),
            (State::ReadingPeer { failed }, Event::PeerSkipped(sources)) => Self::select(failed, sources),
            (State::ReadingPeer { failed }, Event::PeerFailed(sources)) => Self::select(failed + 1, sources),
            (State::ReadingPeer { .. } | State::ReadingOrigin, Event::Verified) => (State::Admitting, Action::Store),
            (State::ReadingOrigin, Event::OriginFailed) => (State::Failed, Action::Return),
            (State::Admitting, Event::Stored) => (State::Advertising, Action::Announce),
            (State::Advertising, Event::Announced) => (State::Complete, Action::Return),
            _ => return None,
        })
    }

    fn select(failed: u8, sources: Sources) -> (ChunkState, ChunkAction) {
        if let Some(peer) = sources.peer.filter(|_| failed < MAX_PEER_ATTEMPTS) {
            (ChunkState::ReadingPeer { failed }, ChunkAction::ReadPeer(peer))
        } else if sources.origin_available {
            (ChunkState::ReadingOrigin, ChunkAction::ReadOrigin)
        } else {
            (ChunkState::Failed, ChunkAction::Return)
        }
    }
}

// [impl -> dsn~xlb-supervised-lifecycle~1]
impl StateMachineImpl for ChunkTransfer {
    type State = ChunkState;
    type Input = ChunkEvent;
    type Output = ChunkAction;

    const INITIAL_STATE: Self::State = ChunkState::LookingUp;

    fn transition(state: &Self::State, input: &Self::Input) -> Option<Self::State> {
        Self::next(*state, *input).map(|(state, _)| state)
    }

    fn output(state: &Self::State, input: &Self::Input) -> Option<Self::Output> {
        Self::next(*state, *input).map(|(_, action)| action)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // [utest -> dsn~xlb-supervised-lifecycle~1]
    #[test]
    fn publication_requires_verification_and_admission() {
        let mut transfer = rust_fsm::StateMachine::<ChunkTransfer>::new();
        let sources = Sources { peer: None, origin_available: true };
        assert_eq!(transfer.consume(&ChunkEvent::CacheMiss(sources)).unwrap(), Some(ChunkAction::ReadOrigin));
        assert!(transfer.consume(&ChunkEvent::Stored).is_err());
        assert_eq!(transfer.consume(&ChunkEvent::Verified).unwrap(), Some(ChunkAction::Store));
        assert_eq!(transfer.consume(&ChunkEvent::Stored).unwrap(), Some(ChunkAction::Announce));
        assert_eq!(transfer.consume(&ChunkEvent::Announced).unwrap(), Some(ChunkAction::Return));
        assert_eq!(*transfer.state(), ChunkState::Complete);
    }

    #[test]
    fn the_fifth_failed_peer_attempt_selects_the_origin() {
        let mut transfer = rust_fsm::StateMachine::<ChunkTransfer>::new();
        let node = iroh::SecretKey::generate().public();
        let sources = Sources { peer: Some(node), origin_available: true };
        assert_eq!(transfer.consume(&ChunkEvent::CacheMiss(sources)).unwrap(), Some(ChunkAction::ReadPeer(node)));
        for _ in 0..5 {
            assert_eq!(transfer.consume(&ChunkEvent::PeerSkipped(sources)).unwrap(), Some(ChunkAction::ReadPeer(node)));
        }
        for _ in 0..MAX_PEER_ATTEMPTS - 1 {
            assert_eq!(transfer.consume(&ChunkEvent::PeerFailed(sources)).unwrap(), Some(ChunkAction::ReadPeer(node)));
        }
        assert_eq!(transfer.consume(&ChunkEvent::PeerFailed(sources)).unwrap(), Some(ChunkAction::ReadOrigin));
    }
}
