//! Decide when a topic needs a checkpoint or a delta.

use std::time::{Duration, Instant};

const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(90);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

// [impl -> dsn~xlb-chunk-discovery~1,dsn~xlb-supervised-lifecycle~1]
rust_fsm::state_machine! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    publication(NeedsCheckpoint)

    NeedsCheckpoint => {
        CheckpointDue => WritingCheckpoint [Checkpoint],
        DeltaDue => WritingCheckpoint [Checkpoint],
        Unchanged => WritingCheckpoint [Checkpoint],
        ChunkAdmitted => NeedsCheckpoint,
    },
    Current => {
        CheckpointDue => WritingCheckpoint [Checkpoint],
        DeltaDue => WritingDelta [Delta],
        ChunkAdmitted => WritingDelta [Delta],
        Unchanged => Current,
    },
    WritingCheckpoint => {
        Published => Current,
        Failed => NeedsCheckpoint,
    },
    WritingDelta => {
        Published => Current,
        Failed => NeedsCheckpoint,
    },
}

use publication::{Input, State, StateMachine};
pub(crate) use publication::Output;

pub(crate) struct InventoryPublication {
    machine: StateMachine,
    last_checkpoint: Option<Instant>,
    last_publish: Option<Instant>,
}

impl InventoryPublication {
    pub(crate) fn new() -> Self {
        Self {
            machine: StateMachine::new(),
            last_checkpoint: None,
            last_publish: None,
        }
    }

    pub(crate) fn reconcile(
        &mut self, now: Instant, inventory_changed: bool, expiry: Duration,
    ) -> Option<Output> {
        let heartbeat = HEARTBEAT_INTERVAL.min(expiry / 3);
        let checkpoint_due = self.last_checkpoint.is_none_or(|last| now - last >= CHECKPOINT_INTERVAL);
        let heartbeat_due = self.last_publish.is_none_or(|last| now - last >= heartbeat);
        let event = match (checkpoint_due, inventory_changed || heartbeat_due) {
            (true, _) => Input::CheckpointDue,
            (false, true) => Input::DeltaDue,
            (false, false) => Input::Unchanged,
        };
        self.machine.consume(&event).expect("reconcile between publications")
    }

    pub(crate) fn admit(&mut self) -> Option<Output> {
        self.machine.consume(&Input::ChunkAdmitted).expect("admit between publications")
    }

    pub(crate) fn finish(&mut self, now: Instant, published: bool) {
        let wrote_checkpoint = *self.machine.state() == State::WritingCheckpoint;
        let event = if published { Input::Published } else { Input::Failed };
        self.machine.consume(&event).expect("a publication must be in flight");
        if published {
            self.last_publish = Some(now);
            if wrote_checkpoint {
                self.last_checkpoint = Some(now);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPIRY: Duration = Duration::from_secs(90);

    // [utest -> dsn~xlb-chunk-discovery~1]
    #[test]
    fn an_admission_can_only_publish_after_an_initial_checkpoint() {
        let now = Instant::now();
        let mut publication = InventoryPublication::new();
        assert_eq!(publication.admit(), None);
        assert_eq!(publication.reconcile(now, false, EXPIRY), Some(Output::Checkpoint));
        publication.finish(now, true);
        assert_eq!(publication.admit(), Some(Output::Delta));
    }

    #[test]
    fn reconciliation_publishes_changes_and_renews_idle_inventory() {
        let start = Instant::now();
        let mut publication = InventoryPublication::new();
        assert_eq!(publication.reconcile(start, false, EXPIRY), Some(Output::Checkpoint));
        publication.finish(start, true);

        let changed = start + Duration::from_secs(1);
        assert_eq!(publication.reconcile(changed, true, EXPIRY), Some(Output::Delta));
        publication.finish(changed, true);
        assert_eq!(publication.reconcile(start + HEARTBEAT_INTERVAL, false, EXPIRY), None);

        let renewal = changed + HEARTBEAT_INTERVAL;
        assert_eq!(publication.reconcile(renewal, false, EXPIRY), Some(Output::Delta));
        publication.finish(renewal, true);
        assert_eq!(
            publication.reconcile(start + CHECKPOINT_INTERVAL, true, EXPIRY),
            Some(Output::Checkpoint),
        );
    }

    #[test]
    fn a_failed_delta_requires_a_checkpoint_even_before_the_next_deadline() {
        let start = Instant::now();
        let mut publication = InventoryPublication::new();
        assert_eq!(publication.reconcile(start, false, EXPIRY), Some(Output::Checkpoint));
        publication.finish(start, true);

        let failed = start + Duration::from_secs(1);
        assert_eq!(publication.admit(), Some(Output::Delta));
        publication.finish(failed, false);
        assert_eq!(publication.admit(), None);
        assert_eq!(publication.reconcile(failed, false, EXPIRY), Some(Output::Checkpoint));
    }
}
