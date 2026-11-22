//! Node-wide chunk transfer admission that serves demand reads before prefetch.

use std::{collections::BTreeMap, sync::{Arc, Mutex, MutexGuard}};

use tokio::sync::{Notify, oneshot};

/// Whether a reader is waiting for the chunk or XLB fetches it ahead of reads.
/// Demand sorts first in the admission queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransferPriority {
    Demand,
    Background,
}

/// Limits active chunk transfers across asset classes. Waiting transfers form
/// a priority queue ordered by priority, then arrival.
#[derive(Clone)]
pub struct ChunkAdmission(Arc<Mutex<AdmissionQueue>>);

struct AdmissionQueue {
    free: usize,
    arrivals: u64,
    waiting: BTreeMap<(TransferPriority, u64), oneshot::Sender<()>>,
}

impl AdmissionQueue {
    /// Give a freed slot to the first live waiter, or keep it free.
    fn release(&mut self) {
        while let Some((_, grant)) = self.waiting.pop_first() {
            if grant.send(()).is_ok() {
                return;
            }
        }
        self.free += 1;
    }
}

/// A transfer slot, returned to the queue when dropped.
pub(crate) struct ChunkSlot(ChunkAdmission);

impl Drop for ChunkSlot {
    fn drop(&mut self) {
        self.0.queue().release();
    }
}

/// A queued transfer. Dropping it leaves the queue, and returns a slot that
/// was granted but not yet taken.
struct QueuedTransfer {
    admission: ChunkAdmission,
    key: (TransferPriority, u64),
    grant: Option<oneshot::Receiver<()>>,
}

impl Drop for QueuedTransfer {
    fn drop(&mut self) {
        let Some(mut grant) = self.grant.take() else { return };
        let mut queue = self.admission.queue();
        queue.waiting.remove(&self.key);
        grant.close();
        if grant.try_recv().is_ok() {
            queue.release();
        }
    }
}

impl ChunkAdmission {
    pub fn new(limit: usize) -> Self {
        Self(Arc::new(Mutex::new(AdmissionQueue { free: limit, arrivals: 0, waiting: BTreeMap::new() })))
    }

    fn queue(&self) -> MutexGuard<'_, AdmissionQueue> {
        self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Wait for a transfer slot. A background wait moves to demand, keeping
    /// its arrival, as soon as `promoted` is notified.
    // [impl -> dsn~xlb-fuse-snapshots~1]
    pub(crate) async fn admit(&self, priority: TransferPriority, promoted: &Notify) -> ChunkSlot {
        let mut queued = {
            let mut queue = self.queue();
            if queue.free > 0 {
                queue.free -= 1;
                return ChunkSlot(self.clone());
            }
            let (grant, receiver) = oneshot::channel();
            queue.arrivals += 1;
            let key = (priority, queue.arrivals);
            queue.waiting.insert(key, grant);
            QueuedTransfer { admission: self.clone(), key, grant: Some(receiver) }
        };
        loop {
            let background = queued.key.0 == TransferPriority::Background;
            tokio::select! {
                granted = queued.grant.as_mut().expect("queued transfer awaits a grant") => {
                    granted.expect("the queue grants each waiter before dropping it");
                    queued.grant = None;
                    return ChunkSlot(self.clone());
                }
                () = promoted.notified(), if background => {
                    let mut queue = self.queue();
                    if let Some(grant) = queue.waiting.remove(&queued.key) {
                        queued.key.0 = TransferPriority::Demand;
                        queue.waiting.insert(queued.key, grant);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use TransferPriority::{Background, Demand};

    // [utest -> dsn~xlb-fuse-snapshots~1]
    #[tokio::test]
    async fn freed_slots_go_to_demand_before_queued_prefetch() {
        let admission = ChunkAdmission::new(1);
        let unused = Notify::new();
        let held = admission.admit(Demand, &unused).await;
        let mut first = Box::pin(admission.admit(Background, &unused));
        let mut second = Box::pin(admission.admit(Background, &unused));
        let mut demand = Box::pin(admission.admit(Demand, &unused));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        assert!(futures::poll!(&mut demand).is_pending());

        drop(held);
        let demand = demand.await;
        assert!(futures::poll!(&mut first).is_pending(), "the demand transfer goes before queued prefetch");
        drop(demand);
        let first = first.await;
        assert!(futures::poll!(&mut second).is_pending(), "prefetch keeps arrival order");
        drop(first);
        drop(second.await);
    }

    // [utest -> dsn~xlb-fuse-snapshots~1]
    #[tokio::test]
    async fn a_promoted_background_transfer_goes_before_queued_prefetch() {
        let admission = ChunkAdmission::new(1);
        let unused = Notify::new();
        let promotion = Notify::new();
        let held = admission.admit(Demand, &unused).await;
        let mut first = Box::pin(admission.admit(Background, &unused));
        let mut promoted = Box::pin(admission.admit(Background, &promotion));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut promoted).is_pending());

        promotion.notify_one();
        assert!(futures::poll!(&mut promoted).is_pending());
        drop(held);
        let promoted = promoted.await;
        assert!(futures::poll!(&mut first).is_pending(), "the promoted transfer goes before queued prefetch");
        drop(promoted);
        drop(first.await);
    }

    // [utest -> dsn~xlb-fuse-snapshots~1]
    #[tokio::test]
    async fn a_cancelled_waiter_returns_its_granted_slot() {
        let admission = ChunkAdmission::new(1);
        let unused = Notify::new();
        let held = admission.admit(Demand, &unused).await;
        let mut cancelled = Box::pin(admission.admit(Demand, &unused));
        assert!(futures::poll!(&mut cancelled).is_pending());
        drop(held);
        drop(cancelled);
        drop(admission.admit(Background, &unused).await);
    }
}
