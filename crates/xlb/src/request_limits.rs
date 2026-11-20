use std::{convert::Infallible, future::Ready, num::NonZeroUsize};

use tower::{
    limit::{ConcurrencyLimit, GlobalConcurrencyLimitLayer},
    service_fn,
    util::ServiceFn,
    Layer, ServiceExt,
};

use crate::Error;

/// Outgoing request limits shared by the daemon's asset classes.
#[derive(Clone)]
pub struct RequestLimits {
    total: GlobalConcurrencyLimitLayer,
    origin: GlobalConcurrencyLimitLayer,
    peer: GlobalConcurrencyLimitLayer,
    total_capacity: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sources_share_total_capacity_and_keep_their_own_limits() {
        let two = NonZeroUsize::new(2).unwrap();
        let three = NonZeroUsize::new(3).unwrap();
        let limits = RequestLimits::new(three, two, two).unwrap();
        let other_class = limits.clone();

        let first_origin = limits.origin().await;
        let second_origin = other_class.origin().await;
        let mut extra_origin = Box::pin(limits.origin());
        assert!(futures::poll!(&mut extra_origin).is_pending());

        let first_peer = limits.peer().await;
        let mut second_peer = Box::pin(other_class.peer());
        assert!(futures::poll!(&mut second_peer).is_pending());

        drop(first_origin);
        drop(extra_origin);
        let second_peer = second_peer.await;
        drop(second_origin);
        let mut extra_peer = Box::pin(limits.peer());
        assert!(futures::poll!(&mut extra_peer).is_pending());

        drop(first_peer);
        let third_peer = extra_peer.await;
        let origin = other_class.origin().await;
        drop((second_peer, third_peer, origin));
    }
}

type Admission = fn(()) -> Ready<Result<(), Infallible>>;
type LimitedAdmission = ConcurrencyLimit<ConcurrencyLimit<ServiceFn<Admission>>>;

/// A ready Tower service retains both reservations until it is dropped.
pub(crate) struct RequestSlot {
    _admission: LimitedAdmission,
}

impl RequestLimits {
    pub fn new(total: NonZeroUsize, origin: NonZeroUsize, peer: NonZeroUsize) -> Result<Self, Error> {
        let (total, origin, peer) = (total.get(), origin.get(), peer.get());
        if total.max(origin).max(peer) > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(Error::Config("XLB request limit exceeds Tower capacity".into()));
        }
        Ok(Self {
            total: GlobalConcurrencyLimitLayer::new(total),
            origin: GlobalConcurrencyLimitLayer::new(origin),
            peer: GlobalConcurrencyLimitLayer::new(peer),
            total_capacity: total,
        })
    }

    pub(crate) fn total_capacity(&self) -> usize {
        self.total_capacity
    }

    pub(crate) async fn origin(&self) -> RequestSlot {
        self.admit(&self.origin).await
    }

    pub(crate) async fn peer(&self) -> RequestSlot {
        self.admit(&self.peer).await
    }

    async fn admit(&self, source: &GlobalConcurrencyLimitLayer) -> RequestSlot {
        let empty: Admission = |()| std::future::ready(Ok(()));
        // Tower reserves at readiness. Acquire the source slot first so a
        // queued request cannot take a total slot away from the other source.
        let admission = source.layer(self.total.layer(service_fn(empty)));
        RequestSlot { _admission: admission.ready_oneshot().await.expect("infallible admission") }
    }
}
