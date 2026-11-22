use std::{convert::Infallible, future::Ready, num::NonZeroUsize, sync::{Arc, atomic::{AtomicUsize, Ordering}}};

use opentelemetry::{KeyValue, global, metrics::UpDownCounter};

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
    counts: Arc<RequestCounts>,
    metric: UpDownCounter<i64>,
}

#[derive(Default)]
struct RequestCounts {
    total: AtomicUsize,
    origin: AtomicUsize,
    peer: AtomicUsize,
    total_peak: AtomicUsize,
    origin_peak: AtomicUsize,
    peer_peak: AtomicUsize,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RequestStats {
    pub total: usize, pub origin: usize, pub peer: usize,
    pub total_peak: usize, pub origin_peak: usize, pub peer_peak: usize,
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
        let stats = limits.stats();
        assert_eq!((stats.total, stats.origin, stats.peer), (3, 2, 1));
        assert_eq!((stats.total_peak, stats.origin_peak, stats.peer_peak), (3, 2, 1));
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
        let stats = limits.stats();
        assert_eq!((stats.total, stats.origin, stats.peer), (0, 0, 0));
        assert_eq!((stats.total_peak, stats.origin_peak, stats.peer_peak), (3, 2, 2));
    }
}

type Admission = fn(()) -> Ready<Result<(), Infallible>>;
type LimitedAdmission = ConcurrencyLimit<ConcurrencyLimit<ServiceFn<Admission>>>;

/// A ready Tower service retains both reservations until it is dropped.
pub(crate) struct RequestSlot {
    _admission: LimitedAdmission,
    counts: Arc<RequestCounts>,
    source: &'static str,
    metric: UpDownCounter<i64>,
}

impl Drop for RequestSlot {
    fn drop(&mut self) {
        self.counts.total.fetch_sub(1, Ordering::Relaxed);
        let count = if self.source == "s3" { &self.counts.origin } else { &self.counts.peer };
        count.fetch_sub(1, Ordering::Relaxed);
        self.metric.add(-1, &[KeyValue::new("source", self.source)]);
    }
}

impl RequestLimits {
    pub fn new(total: NonZeroUsize, origin: NonZeroUsize, peer: NonZeroUsize) -> Result<Self, Error> {
        let (total, origin, peer) = (total.get(), origin.get(), peer.get());
        if total.max(origin).max(peer) > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(Error::Config("XLB request limit exceeds Tower capacity".into()));
        }
        // [impl -> dsn~xlb-connection-transfer-metrics~1]
        Ok(Self {
            total: GlobalConcurrencyLimitLayer::new(total),
            origin: GlobalConcurrencyLimitLayer::new(origin),
            peer: GlobalConcurrencyLimitLayer::new(peer),
            counts: Arc::new(RequestCounts::default()),
            metric: global::meter("imgcloud.xlb").i64_up_down_counter("xlb.requests.active").build(),
        })
    }

    pub(crate) async fn origin(&self) -> RequestSlot {
        self.admit(&self.origin, "s3").await
    }

    pub(crate) async fn peer(&self) -> RequestSlot {
        self.admit(&self.peer, "peer").await
    }

    pub fn stats(&self) -> RequestStats {
        let c = &self.counts;
        RequestStats { total: c.total.load(Ordering::Relaxed), origin: c.origin.load(Ordering::Relaxed), peer: c.peer.load(Ordering::Relaxed), total_peak: c.total_peak.load(Ordering::Relaxed), origin_peak: c.origin_peak.load(Ordering::Relaxed), peer_peak: c.peer_peak.load(Ordering::Relaxed) }
    }

    async fn admit(&self, source: &GlobalConcurrencyLimitLayer, source_name: &'static str) -> RequestSlot {
        let empty: Admission = |()| std::future::ready(Ok(()));
        // Tower reserves at readiness. Acquire the source slot first so a
        // queued request cannot take a total slot away from the other source.
        let admission = source.layer(self.total.layer(service_fn(empty)));
        let admission = admission.ready_oneshot().await.expect("infallible admission");
        let total = self.counts.total.fetch_add(1, Ordering::Relaxed) + 1;
        self.counts.total_peak.fetch_max(total, Ordering::Relaxed);
        let (count, peak) = if source_name == "s3" { (&self.counts.origin, &self.counts.origin_peak) } else { (&self.counts.peer, &self.counts.peer_peak) };
        let current = count.fetch_add(1, Ordering::Relaxed) + 1;
        peak.fetch_max(current, Ordering::Relaxed);
        let metric = self.metric.clone();
        metric.add(1, &[KeyValue::new("source", source_name)]);
        RequestSlot { _admission: admission, counts: self.counts.clone(), source: source_name, metric }
    }
}
