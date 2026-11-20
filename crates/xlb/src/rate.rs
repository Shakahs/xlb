//! Tower admission for starts in each transfer's peer and origin tiers.

use std::{convert::Infallible, future::{Ready, ready}, time::Duration};

use tokio::sync::Mutex;
use tower::{Service, ServiceExt, limit::{RateLimit, rate::Rate}, util::{ServiceFn, service_fn}};

const REQUEST_WINDOW: Duration = Duration::from_millis(50);
type Admission = fn(()) -> Ready<Result<(), Infallible>>;

pub(crate) struct RequestRate(Mutex<RateLimit<ServiceFn<Admission>>>);

impl RequestRate {
    pub(crate) fn new(starts: usize) -> Self {
        let admit: Admission = |_| ready(Ok(()));
        Self(Mutex::new(RateLimit::new(
            service_fn(admit),
            Rate::new(starts.max(1) as u64, REQUEST_WINDOW),
        )))
    }

    pub(crate) async fn start(&self) {
        let mut limit = self.0.lock().await;
        let _ = limit.ready().await.expect("Tower rate admission is infallible").call(()).await;
    }
}
