use std::{sync::Arc, time::Duration};

use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio_util::task::AbortOnDropHandle;

use crate::{Error, Result, cas_cache::CasCache};

rust_fsm::state_machine! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    checkpoint_policy(Ready)

    Ready(Tick) => Writing [Write],
    Writing => {
        Persisted => Ready,
        Failed => RetryPending [Report],
    },
    RetryPending(Tick) => Writing [Write],
}

pub(crate) struct CacheCheckpoint(ActorRef<()>);

impl CacheCheckpoint {
    pub(crate) async fn start(cache: Arc<CasCache>) -> Result<Self> {
        let (actor, _) = Actor::spawn(None, CheckpointWorker(cache), ()).await
            .map_err(|error| Error::Store(format!("start CAS checkpoints: {error}")))?;
        Ok(Self(actor))
    }

    pub(crate) async fn stop(&self) {
        let _ = self.0.stop_and_wait(None, None).await;
    }

    pub(crate) async fn stopped(&self) {
        let _ = self.0.wait(None).await;
    }
}

struct CheckpointWorker(Arc<CasCache>);

impl Actor for CheckpointWorker {
    type Msg = ();
    type Arguments = ();
    type State = (checkpoint_policy::StateMachine, AbortOnDropHandle<()>);

    async fn pre_start(&self, myself: ActorRef<()>, _: ()) -> std::result::Result<Self::State, ActorProcessingErr> {
        let timer = myself.send_interval(Duration::from_secs(5), || ());
        Ok((checkpoint_policy::StateMachine::new(), AbortOnDropHandle::new(timer)))
    }

    // [impl -> dsn~xlb-supervised-lifecycle~1]
    async fn handle(
        &self, _: ActorRef<()>, _: (), (policy, _): &mut Self::State,
    ) -> std::result::Result<(), ActorProcessingErr> {
        use checkpoint_policy::{Input, Output};

        if policy.consume(&Input::Tick).expect("checkpoint timer transition") == Some(Output::Write) {
            let result = self.0.checkpoint().await;
            let event = if result.is_ok() { Input::Persisted } else { Input::Failed };
            if policy.consume(&event).expect("checkpoint result transition") == Some(Output::Report) {
                tracing::warn!(error = ?result.err(), "XLB CAS checkpoint will retry");
            }
        }
        Ok(())
    }
}
