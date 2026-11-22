use std::future::Future;

use anyhow::{Context, Result, anyhow};
use futures::future::BoxFuture;
use ractor::{Actor, ActorCell, ActorProcessingErr, ActorRef, SupervisionEvent};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

rust_fsm::state_machine! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    service_lifecycle(Serving)

    Serving => {
        Shutdown => Releasing [Stop],
        Failed => Releasing [Stop],
    },
    Releasing(Released) => ClosingStorage [CloseStorage],
    ClosingStorage(StorageClosed) => Stopped [Finish],
}

pub(crate) struct ServiceTask {
    name: String,
    run: BoxFuture<'static, Result<()>>,
}

impl ServiceTask {
    pub(crate) fn new(name: impl Into<String>, run: impl Future<Output = Result<()>> + Send + 'static) -> Self {
        Self { name: name.into(), run: Box::pin(run) }
    }
}

struct ServiceActor;

impl Actor for ServiceActor {
    type Msg = ();
    type Arguments = ServiceTask;
    type State = Option<ServiceTask>;

    async fn pre_start(
        &self, _: ActorRef<Self::Msg>, task: ServiceTask,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(Some(task))
    }

    async fn post_start(
        &self, _: ActorRef<Self::Msg>, state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let ServiceTask { name, run } = state.take().expect("service task starts once");
        let failure = match run.await.with_context(|| name.clone()) {
            Ok(()) => format!("{name} exited unexpectedly"),
            Err(error) => format!("{error:#}"),
        };
        Err(failure.into())
    }
}

struct ServiceSupervisor;

struct SupervisedServices {
    children: Vec<ActorCell>,
    cancel: CancellationToken,
    failure: Option<oneshot::Sender<anyhow::Error>>,
}

impl Actor for ServiceSupervisor {
    type Msg = ();
    type Arguments = (Vec<ServiceTask>, CancellationToken, oneshot::Sender<anyhow::Error>);
    type State = SupervisedServices;

    async fn pre_start(
        &self, myself: ActorRef<Self::Msg>, (tasks, cancel, failure): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let mut children = Vec::with_capacity(tasks.len());
        for task in tasks {
            let (child, _) = Actor::spawn_linked(None, ServiceActor, task, myself.get_cell()).await?;
            children.push(child.get_cell());
        }
        Ok(SupervisedServices { children, cancel, failure: Some(failure) })
    }

    async fn handle_supervisor_evt(
        &self, _: ActorRef<Self::Msg>, event: SupervisionEvent, services: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let failure = match event {
            SupervisionEvent::ActorFailed(_, failure) => anyhow!("{failure}"),
            SupervisionEvent::ActorTerminated(_, _, _) => anyhow!("an XLB serving actor stopped"),
            _ => return Ok(()),
        };
        if let Some(notify) = services.failure.take() {
            let _ = notify.send(failure);
        }
        Ok(())
    }

    async fn post_stop(&self, _: ActorRef<Self::Msg>, services: &mut Self::State) -> Result<(), ActorProcessingErr> {
        services.cancel.cancel();
        for child in &services.children {
            let _ = child.wait(None).await;
        }
        Ok(())
    }
}

// [impl -> dsn~xlb-supervised-lifecycle~1]
pub(crate) async fn supervise(
    tasks: Vec<ServiceTask>,
    cancel: CancellationToken,
    shutdown: impl Future<Output = Result<()>>,
    close_storage: impl Future<Output = Result<()>>,
) -> Result<()> {
    use service_lifecycle::{Input, Output, StateMachine};

    let (notify, failed) = oneshot::channel();
    let (supervisor, mut worker) = match Actor::spawn(None, ServiceSupervisor, (tasks, cancel.clone(), notify)).await {
        Ok(started) => started,
        Err(error) => {
            cancel.cancel();
            let _ = close_storage.await;
            return Err(error.into());
        }
    };
    let mut joined = false;
    let (event, outcome) = tokio::select! {
        outcome = shutdown => (Input::Shutdown, outcome),
        failure = failed => (Input::Failed, Err(failure.unwrap_or_else(|_| anyhow!("XLB service supervisor exited")))),
        result = &mut worker => {
            joined = true;
            let outcome = result.context("XLB service supervisor exited")
                .and(Err(anyhow!("XLB service supervisor exited unexpectedly")));
            (Input::Failed, outcome)
        }
    };
    let mut lifecycle = StateMachine::new();
    let mut event = event;
    let mut worker = (!joined).then_some(worker);
    let mut close_storage = Some(close_storage);
    let mut outcome = outcome;
    loop {
        event = match lifecycle.consume(&event).expect("valid lifecycle transition") {
            Some(Output::Stop) => {
                if let Some(worker) = worker.take() {
                    supervisor.stop(None);
                    outcome = outcome.and(worker.await.context("stop XLB service supervisor"));
                } else {
                    cancel.cancel();
                }
                Input::Released
            }
            Some(Output::CloseStorage) => {
                outcome = outcome.and(close_storage.take().expect("close shared storage once").await);
                Input::StorageClosed
            }
            Some(Output::Finish) => return outcome,
            None => unreachable!("every lifecycle transition issues an action"),
        };
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

    use super::*;

    // [itest -> dsn~xlb-supervised-lifecycle~1]
    #[tokio::test]
    async fn a_failed_service_releases_other_workers_before_reporting_failure() {
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let released = Arc::new(AtomicBool::new(false));
        let observed = released.clone();
        let workers = vec![
            ServiceTask::new("consumer", async move {
                stop.cancelled().await;
                observed.store(true, Ordering::Release);
                Ok(())
            }),
            ServiceTask::new("broken mount", async { Err(anyhow!("FUSE has disconnected")) }),
        ];
        let close_storage = async {
            assert!(released.load(Ordering::Acquire));
            Ok(())
        };
        let error = supervise(workers, cancel, std::future::pending(), close_storage).await.unwrap_err();
        assert!(error.to_string().contains("FUSE has disconnected"));
        assert!(released.load(Ordering::Acquire));
    }
}
