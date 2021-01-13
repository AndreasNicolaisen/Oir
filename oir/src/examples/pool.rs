use async_trait::async_trait;

use tokio;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::actor::*;
use crate::mailbox::*;
use crate::request_handler::*;
use crate::supervisor::*;

pub const POOL_SERV_NAME: &'static str = "pool_serv";

struct PoolServActor;
#[derive(Clone)]
struct WorkItem;
#[derive(Clone)]
enum WorkResult {
    Started,
    Completed,
}

impl PoolServActor {
    fn new() -> PoolServActor {
        PoolServActor
    }
}

impl Actor for PoolServActor {
    type Arg = ();
    type Message = (oneshot::Sender<WorkResult>, WorkItem);

    fn start(
        sup: Option<&crate::supervisor::SupervisorMailbox>,
        _: (),
    ) -> (UnnamedMailbox<Self::Message>, tokio::task::JoinHandle<()>) {
        request_actor(PoolServActor::new())
    }
}

#[async_trait]
impl RequestHandler<WorkItem, WorkResult> for PoolServActor {
    async fn on_request(
        &mut self,
        deferred_sender: &mut mpsc::Sender<(RequestId, WorkResult)>,
        request_id: RequestId,
        request: WorkItem,
    ) -> Result<Response<WorkResult>, ErrorBox> {
        // TODO:
        Ok(Response::Reply(WorkResult::Started))
    }
}

struct PoolWorkerActor;

#[async_trait]
impl RequestHandler<WorkItem, WorkResult> for PoolWorkerActor {
    async fn on_request(
        &mut self,
        deferred_sender: &mut mpsc::Sender<(RequestId, WorkResult)>,
        request_id: RequestId,
        request: WorkItem,
    ) -> Result<Response<WorkResult>, ErrorBox> {
        // TODO:
        Ok(Response::Reply(WorkResult::Completed))
    }
}

impl Actor for PoolWorkerActor {
    type Arg = ();
    type Message = (oneshot::Sender<WorkResult>, WorkItem);

    fn start(
        sup: Option<&crate::supervisor::SupervisorMailbox>,
        _: (),
    ) -> (UnnamedMailbox<Self::Message>, tokio::task::JoinHandle<()>) {
        request_actor(PoolWorkerActor)
    }
}

fn pool(
    num_workers: usize,
) -> (
    UnnamedMailbox<SupervisorRequest>,
    tokio::task::JoinHandle<()>,
) {
    supervise(
        RestartStrategy::OneForAll,
        vec![
            child::<PoolServActor>(RestartPolicy::Permanent, ())
                .globally_named(POOL_SERV_NAME.to_owned()),
            supervisor(
                RestartPolicy::Permanent,
                RestartStrategy::OneForOne,
                vec![child::<PoolWorkerActor>(RestartPolicy::Temporary, ()); num_workers],
            ),
        ],
    )
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_pool() {
        let (sup_rs, sup_h) = pool(5);
        let mut mb =
            NamedMailbox::<(oneshot::Sender<WorkResult>, WorkItem)>::new(POOL_SERV_NAME.to_owned());

        while mb.resolve().is_err() {
            tokio::task::yield_now().await;
        }
    }
}
