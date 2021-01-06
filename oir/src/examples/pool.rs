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

trait ActorArg: Clone + Send + Sync + 'static {}

trait Actor {
    type Arg: ActorArg;

    fn from_arg(arg: Self::Arg) -> Self;
}

struct ChildSpec {
    policy: RestartPolicy,
    sender: DynamicMailbox,
    starter: Box<dyn Fn() -> () + std::marker::Send + std::marker::Sync + 'static>,
}

fn child<A: Actor>(
    r: RestartPolicy,
    s: DynamicMailbox,
    b: <A as Actor>::Arg,
) -> ChildSpec {
    ChildSpec {
        policy: r,
        sender: s,
        starter: Box::new(move || {
            <A as Actor>::from_arg(b.clone());
        }),
    }
}

// supervisor_tree! {
//     AllForOne,
//     [
//         permanent Supervisor {
//             OneForOne,
//             [
//                 permenent PoolWorker {},
//                 ...
//             ]
//         },
//         permanent PoolServActor "WOw" {},
//     ]
// };

fn pool(num_workers: usize) -> (DynamicMailbox, tokio::task::JoinHandle<()>) {
    supervise_multi(
        vec![
            Box::new(move || {
                let (mb, h) = request_actor(PoolServActor::new());
                let dmb = DynamicMailbox::from(mb.clone());
                mb.register(POOL_SERV_NAME.to_owned());
                (dmb, h, RestartPolicy::Permanent)
            }),
            Box::new(move || {
                let mut worker_spec = Vec::new();
                for i in 0..num_workers {
                    let b: Box<
                        dyn Fn() -> (
                                DynamicMailbox,
                                tokio::task::JoinHandle<()>,
                                RestartPolicy,
                            ) + Send
                            + Sync
                            + 'static,
                    > = Box::new(move || {
                        let (mb, h) = request_actor(PoolServActor::new());
                        (mb.into(), h, RestartPolicy::Permanent)
                    });
                    worker_spec.push(b);
                }
                let (rs, h) = supervise_multi(worker_spec, RestartStrategy::OneForOne);
                (rs, h, RestartPolicy::Permanent)
            }),
        ],
        RestartStrategy::OneForAll,
    )
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_pool() {
        let (sup_rs, sup_h) = pool(5);
        let mut mb = NamedMailbox::<(oneshot::Sender<WorkResult>, WorkItem)>::new(POOL_SERV_NAME.to_owned());

        while mb.resolve().is_err() {
            tokio::task::yield_now().await;
        }
    }
}
