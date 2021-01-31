use async_trait::async_trait;

use tokio;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::actor::*;
use crate::mailbox::*;
use crate::request_handler::*;
use crate::supervisor::*;

pub const POOL_SERV_NAME: &'static str = "pool_serv";
pub const POOL_SUP_NAME: &'static str = "pool_sup";

struct PoolServActor {
    mailbox: Option<UnnamedMailbox<(oneshot::Sender<WorkResult>, PoolServMessage)>>,
    workers: Vec<(
        LocalId,
        UnnamedMailbox<(oneshot::Sender<WorkerResp>, WorkItem)>,
    )>,
    next: usize,
    supervisor: SupervisorMailbox,
    pool_sup: Option<SupervisorMailbox>,
}

type WorkItem = Vec<i32>;
type WorkerResp = i32;

enum PoolServMessage {
    Start(Vec<i32>),
    Result(RequestId, i32),
    WorkerMonitor(LocalId, MonitorMessage),
}

#[derive(Clone, PartialEq, Eq, Debug)]
enum WorkResult {
    Completed(i32),
    Failed,
    Ok,
}

impl PoolServActor {
    fn new(sup: SupervisorMailbox) -> PoolServActor {
        PoolServActor {
            workers: Vec::new(),
            next: 0,
            supervisor: sup,
            pool_sup: None,
            mailbox: None,
        }
    }
}

impl Actor for PoolServActor {
    type Arg = ();
    type Message = (oneshot::Sender<WorkResult>, PoolServMessage);

    fn start(
        sup: Option<&crate::supervisor::SupervisorMailbox>,
        _: (),
    ) -> (UnnamedMailbox<Self::Message>, tokio::task::JoinHandle<()>) {
        request_actor(PoolServActor::new(
            sup.expect("pool_serv needs a supervisor").clone(),
        ))
    }
}

#[async_trait]
impl RequestHandler<PoolServMessage, WorkResult> for PoolServActor {
    async fn init(&mut self, mb: UnnamedMailbox<(oneshot::Sender<WorkResult>, PoolServMessage)>) {
        // Get the pool supervisor
        let mut pool_sup = which_children(&mut self.supervisor)
            .await
            .unwrap()
            .into_iter()
            .find(|ch| ch.local_id == LocalId::Named(POOL_SUP_NAME.to_string()))
            .unwrap()
            .mailbox
            .into_typed::<SupervisorRequest>()
            .unwrap();

        for worker in which_children(&mut pool_sup).await.unwrap() {
            let wmb = worker
                .mailbox
                .into_typed::<(oneshot::Sender<WorkerResp>, WorkItem)>()
                .unwrap();

            let mut mon = monitor(&mut pool_sup, worker.local_id.clone())
                .await
                .unwrap()
                .unwrap();

            {
                let mut mb = mb.clone();
                let li = worker.local_id.clone();
                tokio::spawn(async move {
                    loop {
                        match mon.recv().await {
                            None | Some(MonitorMessage::Shutdown) => {
                                let (ss, rs) = oneshot::channel();
                                mb.send((
                                    ss,
                                    PoolServMessage::WorkerMonitor(
                                        li.clone(),
                                        MonitorMessage::Shutdown,
                                    ),
                                ))
                                .await;
                                break;
                            }
                            Some(MonitorMessage::Restarted(new_mb)) => {
                                let (ss, rs) = oneshot::channel();
                                mb.send((
                                    ss,
                                    PoolServMessage::WorkerMonitor(
                                        li.clone(),
                                        MonitorMessage::Restarted(new_mb),
                                    ),
                                ))
                                .await;
                            }
                        }
                    }
                });
            }

            self.workers.push((worker.local_id, wmb));
        }

        self.pool_sup = Some(pool_sup);
        self.mailbox = Some(mb);
    }

    async fn on_request(
        &mut self,
        deferred_sender: &mut mpsc::Sender<(RequestId, WorkResult)>,
        request_id: RequestId,
        request: PoolServMessage,
    ) -> Result<Response<WorkResult>, ErrorBox> {
        match request {
            PoolServMessage::Start(work_item) => {
                let (ss, rs) = oneshot::channel();
                if self.workers[self.next]
                    .1
                    .send((ss, work_item))
                    .await
                    .is_err()
                {
                    panic!("Doesn't send worker work item");
                }
                self.next = (self.next + 1) % self.workers.len();

                let mut mb = self.mailbox.as_ref().unwrap().clone();

                tokio::spawn(async move {
                    let r = rs.await.unwrap();
                    let (ss, rs) = oneshot::channel();
                    mb.send((ss, PoolServMessage::Result(request_id, r))).await;
                });

                Ok(Response::NoReply)
            }
            PoolServMessage::Result(ri, result) => {
                deferred_sender
                    .send((ri, WorkResult::Completed(result)))
                    .await;

                Ok(Response::Reply(WorkResult::Ok))
            }
            PoolServMessage::WorkerMonitor(li, status) => {
                if let MonitorMessage::Restarted(new_mb) = status {
                    // Update the worker's mailbox
                    let worker = self.workers.iter_mut().find(|(l, _)| l == &li).unwrap();
                    worker.1 = new_mb
                        .into_typed::<(oneshot::Sender<WorkerResp>, WorkItem)>()
                        .unwrap();
                } else {
                    panic!("Worker pool disturbed");
                }
                Ok(Response::Reply(WorkResult::Ok))
            }
        }
    }
}

struct PoolWorkerActor;

#[async_trait]
impl RequestHandler<WorkItem, i32> for PoolWorkerActor {
    async fn on_request(
        &mut self,
        _deferred_sender: &mut mpsc::Sender<(RequestId, WorkerResp)>,
        _request_id: RequestId,
        items: WorkItem,
    ) -> Result<Response<WorkerResp>, ErrorBox> {
        let r = items.into_iter().sum();
        Ok(Response::Reply(r))
    }
}

impl Actor for PoolWorkerActor {
    type Arg = ();
    type Message = (oneshot::Sender<WorkerResp>, WorkItem);

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
            supervisor(
                RestartPolicy::Permanent,
                RestartStrategy::OneForOne,
                vec![child::<PoolWorkerActor>(RestartPolicy::Permanent, ()); num_workers],
            )
            .named(POOL_SUP_NAME),
            child::<PoolServActor>(RestartPolicy::Permanent, ()).globally_named(POOL_SERV_NAME),
        ],
    )
}

type PoolServReqMsg = (oneshot::Sender<WorkResult>, PoolServMessage);

async fn start_work_async<M>(mb: &mut M, items: Vec<i32>) -> Option<oneshot::Receiver<WorkResult>>
where
    M: Mailbox<PoolServReqMsg>,
{
    let (ss, rs) = oneshot::channel();
    mb.send((ss, PoolServMessage::Start(items))).await.ok()?;
    Some(rs)
}

async fn start_work<M>(mb: &mut M, items: Vec<i32>) -> Option<WorkResult>
where
    M: Mailbox<PoolServReqMsg>,
{
    let mut rs = start_work_async(mb, items).await?;
    rs.await.ok()
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_pool() {
        let (sup_rs, sup_h) = pool(5);
        let mut mb = NamedMailbox::<PoolServReqMsg>::new(POOL_SERV_NAME.to_owned());

        // Wait for the PoolServActor to be ready
        while mb.resolve().is_err() {
            tokio::task::yield_now().await;
        }

        // Test single work dispatch
        assert!(matches!(
            start_work(&mut mb, (1..=100).collect::<Vec<_>>())
                .await
                .unwrap(),
            WorkResult::Completed(5050)
        ));

        // Check fully saturating work dispatch:
        let mut incoming_results = Vec::new();
        for i in 0..50 {
            incoming_results.push(
                start_work_async(&mut mb, (i * 100..=(1 + i) * 100).collect::<Vec<_>>())
                    .await
                    .unwrap(),
            );
        }
        for (i, inc) in incoming_results.into_iter().enumerate() {
            let r = (i * 100..=(1 + i) * 100).sum::<usize>() as i32;
            assert_eq!(inc.await.unwrap(), WorkResult::Completed(r))
        }
    }
}
