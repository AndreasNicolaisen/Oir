use uuid::Uuid;

use crate::actor::ErrorBox;
use crate::actor::ShutdownReason;
use crate::actor::SystemMessage;

use tokio;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use async_trait::async_trait;
use std::collections::HashMap;

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct RequestId(Uuid);

pub enum Response<T> {
    Reply(T),
    NoReply,
}

#[async_trait]
pub trait RequestHandler<T, U>: Send + 'static
where
    T: Send + 'static,
    U: Send + 'static,
{
    async fn on_request(
        &mut self,
        deferred_sender: &mut mpsc::Sender<(RequestId, U)>,
        request_id: RequestId,
        request: T,
    ) -> Result<Response<U>, ErrorBox>;
}

pub fn request_actor<T, U, M>(
    mut actor: M,
    mut rs: mpsc::Receiver<SystemMessage>,
    mut rp: mpsc::Receiver<(oneshot::Sender<U>, T)>,
) -> tokio::task::JoinHandle<()>
where
    T: Send + 'static,
    U: Send + 'static,
    M: RequestHandler<T, U> + Send + 'static,
{
    tokio::spawn(async move {
        use Response::*;
        let mut parent = None;
        let mut pending = HashMap::new();
        let (mut lss, mut lsr) = mpsc::channel::<(RequestId, U)>(512);

        let reason;
        'outer: loop {
            tokio::select! {
                sys_msg = rs.recv() => {
                    match sys_msg {
                        Some(SystemMessage::Shutdown) => {
                            reason = ShutdownReason::Shutdown;
                            break 'outer;
                        },
                        Some(SystemMessage::Link(sender)) => {
                            parent = Some(sender);
                        },
                        _ => {}
                    }
                },
                request_msg = rp.recv() => {
                    if let Some((respond_sender, msg)) = request_msg {
                        let result;
                        let request_id = RequestId(Uuid::new_v4());
                        match actor.on_request(&mut lss, request_id, msg).await {
                            Ok(Reply(r)) => {
                                result = r;

                                if respond_sender.send(result).is_err() {
                                    reason = ShutdownReason::Crashed;
                                    break 'outer;
                                }
                            },
                            Ok(NoReply) => {
                                pending.insert(request_id, respond_sender);
                            }
                            Err(_err) => {
                                reason = ShutdownReason::Crashed;
                                break 'outer;
                            },
                        }
                    }
                }
                late_response_msg = lsr.recv() => {
                    if let Some((request_id, result)) = late_response_msg {
                        if let None = pending
                            .remove(&request_id)
                            .and_then(|reply_sender| reply_sender.send(result).ok())
                        {
                            reason = ShutdownReason::Crashed;
                            break 'outer;
                        }
                    }
                }
            }
        }

        if let Some(parent) = parent {
            let _ = parent.send(SystemMessage::Stopped(reason)).await;
        }
    })
}

#[derive(Debug, Clone, Copy)]
pub enum StoreRequest<K, V> {
    Get(K),
    Set(K, V),
}

pub struct Stactor<K, V> {
    store: std::collections::HashMap<K, V>,
    pending: Vec<(RequestId, K)>,
}

impl<K, V> Stactor<K, V> {
    pub fn new() -> Stactor<K, V> {
        Stactor {
            store: HashMap::new(),
            pending: Vec::new(),
        }
    }
}

#[async_trait]
impl<K, V> RequestHandler<StoreRequest<K, V>, Option<V>> for Stactor<K, V>
where
    K: Send + Sync + Clone + std::cmp::Eq + std::hash::Hash + std::fmt::Debug + 'static,
    V: Send + Sync + Clone + std::fmt::Debug + 'static,
{
    async fn on_request(
        &mut self,
        deferred_sender: &mut mpsc::Sender<(RequestId, Option<V>)>,
        request_id: RequestId,
        request: StoreRequest<K, V>,
    ) -> Result<Response<Option<V>>, ErrorBox> {
        use std::collections::hash_map::Entry;
        use Response::*;
        match request {
            StoreRequest::Get(key) => {
                if let Some(v) = self.store.get(&key).cloned() {
                    Ok(Reply(Some(v)))
                } else {
                    self.pending.push((request_id, key));
                    Ok(NoReply)
                }
            }
            StoreRequest::Set(key, value) => match self.store.entry(key.clone()) {
                Entry::Occupied(mut e) => Ok(Reply(Some(e.insert(value)))),
                Entry::Vacant(e) => {
                    for (id, _) in self.pending.drain_filter(|&mut (id, ref k)| k == &key) {
                        deferred_sender.send((id, Some(value.clone()))).await;
                    }
                    e.insert(value);
                    Ok(Reply(None))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_stactor() -> mpsc::Sender<(oneshot::Sender<Option<i32>>, StoreRequest<i32, i32>)> {
        let (_ts0, rs0) = mpsc::channel::<SystemMessage>(512);
        let (tp0, rp0) =
            mpsc::channel::<(oneshot::Sender<Option<i32>>, StoreRequest<i32, i32>)>(1024);
        let _h0 = request_actor(Stactor::new(), rs0, rp0);

        tp0
    }

    #[tokio::test]
    async fn stactor_simple_test() {
        let tp = setup_stactor();

        {
            let (one_s, one_r) = oneshot::channel::<Option<i32>>();
            tp.send((one_s, StoreRequest::Set(0, 0))).await.unwrap();

            assert_eq!(Ok(None), one_r.await);
        }

        {
            let (one_s, one_r) = oneshot::channel::<Option<i32>>();
            tp.send((one_s, StoreRequest::Get(0))).await.unwrap();
            assert_eq!(Ok(Some(0)), one_r.await);
        }
    }

    #[tokio::test]
    async fn stactor_deferred_response_test() {
        let tp = setup_stactor();
        let tp1 = tp.clone();

        let handle = tokio::spawn(async move {
            let (one_s, one_r) = oneshot::channel::<Option<i32>>();
            tp1.send((one_s, StoreRequest::Get(0))).await.unwrap();
            assert_eq!(Ok(Some(0)), one_r.await);
        });

        {
            let (one_s, one_r) = oneshot::channel::<Option<i32>>();
            tp.send((one_s, StoreRequest::Set(0, 0))).await.unwrap();
            assert_eq!(Ok(None), one_r.await);
        }

        handle.await.unwrap();
    }
}
