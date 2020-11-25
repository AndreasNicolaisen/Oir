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
        deferred_sender: mpsc::Sender<(RequestId, U)>,
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
        let (lss, mut lsr) = mpsc::channel::<(RequestId, U)>(512);

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
                        match actor.on_request(lss.clone(), request_id, msg).await {
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
    pub store: std::collections::HashMap<K, V>,
}

#[async_trait]
impl<K, V> RequestHandler<StoreRequest<K, V>, Option<V>> for Stactor<K, V>
where
    K: Send + std::cmp::Eq + std::hash::Hash + std::fmt::Debug + 'static,
    V: Send + Clone + std::fmt::Debug + 'static,
{
    async fn on_request(
        &mut self,
        _deferred_sender: mpsc::Sender<(RequestId, Option<V>)>,
        _request_id: RequestId,
        request: StoreRequest<K, V>,
    ) -> Result<Response<Option<V>>, ErrorBox> {
        use std::collections::hash_map::Entry;
        use Response::*;
        match request {
            StoreRequest::Get(key) => Ok(Reply(self.store.get(&key).cloned())),
            StoreRequest::Set(key, value) => match self.store.entry(key) {
                Entry::Occupied(mut e) => Ok(Reply(Some(e.insert(value)))),
                Entry::Vacant(e) => {
                    e.insert(value);
                    Ok(Reply(None))
                }
            },
        }
    }
}
