use uuid::Uuid;

use crate::actor::ErrorBox;
use crate::actor::ShutdownReason;
use crate::actor::SystemMessage;

use tokio;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use async_trait::async_trait;

pub struct RequestId(Uuid);

pub enum Response<T> {
    Reply(T),
    NoReply
}

#[async_trait]
pub trait RequestHandler<T, U>: Send + 'static
where
    T: Send + 'static,
    U: Send + 'static,
{
    async fn on_request(&mut self, request_id: RequestId, request: T) -> Result<U, ErrorBox>;
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
    // late submissions channel here
    // set of request_id that have not been replied to
    tokio::spawn(async move {
        let mut parent = None;
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
                        match actor.on_request(request_id, msg).await {
                            Ok(r) => {
                                result = r;
                            },
                            Err(_err) => {
                                reason = ShutdownReason::Crashed;
                                break 'outer;
                            },
                        }

                        if respond_sender.send(result).is_err() {
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
    async fn on_request(&mut self, request_id: RequestId, request: StoreRequest<K, V>) -> Result<Option<V>, ErrorBox> {
        use std::collections::hash_map::Entry;
        match request {
            StoreRequest::Get(key) => Ok(self.store.get(&key).cloned()),
            StoreRequest::Set(key, value) => match self.store.entry(key) {
                Entry::Occupied(mut e) => Ok(Some(e.insert(value))),
                Entry::Vacant(e) => {
                    e.insert(value);
                    Ok(None)
                }
            },
        }
    }
}
