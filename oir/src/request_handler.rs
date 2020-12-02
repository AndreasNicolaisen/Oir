use uuid::Uuid;

use crate::actor::ErrorBox;
use crate::actor::ShutdownReason;
use crate::actor::SystemMessage;

use crate::mailbox::UnnamedMailbox;

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
) -> (
    UnnamedMailbox<(oneshot::Sender<U>, T)>,
    tokio::task::JoinHandle<()>,
)
where
    T: Send + Sync + 'static,
    U: Send + Sync + 'static,
    M: RequestHandler<T, U> + Send + 'static,
{
    let (ss, mut rs) = mpsc::channel::<SystemMessage>(512);
    let (sp, mut rp) = mpsc::channel::<(oneshot::Sender<U>, T)>(512);

    (
        UnnamedMailbox::new(ss, sp),
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
                let _ = parent.send(reason).await;
            }
        }),
    )
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
                    for (id, _) in self.pending.drain_filter(|&mut (_id, ref k)| k == &key) {
                        let _ = deferred_sender.send((id, Some(value.clone()))).await;
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

    fn setup_stactor() -> UnnamedMailbox<(oneshot::Sender<Option<i32>>, StoreRequest<i32, i32>)> {
        let (tp0, _h0) = request_actor(Stactor::new());

        tp0
    }

    #[tokio::test]
    async fn stactor_simple_test() {
        let mut tp = setup_stactor();

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
        let mut tp = setup_stactor();
        let mut tp1 = tp.clone();

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

    #[tokio::test]
    async fn stactor_reduce_test() {
        let mut tp = setup_stactor();

        async fn req(
            tp: &mut UnnamedMailbox<(oneshot::Sender<Option<i32>>, StoreRequest<i32, i32>)>,
            r: StoreRequest<i32, i32>,
        ) -> Option<i32> {
            let (one_s, one_r) = oneshot::channel::<Option<i32>>();
            tp.send((one_s, r)).await.unwrap();
            one_r.await.unwrap()
        }

        let init_size = 16;
        let mut hs = Vec::new();

        let mut adder = |x, y, z, mut tp| {
            hs.push(tokio::spawn(async move {
                let a = req(&mut tp, StoreRequest::Get(x)).await.unwrap();
                let b = req(&mut tp, StoreRequest::Get(y)).await.unwrap();
                assert!(req(&mut tp, StoreRequest::Set(z, a + b)).await.is_none());
            }));
        };

        //  0                            15  16             23 24     27 28 29 30
        // [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1] [2 2 2 2 2 2 2 2] [4 4 4 4] [8 8] [16]
        adder(28, 29, 30, tp.clone());
        for i in 0..2 {
            adder(i * 2 + 24, i * 2 + 24 + 1, 28 + i, tp.clone());
        }
        for i in 0..4 {
            adder(i * 2 + 16, i * 2 + 16 + 1, 24 + i, tp.clone());
        }
        for i in 0..8 {
            adder(i * 2, i * 2 + 1, 16 + i, tp.clone());
        }
        // Suppliers
        for i in 0..init_size {
            let mut tp = tp.clone();
            if i < init_size {
                hs.push(tokio::spawn(async move {
                    assert!(req(&mut tp, StoreRequest::Set(i, 1)).await.is_none());
                }));
            }
        }

        assert_eq!(Some(16), req(&mut tp, StoreRequest::Get(30)).await);
        for h in hs.drain(..) {
            h.await.unwrap();
        }
    }
}
