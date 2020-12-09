use crate::actor::ErrorBox;
use crate::actor::ShutdownReason;
use crate::actor::SystemMessage;
use crate::mailbox::{Mailbox, UnnamedMailbox};

use tokio;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use async_trait::async_trait;

pub fn supervise_single<F>(
    mut rs: mpsc::Receiver<SystemMessage>,
    child_starter: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn() -> (mpsc::Sender<SystemMessage>, tokio::task::JoinHandle<()>) + Send + Sync + 'static,
{
    use crate::request_handler::{request_actor, Stactor, StoreRequest};

    tokio::spawn(async move {
        let (js, mut joins) = mpsc::channel::<Result<(), tokio::task::JoinError>>(512);

        let start_child = || {
            let js = js.clone();
            let (_, h) = child_starter();
            tokio::spawn(async move {
                js.send(h.await).await;
            });
        };

        let _ = start_child();

        let reason;
        'outer: loop {
            tokio::select! {
                sys_msg = rs.recv() => {
                    match sys_msg {
                        Some(SystemMessage::Shutdown) => {
                            reason = ShutdownReason::Shutdown;
                            break 'outer;
                        },
                        _ => {}
                    }
                },
                child_res = joins.recv() => {
                    match child_res {
                        Some(Ok(())) => { /* Child died sucessfully */ },
                        Some(Err(_)) => {
                            // TODO: Add restart limit
                            let _ = start_child();
                        },
                        _ => {}
                    }
                }
            }
        }
    })
}
struct SupervisorState {
    shutdown: bool,
    joins: mpsc::Sender<(usize, Result<(), tokio::task::JoinError>)>,
    starters: Vec<Box<dyn Fn() -> (mpsc::Sender<SystemMessage>, tokio::task::JoinHandle<()>) + std::marker::Send + std::marker::Sync + 'static>>,
    system_senders: Vec<mpsc::Sender<SystemMessage>>,
}

impl SupervisorState {
    fn start_children(
        joins: mpsc::Sender<(usize, Result<(), tokio::task::JoinError>)>,
        starters: Vec<Box<dyn Fn() -> (mpsc::Sender<SystemMessage>, tokio::task::JoinHandle<()>) + std::marker::Send + std::marker::Sync + 'static>>)
        -> SupervisorState {
        let senders = starters
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let js = joins.clone();
                let (sys, h) = f();
                tokio::spawn(async move {
                    js.send((i, h.await)).await;
                });
                sys
            })
            .collect();
        SupervisorState {
            shutdown: false,
            joins,
            starters,
            system_senders: senders,
        }
    }

    async fn shutdown_children(mut self, mut joins: mpsc::Receiver<(usize, Result<(), tokio::task::JoinError>)>) {
        for sender in &mut self.system_senders {
            let _ = sender.send(SystemMessage::Shutdown).await;
        }

        for sender in &mut self.system_senders {
            while !sender.is_closed() {
                joins.recv().await.unwrap();
            }
        }
    }

    fn restart(&mut self, i : usize) {
        assert!(i < self.starters.len());

        let js = self.joins.clone();
        let (sys, h) = self.starters[i]();
        // TODO: Make sure we shutdown the old one if it's not already dead
        tokio::spawn(async move {
            js.send((i, h.await)).await;
        });
        self.system_senders[i] = sys;
    }
}

impl Drop for SupervisorState {
    fn drop(&mut self) {
        if self.shutdown {
            return;
        }

        for sys in &mut self.system_senders {
            let _ = sys.try_send(SystemMessage::Shutdown);
        }
    }
}

pub fn supervise_multi(
    mut rs: mpsc::Receiver<SystemMessage>,
    child_starters: Vec<
        Box<
            dyn Fn() -> (mpsc::Sender<SystemMessage>, tokio::task::JoinHandle<()>)
                + Send
                + Sync
                + 'static,
        >,
    >,
) -> tokio::task::JoinHandle<()> {
    use crate::request_handler::{request_actor, Stactor, StoreRequest};


    tokio::spawn(async move {
        let (js, mut joins) = mpsc::channel(512);
        let mut state = SupervisorState::start_children(js, child_starters);

        let reason;
        'outer: loop {
            tokio::select! {
                sys_msg = rs.recv() => {
                    match sys_msg {
                        Some(SystemMessage::Shutdown) => {
                            reason = ShutdownReason::Shutdown;
                            state.shutdown_children(joins).await;
                            break 'outer;
                        },
                        _ => {}
                    }
                },
                child_res = joins.recv() => {
                    match child_res {
                        Some((_, Ok(()))) => { /* Child died sucessfully */ },
                        Some((i, Err(_))) => {
                            // TODO: Add restart limit
                            let _ = state.restart(i);
                        },
                        _ => {}
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gensym::gensym;
    use crate::mailbox::*;
    use crate::request_handler::{request_actor, Stactor, StoreRequest};
    use std::hash::{Hash, Hasher};

    #[derive(Copy, Clone, PartialEq, Eq, Debug)]
    enum BadKey {
        Good(i32),
        Bad,
    }

    impl Hash for BadKey {
        fn hash<H: Hasher>(&self, state: &mut H) {
            match self {
                Self::Good(i) => i.hash(state),
                Self::Bad => panic!("bad!"),
            }
        }
    }

    #[tokio::test]
    async fn stactor_supervisor_simple_test() {
        let name = gensym();
        let (rs, rr) = mpsc::channel::<SystemMessage>(512);
        let h;
        {
            let name = name.clone();
            h = supervise_single(rr, move || {
                let (mb, h) = request_actor(Stactor::<BadKey, i32>::new());
                let sys = mb.sys.clone();
                mb.register(name.clone());
                (sys, h)
            });
        }
        let mut mb = NamedMailbox::new(name);

        while mb.resolve().is_err() {
            tokio::task::yield_now().await;
        }

        // Set a valid key
        let (rs, rr) = oneshot::channel();
        mb.send((rs, StoreRequest::Set(BadKey::Good(0), 0xffi32)))
            .await
            .unwrap();
        assert_eq!(Ok(None), rr.await);

        // Make sure it's actually set
        let (rs, rr) = oneshot::channel();
        mb.send((rs, StoreRequest::Get(BadKey::Good(0))))
            .await
            .unwrap();
        assert_eq!(Ok(Some(0xffi32)), rr.await);

        // Crash the stactor actor, then the supervisor should restart it
        // and the state should be clear again.
        let (rs, rr) = oneshot::channel();
        mb.send((rs, StoreRequest::Set(BadKey::Bad, 0xffi32)))
            .await
            .unwrap();
        assert!(matches!(rr.await, Err(_)));

        // Set the key again and make sure that there was no prior key
        let (rs, rr) = oneshot::channel();
        mb.send((rs, StoreRequest::Set(BadKey::Good(0), 0xffi32)))
            .await
            .unwrap();
        assert_eq!(Ok(None), rr.await);
    }

    #[tokio::test]
    async fn stactor_supervisor_simple_multi_test() {
        let name0 = gensym();
        let name1 = gensym();
        let (rs, rr) = mpsc::channel::<SystemMessage>(512);
        let h;
        {
            let starter = move |name| {
                let (mb, h) = request_actor(Stactor::<BadKey, i32>::new());
                let sys = mb.sys.clone();
                mb.register(name);
                (sys, h)
            };
            let name0 = name0.clone();
            let name1 = name1.clone();
            h = supervise_multi(
                rr,
                vec![
                    Box::new(move || starter(name0.clone())),
                    Box::new(move || starter(name1.clone())),
                ],
            );
        }
        for (i, name) in [name0, name1].into_iter().enumerate() {
            let i = i as i32;
            let mut mb = NamedMailbox::new(name.clone());

            while mb.resolve().is_err() {
                tokio::task::yield_now().await;
            }

            // Set a valid key
            let (rs, rr) = oneshot::channel();
            mb.send((rs, StoreRequest::Set(BadKey::Good(i), 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);

            // Make sure it's actually set
            let (rs, rr) = oneshot::channel();
            mb.send((rs, StoreRequest::Get(BadKey::Good(i))))
                .await
                .unwrap();
            assert_eq!(Ok(Some(0xffi32)), rr.await);

            // Crash the stactor actor, then the supervisor should restart it
            // and the state should be clear again.
            let (rs, rr) = oneshot::channel();
            mb.send((rs, StoreRequest::Set(BadKey::Bad, 0xffi32)))
                .await
                .unwrap();
            assert!(matches!(rr.await, Err(_)));

            // Set the key again and make sure that there was no prior key
            let (rs, rr) = oneshot::channel();
            mb.send((rs, StoreRequest::Set(BadKey::Good(i), 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);
        }
    }

    #[tokio::test]
    async fn nested_supervisor_test() {
        let name = gensym();
        let (rs, rr) = mpsc::channel::<SystemMessage>(512);
        let h;
        {
            let name = name.clone();
            let subsuper = move || {
                let worker = move |name: String| {
                    let (mb, h) = request_actor(Stactor::<i32, i32>::new());
                    let sys = mb.sys.clone();
                    mb.register(name.clone());
                    (sys, h)
                };
                let name = name.clone();
                let (rs, rr) = mpsc::channel::<SystemMessage>(512);
                let h = supervise_multi(rr, vec![Box::new(move || worker(name.clone()))]);
                (rs, h)
            };
            h = supervise_multi(rr, vec![Box::new(subsuper)]);
        }

        let mut mb = NamedMailbox::new(name.clone());

        while matches!(mb.resolve(), Err(ResolutionError::NameNotFound)) {
            tokio::task::yield_now().await;
        }

        let (rs, rr) = oneshot::channel::<Option<i32>>();

        mb.send((rs, StoreRequest::Set(0i32, 3i32))).await.unwrap();

        assert_eq!(Ok(None), rr.await);
    }

    #[tokio::test]
    async fn stactor_supervisor_shutdown_test() {
        let name0 = gensym();
        let name1 = gensym();
        let (rs, rr) = mpsc::channel::<SystemMessage>(512);
        let h;
        {
            let starter = move |name| {
                let (mb, h) = request_actor(Stactor::<i32, i32>::new());
                let sys = mb.sys.clone();
                mb.register(name);
                (sys, h)
            };
            let name0 = name0.clone();
            let name1 = name1.clone();
            h = supervise_multi(
                rr,
                vec![
                    Box::new(move || starter(name0.clone())),
                    Box::new(move || starter(name1.clone())),
                ],
            );
        }

        let mut mb0 = NamedMailbox::new(name0);
        let mut mb1 = NamedMailbox::new(name1);

        while matches!(mb0.resolve(), Err(ResolutionError::NameNotFound)) ||
              matches!(mb1.resolve(), Err(ResolutionError::NameNotFound)) {

            tokio::task::yield_now().await;
        }

        {
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb0.send((rs, StoreRequest::Set(0i32, 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb1.send((rs, StoreRequest::Set(0i32, 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);
        }

        rs.send(SystemMessage::Shutdown).await.unwrap();
        h.await;

        {
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            assert!(matches!(mb0.send((rs, StoreRequest::Set(0i32, 1i32))).await, Err(_)));
        }
    }
}
