use crate::actor::ErrorBox;
use crate::actor::ShutdownReason;
use crate::actor::SystemMessage;
use crate::mailbox::{UnnamedMailbox, Mailbox};

use tokio;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use async_trait::async_trait;

pub fn supervise_single<F>(mut rs: mpsc::Receiver<SystemMessage>, child_starter: F)
                           -> tokio::task::JoinHandle<()>
where F: Fn() -> (mpsc::Sender<SystemMessage>, tokio::task::JoinHandle<()>) + Send + Sync + 'static {
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

#[cfg(test)]
mod tests {
    use crate::mailbox::*;
    use crate::request_handler::{request_actor, Stactor, StoreRequest};
    use crate::gensym::gensym;
    use std::hash::{Hash, Hasher};
    use super::*;

    #[derive(Copy, Clone, PartialEq, Eq, Debug)]
    enum BadKey {
        Good(i32),
        Bad
    }

    impl Hash for BadKey {
        fn hash<H: Hasher>(&self, state: &mut H) {
            match self {
                Self::Good(i) => i.hash(state),
                Self::Bad => panic!("bad!")
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

        while mb.resolve().is_err() { tokio::task::yield_now().await; }

        // Set a valid key
        let (rs, rr) = oneshot::channel();
        mb.send((rs, StoreRequest::Set(BadKey::Good(0), 0xffi32))).await.unwrap();
        assert_eq!(Ok(None), rr.await);

        // Make sure it's actually set
        let (rs, rr) = oneshot::channel();
        mb.send((rs, StoreRequest::Get(BadKey::Good(0)))).await.unwrap();
        assert_eq!(Ok(Some(0xffi32)), rr.await);

        // Crash the stactor actor, then the supervisor should restart it
        // and the state should be clear again.
        let (rs, rr) = oneshot::channel();
        mb.send((rs, StoreRequest::Set(BadKey::Bad, 0xffi32))).await.unwrap();
        assert!(matches!(rr.await, Err(_)));

        // Set the key again and make sure that there was no prior key
        let (rs, rr) = oneshot::channel();
        mb.send((rs, StoreRequest::Set(BadKey::Good(0), 0xffi32))).await.unwrap();
        assert_eq!(Ok(None), rr.await);
    }
}
