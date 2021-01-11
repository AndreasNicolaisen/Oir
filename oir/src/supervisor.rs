use crate::actor::ErrorBox;
use crate::actor::ShutdownReason;
use crate::actor::SystemMessage;
use crate::mailbox::{Mailbox, UnnamedMailbox, DynamicMailbox};

use tokio;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use async_trait::async_trait;

use std::collections::VecDeque;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum RestartPolicy {
    Permanent,
    Transient,
    Temporary,
}

impl RestartPolicy {
    fn should_restart(&self, abnormal_crash: bool) -> bool {
        if RestartPolicy::Transient == *self {
            abnormal_crash
        } else {
            RestartPolicy::Permanent == *self
        }
    }
}

pub type ChildDefinition = DynamicMailbox;

pub enum SupervisorRequest {
    WhichChildren(oneshot::Sender<Vec<ChildDefinition>>),
}

struct SupervisorState {
    shutdown: bool,
    join_sender: mpsc::Sender<(usize, Result<(), tokio::task::JoinError>)>,
    join_receiver: mpsc::Receiver<(usize, Result<(), tokio::task::JoinError>)>,
    starters: Vec<
        Box<
            dyn Fn() -> (DynamicMailbox, tokio::task::JoinHandle<()>, RestartPolicy)
                + std::marker::Send
                + std::marker::Sync
                + 'static,
        >,
    >,
    system_senders: Vec<Option<DynamicMailbox>>,
    policies: Vec<RestartPolicy>,
    restart_strategy: RestartStrategy,
    joins_queue: VecDeque<(usize, Result<(), tokio::task::JoinError>)>,
}

impl SupervisorState {
    fn new(
        starters: Vec<
            Box<
                dyn Fn() -> (DynamicMailbox, tokio::task::JoinHandle<()>, RestartPolicy)
                    + std::marker::Send
                    + std::marker::Sync
                    + 'static,
            >,
        >,
        restart_strategy: RestartStrategy,
    ) -> SupervisorState {
        let (js, mut jr) = mpsc::channel(512);
        let mut state = SupervisorState {
            shutdown: false,
            join_sender: js,
            join_receiver: jr,
            starters,
            system_senders: Vec::new(),
            policies: Vec::new(),
            restart_strategy,
            joins_queue: VecDeque::new(),
        };
        state.start_children();
        state
    }

    fn start_children(&mut self) {
        assert!(self.system_senders.is_empty());
        let (senders, policies) = self
            .starters
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let js = self.join_sender.clone();
                let (mb, h, rp) = f();
                tokio::spawn(async move {
                    js.send((i, h.await)).await;
                });
                (Some(mb), rp)
            })
            .unzip();
        self.system_senders = senders;
        self.policies = policies;
    }

    async fn shutdown_children(&mut self) {
        for sender in self.system_senders.iter_mut().filter_map(|s| s.as_mut()) {
            let _ = sender.send_system(SystemMessage::Shutdown).await;
        }

        for i in 0..self.system_senders.len() {
            while self.system_senders[i].as_ref().map_or(false, |s| !s.is_closed()) {
                self.receive_from(i).await;
            }
            self.system_senders[i] = None;
        }
    }

    fn start(&mut self, i: usize) {
        assert!(i < self.starters.len());

        let js = self.join_sender.clone();
        let (sys, h, p) = self.starters[i]();
        // TODO: Make sure we shutdown the old one if it's not already dead
        tokio::spawn(async move {
            js.send((i, h.await)).await;
        });
        self.policies[i] = p;
        self.system_senders[i] = Some(sys);
    }

    async fn receive_from(&mut self, i: usize) -> Result<(), tokio::task::JoinError> {
        loop {
            match self.join_receiver.recv().await {
                Some((j, jr)) if j == i => {
                    return jr;
                },
                Some(x) => {
                    self.joins_queue.push_back(x);
                },
                None => {
                    panic!("Join receiver was closed");
                }
            }
        }
    }

    async fn handle(&mut self, child_res: Option<(usize, Result<(), tokio::task::JoinError>)>) {
        match child_res {
            Some((i, je)) => {
                let abnormal_crash = je.is_err();
                let policy = self.policies[i];

                match self.restart_strategy {
                    RestartStrategy::OneForOne => {
                        if policy.should_restart(abnormal_crash) {
                            let _ = self.start(i);
                        }
                    }
                    RestartStrategy::OneForAll => {
                        self.shutdown_children().await;

                        for n in 0..self.starters.len() {
                            // NOTE: We don't handle the case where a worker crashes
                            // during shutdown, therefore its a normal shutdown.
                            if self.policies[n].should_restart(false) {
                                self.start(n);
                            }
                        }
                    }
                    RestartStrategy::RestForOne => {
                        let mut receive_statuses = vec![false; self.system_senders.len() - (i+1)];

                        for (i, sender) in self.system_senders[i+1..].iter_mut().enumerate() {
                            if let Some(s) = sender {
                                let _ = s.send_system(SystemMessage::Shutdown).await;
                            } else {
                                receive_statuses[i] = true;
                            }
                        }

                        while !receive_statuses.iter().all(|&b| b) {
                            if let Some((n, j)) = self.join_receiver.recv().await {
                                if n > i {
                                    receive_statuses[n - (i+1)] = true;
                                } else {
                                    self.joins_queue.push_back((n, j));
                                }
                            } else {
                                panic!("Joins receiver was closed");
                            }
                        }

                        for n in i..self.system_senders.len() {
                            // NOTE: We don't handle the case where a worker crashes
                            // during shutdown, therefore its a normal shutdown.
                            if self.policies[n].should_restart(false) {
                                let _ = self.start(n);
                            }
                        }
                    }
                }
            },
            _ => {}
        }
    }

    async fn handle_request(&self, msg: SupervisorRequest) {
        match msg {
            SupervisorRequest::WhichChildren(sender) => {
                sender.send(self.system_senders.iter().filter_map(|ss| ss.as_ref().cloned()).collect::<Vec<_>>());
            }
        }
    }

    async fn recv_joins(&mut self) -> Option<(usize, Result<(), tokio::task::JoinError>)> {
        if let Some(res) = self.joins_queue.pop_front() {
            Some(res)
        } else {
            self.join_receiver.recv().await
        }
    }
}

impl Drop for SupervisorState {
    fn drop(&mut self) {
        if self.shutdown {
            return;
        }

        for sys in &mut self.system_senders {
            if let Some(s) = sys {
                let _ = s.try_send_system(SystemMessage::Shutdown);
            }
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum RestartStrategy {
    OneForOne,
    OneForAll,
    RestForOne,
}

pub fn supervise(
    child_starters: Vec<
        Box<
            dyn Fn() -> (DynamicMailbox, tokio::task::JoinHandle<()>, RestartPolicy)
                + Send
                + Sync
                + 'static,
        >,
    >,
    restart_strategy: RestartStrategy,
) -> (UnnamedMailbox<SupervisorRequest>, tokio::task::JoinHandle<()>) {
    use crate::request_handler::{request_actor, Stactor, StoreRequest};

    let (ss, mut rs) = mpsc::channel(512);
    let (sups, mut supr) = mpsc::channel::<SupervisorRequest>(512);

    let h = tokio::spawn(async move {
        let mut state = SupervisorState::new(child_starters, restart_strategy);

        let reason;
        'outer: loop {
            tokio::select! {
                sys_msg = rs.recv() => {
                    match sys_msg {
                        Some(SystemMessage::Shutdown) => {
                            reason = ShutdownReason::Shutdown;
                            state.shutdown_children().await;
                            break 'outer;
                        },
                        _ => {}
                    }
                },
                child_res = state.recv_joins() => {
                    state.handle(child_res).await;
                },
                sup_msg = supr.recv() => {
                    if let Some(sup_msg) = sup_msg {
                        state.handle_request(sup_msg).await;
                    } else {
                        break 'outer;
                    }
                }
            }
        }
    });

    (UnnamedMailbox::new(ss, sups), h)
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

    fn starter<K, V>(name: String) -> (DynamicMailbox, tokio::task::JoinHandle<()>, RestartPolicy)
    where
        K: Send + Sync + Clone + std::cmp::Eq + std::hash::Hash + std::fmt::Debug + 'static,
        V: Send + Sync + Clone + std::fmt::Debug + 'static,
    {
        let (mb, h) = request_actor(Stactor::<K, V>::new());
        let dmb = mb.clone().into();
        mb.register(name);
        (dmb, h, RestartPolicy::Transient)
    }

    fn starter_no_name<K, V>() -> (DynamicMailbox, tokio::task::JoinHandle<()>, RestartPolicy)
    where
        K: Send + Sync + Clone + std::cmp::Eq + std::hash::Hash + std::fmt::Debug + 'static,
        V: Send + Sync + Clone + std::fmt::Debug + 'static,
    {
        let (mb, h) = request_actor(Stactor::<K, V>::new());
        let dmb = mb.clone().into();
        (dmb, h, RestartPolicy::Transient)
    }

    fn starterPermanent<K, V>(name: String) -> (DynamicMailbox, tokio::task::JoinHandle<()>, RestartPolicy)
    where
        K: Send + Sync + Clone + std::cmp::Eq + std::hash::Hash + std::fmt::Debug + 'static,
        V: Send + Sync + Clone + std::fmt::Debug + 'static,
    {
        let (mb, h) = request_actor(Stactor::<K, V>::new());
        let dmb = mb.clone().into();
        mb.register(name);
        (dmb, h, RestartPolicy::Permanent)
    }

    #[tokio::test]
    async fn stactor_supervisor_simple_test() {
        let name0 = gensym();
        let name1 = gensym();
        let (rs, h) =
        {
            let name0 = name0.clone();
            let name1 = name1.clone();
            supervise(
                vec![
                    Box::new(move || starter::<BadKey, i32>(name0.clone())),
                    Box::new(move || starter::<BadKey, i32>(name1.clone())),
                ],
                RestartStrategy::OneForOne,
            )
        };
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
        let h =
        {
            let name = name.clone();
            let subsuper = move || {
                // let worker = move |name: String| {
                //     let (mb, h) = request_actor(Stactor::<i32, i32>::new());
                //     let sys = mb.sys.clone();
                //     mb.register(name.clone());
                //     (sys, h)
                // };
                let name = name.clone();
                let (rs, h) = supervise(
                    vec![Box::new(move || starter::<i32, i32>(name.clone()))],
                    RestartStrategy::OneForOne,
                );
                (rs.into(), h, RestartPolicy::Transient,)
            };
            supervise(vec![Box::new(subsuper)], RestartStrategy::OneForOne)
        };

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
        let (mut rs, h) =
        {
            let name0 = name0.clone();
            let name1 = name1.clone();
            supervise(
                vec![
                    Box::new(move || starter::<i32, i32>(name0.clone())),
                    Box::new(move || starter::<i32, i32>(name1.clone())),
                ],
                RestartStrategy::OneForOne,
            )
        };

        let mut mb0 = NamedMailbox::new(name0);
        let mut mb1 = NamedMailbox::new(name1);

        while matches!(mb0.resolve(), Err(ResolutionError::NameNotFound))
            || matches!(mb1.resolve(), Err(ResolutionError::NameNotFound))
        {
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

        rs.send_system(SystemMessage::Shutdown).await.unwrap();
        h.await;

        {
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            assert!(matches!(
                mb0.send((rs, StoreRequest::Set(0i32, 1i32))).await,
                Err(_)
            ));
        }
    }

    #[tokio::test]
    async fn stactor_one_for_all_supervisor_shutdown_test() {
        let name0 = gensym();
        let name1 = gensym();
        let (rs, h) =
        {
            let name0 = name0.clone();
            let name1 = name1.clone();
            supervise(
                vec![
                    Box::new(move || starterPermanent::<BadKey, i32>(name0.clone())),
                    Box::new(move || starterPermanent::<BadKey, i32>(name1.clone())),
                ],
                RestartStrategy::OneForAll,
            )
        };

        let mut mb0 = NamedMailbox::new(name0);
        let mut mb1 = NamedMailbox::new(name1);

        while matches!(mb0.resolve(), Err(ResolutionError::NameNotFound))
            || matches!(mb1.resolve(), Err(ResolutionError::NameNotFound))
        {
            tokio::task::yield_now().await;
        }

        {
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb0.send((rs, StoreRequest::Set(BadKey::Good(0i32), 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb1.send((rs, StoreRequest::Set(BadKey::Good(0i32), 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);
        }

        {
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb0.send((rs, StoreRequest::Set(BadKey::Bad, 0xffi32)))
                .await
                .unwrap();
            assert!(matches!(rr.await, Err(_)));
        }

        while matches!(mb0.resolve(), Err(ResolutionError::NameNotFound))
            || matches!(mb1.resolve(), Err(ResolutionError::NameNotFound))
        {
            tokio::task::yield_now().await;
        }

        {
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb0.send((rs, StoreRequest::Set(BadKey::Good(0i32), 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb1.send((rs, StoreRequest::Set(BadKey::Good(0i32), 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);
        }
    }

    #[tokio::test]
    async fn stactor_rest_for_one_supervisor_shutdown_test() {
        let name0 = gensym();
        let name1 = gensym();
        let name2 = gensym();
        let (rs, h) =
        {
            let name0 = name0.clone();
            let name1 = name1.clone();
            let name2 = name2.clone();
            supervise(
                vec![
                    Box::new(move || starterPermanent::<BadKey, i32>(name0.clone())),
                    Box::new(move || starterPermanent::<BadKey, i32>(name1.clone())),
                    Box::new(move || starterPermanent::<BadKey, i32>(name2.clone())),
                ],
                RestartStrategy::RestForOne,
            )
        };

        let mut mb0 = NamedMailbox::new(name0);
        let mut mb1 = NamedMailbox::new(name1);
        let mut mb2 = NamedMailbox::new(name2);

        while matches!(mb0.resolve(), Err(ResolutionError::NameNotFound))
            || matches!(mb1.resolve(), Err(ResolutionError::NameNotFound))
            || matches!(mb2.resolve(), Err(ResolutionError::NameNotFound))
        {
            tokio::task::yield_now().await;
        }

        {
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb0.send((rs, StoreRequest::Set(BadKey::Good(0i32), 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb1.send((rs, StoreRequest::Set(BadKey::Good(0i32), 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb2.send((rs, StoreRequest::Set(BadKey::Good(0i32), 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);
        }

        {
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb1.send((rs, StoreRequest::Set(BadKey::Bad, 0xffi32)))
                .await
                .unwrap();
            assert!(matches!(rr.await, Err(_)));
        }

        while matches!(mb0.resolve(), Err(ResolutionError::NameNotFound))
            || matches!(mb1.resolve(), Err(ResolutionError::NameNotFound))
            || matches!(mb2.resolve(), Err(ResolutionError::NameNotFound))
        {
            tokio::task::yield_now().await;
        }

        {
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb0.send((rs, StoreRequest::Set(BadKey::Good(0i32), 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(Some(0xffi32)), rr.await);
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb1.send((rs, StoreRequest::Set(BadKey::Good(0i32), 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);
            let (rs, rr) = oneshot::channel::<Option<i32>>();
            mb2.send((rs, StoreRequest::Set(BadKey::Good(0i32), 0xffi32)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);
        }
    }

    #[tokio::test]
    async fn supervisor_which_children_test() {
        let (mut rs, h) =
            supervise(
                vec![
                    Box::new(move || starter_no_name::<BadKey, i32>()),
                    Box::new(move || starter_no_name::<BadKey, i64>()),
                    Box::new(move || starter_no_name::<BadKey, bool>()),
                ],
                RestartStrategy::OneForOne,
            );

        let (crs, crr) = oneshot::channel();
        let request = SupervisorRequest::WhichChildren(crs);
        rs.send(request).await;
        let mut children = crr.await.unwrap();

        async fn assert_child_works<T>(child: ChildDefinition, value: T)
        where T : Send + Sync + Copy + Eq + std::fmt::Debug + 'static
        {
            let mut mb = child.into_typed::<(oneshot::Sender<Option<T>>, StoreRequest<BadKey, T>)>().unwrap();

            // Set a valid key
            let (rs, rr) = oneshot::channel();
            mb.send((rs, StoreRequest::Set(BadKey::Good(1), value)))
                .await
                .unwrap();
            assert_eq!(Ok(None), rr.await);

            // Make sure it's actually set
            let (rs, rr) = oneshot::channel();
            mb.send((rs, StoreRequest::Get(BadKey::Good(1))))
                .await
                .unwrap();
            assert_eq!(Ok(Some(value)), rr.await);
        }

        assert_eq!(children.len(), 3);
        assert_child_works::<bool>(children.pop().unwrap(), true).await;
        assert_child_works::<i64>(children.pop().unwrap(), 123i64).await;
        assert_child_works::<i32>(children.pop().unwrap(), 321i32).await;
    }
}
