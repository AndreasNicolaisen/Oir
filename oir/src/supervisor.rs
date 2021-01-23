use crate::actor::ErrorBox;
use crate::actor::ShutdownReason;
use crate::actor::SystemMessage;
use crate::anybox::AnyBox;
use crate::mailbox::{DynamicMailbox, Mailbox, UnnamedMailbox};

use tokio;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use async_trait::async_trait;

use std::any::{Any, TypeId};
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

pub trait Actor: Send + 'static {
    type Arg: Clone + Sync + Send + 'static;
    type Message: Send + 'static;

    fn start(
        sup: Option<&SupervisorMailbox>,
        arg: Self::Arg,
    ) -> (UnnamedMailbox<Self::Message>, tokio::task::JoinHandle<()>);
}

#[derive(Clone)]
struct SupervisorSpec {
    strategy: RestartStrategy,
    child_specs: Vec<ChildSpec>,
}

#[derive(Clone)]
pub struct ChildSpec {
    policy: RestartPolicy,
    name: Option<String>,
    registered_globally: bool,
    type_id: TypeId,
    arg: AnyBox,
    starter:
        fn(Option<&SupervisorMailbox>, &ChildSpec) -> (DynamicMailbox, tokio::task::JoinHandle<()>),
}

pub fn child<A: Actor>(r: RestartPolicy, b: <A as Actor>::Arg) -> ChildSpec {
    fn do_start<T: Actor>(
        mb: Option<&SupervisorMailbox>,
        child_spec: &ChildSpec,
    ) -> (DynamicMailbox, tokio::task::JoinHandle<()>) {
        let (mb, h) =
            <T as Actor>::start(mb, child_spec.arg.clone().into_typed::<T::Arg>().unwrap());
        let dmb = mb.clone().into();
        if child_spec.registered_globally {
            if let Some(name) = &child_spec.name {
                mb.register(name.clone());
            } else {
                panic!("Tried to register child globally, without a name");
            }
        }
        (dmb, h)
    }

    ChildSpec {
        policy: r,
        name: None,
        registered_globally: false,
        type_id: TypeId::of::<A>(),
        arg: AnyBox::new(b),
        starter: do_start::<A>,
    }
}

impl ChildSpec {
    pub fn named(mut self, name: String) -> Self {
        self.name = Some(name);
        self.registered_globally = false;
        self
    }

    pub fn globally_named(mut self, name: String) -> Self {
        self.name = Some(name);
        self.registered_globally = true;
        self
    }
}

pub fn supervisor(r: RestartPolicy, s: RestartStrategy, c: Vec<ChildSpec>) -> ChildSpec {
    child::<SupervisorState>(
        r,
        SupervisorSpec {
            strategy: s,
            child_specs: c,
        },
    )
}

// macro_rules! supervisor_tree {
//     { $strat:expr, [ $($policy:ident $actor:ty : $($name:expr)? $arg:tt ),* ] } => {
//         supervise(vec![ $( supervisor_tree!( @child $policy $actor $($name)? | $arg ) ),* ], $strat)
//     };
//     ( @child $policy:ident $actor:ty | ( $arg:expr ) ) => {
//         child::<$actor>($policy, None, $arg)
//     };
//     ( @child $policy:ident $actor:ty | { $($rest:tt)* } ) => {
//         child::<$actor>($policy, None, supervisor_tree!( @spec $($rest)* ) );
//     };
//     ( @spec $strat:expr, [ $($policy:ident $actor:ty : $($name:expr)? $arg:tt ),* ] ) => {
//         SupervisorSpec {
//             strategy: $strat,
//             children: vec![ $(supervisor_tree!( @child $policy $actor $($name)? | $arg )),* ]
//         }
//     };
// }

// supervisor_tree! {
//     AllForOne,
//     [
//         permanent Supervisor: {
//             OneForOne,
//             [
//                 permenent PoolWorker: (),
//                 ...
//             ]
//         },
//         permanent PoolServActor: "WOw" {},
//     ]
// };

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LocalId {
    Named(String),
    Unnamed(usize),
}

pub struct Child {
    mailbox: DynamicMailbox,
    type_id: TypeId,
    local_id: LocalId,
    registered_globally: bool,
}

pub enum SupervisorRequest {
    WhichChildren(oneshot::Sender<Vec<Child>>),
    StartChild(oneshot::Sender<Child>, ChildSpec),
}

pub type SupervisorMailbox = UnnamedMailbox<SupervisorRequest>;

struct ChildState {
    spec: ChildSpec,
    mailbox: Option<DynamicMailbox>,
    local_id: LocalId,
}

struct SupervisorState {
    self_mailbox: SupervisorMailbox,
    shutdown: bool,
    join_sender: mpsc::Sender<(LocalId, Result<(), tokio::task::JoinError>)>,
    join_receiver: mpsc::Receiver<(LocalId, Result<(), tokio::task::JoinError>)>,
    children: Vec<ChildState>,
    restart_strategy: RestartStrategy,
    joins_queue: VecDeque<(LocalId, Result<(), tokio::task::JoinError>)>,
    next_id: usize,
}

impl SupervisorState {
    fn new(
        self_mailbox: SupervisorMailbox,
        child_specs: Vec<ChildSpec>,
        restart_strategy: RestartStrategy,
    ) -> SupervisorState {
        let (js, mut jr) = mpsc::channel(512);
        let (next_id, children) =
            SupervisorState::start_children(child_specs, js.clone(), &self_mailbox);
        let mut state = SupervisorState {
            self_mailbox,
            shutdown: false,
            join_sender: js,
            join_receiver: jr,
            children,
            restart_strategy,
            joins_queue: VecDeque::new(),
            next_id,
        };
        state
    }

    fn start_children(
        child_specs: Vec<ChildSpec>,
        join_sender: mpsc::Sender<(LocalId, Result<(), tokio::task::JoinError>)>,
        mailbox: &SupervisorMailbox,
    ) -> (usize, Vec<ChildState>) {
        let mut id = 0;
        let children = child_specs
            .into_iter()
            .map(|spec| {
                let js = join_sender.clone();
                let (mb, h) = (spec.starter)(Some(mailbox), &spec);
                let local_id = spec.name.as_ref().map_or_else(
                    || {
                        let local_id = LocalId::Unnamed(id);
                        id += 1;
                        local_id
                    },
                    |name| LocalId::Named(name.clone()),
                );
                let li = local_id.clone();
                tokio::spawn(async move {
                    js.send((li, h.await)).await;
                });
                ChildState {
                    spec,
                    mailbox: Some(mb),
                    local_id,
                }
            })
            .collect::<Vec<_>>();
        (id, children)
    }

    async fn shutdown_children(&mut self) {
        for i in 0..self.children.len() {
            let cs = &mut self.children[i];
            if let Some(mb) = &mut cs.mailbox {
                mb.send_system(SystemMessage::Shutdown).await;
            }
        }

        for cs in &self.children {
            while cs.mailbox.as_ref().map_or(false, |s| !s.is_closed()) {
                loop {
                    match self.join_receiver.recv().await {
                        Some((j, jr)) if j == cs.local_id => {
                            break;
                        }
                        Some(x) => {
                            self.joins_queue.push_back(x);
                        }
                        None => {
                            panic!("Join receiver was closed");
                        }
                    }
                }
            }
        }

        for cs in &mut self.children {
            cs.mailbox = None;
        }
    }

    fn start_child(&mut self, li: LocalId) {
        let js = self.join_sender.clone();
        if let Some(cs) = self.children.iter_mut().find(|cs| cs.local_id == li) {
            let (mb, h) = (cs.spec.starter)(Some(&self.self_mailbox), &cs.spec);
            // TODO: Make sure we shutdown the old one if it's not already dead
            tokio::spawn(async move {
                js.send((li, h.await)).await;
            });
            cs.mailbox = Some(mb);
        } else {
            panic!("Could not find child to start")
        }
    }

    async fn handle(&mut self, child_res: Option<(LocalId, Result<(), tokio::task::JoinError>)>) {
        match child_res {
            Some((li, je)) => {
                let abnormal_crash = je.is_err();
                let policy = self
                    .children
                    .iter()
                    .find(|cs| cs.local_id == li)
                    .unwrap()
                    .spec
                    .policy;

                match self.restart_strategy {
                    RestartStrategy::OneForOne => {
                        if policy.should_restart(abnormal_crash) {
                            let _ = self.start_child(li);
                        }
                    }
                    RestartStrategy::OneForAll => {
                        self.shutdown_children().await;

                        for i in 0..self.children.len() {
                            // NOTE: We don't handle the case where a worker crashes
                            // during shutdown, therefore its a normal shutdown.
                            let cs = &self.children[i];
                            if cs.spec.policy.should_restart(false) {
                                let li = cs.local_id.clone();
                                self.start_child(li);
                            }
                        }
                    }
                    RestartStrategy::RestForOne => {
                        let i = self
                            .children
                            .iter()
                            .position(|cs| cs.local_id == li)
                            .unwrap();
                        let mut receive_statuses = vec![false; self.children.len() - (i + 1)];

                        for (i, child) in self.children[i + 1..].iter_mut().enumerate() {
                            if let Some(s) = &mut child.mailbox {
                                let _ = s.send_system(SystemMessage::Shutdown).await;
                            } else {
                                receive_statuses[i] = true;
                            }
                        }

                        while !receive_statuses.iter().all(|&b| b) {
                            if let Some((li, j)) = self.join_receiver.recv().await {
                                let n = self
                                    .children
                                    .iter()
                                    .position(|cs| cs.local_id == li)
                                    .unwrap();
                                if n > i {
                                    receive_statuses[n - (i + 1)] = true;
                                } else {
                                    self.joins_queue.push_back((li, j));
                                }
                            } else {
                                panic!("Joins receiver was closed");
                            }
                        }

                        for j in i..self.children.len() {
                            // NOTE: We don't handle the case where a worker crashes
                            // during shutdown, therefore its a normal shutdown.
                            let cs = &self.children[j];
                            if cs.spec.policy.should_restart(false) {
                                let li = cs.local_id.clone();
                                let _ = self.start_child(li);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    async fn handle_request(&mut self, msg: SupervisorRequest) {
        match msg {
            SupervisorRequest::WhichChildren(sender) => {
                let children = self
                    .children
                    .iter()
                    .filter_map(|cs| {
                        cs.mailbox.as_ref().map(|mb| Child {
                            mailbox: mb.clone(),
                            type_id: cs.spec.type_id,
                            local_id: cs.local_id.clone(),
                            registered_globally: cs.spec.registered_globally,
                        })
                    })
                    .collect::<Vec<_>>();
                sender.send(children);
            }
            SupervisorRequest::StartChild(sender, child_spec) => {
                let li = child_spec.name.as_ref().map_or_else(
                    || {
                        let id = LocalId::Unnamed(self.next_id);
                        self.next_id += 1;
                        id
                    },
                    |n| LocalId::Named(n.clone()),
                );
                let child = ChildState {
                    spec: child_spec,
                    local_id: li.clone(),
                    mailbox: None,
                };
                let idx = self.children.len();
                self.children.push(child);
                self.start_child(li);
                let child = &self.children[idx];
                sender.send(Child {
                    mailbox: child.mailbox.as_ref().unwrap().clone(),
                    type_id: child.spec.type_id,
                    local_id: child.local_id.clone(),
                    registered_globally: child.spec.registered_globally,
                });
            }
        }
    }

    async fn recv_joins(&mut self) -> Option<(LocalId, Result<(), tokio::task::JoinError>)> {
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

        for i in 0..self.children.len() {
            let cs = &mut self.children[i];
            if let Some(s) = &mut cs.mailbox {
                let _ = s.try_send_system(SystemMessage::Shutdown);
            }
        }
    }
}

impl Actor for SupervisorState {
    type Arg = SupervisorSpec;
    type Message = SupervisorRequest;

    fn start(
        sup: Option<&SupervisorMailbox>,
        arg: Self::Arg,
    ) -> (SupervisorMailbox, tokio::task::JoinHandle<()>) {
        let (ss, mut rs) = mpsc::channel(512);
        let (sups, mut supr) = mpsc::channel::<SupervisorRequest>(512);
        let mb = UnnamedMailbox::new(ss.clone(), sups.clone());

        let h = tokio::spawn(async move {
            let mut state = SupervisorState::new(mb, arg.child_specs, arg.strategy);

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
}

pub async fn which_children(sup: &mut SupervisorMailbox) -> Option<Vec<Child>> {
    let (ss, rs) = oneshot::channel();
    sup.send(SupervisorRequest::WhichChildren(ss)).await.ok()?;
    let r = rs.await.ok()?;
    Some(r)
}


pub async fn start_child(sup: &mut SupervisorMailbox, cs: ChildSpec) -> Option<Child> {
    let (ss, rs) = oneshot::channel();
    sup.send(SupervisorRequest::StartChild(ss, cs)).await.ok()?;
    let r = rs.await.ok()?;
    Some(r)
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum RestartStrategy {
    OneForOne,
    OneForAll,
    RestForOne,
}

pub fn supervise(
    restart_strategy: RestartStrategy,
    child_specs: Vec<ChildSpec>,
) -> (
    UnnamedMailbox<SupervisorRequest>,
    tokio::task::JoinHandle<()>,
) {
    SupervisorState::start(
        None,
        SupervisorSpec {
            strategy: restart_strategy,
            child_specs,
        },
    )
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
        let name0 = gensym();
        let name1 = gensym();
        let (rs, h) = {
            let name0 = name0.clone();
            let name1 = name1.clone();
            supervise(
                RestartStrategy::OneForOne,
                vec![
                    child::<Stactor<BadKey, i32>>(RestartPolicy::Transient, ())
                        .globally_named(name0),
                    child::<Stactor<BadKey, i32>>(RestartPolicy::Transient, ())
                        .globally_named(name1),
                ],
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
        let h = {
            let name = name.clone();
            supervise(
                RestartStrategy::OneForOne,
                vec![supervisor(
                    RestartPolicy::Transient,
                    RestartStrategy::OneForOne,
                    vec![child::<Stactor<i32, i32>>(RestartPolicy::Transient, ())
                        .globally_named(name)],
                )],
            )
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
        let (mut rs, h) = {
            let name0 = name0.clone();
            let name1 = name1.clone();
            supervise(
                RestartStrategy::OneForOne,
                vec![
                    child::<Stactor<i32, i32>>(RestartPolicy::Transient, ()).globally_named(name0),
                    child::<Stactor<i32, i32>>(RestartPolicy::Transient, ()).globally_named(name1),
                ],
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
        let (rs, h) = {
            let name0 = name0.clone();
            let name1 = name1.clone();
            supervise(
                RestartStrategy::OneForAll,
                vec![
                    child::<Stactor<BadKey, i32>>(RestartPolicy::Permanent, ())
                        .globally_named(name0),
                    child::<Stactor<BadKey, i32>>(RestartPolicy::Permanent, ())
                        .globally_named(name1),
                ],
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
        let (rs, h) = {
            let name0 = name0.clone();
            let name1 = name1.clone();
            let name2 = name2.clone();
            supervise(
                RestartStrategy::RestForOne,
                vec![
                    child::<Stactor<BadKey, i32>>(RestartPolicy::Permanent, ())
                        .globally_named(name0),
                    child::<Stactor<BadKey, i32>>(RestartPolicy::Permanent, ())
                        .globally_named(name1),
                    child::<Stactor<BadKey, i32>>(RestartPolicy::Permanent, ())
                        .globally_named(name2),
                ],
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
        let (mut rs, h) = supervise(
            RestartStrategy::OneForOne,
            vec![
                child::<Stactor<BadKey, i32>>(RestartPolicy::Transient, ()),
                child::<Stactor<BadKey, i64>>(RestartPolicy::Transient, ()),
                child::<Stactor<BadKey, bool>>(RestartPolicy::Transient, ()),
            ],
        );

        let mut children = which_children(&mut rs).await.unwrap();

        async fn assert_child_works<T>(child: Child, value: T)
        where
            T: Send + Copy + Eq + std::fmt::Debug + 'static,
        {
            let mut mb = child
                .mailbox
                .into_typed::<(oneshot::Sender<Option<T>>, StoreRequest<BadKey, T>)>()
                .unwrap();

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

    #[tokio::test]
    async fn supervisor_start_child_test() {
        let (mut rs, h) = supervise(RestartStrategy::OneForOne, vec![]);

        let child = start_child(&mut rs, child::<Stactor<i32, i32>>(RestartPolicy::Transient, ())).await.unwrap();
        let mut mb = child
            .mailbox
            .into_typed::<(oneshot::Sender<Option<i32>>, StoreRequest<i32, i32>)>()
            .unwrap();

        let (rs, rr) = oneshot::channel();
        mb.send((rs, StoreRequest::Set(3, 1))).await.unwrap();
        assert_eq!(Ok(None), rr.await);

        let (rs, rr) = oneshot::channel();
        mb.send((rs, StoreRequest::Get(3))).await.unwrap();
        assert_eq!(Ok(Some(1)), rr.await);
    }
        let (crs, crr) = oneshot::channel();
        let request = SupervisorRequest::StartChild(
            crs,
            child::<Stactor<i32, i32>>(RestartPolicy::Transient, ()),
        );
        rs.send(request).await;
        let child = crr.await.unwrap();
        let mut mb = child
            .mailbox
            .into_typed::<(oneshot::Sender<Option<i32>>, StoreRequest<i32, i32>)>()
            .unwrap();

        let (rs, rr) = oneshot::channel();
        mb.send((rs, StoreRequest::Set(3, 1))).await.unwrap();
        assert_eq!(Ok(None), rr.await);

        let (rs, rr) = oneshot::channel();
        mb.send((rs, StoreRequest::Get(3))).await.unwrap();
        assert_eq!(Ok(Some(1)), rr.await);
    }
}
