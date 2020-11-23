use tokio;
use tokio::sync::mpsc;

use async_trait::async_trait;

// use std::future::Future;

type ErrorBox = Box<dyn std::error::Error>;

// #[derive(Copy, Clone, Debug, PartialEq, Eq)]
// enum ActorStatus {
//     Running,
//     Stopped
// }

// trait Message: Send {}

// struct Address;

// trait Actor {
//     type InMessage;
//     type OutMessage;

//     fn start(&mut self, parent: Option<Address>);

//     fn status(&self) -> ActorStatus;

//     fn on_message(&mut self, msg: Self::InMessage);

//     fn shutdown(&mut self);
// }

// struct BasicActor {
//     parent: Option<Address>
// }

// impl Actor for BasicActor {
//     type InMessage = ();
//     type OutMessage = ();

//     fn start(&mut self, parent: Option<Address>) {
//         self.parent = parent;
//         println!("Ohoy!");
//     }

//     fn status(&self) -> ActorStatus {
//         ActorStatus::Stopped
//     }

//     fn on_message(&mut self, msg: ()) {}

//     fn shutdown(&mut self) {
//     }
// }

// async fn spawn_actor<A: Actor>(act: A) -> tokio::Result<()> {
//     tokio::spawn(move || {
//         act.start(None);
//         while act.status() == ActorStatus::Running {

//         }
//         act.shutdown();
//     })?;
//     Ok(())
// }

#[derive(Debug)]
enum ShutdownReason {
    Shutdown,
    Crashed,
}

#[derive(Debug)]
enum SystemMessage {
    Shutdown,
    Stopped(ShutdownReason),
    Link(mpsc::Sender<SystemMessage>),
}

#[async_trait]
trait MessageHandler<T>: Send + 'static
where
    T: Send + 'static,
{
    async fn on_message(&mut self, msg: T) -> Result<(), ErrorBox>;
}

struct PingActor {
    subject: Option<mpsc::Sender<PingMessage>>,
}

#[async_trait]
impl MessageHandler<PingMessage> for PingActor {
    async fn on_message(&mut self, msg: PingMessage) -> Result<(), ErrorBox> {
        match msg {
            PingMessage::Setup(subj) => {
                self.subject = Some(subj);
            }
            PingMessage::Ping => {
                if let Some(ref mut subj) = self.subject {
                    subj.send(PingMessage::Pong).await?;
                }
            }
            PingMessage::Pong => {
                println!("Pong!");
                if let Some(ref mut subj) = self.subject {
                    subj.send(PingMessage::Ping).await?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
enum PingMessage {
    Setup(mpsc::Sender<PingMessage>),
    Ping,
    Pong,
}

fn message_actor<T, M>(
    mut actor: M,
    mut rs: mpsc::Receiver<SystemMessage>,
    mut rp: mpsc::Receiver<T>,
) -> tokio::task::JoinHandle<()>
where
    M: MessageHandler<T> + Send + 'static,
    T: Send + 'static,
{
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
                ping_msg = rp.recv() => if let Some(msg) = ping_msg {
                    if actor.on_message(msg).await.is_err() {
                        reason = ShutdownReason::Crashed;
                        break 'outer;
                    }
                }
            }
        }

        if let Some(parent) = parent {
            let _ = parent.send(SystemMessage::Stopped(reason)).await;
        }
    })
}

#[async_trait]
trait RequestHandler<T, U>: Send + 'static
where
    T: Send + 'static,
    U: Send + 'static,
{
    async fn on_request(&mut self, request: T) -> Result<U, ErrorBox>;
}

fn request_actor<T, U, M>(
    mut actor: M,
    mut rs: mpsc::Receiver<SystemMessage>,
    mut rp: mpsc::Receiver<T>,
) -> (mpsc::Receiver<U>, tokio::task::JoinHandle<()>)
where
    T: Send + 'static,
    U: Send + 'static,
    M: RequestHandler<T, U> + Send + 'static,
{
    let (trh, rrh) = mpsc::channel::<U>(512);
    let handle = tokio::spawn(async move {
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
                    if let Some(msg) = request_msg {
                        let result;
                        match actor.on_request(msg).await {
                            Ok(r) => {
                                result = r;
                            },
                            Err(_err) => {
                                reason = ShutdownReason::Crashed;
                                break 'outer;
                            },
                        }

                        if trh.send(result).await.is_err() {
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
    });
    (rrh, handle)
}

#[derive(Debug, Clone, Copy)]
enum StoreRequest<K, V> {
    Get(K),
    Set(K, V),
}

struct Stactor<K, V> {
    store: std::collections::HashMap<K, V>,
}

#[async_trait]
impl<K, V> RequestHandler<StoreRequest<K, V>, Option<V>> for Stactor<K, V>
where
    K: Send + std::cmp::Eq + std::hash::Hash + std::fmt::Debug + 'static,
    V: Send + Clone + std::fmt::Debug + 'static,
{
    async fn on_request(&mut self, request: StoreRequest<K, V>) -> Result<Option<V>, ErrorBox> {
        use std::collections::hash_map::Entry;
        println!("{:?}", request);
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Runtime::new()?;
    let _: Result<(), ErrorBox> = rt.block_on(async {
        let (ts0, rs0) = mpsc::channel::<SystemMessage>(512);
        let (tp0, rp0) = mpsc::channel::<PingMessage>(512);
        let h0 = message_actor(PingActor { subject: None }, rs0, rp0);

        let (ts1, rs1) = mpsc::channel::<SystemMessage>(512);
        let (tp1, rp1) = mpsc::channel::<PingMessage>(512);
        let h1 = message_actor(PingActor { subject: None }, rs1, rp1);

        // Pings
        tp0.send(PingMessage::Setup(tp1.clone())).await?;
        tp1.send(PingMessage::Setup(tp0.clone())).await?;
        tp0.send(PingMessage::Ping).await?;
        tokio::time::sleep(tokio::time::Duration::new(0, 1)).await;

        // Shutdown
        ts0.send(SystemMessage::Shutdown).await?;
        ts1.send(SystemMessage::Shutdown).await?;
        h0.await?;
        h1.await?;
        Ok(())
    });
    rt.block_on(async {
        let (   _ts0, rs0) = mpsc::channel::<SystemMessage>(512);
        let (    tp0, rp0) = mpsc::channel::<StoreRequest<i32, i32>>(1024);
        let (mut rsp, _h0) = request_actor(
            Stactor {
                store: std::collections::HashMap::new(),
            },
            rs0,
            rp0,
        );

        let mut vec = Vec::new();

        for i in 0..1024 {
            let tp0 = tp0.clone();
            vec.push(tokio::spawn(
                async move { let _ = tp0.send(StoreRequest::Set(i, i)).await; },
            ));
        }

        for h in vec.drain(..) {
            h.await?;
        }

        for i in 0..1024 {
            tp0.send(StoreRequest::Get(i)).await?;
            let result = rsp.recv().await;
            assert_eq!(Some(Some(i)), result);
        }
        Ok(())
    })
}
