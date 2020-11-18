use tokio;
use tokio::sync::mpsc;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ActorStatus {
    Running,
    Stopped
}

trait Message: Send {}

struct Address;

trait Actor {
    type InMessage;
    type OutMessage;

    fn start(&mut self, parent: Option<Address>);

    fn status(&self) -> ActorStatus;

    fn on_message(&mut self, msg: Self::InMessage);

    fn shutdown(&mut self);
}

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

enum SystemMessage {
    Shutdown
}

enum PingMessage {
    Setup(mpsc::Sender<PingMessage>),
    Ping,
    Pong
}


fn ping_actor(rs: mpsc::Receiver<SystemMessage>, rp: mpsc::Receiver<PingMessage>) -> tokio::JoinHandle {
    tokio::spawn(async move {
        let mut subject = None;
        'outer: loop {
            tokio::select! {
                sys_msg = rs.recv() => {
                    if let Some(SystemMessage::Shutdown) = sys_msg {
                        break 'outer;
                    }
                },
                ping_msg = rp.recv() => {
                    if let Some(ping_msg) = ping_msg {
                        match ping_msg {
                            PingMessage::Setup(subj) => {
                                subject = Some(subj);
                            },
                            PingMessage::Ping => {
                                if let Some(ref mut subj) = subject {
                                    subj.send(PingMessage::Pong);
                                }
                            },
                            PingMessage::Pong => {
                                println!("Pong!");
                                if let Some(ref mut subj) = subject {
                                    subj.send(PingMessage::Ping);
                                }
                            }
                        }
                    }
                }
            }
        }
    });
}

#[tokio::main]
async fn main() {
    let (ts0, mut rs0) = mpsc::channel::<SystemMessage>(512);
    let (tp0, mut rp0) = mpsc::channel::<PingMessage>(512);
    let h0 = ping_actor(rs0, rp0);

    let (ts1, mut rs1) = mpsc::channel::<SystemMessage>(512);
    let (tp1, mut rp1) = mpsc::channel::<PingMessage>(512);
    let h1 = ping_actor(rs1, rp1);

    tp0.send(PingMessage::Setup(tp1.clone()));
    tp1.send(PingMessage::Setup(tp0.clone()));
    tp0.send(PingMessage::Ping);
    tokio::time::delay_for(tokio::time::Duration::new(0, 10)).await;
}
