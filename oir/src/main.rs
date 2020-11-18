use tokio;
use tokio::sync::mpsc;

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
enum SystemMessage {
    Shutdown
}

#[derive(Debug)]
enum PingMessage {
    Setup(mpsc::Sender<PingMessage>),
    Ping,
    Pong
}


fn ping_actor(mut rs: mpsc::Receiver<SystemMessage>, mut rp: mpsc::Receiver<PingMessage>) -> tokio::task::JoinHandle<()> {
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
                                    subj.send(PingMessage::Pong).await.unwrap();
                                }
                            },
                            PingMessage::Pong => {
                                println!("Pong!");
                                if let Some(ref mut subj) = subject {
                                    subj.send(PingMessage::Ping).await.unwrap();
                                }
                            }
                        }
                    }
                }
            }
        }
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let (ts0, rs0) = mpsc::channel::<SystemMessage>(512);
        let (tp0, rp0) = mpsc::channel::<PingMessage>(512);
        let h0 = ping_actor(rs0, rp0);

        let (ts1, rs1) = mpsc::channel::<SystemMessage>(512);
        let (tp1, rp1) = mpsc::channel::<PingMessage>(512);
        let h1 = ping_actor(rs1, rp1);

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
    })
}
