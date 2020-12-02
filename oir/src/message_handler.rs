use crate::actor::ErrorBox;
use crate::actor::ShutdownReason;
use crate::actor::SystemMessage;

use tokio;
use tokio::sync::mpsc;

use async_trait::async_trait;

use crate::mailbox::{Mailbox, UnnamedMailbox};

#[async_trait]
pub trait MessageHandler<T>: Send + 'static
where
    T: Send + Sync + 'static,
{
    async fn on_message(&mut self, msg: T) -> Result<(), ErrorBox>;
}

pub struct PingActor {
    pub subject: Option<UnnamedMailbox<PingMessage>>,
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
pub enum PingMessage {
    Setup(UnnamedMailbox<PingMessage>),
    Ping,
    Pong,
}

pub fn message_actor<T, M>(mut actor: M) -> (UnnamedMailbox<T>, tokio::task::JoinHandle<()>)
where
    M: MessageHandler<T> + Send + 'static,
    T: Send + Sync + 'static,
{
    let (ss, mut rs) = mpsc::channel::<SystemMessage>(512);
    let (sp, mut rp) = mpsc::channel::<T>(512);

    (
        UnnamedMailbox::new(ss, sp),
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
                let _ = parent.send(reason).await;
            }
        }),
    )
}
