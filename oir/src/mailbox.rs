use std::any::Any;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::actor::SystemMessage;

use tokio;
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum MailboxSendError<T> {
    ResolutionError(ResolutionError),
    SendError(mpsc::error::SendError<T>),
}

impl<T> From<ResolutionError> for MailboxSendError<T> {
    fn from(err: ResolutionError) -> Self {
        MailboxSendError::ResolutionError(err)
    }
}

impl<T> From<mpsc::error::SendError<T>> for MailboxSendError<T> {
    fn from(err: mpsc::error::SendError<T>) -> Self {
        MailboxSendError::SendError(err)
    }
}

pub struct Mailbox<T> {
    senders: Option<(mpsc::Sender<SystemMessage>, mpsc::Sender<T>)>,
    name: String,
}

impl<T> Mailbox<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(name: String) -> Mailbox<T> {
        Mailbox {
            senders: None,
            name,
        }
    }

    pub fn resolve(&mut self) -> Result<(), ResolutionError> {
        if self
            .senders
            .as_ref()
            .map_or(true, |(_, ref s)| s.is_closed())
        {
            ActorDirectory::resolve(&self.name).map(|senders| {
                self.senders = Some(senders);
            })
        } else {
            Ok(())
        }
    }

    pub async fn send(&mut self, msg: T) -> Result<(), MailboxSendError<T>> {
        self.resolve()?;

        let msg_sender = if let Some((_, ref x)) = self.senders {
            x
        } else {
            // self.resolve() will have returned an error, if senders couldn't be resolved,
            // ie. they would have been None.
            unreachable!()
        };

        Ok(msg_sender.send(msg).await?)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ResolutionError {
    NameNotFound,
    WrongType,
}

pub struct ActorDirectory {
    map: HashMap<String, Box<dyn Any + Sync + Send + 'static>>,
}

lazy_static! {
    static ref ACTOR_DIRECTORY: RwLock<ActorDirectory> = RwLock::new(ActorDirectory {
        map: HashMap::new()
    });
}

impl ActorDirectory {
    fn resolve_name<T>(
        &self,
        name: &str,
    ) -> Result<(mpsc::Sender<SystemMessage>, mpsc::Sender<T>), ResolutionError>
    where
        T: Send + Sync + 'static,
    {
        self.map
            .get(name)
            .map_or(Err(ResolutionError::NameNotFound), |res| {
                res.downcast_ref::<(mpsc::Sender<SystemMessage>, mpsc::Sender<T>)>()
                    .map_or(Err(ResolutionError::WrongType), |senders| {
                        Ok(senders.clone())
                    })
            })
    }

    fn resolve<T>(
        name: &str,
    ) -> Result<(mpsc::Sender<SystemMessage>, mpsc::Sender<T>), ResolutionError>
    where
        T: Send + Sync + 'static,
    {
        ACTOR_DIRECTORY.read().unwrap().resolve_name(name)
    }

    fn register<T>(name: String, senders: (mpsc::Sender<SystemMessage>, mpsc::Sender<T>))
    where
        T: Send + Sync + 'static,
    {
        ACTOR_DIRECTORY
            .write()
            .unwrap()
            .map
            .insert(name, Box::new(senders));
    }
}

#[cfg(test)]
mod tests {
    use rand;
    use super::*;

    fn gensym() -> String {
       format!("${:016x}", rand::random::<u64>())
    }

    #[test]
    fn register_and_resolve() {
        let (ss, _) = mpsc::channel::<SystemMessage>(1);
        let (sm, _) = mpsc::channel::<i32>(1);
        let name = gensym();
        ActorDirectory::register(name.clone(), (ss, sm));
        ActorDirectory::resolve::<i32>(&name).unwrap();
    }

    #[test]
    fn resolve_not_found() {
        assert_eq!(
            ResolutionError::NameNotFound,
            ActorDirectory::resolve::<i32>(&gensym()).unwrap_err()
        );
    }

    #[test]
    fn resolve_wrong_type() {
        let (ss, _) = mpsc::channel::<SystemMessage>(1);
        let (sm, _) = mpsc::channel::<i32>(1);
        let name = gensym();
        ActorDirectory::register(name.clone(), (ss, sm));

        assert_eq!(
            ResolutionError::WrongType,
            ActorDirectory::resolve::<i64>(&name).unwrap_err()
        );
    }

    #[tokio::test]
    async fn mailbox_send() {
        let (ss, _) = mpsc::channel::<SystemMessage>(2);
        let (sm, mut rm) = mpsc::channel::<i32>(2);
        let name = gensym();
        ActorDirectory::register(name.clone(), (ss, sm));

        let mut mailbox = Mailbox::new(name);

        mailbox.send(1).await.unwrap();
        mailbox.send(2).await.unwrap();

        assert_eq!(1, rm.recv().await.unwrap());
        assert_eq!(2, rm.recv().await.unwrap());
    }

    #[tokio::test]
    async fn mailbox_send_to_unregisted() {
        let mut mailbox = Mailbox::new(gensym());
        assert!(matches!(
            mailbox.send(1i32).await,
            Err(MailboxSendError::ResolutionError(
                ResolutionError::NameNotFound
            ))
        ));
    }

    #[tokio::test]
    async fn mailbox_send_to_closed() {
        let (ss, _)  = mpsc::channel::<SystemMessage>(2);
        let (sm, mut rm) = mpsc::channel::<i32>(2);
        let name = gensym();
        ActorDirectory::register(name.clone(), (ss, sm));

        let mut mailbox = Mailbox::new(name);

        mailbox.send(1).await.unwrap();
        assert_eq!(1, rm.recv().await.unwrap());

        rm.close();
        assert!(matches!(
            mailbox.send(2).await,
            Err(MailboxSendError::SendError(
                mpsc::error::SendError(2)
            ))
        ))
    }

    #[tokio::test]
    async fn mailbox_send_to_reregistered() {
        let (ss1, _)  = mpsc::channel::<SystemMessage>(2);
        let (sm1, mut rm1) = mpsc::channel::<i32>(2);
        let name = gensym();
        ActorDirectory::register(name.clone(), (ss1, sm1));

        let mut mailbox = Mailbox::new(name.clone());

        mailbox.send(1).await.unwrap();
        assert_eq!(1, rm1.recv().await.unwrap());
        rm1.close();

        let (ss2, _)  = mpsc::channel::<SystemMessage>(2);
        let (sm2, mut rm2) = mpsc::channel::<i32>(2);
        ActorDirectory::register(name, (ss2, sm2));

        mailbox.send(2).await.unwrap();
        assert_eq!(2, rm2.recv().await.unwrap());
    }

    #[tokio::test]
    async fn mailbox_send_to_reregistered_and_retyped() {
        let (ss1, _)  = mpsc::channel::<SystemMessage>(2);
        let (sm1, mut rm1) = mpsc::channel::<i32>(2);
        let name = gensym();
        ActorDirectory::register(name.clone(), (ss1, sm1));

        let mut mailbox = Mailbox::new(name.clone());

        mailbox.send(1).await.unwrap();
        assert_eq!(1, rm1.recv().await.unwrap());
        rm1.close();

        let (ss2, _)  = mpsc::channel::<SystemMessage>(2);
        let (sm2, mut rm2) = mpsc::channel::<i64>(2);
        ActorDirectory::register(name, (ss2, sm2));

        assert!(matches!(
            mailbox.send(2).await,
            Err(MailboxSendError::ResolutionError(
                ResolutionError::WrongType
            ))
        ))
    }
}
