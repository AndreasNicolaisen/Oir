use std::any::Any;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::sync::RwLock;

use crate::actor::SystemMessage;

use async_trait::async_trait;
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

impl<T> fmt::Display for MailboxSendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MailboxSendError::*;
        write!(
            f,
            "Failed to send message: {}",
            match self {
                ResolutionError(_) => "could not resolve name",
                SendError(_) => "channel closed",
            }
        )
    }
}

impl<T> error::Error for MailboxSendError<T> where T: fmt::Debug {}

#[async_trait]
pub trait Mailbox<T>
where
    T: Send + Sync + 'static,
{
    async fn send(&mut self, msg: T) -> Result<(), MailboxSendError<T>>;

    async fn send_system(
        &mut self,
        msg: SystemMessage,
    ) -> Result<(), MailboxSendError<SystemMessage>>;

    async fn shutdown(&mut self) -> Result<(), MailboxSendError<SystemMessage>> {
        self.send_system(SystemMessage::Shutdown).await
    }
}

#[derive(Debug)]
pub struct NamedMailbox<T> {
    senders: Option<(mpsc::Sender<SystemMessage>, mpsc::Sender<T>)>,
    name: String,
}

impl<T> NamedMailbox<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(name: String) -> NamedMailbox<T> {
        NamedMailbox {
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
}

#[async_trait]
impl<T> Mailbox<T> for NamedMailbox<T>
where
    T: Send + Sync + 'static,
{
    async fn send(&mut self, msg: T) -> Result<(), MailboxSendError<T>> {
        self.resolve()?;

        let msg_s = if let Some((_, ref y)) = self.senders {
            y
        } else {
            // self.resolve() will have returned an error, if senders couldn't be resolved,
            // ie. they would have been None.
            unreachable!()
        };

        Ok(msg_s.send(msg).await?)
    }

    async fn send_system(
        &mut self,
        msg: SystemMessage,
    ) -> Result<(), MailboxSendError<SystemMessage>> {
        self.resolve()?;

        let sys_s = if let Some((ref x, _)) = self.senders {
            x
        } else {
            // self.resolve() will have returned an error, if senders couldn't be resolved,
            // ie. they would have been None.
            unreachable!()
        };

        Ok(sys_s.send(msg).await?)
    }
}

impl<T> Clone for NamedMailbox<T>
where
    T: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        NamedMailbox {
            senders: self.senders.clone(),
            name: self.name.clone(),
        }
    }
}

#[derive(Debug)]
pub struct UnnamedMailbox<T> {
    pub sys: mpsc::Sender<SystemMessage>,
    pub msg: mpsc::Sender<T>,
}

impl<T> UnnamedMailbox<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(sys: mpsc::Sender<SystemMessage>, msg: mpsc::Sender<T>) -> UnnamedMailbox<T> {
        UnnamedMailbox { sys, msg }
    }

    pub fn register(self, name: String) -> NamedMailbox<T> {
        ActorDirectory::register(name.clone(), (self.sys.clone(), self.msg.clone()));
        NamedMailbox {
            name,
            senders: Some((self.sys, self.msg)),
        }
    }
}

#[async_trait]
impl<T> Mailbox<T> for UnnamedMailbox<T>
where
    T: Send + Sync + 'static,
{
    async fn send(&mut self, msg: T) -> Result<(), MailboxSendError<T>> {
        Ok(self.msg.send(msg).await?)
    }

    async fn send_system(
        &mut self,
        msg: SystemMessage,
    ) -> Result<(), MailboxSendError<SystemMessage>> {
        Ok(self.sys.send(msg).await?)
    }
}

impl<T> Clone for UnnamedMailbox<T>
where
    T: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        UnnamedMailbox {
            sys: self.sys.clone(),
            msg: self.msg.clone(),
        }
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
    use super::*;
    use crate::gensym::gensym;

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

        let mut mailbox = NamedMailbox::new(name);

        mailbox.send(1).await.unwrap();
        mailbox.send(2).await.unwrap();

        assert_eq!(1, rm.recv().await.unwrap());
        assert_eq!(2, rm.recv().await.unwrap());
    }

    #[tokio::test]
    async fn mailbox_send_to_unregisted() {
        let mut mailbox = NamedMailbox::new(gensym());
        assert!(matches!(
            mailbox.send(1i32).await,
            Err(MailboxSendError::ResolutionError(
                ResolutionError::NameNotFound
            ))
        ));
    }

    #[tokio::test]
    async fn mailbox_send_to_closed() {
        let (ss, _) = mpsc::channel::<SystemMessage>(2);
        let (sm, mut rm) = mpsc::channel::<i32>(2);
        let name = gensym();
        ActorDirectory::register(name.clone(), (ss, sm));

        let mut mailbox = NamedMailbox::new(name);

        mailbox.send(1).await.unwrap();
        assert_eq!(1, rm.recv().await.unwrap());

        rm.close();
        assert!(matches!(
            mailbox.send(2).await,
            Err(MailboxSendError::SendError(mpsc::error::SendError(2)))
        ))
    }

    #[tokio::test]
    async fn mailbox_send_to_reregistered() {
        let (ss1, _) = mpsc::channel::<SystemMessage>(2);
        let (sm1, mut rm1) = mpsc::channel::<i32>(2);
        let name = gensym();
        ActorDirectory::register(name.clone(), (ss1, sm1));

        let mut mailbox = NamedMailbox::new(name.clone());

        mailbox.send(1).await.unwrap();
        assert_eq!(1, rm1.recv().await.unwrap());
        rm1.close();

        let (ss2, _) = mpsc::channel::<SystemMessage>(2);
        let (sm2, mut rm2) = mpsc::channel::<i32>(2);
        ActorDirectory::register(name, (ss2, sm2));

        mailbox.send(2).await.unwrap();
        assert_eq!(2, rm2.recv().await.unwrap());
    }

    #[tokio::test]
    async fn mailbox_send_to_reregistered_and_retyped() {
        let (ss1, _) = mpsc::channel::<SystemMessage>(2);
        let (sm1, mut rm1) = mpsc::channel::<i32>(2);
        let name = gensym();
        ActorDirectory::register(name.clone(), (ss1, sm1));

        let mut mailbox = NamedMailbox::new(name.clone());

        mailbox.send(1).await.unwrap();
        assert_eq!(1, rm1.recv().await.unwrap());
        rm1.close();

        let (ss2, _) = mpsc::channel::<SystemMessage>(2);
        let (sm2, mut rm2) = mpsc::channel::<i64>(2);
        ActorDirectory::register(name, (ss2, sm2));

        assert!(matches!(
            mailbox.send(2).await,
            Err(MailboxSendError::ResolutionError(
                ResolutionError::WrongType
            ))
        ))
    }

    #[tokio::test]
    async fn mailbox_register_unnamed() {
        let (ss1, _) = mpsc::channel::<SystemMessage>(2);
        let (sm1, mut rm1) = mpsc::channel::<i32>(2);
        let mut unnamed_mailbox = UnnamedMailbox::new(ss1, sm1);

        let name = gensym();
        let _ = unnamed_mailbox.register(name.clone());

        let mut named_mailbox = NamedMailbox::new(name);
        named_mailbox.send(1).await.unwrap();

        assert_eq!(1, rm1.recv().await.unwrap());
    }
}
