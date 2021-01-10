use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::sync::RwLock;
use std::mem;

use crate::actor::SystemMessage;

use async_trait::async_trait;
use tokio;
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum MailboxSendError<T> {
    ResolutionError(ResolutionError),
    SendError(mpsc::error::SendError<T>),
    TypeError,
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
                TypeError => "message type is invalid for this mailbox"
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

    fn is_closed(&self) -> bool;
}

#[derive(Debug)]
pub struct NamedMailbox<T> {
    pub senders: Option<(mpsc::Sender<SystemMessage>, mpsc::Sender<T>)>,
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

    fn is_closed(&self) -> bool {
        if let Some((a, b)) = &self.senders {
            a.is_closed() || b.is_closed()
        } else {
            true
        }
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

    fn is_closed(&self) -> bool {
        self.sys.is_closed() || self.msg.is_closed()
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


#[derive(Debug)]
pub struct DynamicMailbox {
    type_id: TypeId,
    raw_sender: *mut u8,
    dropper: fn (*mut u8),
    cloner: fn (*mut u8) -> *mut u8,
    sys: mpsc::Sender<SystemMessage>,
}

unsafe impl Send for DynamicMailbox {}
unsafe impl Sync for DynamicMailbox {}

impl DynamicMailbox {
    pub fn new<T: Send + Sync + 'static>(sys: mpsc::Sender<SystemMessage>, msg: mpsc::Sender<T>) -> DynamicMailbox {
        fn drop_it<U>(raw: *mut u8) {
            unsafe {
                let _ = Box::from_raw(mem::transmute::<*mut u8, *mut mpsc::Sender<U>>(raw));
            }
        }

        fn clone_it<U>(src: *mut u8) -> *mut u8 {
            unsafe {
                let b_src = Box::from_raw(mem::transmute::<*mut u8, *mut mpsc::Sender<U>>(src));
                let b_dst = b_src.clone();
                // Forget the original box because it's still actually owned by the
                // dynamic mailbox and should not be dropped here
                mem::forget(b_src);
                mem::transmute(Box::into_raw(b_dst))
            }
        }

        unsafe {
            let raw = Box::into_raw(Box::new(msg));

            DynamicMailbox {
                type_id: TypeId::of::<T>(),
                raw_sender: mem::transmute(raw),
                dropper: drop_it::<T>,
                cloner: clone_it::<T>,
                sys
            }
        }
    }

    pub async fn send_system(
        &mut self,
        msg: SystemMessage,
    ) -> Result<(), MailboxSendError<SystemMessage>> {
        Ok(self.sys.send(msg).await?)
    }

    pub fn try_send_system(&mut self, msg: SystemMessage) -> bool {
        self.sys.try_send(msg).is_ok()
    }

    pub fn into_typed<T: Send + Sync + 'static>(mut self) -> Result<UnnamedMailbox<T>, DynamicMailbox> {
        if self.type_id == TypeId::of::<T>() {
            let mut raw_sender = std::ptr::null_mut();
            mem::swap(&mut self.raw_sender, &mut raw_sender);

            let mb: Box<mpsc::Sender<T>>;
            unsafe {
                mb = Box::from_raw(mem::transmute::<_, *mut mpsc::Sender<T>>(raw_sender));
            }
            return Ok(UnnamedMailbox::new(self.sys.clone(), *mb));
        }

        Err(self)
    }

    pub fn is_closed(&self) -> bool {
        // NOTE: We don't check msg because we don't know its actual type here
        self.sys.is_closed()
    }
}

impl<T> From<UnnamedMailbox<T>> for DynamicMailbox
where T: Send + Sync + 'static {
    fn from(x: UnnamedMailbox<T>) -> Self {
        DynamicMailbox::new(x.sys, x.msg)
    }
}

impl Drop for DynamicMailbox {
    fn drop(&mut self) {
        if self.raw_sender.is_null() {
            return;
        }
        // Drop the allocation
        unsafe {
            (self.dropper)(self.raw_sender);
        }
        self.raw_sender = std::ptr::null_mut();
    }
}

impl Clone for DynamicMailbox {
    fn clone(&self) -> Self {
        DynamicMailbox {
            sys: self.sys.clone(),
            raw_sender: (self.cloner)(self.raw_sender),
            type_id: self.type_id,
            cloner: self.cloner,
            dropper: self.dropper,
        }
    }
}


// #[async_trait]
// impl<T> Mailbox<T> for DynamicMailbox
// where T: Clone + Sync + Send + 'static {

//     async fn send(&mut self, msg: T) -> Result<(), MailboxSendError<T>> {
//         if let Some(mb) = self.msg.downcast_mut::<mpsc::Sender<T>>() {
//             Ok(mb.send(msg).await?)
//         } else {
//             Err(MailboxSendError::TypeError)
//         }
//     }

//     async fn send_system(
//         &mut self,
//         msg: SystemMessage,
//     ) -> Result<(), MailboxSendError<SystemMessage>> {
//         Ok(self.sys.send(msg).await?)
//     }


//     fn is_closed(&self) -> bool {
//         // NOTE: We don't check msg because we don't know its actual type here
//         self.sys.is_closed()
//     }
// }

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ResolutionError {
    NameNotFound,
    WrongType,
}

#[derive(Debug)]
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
        ACTOR_DIRECTORY
            .read()
            .unwrap()
            .resolve_name(name)
            .and_then(|res| {
                if res.1.is_closed() {
                    Err(ResolutionError::NameNotFound)
                } else {
                    Ok(res)
                }
            })
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

    fn register_and_resolve() {
        let (ss, ssr) = mpsc::channel::<SystemMessage>(1);
        let (sm, smr) = mpsc::channel::<i32>(1);
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
            mailbox.send(2i32).await,
            Err(MailboxSendError::ResolutionError(
                ResolutionError::NameNotFound
            ))
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

    #[tokio::test]
    async fn dynamic_mailbox_drop_safe() {
        let (s, mut r) = mpsc::channel::<i32>(2);
        let (ss, sr) = mpsc::channel::<SystemMessage>(2);
        let mb = DynamicMailbox::new(ss, s.clone());
        // Make sure creating a dynamic mailbox doesn't break the channel
        s.send(3).await;
        assert_eq!(r.recv().await, Some(3));
        // Drop the typed channel, and make sure the channel is still open
        // (because of dynamic mailbox)
        drop(s);
        assert_eq!(r.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        drop(mb);
        assert_eq!(r.try_recv(), Err(mpsc::error::TryRecvError::Closed));
    }

    #[tokio::test]
    async fn dynamic_mailbox_into_typed_safe() {
        let (s, mut r) = mpsc::channel::<i32>(2);
        let (ss, sr) = mpsc::channel::<SystemMessage>(2);
        let mb = DynamicMailbox::new(ss, s.clone());
        // Make sure creating a dynamic mailbox doesn't break the channel
        s.send(3).await;
        assert_eq!(r.recv().await, Some(3));
        drop(s);
        assert_eq!(r.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        // Make sure that casting back into a typed channel keeps it alive
        // (and doesn't crash heh)
        let mut v;
        if let Ok(_v) = mb.into_typed::<i32>() {
            v = _v;
        } else {
            panic!("Expect dynamic to typed mailbox conversion to succeed");
        }

        v.send(1).await;
        assert_eq!(r.recv().await, Some(1));
        drop(v);
        assert_eq!(r.try_recv(), Err(mpsc::error::TryRecvError::Closed));
    }

    #[tokio::test]
    async fn dynamic_mailbox_wrong_type() {
        let (s, mut r) = mpsc::channel::<i32>(2);
        let (ss, sr) = mpsc::channel::<SystemMessage>(2);
        let mb = DynamicMailbox::new(ss, s.clone());
        assert!(mb.into_typed::<bool>().is_err());
    }

    #[tokio::test]
    async fn dynamic_mailbox_clone_safe() {
        let (s, mut r) = mpsc::channel::<i32>(2);
        let (ss, sr) = mpsc::channel::<SystemMessage>(2);
        let mb0 = DynamicMailbox::new(ss, s.clone());
        // Make sure creating a dynamic mailbox doesn't break the channel
        s.send(3).await;
        assert_eq!(r.recv().await, Some(3));
        // Drop the typed channel, and make sure the channel is still open
        // (because of dynamic mailbox)
        drop(s);
        assert_eq!(r.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        let mb1 = mb0.clone();
        assert_eq!(r.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        drop(mb0);
        assert_eq!(r.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        drop(mb1);
        assert_eq!(r.try_recv(), Err(mpsc::error::TryRecvError::Closed));
    }
}
