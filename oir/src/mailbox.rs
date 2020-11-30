use std::any::Any;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::actor::SystemMessage;

use tokio;
use tokio::sync::mpsc;

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
        if self.senders.is_none() {
            ActorDirectory::resolve(&self.name).map(|senders| {
                self.senders = Some(senders);
            })
        } else {
            Ok(())
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

    #[test]
    fn register_and_resolve() {
        let (ss, _) = mpsc::channel::<SystemMessage>(1);
        let (sm, _) = mpsc::channel::<i32>(1);
        ActorDirectory::register("name1".to_owned(), (ss, sm));
        ActorDirectory::resolve::<i32>("name1").unwrap();
    }

    #[test]
    fn resolve_not_found() {
        assert_eq!(
            ResolutionError::NameNotFound,
            ActorDirectory::resolve::<i32>("name2").unwrap_err()
        );
    }

    #[test]
    fn resolve_wrong_type() {
        let (ss, _) = mpsc::channel::<SystemMessage>(1);
        let (sm, _) = mpsc::channel::<i32>(1);
        ActorDirectory::register("name3".to_owned(), (ss, sm));

        assert_eq!(
            ResolutionError::WrongType,
            ActorDirectory::resolve::<i64>("name3").unwrap_err()
        );
    }
}
