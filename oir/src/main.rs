#![feature(drain_filter)]

#[macro_use]
extern crate lazy_static;

use crate::actor::ErrorBox;

mod actor;
mod anybox;
mod examples;
mod gensym;
mod mailbox;
mod message_handler;
mod request_handler;
mod supervisor;

use crate::message_handler::message_actor;
use crate::message_handler::PingActor;
use crate::message_handler::PingMessage;

use crate::request_handler::request_actor;
use crate::request_handler::Stactor;
use crate::request_handler::StoreRequest;

use crate::mailbox::Mailbox;
use crate::mailbox::NamedMailbox;

use tokio;
use tokio::sync::oneshot;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Runtime::new()?;
    let _: Result<(), ErrorBox> = rt.block_on(async {
        let (mut mb0, h0) = message_actor(PingActor { subject: None });
        let (mut mb1, h1) = message_actor(PingActor { subject: None });

        // Pings
        mb0.send(PingMessage::Setup(mb1.clone())).await?;
        mb1.send(PingMessage::Setup(mb0.clone())).await?;
        mb0.send(PingMessage::Ping).await?;
        tokio::time::sleep(tokio::time::Duration::new(0, 1)).await;

        // Shutdown
        mb0.shutdown().await?;
        mb1.shutdown().await?;
        h0.await?;
        h1.await?;
        Ok(())
    });
    rt.block_on(async {
        let (unnamed_mailbox, _h0) = request_actor(Stactor::new());
        let mut named_mailbox = unnamed_mailbox.register("stactor".into());

        let mut vec = Vec::new();
        for i in 0..1024i32 {
            let (one_s, one_r) = oneshot::channel::<Option<i32>>();
            vec.push((
                one_r,
                tokio::spawn(async move {
                    let _ = NamedMailbox::new("stactor".into())
                        .send((one_s, StoreRequest::Set(i, i)))
                        .await;
                }),
            ));
        }

        for (r, h) in vec.drain(..) {
            h.await?;
            let _result = r.await?;
        }

        for i in 0..1024 {
            let (one_s, one_r) = oneshot::channel::<Option<i32>>();
            named_mailbox
                .send((one_s, StoreRequest::Get(i)))
                .await
                .unwrap();
            let result = one_r.await?;
            assert_eq!(Some(i), result);
        }
        Ok(())
    })
}
