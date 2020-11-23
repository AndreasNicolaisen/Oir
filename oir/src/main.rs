use crate::actor::ErrorBox;
use crate::actor::SystemMessage;

mod actor;
mod message_handler;
mod request_handler;

use crate::message_handler::message_actor;
use crate::message_handler::PingMessage;
use crate::message_handler::PingActor;

use crate::request_handler::StoreRequest;
use crate::request_handler::request_actor;
use crate::request_handler::Stactor;

use tokio;
use tokio::sync::mpsc;

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
        let (_ts0, rs0) = mpsc::channel::<SystemMessage>(512);
        let (tp0, rp0) = mpsc::channel::<StoreRequest<i32, i32>>(1024);
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
            vec.push(tokio::spawn(async move {
                let _ = tp0.send(StoreRequest::Set(i, i)).await;
            }));
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
