use futures::{
    channel::mpsc::unbounded, executor::LocalPool, stream::iter, task::LocalSpawnExt, Stream,
    StreamExt,
};
use protocol::{protocol, ProtocolError};
use protocol_mve_transport::{Coalesce, Unravel};
use std::pin::Pin;
use void::Void;

#[protocol]
#[derive(Debug)]
pub struct Shim;

impl From<ProtocolError> for Shim {
    fn from(error: ProtocolError) -> Self {
        eprintln!("{}", error);
        Shim
    }
}

fn main() {
    let mut pool = LocalPool::new();

    let s = pool.spawner();
    let spawner = s.clone();

    let (a_sender, a_receiver) = unbounded();
    let (b_sender, b_receiver) = unbounded();

    s.spawn_local(async move {
        Unravel::<Void, _, _, _, Pin<Box<dyn Stream<Item = Result<String, Shim>>>>>::new(
            a_receiver.map(Ok::<Vec<u8>, Void>),
            b_sender,
            spawner,
            Box::pin(iter((0u8..10).into_iter().map(|i| Ok(format!("{}", i))))),
        )
        .await
        .unwrap();
    })
    .unwrap();

    let spawner = s.clone();

    s.spawn_local(async move {
        let data = Coalesce::<_, _, _, _, Pin<Box<dyn Stream<Item = Result<String, Shim>>>>>::new(
            b_receiver.map(Ok::<Vec<u8>, Void>),
            a_sender,
            spawner,
        );
        let mut data = data.await.unwrap();

        while let Some(item) = data.next().await {
            println!("{:?}", item);
        }
    })
    .unwrap();

    pool.run();
}
