use futures::{channel::mpsc::unbounded, executor::LocalPool, task::LocalSpawnExt, StreamExt};
use protocol::{
    future::{ok, Ready},
    ProtocolError,
    protocol
};
use protocol_mve_transport::{Coalesce, Unravel};
use std::{future::Future, pin::Pin};
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
        Unravel::<
            Void,
            _,
            _,
            _,
            Box<dyn FnOnce(String) -> Pin<Box<dyn Future<Output = Result<String, Shim>>>>>,
        >::new(
            a_receiver.map(Ok::<Vec<u8>, Void>),
            b_sender,
            spawner,
            Box::new(|data| Box::pin(async move { Ok(format!("echo: {}", data)) })),
        )
        .await
        .unwrap();
    })
    .unwrap();

    let spawner = s.clone();

    s.spawn_local(async move {
        let data = Coalesce::<
            _,
            _,
            _,
            _,
            Box<dyn FnOnce(String) -> Pin<Box<dyn Future<Output = Result<String, Shim>>>>>,
        >::new(b_receiver.map(Ok::<Vec<u8>, Void>), a_sender, spawner);
        let call = data.await.unwrap();
        println!("{:?}", (call)("hello".to_owned()).await);
    })
    .unwrap();

    pool.run();
}
