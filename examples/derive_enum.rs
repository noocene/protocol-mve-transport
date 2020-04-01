use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    executor::{LocalPool, LocalSpawner},
    stream::Map,
    task::LocalSpawnExt,
    StreamExt,
};
use protocol::protocol;
use protocol_mve_transport::{Coalesce, Unravel};
use void::Void;
#[protocol]
#[derive(Debug)]
pub enum Test {
    Data { item: u8, other: String },
    Test(u8),
    None,
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
            LocalSpawner,
            Map<UnboundedReceiver<Vec<u8>>, fn(Vec<u8>) -> Result<Vec<u8>, Void>>,
            UnboundedSender<Vec<u8>>,
            Test,
        >::new(
            a_receiver.map(Ok::<Vec<u8>, Void>),
            b_sender,
            spawner,
            Test::Data {
                item: 11,
                other: "hello there".to_owned(),
            },
        )
        .await
        .unwrap();
    })
    .unwrap();

    let spawner = s.clone();

    s.spawn_local(async move {
        let data = Coalesce::<
            Void,
            LocalSpawner,
            Map<UnboundedReceiver<Vec<u8>>, fn(Vec<u8>) -> Result<Vec<u8>, Void>>,
            UnboundedSender<Vec<u8>>,
            Test,
        >::new(b_receiver.map(Ok::<Vec<u8>, Void>), a_sender, spawner);
        println!("{:?}", data.await.unwrap())
    })
    .unwrap();

    pool.run();
}
