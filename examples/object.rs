use core::marker::PhantomData;
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
trait Test<T, U> {
    type A: Copy;
}

pub struct Implementor<A>(PhantomData<A>);

impl<T, U, A: Copy> Test<T, U> for Implementor<A> {
    type A = A;
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
            Box<dyn Test<(), String, A = u8>>,
        >::new(
            a_receiver.map(Ok::<Vec<u8>, Void>),
            b_sender,
            spawner,
            Box::new(Implementor(PhantomData)),
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
            Box<dyn Test<(), String, A = u8>>,
        >::new(b_receiver.map(Ok::<Vec<u8>, Void>), a_sender, spawner);
        data.await.unwrap();
    })
    .unwrap();

    pool.run();
}
