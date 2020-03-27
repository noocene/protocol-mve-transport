use core::fmt::{self, Display, Formatter};
use core_error::Error;
use futures::{channel::mpsc::unbounded, executor::LocalPool, task::LocalSpawnExt, StreamExt};
use protocol::protocol;
use protocol_mve_transport::{Coalesce, Unravel};
use void::Void;

#[protocol]
#[derive(Debug)]
pub struct Unit;

#[protocol]
#[derive(Debug)]
pub struct Tuple(Vec<String>, u8);

#[protocol]
#[derive(Debug)]
pub struct Test {
    data: u8,
    other: String,
    unit: Unit,
    tuple: Tuple,
}

fn main() {
    let mut pool = LocalPool::new();

    let s = pool.spawner();
    let spawner = s.clone();

    let (a_sender, a_receiver) = unbounded();
    let (b_sender, b_receiver) = unbounded();

    s.spawn_local(async move {
        Unravel::<Void, _, _, _, Test>::new(
            a_receiver.map(Ok::<Vec<u8>, Void>),
            b_sender,
            spawner,
            Test {
                data: 10,
                other: "hello".to_owned(),
                unit: Unit,
                tuple: Tuple(
                    ["there", "general", "kenobi"]
                        .iter()
                        .cloned()
                        .map(String::from)
                        .collect(),
                    5,
                ),
            },
        )
        .await
        .unwrap();
    })
    .unwrap();

    let spawner = s.clone();

    s.spawn_local(async move {
        let data = Coalesce::<_, _, _, _, Test>::new(
            b_receiver.map(Ok::<Vec<u8>, Void>),
            a_sender,
            spawner,
        );
        println!("{:?}", data.await.unwrap())
    })
    .unwrap();

    pool.run();
}
