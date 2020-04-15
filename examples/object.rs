use core::{future, pin::Pin};
use futures::{channel::mpsc::unbounded, executor::LocalPool, task::LocalSpawnExt, StreamExt};
use protocol::{protocol, ProtocolError};
use protocol_mve_transport::{Coalesce, Unravel};
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

type Future<T> = Pin<Box<dyn future::Future<Output = Result<T, Shim>>>>;

#[protocol]
pub trait Test<T, U> {
    type A: Copy;

    fn method(&self, test: T, other: Self::A) -> Future<String>;
    fn other(self: Box<Self>, test: T, other: U, last: Self::A) -> Future<String>;
    fn empty(&mut self) -> Future<()>;
    fn empty_move(self: Box<Self>) -> Future<T>;
}

struct Implementor;

impl Test<String, u8> for Implementor {
    type A = u16;

    fn method(&self, test: String, other: Self::A) -> Future<String> {
        Box::pin(async move {
            Ok(format!(
                "in borrow string is \"{}\", other is {}",
                test, other
            ))
        })
    }

    fn other(self: Box<Self>, test: String, other: u8, last: Self::A) -> Future<String> {
        Box::pin(async move {
            Ok(format!(
                "string is \"{}\", other is {}, last is {}",
                test, other, last
            ))
        })
    }

    fn empty(&mut self) -> Future<()> {
        Box::pin(async { Ok(()) })
    }

    fn empty_move(self: Box<Self>) -> Future<String> {
        Box::pin(async { Ok("hello".to_owned()) })
    }
}

fn main() {
    let mut pool = LocalPool::new();

    let s = pool.spawner();
    let spawner = s.clone();

    let (a_sender, a_receiver) = unbounded();
    let (b_sender, b_receiver) = unbounded();

    s.spawn_local(async move {
        Unravel::<Void, _, _, _, Box<dyn Test<String, u8, A = u16>>>::new(
            a_receiver.map(Ok::<Vec<u8>, Void>),
            b_sender,
            spawner,
            Box::new(Implementor),
        )
        .await
        .unwrap();
    })
    .unwrap();

    let spawner = s.clone();

    s.spawn_local(async move {
        let data = Coalesce::<_, _, _, _, Box<dyn Test<String, u8, A = u16>>>::new(
            b_receiver.map(Ok::<Vec<u8>, Void>),
            a_sender,
            spawner,
        );
        let data = data.await.unwrap();
        println!("{}", data.method("lol".to_string(), 10).await.unwrap());
        println!("{}", data.other("test".to_string(), 10, 15).await.unwrap());
    })
    .unwrap();

    pool.run();
}
