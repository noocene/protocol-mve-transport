use core::fmt::{self, Display, Formatter};
use core_error::Error;
use futures::{channel::mpsc::unbounded, executor::LocalPool, task::LocalSpawnExt, StreamExt};
use protocol_mve_transport::{Coalesce, Unravel};
use void::Void;

#[derive(Debug)]
pub struct Shim;

impl Display for Shim {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "shim error")
    }
}

impl Error for Shim {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&ShimSource)
    }
}

#[derive(Debug)]
pub struct ShimSource;

impl Display for ShimSource {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "shim error source")
    }
}

impl Error for ShimSource {}

fn main() {
    let mut pool = LocalPool::new();

    let s = pool.spawner();
    let spawner = s.clone();

    let (a_sender, a_receiver) = unbounded();
    let (b_sender, b_receiver) = unbounded();

    s.spawn_local(async move {
        Unravel::<Void, _, _, _, Box<dyn Error>>::new(
            a_receiver.map(Ok::<Vec<u8>, Void>),
            b_sender,
            spawner,
            Box::new(Shim),
        )
        .await
        .unwrap();
    })
    .unwrap();

    let spawner = s.clone();

    s.spawn_local(async move {
        let data = Coalesce::<_, _, _, _, Box<dyn Error>>::new(
            b_receiver.map(Ok::<Vec<u8>, Void>),
            a_sender,
            spawner,
        );
        let data = data.await.unwrap();
        println!("{:?}", data);
        println!("{}", data);
        let source = data.source().unwrap();
        println!("{:?}", source);
        println!("{}", source);
    })
    .unwrap();

    pool.run();
}
