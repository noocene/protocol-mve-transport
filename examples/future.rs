use futures::{
    channel::mpsc::unbounded, executor::block_on, sink::drain, stream::empty, StreamExt,
};
use protocol::{ProtocolError, future::{Ready, ready}};
use protocol_mve_transport::{Coalesce, Unravel};
use std::{future::Future, pin::Pin};
use void::Void;

#[derive(Debug)]
pub struct Shim;

impl<C: ?Sized> protocol::Unravel<C> for Shim {
    type Finalize = Ready<()>;
    type Target = Ready<Ready<()>>;

    fn unravel(self) -> Self::Target {
        ready(ready(()))
    }
}

impl<C: ?Sized> protocol::Coalesce<C> for Shim {
    type Future = Ready<Shim>;

    fn coalesce() -> Self::Future {
        ready(Shim)
    }
}

impl From<ProtocolError> for Shim {
    fn from(_: ProtocolError) -> Self {
        Shim
    }
}

fn main() {
    block_on(async {
        let (sender, receiver) = unbounded();
        Unravel::<(), _, _, Pin<Box<dyn Future<Output = Result<String, Shim>>>>>::new(
            empty(),
            sender,
            Box::pin(async { Ok("hello".to_owned()) }),
        )
        .await
        .unwrap();
        let data =
            Coalesce::<_, _, _, Pin<Box<dyn Future<Output = Result<String, Shim>>>>>::new(
                receiver.map(Ok::<Vec<u8>, Void>),
                drain(),
            );
        println!("{:?}", data.await.unwrap().await);
    });
}
