use core::fmt::{self, Display, Formatter};
use core_error::Error;
use futures::{
    channel::mpsc::unbounded, executor::block_on, sink::drain, stream::empty, StreamExt,
};
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
    block_on(async {
        let (sender, receiver) = unbounded();
        Unravel::<(), _, _, Box<dyn Error>>::new(empty(), sender, Box::new(Shim))
            .await
            .unwrap();
        let data =
            Coalesce::<_, _, _, Box<dyn Error>>::new(receiver.map(Ok::<Vec<u8>, Void>), drain())
                .await
                .unwrap();
        println!("{:?}", data);
        println!("{}", data);
        let source = data.source().unwrap();
        println!("{:?}", source);
        println!("{}", source);
    });
}
