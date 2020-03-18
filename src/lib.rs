use core_error::Error;
use futures::{ready, Sink, Stream};
use protocol::{
    future::{ready, Ready},
    CoalesceContextualizer, ContextualizeCoalesce, ContextualizeUnravel, Contextualizer, Dispatch,
    Fork, Future as _, Join, Read, UnravelContext, Write,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_cbor::{error::Error as CborError, from_slice, to_vec};
use std::{
    borrow::BorrowMut,
    collections::{HashMap, VecDeque},
    convert::TryInto,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use thiserror::Error;

#[derive(Hash, PartialEq, Eq, Clone, Copy)]
pub struct ContextHandle(u32);

pub struct TransportInner<E, T: Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>> {
    stream: T,
    next_id: u32,
    sink: U,
    buffer: HashMap<ContextHandle, VecDeque<Vec<u8>>>,
}

impl<E, T: Unpin + Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>> Unpin
    for TransportInner<E, T, U>
{
}

#[derive(Debug, Error)]
#[bounds(where E: Error + 'static)]
pub enum SerdeReadError<E> {
    #[error("error in underlying stream: {0}")]
    Stream(E),
    #[error("serde error: {0}")]
    Serde(CborError),
    #[error("received insufficient buffer")]
    Insufficient,
    #[error("stream completed early")]
    Terminated,
}

#[derive(Debug, Error)]
#[bounds(where E: Error + 'static)]
pub enum SerdeWriteError<E> {
    #[error("error in underlying sink: {0}")]
    Sink(#[source] E),
    #[error("serde error: {0}")]
    Serde(#[source] CborError),
}

impl<E, T: Unpin + Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>> TransportInner<E, T, U> {
    fn next_id(&mut self) -> ContextHandle {
        let handle = self.next_id;
        self.next_id += 2;
        ContextHandle(handle)
    }

    fn read<I: DeserializeOwned>(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        handle: ContextHandle,
    ) -> Poll<Result<I, SerdeReadError<E>>> {
        let this = &mut *self;

        if let Some(data) = this
            .buffer
            .get_mut(&handle)
            .map(|container| container.pop_back())
            .flatten()
        {
            Poll::Ready(from_slice(&data[4..]).map_err(SerdeReadError::Serde))
        } else {
            let mut stream = Pin::new(&mut this.stream);
            let data = loop {
                let data = ready!(stream.as_mut().poll_next(cx))
                    .ok_or(SerdeReadError::Terminated)?
                    .map_err(SerdeReadError::Stream)?;
                if data.len() < 4 {
                    return Poll::Ready(Err(SerdeReadError::Insufficient));
                }
                if u32::from_be_bytes(
                    data[..4]
                        .try_into()
                        .map_err(|_| SerdeReadError::Insufficient)?,
                ) == handle.0
                {
                    break data;
                } else {
                    this.buffer
                        .entry(handle)
                        .or_insert(VecDeque::new())
                        .push_front(data);
                }
            };
            Poll::Ready(from_slice(&data[4..]).map_err(SerdeReadError::Serde))
        }
    }
}

pub struct Transport<E, T: Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>> {
    id: ContextHandle,
    inner: Arc<Mutex<TransportInner<E, T, U>>>,
}

impl<E, I: DeserializeOwned, T: Unpin + Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>> Read<I>
    for Transport<E, T, U>
{
    type Error = SerdeReadError<E>;

    fn read(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<I, Self::Error>> {
        let id = self.id;
        Pin::new(&mut *self.inner.lock().unwrap()).read(cx, id)
    }
}

impl<E, I: Serialize, T: Unpin + Stream<Item = Result<Vec<u8>, E>>, U: Unpin + Sink<Vec<u8>>>
    Write<I> for Transport<E, T, U>
{
    type Error = SerdeWriteError<U::Error>;

    fn write(mut self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let mut data = self.id.0.to_be_bytes().as_ref().to_owned();
        data.append(&mut to_vec(&item).map_err(SerdeWriteError::Serde)?);
        Pin::new(&mut self.inner.lock().unwrap().sink)
            .start_send(data)
            .map_err(SerdeWriteError::Sink)
    }

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner.lock().unwrap().sink)
            .poll_ready(cx)
            .map_err(SerdeWriteError::Sink)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner.lock().unwrap().sink)
            .poll_flush(cx)
            .map_err(SerdeWriteError::Sink)
    }
}

pub struct Coalesce<
    E,
    T: Stream<Item = Result<Vec<u8>, E>>,
    U: Sink<Vec<u8>>,
    P: protocol::Coalesce<Transport<E, T, U>>,
> {
    fut: P::Future,
    transport: Transport<E, T, U>,
}

enum UnravelState<T, U> {
    Target(T),
    Finalize(U),
}

pub struct Unravel<
    E,
    T: Stream<Item = Result<Vec<u8>, E>>,
    U: Sink<Vec<u8>>,
    P: protocol::Unravel<Transport<E, T, U>>,
> {
    fut: UnravelState<P::Target, P::Finalize>,
    transport: Transport<E, T, U>,
}

impl<
        E,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Transport<E, T, U>>,
    > Future for Unravel<E, T, U, P>
where
    P::Target: Unpin,
    P::Finalize: Unpin,
{
    type Output = Result<(), <P::Target as protocol::Future<Transport<E, T, U>>>::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = &mut *self;

        loop {
            match &mut this.fut {
                UnravelState::Target(future) => {
                    let finalize = ready!(Pin::new(future).poll(cx, &mut this.transport))?;
                    this.fut = UnravelState::Finalize(finalize);
                }
                UnravelState::Finalize(future) => {
                    ready!(Pin::new(future).poll(cx, &mut this.transport))?;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

impl<
        E,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Coalesce<Transport<E, T, U>>,
    > Future for Coalesce<E, T, U, P>
where
    P::Future: Unpin,
{
    type Output = Result<P, <P::Future as protocol::Future<Transport<E, T, U>>>::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = &mut *self;
        Pin::new(&mut this.fut).poll(cx, &mut this.transport)
    }
}

impl<
        E,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Coalesce<Transport<E, T, U>>,
    > Coalesce<E, T, U, P>
where
    P::Future: Unpin,
{
    pub fn new(stream: T, sink: U) -> Self {
        Coalesce {
            transport: Transport {
                inner: Arc::new(Mutex::new(TransportInner {
                    sink,
                    stream,
                    next_id: 2,
                    buffer: HashMap::new(),
                })),
                id: ContextHandle(0),
            },
            fut: P::coalesce(),
        }
    }
}

impl<
        E,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Transport<E, T, U>>,
    > Unravel<E, T, U, P>
where
    P::Target: Unpin,
    P::Finalize: Unpin,
{
    pub fn new(stream: T, sink: U, item: P) -> Self {
        Unravel {
            transport: Transport {
                inner: Arc::new(Mutex::new(TransportInner {
                    sink,
                    next_id: 1,
                    stream,
                    buffer: HashMap::new(),
                })),
                id: ContextHandle(0),
            },
            fut: UnravelState::Target(item.unravel()),
        }
    }
}

impl<
        E,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Dispatch<P> for Transport<E, T, U>
{
    type Handle = ();
}

impl<
        E,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Fork<P> for Transport<E, T, U>
where
    <P as protocol::Unravel<Self>>::Target: Unpin,
{
    type Finalize = <P as protocol::Unravel<Self>>::Finalize;
    type Target = <P as protocol::Unravel<Self>>::Target;
    type Future = Ready<(Self::Target, ())>;

    fn fork(&mut self, item: P) -> Self::Future {
        ready((item.unravel(), ()))
    }
}

impl<
        E,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Join<P> for Transport<E, T, U>
{
    type Future = <P as protocol::Coalesce<Self>>::Future;

    fn join(&mut self, _: ()) -> Self::Future {
        P::coalesce()
    }
}

pub struct Contextualized<E, T: Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>, F> {
    fut: F,
    transport: Transport<E, T, U>,
}

impl<
        E,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        F: Unpin + protocol::Future<Transport<E, T, U>>,
    > Future for Contextualized<E, T, U, F>
{
    type Output = Result<F::Ok, F::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = &mut *self;

        Pin::new(&mut this.fut).poll(cx, &mut this.transport)
    }
}

impl<E, T: Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>> Contextualizer
    for Transport<E, T, U>
{
    type Handle = u32;
}

impl<E, T: Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>> CoalesceContextualizer
    for Transport<E, T, U>
{
    type Target = Self;
}

impl<
        E,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        F: protocol::Future<Transport<E, T, U>> + Unpin,
    > ContextualizeCoalesce<F> for Transport<E, T, U>
{
    type Output = Ready<Contextualized<E, T, U, F>>;
    type Future = Contextualized<E, T, U, F>;

    fn contextualize(&mut self, handle: Self::Handle, fut: F) -> Self::Output {
        ready(Contextualized {
            fut,
            transport: Transport {
                inner: self.inner.clone(),
                id: ContextHandle(handle),
            },
        })
    }
}

impl<E, T: Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>> UnravelContext<Transport<E, T, U>>
    for Transport<E, T, U>
{
    type Target = Transport<E, T, U>;

    fn with<'a, 'b: 'a, R: BorrowMut<Transport<E, T, U>> + 'b>(
        &'a mut self,
        _: R,
    ) -> &'a mut Self::Target {
        self
    }
}

impl<E, T: Unpin + Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>> ContextualizeUnravel
    for Transport<E, T, U>
{
    type Context = Transport<E, T, U>;
    type Output = Ready<(Transport<E, T, U>, u32)>;

    fn contextualize(&mut self) -> Self::Output {
        let id = self.inner.lock().unwrap().next_id();

        ready((
            Transport {
                inner: self.inner.clone(),
                id,
            },
            id.0,
        ))
    }
}
