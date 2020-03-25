use core_error::Error;
use futures::{
    ready,
    task::{LocalSpawn, LocalSpawnExt, SpawnError},
    FutureExt as _, Sink, Stream,
};
use protocol::{
    future::MapOk,
    future::{ok, ready, Ready},
    CloneContext, ShareContext, ContextReference, Contextualize, Dispatch, Finalize, FinalizeImmediate, Fork,
    Future as _, FutureExt, Join, Notify, Read, ReferenceContext, Write,
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

pub struct Transport<E, S: LocalSpawn, T: Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>> {
    id: ContextHandle,
    spawner: S,
    inner: Arc<Mutex<TransportInner<E, T, U>>>,
}

impl<E, S: LocalSpawn + Clone, T: Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>> Clone for Transport<E, S, T, U> {
    fn clone(&self) -> Self {
        Transport {
            id: self.id,
            spawner: self.spawner.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<
        E,
        S: LocalSpawn,
        I: DeserializeOwned,
        T: Unpin + Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
    > Read<I> for Transport<E, S, T, U>
{
    type Error = SerdeReadError<E>;

    fn read(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<I, Self::Error>> {
        let id = self.id;
        Pin::new(&mut *self.inner.lock().unwrap()).read(cx, id)
    }
}

impl<
        E,
        S: LocalSpawn,
        I: Serialize,
        T: Unpin + Stream<Item = Result<Vec<u8>, E>>,
        U: Unpin + Sink<Vec<u8>>,
    > Write<I> for Transport<E, S, T, U>
{
    type Error = SerdeWriteError<U::Error>;

    fn write(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let mut data = self.id.0.to_be_bytes().as_ref().to_owned();
        data.append(&mut to_vec(&item).map_err(SerdeWriteError::Serde)?);
        Pin::new(&mut self.inner.lock().unwrap().sink)
            .start_send(data)
            .map_err(SerdeWriteError::Sink)
    }

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner.lock().unwrap().sink)
            .poll_ready(cx)
            .map_err(SerdeWriteError::Sink)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner.lock().unwrap().sink)
            .poll_flush(cx)
            .map_err(SerdeWriteError::Sink)
    }
}

pub struct Coalesce<
    E,
    S: LocalSpawn,
    T: Stream<Item = Result<Vec<u8>, E>>,
    U: Sink<Vec<u8>>,
    P: protocol::Coalesce<Transport<E, S, T, U>>,
> {
    fut: P::Future,
    transport: Transport<E, S, T, U>,
}

enum UnravelState<T, U> {
    Target(T),
    Finalize(U),
}

pub struct Unravel<
    E,
    S: LocalSpawn,
    T: Stream<Item = Result<Vec<u8>, E>>,
    U: Sink<Vec<u8>>,
    P: protocol::Unravel<Transport<E, S, T, U>>,
> {
    fut: UnravelState<P::Target, P::Finalize>,
    transport: Transport<E, S, T, U>,
}

impl<
        E,
        S: LocalSpawn + Unpin,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Transport<E, S, T, U>>,
    > Future for Unravel<E, S, T, U, P>
where
    P::Target: Unpin,
    P::Finalize: Unpin,
{
    type Output = Result<(), <P::Target as protocol::Future<Transport<E, S, T, U>>>::Error>;

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
        S: LocalSpawn + Unpin,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Coalesce<Transport<E, S, T, U>>,
    > Future for Coalesce<E, S, T, U, P>
where
    P::Future: Unpin,
{
    type Output = Result<P, <P::Future as protocol::Future<Transport<E, S, T, U>>>::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = &mut *self;
        Pin::new(&mut this.fut).poll(cx, &mut this.transport)
    }
}

impl<
        E,
        S: LocalSpawn,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Coalesce<Transport<E, S, T, U>>,
    > Coalesce<E, S, T, U, P>
where
    P::Future: Unpin,
{
    pub fn new(stream: T, sink: U, spawner: S) -> Self {
        Coalesce {
            transport: Transport {
                inner: Arc::new(Mutex::new(TransportInner {
                    sink,
                    stream,
                    next_id: 2,
                    buffer: HashMap::new(),
                })),
                spawner,
                id: ContextHandle(0),
            },
            fut: P::coalesce(),
        }
    }
}

impl<
        E,
        S: LocalSpawn,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Transport<E, S, T, U>>,
    > Unravel<E, S, T, U, P>
where
    P::Target: Unpin,
    P::Finalize: Unpin,
{
    pub fn new(stream: T, sink: U, spawner: S, item: P) -> Self {
        Unravel {
            transport: Transport {
                inner: Arc::new(Mutex::new(TransportInner {
                    sink,
                    next_id: 1,
                    stream,
                    buffer: HashMap::new(),
                })),
                spawner,
                id: ContextHandle(0),
            },
            fut: UnravelState::Target(item.unravel()),
        }
    }
}

impl<
        E,
        S: LocalSpawn,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Dispatch<P> for Transport<E, S, T, U>
{
    type Handle = ();
}

impl<
        E,
        S: LocalSpawn,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Dispatch<Notification<P>> for Transport<E, S, T, U>
{
    type Handle = ();
}

impl<
        E,
        S: LocalSpawn,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Fork<P> for Transport<E, S, T, U>
where
    <P as protocol::Unravel<Self>>::Target: Unpin,
{
    type Finalize = <P as protocol::Unravel<Self>>::Finalize;
    type Target = <P as protocol::Unravel<Self>>::Target;
    type Future = Ready<(Self::Target, ())>;

    fn fork(&mut self, item: P) -> Self::Future {
        ok((item.unravel(), ()))
    }
}

impl<
        E,
        S: LocalSpawn,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Join<P> for Transport<E, S, T, U>
{
    type Future = <P as protocol::Coalesce<Self>>::Future;

    fn join(&mut self, _: ()) -> Self::Future {
        P::coalesce()
    }
}

pub struct Notification<P>(P);

impl<
        E,
        S: LocalSpawn,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Join<Notification<P>> for Transport<E, S, T, U>
where
    <P as protocol::Coalesce<Self>>::Future: Unpin,
{
    type Future = MapOk<<P as protocol::Coalesce<Self>>::Future, fn(P) -> Notification<P>>;

    fn join(&mut self, _: ()) -> Self::Future {
        P::coalesce().map_ok(Notification)
    }
}

impl<
        E,
        S: LocalSpawn,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Fork<Notification<P>> for Transport<E, S, T, U>
where
    <P as protocol::Unravel<Self>>::Target: Unpin,
{
    type Finalize = <P as protocol::Unravel<Self>>::Finalize;
    type Target = <P as protocol::Unravel<Self>>::Target;
    type Future = Ready<(Self::Target, ())>;

    fn fork(&mut self, item: Notification<P>) -> Self::Future {
        ok((item.0.unravel(), ()))
    }
}

impl<
        E,
        S: LocalSpawn,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self> + Unpin,
    > Notify<P> for Transport<E, S, T, U>
where
    <P as protocol::Unravel<Self>>::Target: Unpin,
    <P as protocol::Coalesce<Self>>::Future: Unpin,
{
    type Notification = Notification<P>;
    type Wrap = Ready<Notification<P>>;
    type Unwrap = Ready<P>;

    fn unwrap(&mut self, notification: Self::Notification) -> Self::Unwrap {
        ok(notification.0)
    }

    fn wrap(&mut self, item: P) -> Self::Wrap {
        ok(Notification(item))
    }
}

pub struct Contextualized<
    E,
    S: LocalSpawn,
    T: Stream<Item = Result<Vec<u8>, E>>,
    U: Sink<Vec<u8>>,
    F,
> {
    fut: F,
    transport: Transport<E, S, T, U>,
}

impl<
        E,
        S: LocalSpawn + Unpin,
        T: Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
        F: Unpin + protocol::Future<Transport<E, S, T, U>>,
    > Future for Contextualized<E, S, T, U, F>
{
    type Output = Result<F::Ok, F::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = &mut *self;

        Pin::new(&mut this.fut).poll(cx, &mut this.transport)
    }
}

impl<E, S: LocalSpawn, T: Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>> Contextualize
    for Transport<E, S, T, U>
{
    type Handle = u32;
}

impl<
        E,
        S: LocalSpawn + Clone + Unpin,
        T: Unpin + Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
    > CloneContext for Transport<E, S, T, U>
{
    type Context = Transport<E, S, T, U>;
    type ForkOutput = Ready<(Transport<E, S, T, U>, u32)>;
    type JoinOutput = Ready<Transport<E, S, T, U>>;

    fn fork_owned(&mut self) -> Self::ForkOutput {
        let id = self.inner.lock().unwrap().next_id();

        ok((
            Transport {
                inner: self.inner.clone(),
                spawner: self.spawner.clone(),
                id,
            },
            id.0,
        ))
    }

    fn join_owned(&mut self, id: Self::Handle) -> Self::JoinOutput {
        ok(Transport {
            inner: self.inner.clone(),
            spawner: self.spawner.clone(),
            id: ContextHandle(id),
        })
    }
}

impl<
        E,
        S: LocalSpawn + Clone + Unpin,
        T: Unpin + Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
    > ShareContext for Transport<E, S, T, U>
{
    type Context = Transport<E, S, T, U>;
    type ForkOutput = Ready<(Transport<E, S, T, U>, u32)>;
    type JoinOutput = Ready<Transport<E, S, T, U>>;

    fn fork_shared(&mut self) -> Self::ForkOutput {
        let id = self.inner.lock().unwrap().next_id();

        ok((
            Transport {
                inner: self.inner.clone(),
                spawner: self.spawner.clone(),
                id,
            },
            id.0,
        ))
    }

    fn join_shared(&mut self, id: Self::Handle) -> Self::JoinOutput {
        ok(Transport {
            inner: self.inner.clone(),
            spawner: self.spawner.clone(),
            id: ContextHandle(id),
        })
    }
}

impl<E, S: LocalSpawn, T: Stream<Item = Result<Vec<u8>, E>>, U: Sink<Vec<u8>>>
    ContextReference<Transport<E, S, T, U>> for Transport<E, S, T, U>
{
    type Target = Transport<E, S, T, U>;

    fn with<'a, 'b: 'a, R: BorrowMut<Transport<E, S, T, U>> + 'b>(
        &'a mut self,
        _: R,
    ) -> &'a mut Self::Target {
        self
    }
}

impl<
        E,
        S: LocalSpawn + Clone + Unpin,
        T: Unpin + Stream<Item = Result<Vec<u8>, E>>,
        U: Sink<Vec<u8>>,
    > ReferenceContext for Transport<E, S, T, U>
{
    type Context = Transport<E, S, T, U>;
    type ForkOutput = Ready<(Transport<E, S, T, U>, u32)>;
    type JoinOutput = Ready<Transport<E, S, T, U>>;

    fn fork_ref(&mut self) -> Self::ForkOutput {
        let id = self.inner.lock().unwrap().next_id();

        ok((
            Transport {
                inner: self.inner.clone(),
                spawner: self.spawner.clone(),
                id,
            },
            id.0,
        ))
    }

    fn join_ref(&mut self, id: Self::Handle) -> Self::JoinOutput {
        ok(Transport {
            inner: self.inner.clone(),
            spawner: self.spawner.clone(),
            id: ContextHandle(id),
        })
    }
}

impl<
        F: Unpin + protocol::Future<Self> + 'static,
        E: 'static,
        S: Unpin + LocalSpawn + Clone + 'static,
        T: Unpin + Stream<Item = Result<Vec<u8>, E>> + 'static,
        U: Sink<Vec<u8>> + 'static,
    > Finalize<F> for Transport<E, S, T, U>
{
    type Target = Self;
    type Output = Ready<(), SpawnError>;

    fn finalize(&mut self, fut: F) -> Self::Output {
        ready(
            self.spawner.spawn_local(
                Contextualized {
                    fut,
                    transport: Transport {
                        id: self.id,
                        inner: self.inner.clone(),
                        spawner: self.spawner.clone(),
                    },
                }
                .map(|_| ()),
            ),
        )
    }
}

impl<
        F: Unpin + protocol::Future<Self> + 'static,
        E: 'static,
        S: Unpin + LocalSpawn + Clone + 'static,
        T: Unpin + Stream<Item = Result<Vec<u8>, E>> + 'static,
        U: Sink<Vec<u8>> + 'static,
    > FinalizeImmediate<F> for Transport<E, S, T, U>
{
    type Target = Self;
    type Error = SpawnError;

    fn finalize(&mut self, fut: F) -> Result<(), SpawnError> {
        self.spawner.spawn_local(
            Contextualized {
                fut,
                transport: Transport {
                    id: self.id,
                    inner: self.inner.clone(),
                    spawner: self.spawner.clone(),
                },
            }
            .map(|_| ()),
        )
    }
}
