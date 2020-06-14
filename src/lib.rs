use bincode::{deserialize as from_slice, serialize as to_vec, Error as BincodeError};
use core_error::Error;
use futures::{
    ready,
    task::{Spawn, SpawnError, SpawnExt},
    FutureExt as _, Sink, TryStream,
};
use protocol::{
    future::MapOk,
    future::{ok, ready, Ready},
    CloneContext, ContextReference, Contextualize, Dispatch, Finalize, FinalizeImmediate, Fork,
    Future as _, FutureExt, Join, Notify, Read, ReferenceContext, ShareContext, Write,
};
use serde::{de::DeserializeOwned, Serialize};
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

pub struct TransportInner<T: TryStream<Ok = Vec<u8>>, U: Sink<Vec<u8>>> {
    stream: T,
    next_id: u32,
    sink: U,
    buffer: HashMap<ContextHandle, VecDeque<Vec<u8>>>,
}

impl<T: Unpin + TryStream<Ok = Vec<u8>>, U: Sink<Vec<u8>>> Unpin for TransportInner<T, U> {}

#[derive(Debug, Error)]
#[bounds(where E: Error + 'static)]
pub enum SerdeReadError<E> {
    #[error("error in underlying stream: {0}")]
    Stream(E),
    #[error("serde error: {0}")]
    Serde(BincodeError),
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
    Serde(#[source] BincodeError),
}

impl<T: Unpin + TryStream<Ok = Vec<u8>>, U: Sink<Vec<u8>>> TransportInner<T, U> {
    fn next_id(&mut self) -> ContextHandle {
        let handle = self.next_id;
        self.next_id += 2;
        ContextHandle(handle)
    }

    fn read<I: DeserializeOwned>(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        handle: ContextHandle,
    ) -> Poll<Result<I, SerdeReadError<T::Error>>> {
        let this = &mut *self;

        if let Some(data) = this
            .buffer
            .get_mut(&handle)
            .map(|container| container.pop_front())
            .flatten()
        {
            Poll::Ready(from_slice(&data[4..]).map_err(SerdeReadError::Serde))
        } else {
            let mut stream = Pin::new(&mut this.stream);
            let data = loop {
                let data = ready!(stream.as_mut().try_poll_next(cx))
                    .ok_or(SerdeReadError::Terminated)?
                    .map_err(SerdeReadError::Stream)?;
                if data.len() < 4 {
                    return Poll::Ready(Err(SerdeReadError::Insufficient));
                }

                let target_handle = u32::from_be_bytes(
                    data[..4]
                        .try_into()
                        .map_err(|_| SerdeReadError::Insufficient)?,
                );

                if target_handle == handle.0 {
                    break data;
                } else {
                    this.buffer
                        .entry(ContextHandle(target_handle))
                        .or_insert(VecDeque::new())
                        .push_back(data);
                }
            };
            Poll::Ready(from_slice(&data[4..]).map_err(SerdeReadError::Serde))
        }
    }
}

pub struct Transport<S: Spawn, T: TryStream<Ok = Vec<u8>>, U: Sink<Vec<u8>>> {
    id: ContextHandle,
    spawner: S,
    inner: Arc<Mutex<TransportInner<T, U>>>,
}

impl<S: Spawn + Clone, T: TryStream<Ok = Vec<u8>>, U: Sink<Vec<u8>>> Clone for Transport<S, T, U> {
    fn clone(&self) -> Self {
        Transport {
            id: self.id,
            spawner: self.spawner.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<S: Spawn, I: DeserializeOwned, T: Unpin + TryStream<Ok = Vec<u8>>, U: Sink<Vec<u8>>> Read<I>
    for Transport<S, T, U>
{
    type Error = SerdeReadError<T::Error>;

    fn read(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<I, Self::Error>> {
        let id = self.id;
        Pin::new(&mut *self.inner.lock().unwrap()).read(cx, id)
    }
}

impl<S: Spawn, I: Serialize, T: Unpin + TryStream<Ok = Vec<u8>>, U: Unpin + Sink<Vec<u8>>> Write<I>
    for Transport<S, T, U>
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
    S: Spawn,
    T: TryStream<Ok = Vec<u8>>,
    U: Sink<Vec<u8>>,
    P: protocol::Coalesce<Transport<S, T, U>>,
> where
    P::Future: Unpin,
{
    fut: P::Future,
    transport: Transport<S, T, U>,
}

enum UnravelState<T, U> {
    Target(T),
    Finalize(U),
}

pub struct Unravel<
    S: Spawn,
    T: TryStream<Ok = Vec<u8>>,
    U: Sink<Vec<u8>>,
    P: protocol::Unravel<Transport<S, T, U>>,
> where
    P::Target: Unpin,
    P::Finalize: Unpin,
{
    fut: UnravelState<P::Target, P::Finalize>,
    transport: Transport<S, T, U>,
}

impl<
        S: Spawn + Unpin,
        T: TryStream<Ok = Vec<u8>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Transport<S, T, U>>,
    > Future for Unravel<S, T, U, P>
where
    P::Target: Unpin,
    P::Finalize: Unpin,
{
    type Output = Result<(), <P::Target as protocol::Future<Transport<S, T, U>>>::Error>;

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
        S: Spawn + Unpin,
        T: TryStream<Ok = Vec<u8>>,
        U: Sink<Vec<u8>>,
        P: protocol::Coalesce<Transport<S, T, U>>,
    > Future for Coalesce<S, T, U, P>
where
    P::Future: Unpin,
{
    type Output = Result<P, <P::Future as protocol::Future<Transport<S, T, U>>>::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = &mut *self;
        Pin::new(&mut this.fut).poll(cx, &mut this.transport)
    }
}

impl<
        S: Spawn,
        T: TryStream<Ok = Vec<u8>>,
        U: Sink<Vec<u8>>,
        P: protocol::Coalesce<Transport<S, T, U>>,
    > Coalesce<S, T, U, P>
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
        S: Spawn,
        T: TryStream<Ok = Vec<u8>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Transport<S, T, U>>,
    > Unravel<S, T, U, P>
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
        S: Spawn,
        T: TryStream<Ok = Vec<u8>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Dispatch<P> for Transport<S, T, U>
{
    type Handle = ();
}

impl<
        S: Spawn,
        T: TryStream<Ok = Vec<u8>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Dispatch<Notification<P>> for Transport<S, T, U>
{
    type Handle = ();
}

impl<
        S: Spawn,
        T: TryStream<Ok = Vec<u8>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Fork<P> for Transport<S, T, U>
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
        S: Spawn,
        T: TryStream<Ok = Vec<u8>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Join<P> for Transport<S, T, U>
{
    type Future = <P as protocol::Coalesce<Self>>::Future;

    fn join(&mut self, _: ()) -> Self::Future {
        P::coalesce()
    }
}

pub struct Notification<P>(P);

impl<
        S: Spawn,
        T: TryStream<Ok = Vec<u8>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Join<Notification<P>> for Transport<S, T, U>
where
    <P as protocol::Coalesce<Self>>::Future: Unpin,
{
    type Future = MapOk<<P as protocol::Coalesce<Self>>::Future, fn(P) -> Notification<P>>;

    fn join(&mut self, _: ()) -> Self::Future {
        P::coalesce().map_ok(Notification)
    }
}

impl<
        S: Spawn,
        T: TryStream<Ok = Vec<u8>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self>,
    > Fork<Notification<P>> for Transport<S, T, U>
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
        S: Spawn,
        T: TryStream<Ok = Vec<u8>>,
        U: Sink<Vec<u8>>,
        P: protocol::Unravel<Self> + protocol::Coalesce<Self> + Unpin,
    > Notify<P> for Transport<S, T, U>
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

pub struct Contextualized<S: Spawn, T: TryStream<Ok = Vec<u8>>, U: Sink<Vec<u8>>, F> {
    fut: F,
    transport: Transport<S, T, U>,
}

impl<
        S: Spawn + Unpin,
        T: TryStream<Ok = Vec<u8>>,
        U: Sink<Vec<u8>>,
        F: Unpin + protocol::Future<Transport<S, T, U>>,
    > Future for Contextualized<S, T, U, F>
{
    type Output = Result<F::Ok, F::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = &mut *self;

        Pin::new(&mut this.fut).poll(cx, &mut this.transport)
    }
}

impl<S: Spawn, T: TryStream<Ok = Vec<u8>>, U: Sink<Vec<u8>>> Contextualize for Transport<S, T, U> {
    type Handle = u32;
}

impl<S: Spawn + Clone + Unpin, T: Unpin + TryStream<Ok = Vec<u8>>, U: Sink<Vec<u8>>> CloneContext
    for Transport<S, T, U>
{
    type Context = Transport<S, T, U>;
    type ForkOutput = Ready<(Transport<S, T, U>, u32)>;
    type JoinOutput = Ready<Transport<S, T, U>>;

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

impl<S: Spawn + Clone + Unpin, T: Unpin + TryStream<Ok = Vec<u8>>, U: Sink<Vec<u8>>> ShareContext
    for Transport<S, T, U>
{
    type Context = Transport<S, T, U>;
    type ForkOutput = Ready<(Transport<S, T, U>, u32)>;
    type JoinOutput = Ready<Transport<S, T, U>>;

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

impl<S: Spawn, T: TryStream<Ok = Vec<u8>>, U: Sink<Vec<u8>>> ContextReference<Transport<S, T, U>>
    for Transport<S, T, U>
{
    type Target = Transport<S, T, U>;

    fn with<'a, 'b: 'a, R: BorrowMut<Transport<S, T, U>> + 'b>(
        &'a mut self,
        _: R,
    ) -> &'a mut Self::Target {
        self
    }
}

impl<S: Spawn + Clone + Unpin, T: Unpin + TryStream<Ok = Vec<u8>>, U: Sink<Vec<u8>>>
    ReferenceContext for Transport<S, T, U>
{
    type Context = Transport<S, T, U>;
    type ForkOutput = Ready<(Transport<S, T, U>, u32)>;
    type JoinOutput = Ready<Transport<S, T, U>>;

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
        F: Send + Unpin + protocol::Future<Self> + 'static,
        S: Send + Unpin + Spawn + Clone + 'static,
        T: Send + Unpin + TryStream<Ok = Vec<u8>> + 'static,
        U: Send + Sink<Vec<u8>> + 'static,
    > Finalize<F> for Transport<S, T, U>
{
    type Target = Self;
    type Output = Ready<(), SpawnError>;

    fn finalize(&mut self, fut: F) -> Self::Output {
        ready(
            self.spawner.spawn(
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
        F: Send + Unpin + protocol::Future<Self> + 'static,
        S: Send + Unpin + Spawn + Clone + 'static,
        T: Send + Unpin + TryStream<Ok = Vec<u8>> + 'static,
        U: Send + Sink<Vec<u8>> + 'static,
    > FinalizeImmediate<F> for Transport<S, T, U>
{
    type Target = Self;
    type Error = SpawnError;

    fn finalize_immediate(&mut self, fut: F) -> Result<(), SpawnError> {
        self.spawner.spawn(
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

#[cfg(feature = "vessels")]
pub struct ProtocolMveTransport;

#[cfg(feature = "vessels")]
mod vessels {
    use super::{Coalesce, ProtocolMveTransport, Transport, Unravel};
    use erasure_traits::{FramedTransportCoalesce, FramedTransportUnravel};
    use futures::{task::Spawn, Sink, TryStream};

    impl<
            U: TryStream<Ok = Vec<u8>>,
            V: Sink<Vec<u8>>,
            T: protocol::Coalesce<Transport<S, U, V>>,
            S: Spawn + Unpin,
        > FramedTransportCoalesce<T, U, V, S> for ProtocolMveTransport
    where
        T::Future: Unpin,
    {
        type Coalesce = Coalesce<S, U, V, T>;

        fn coalesce(stream: U, sink: V, spawner: S) -> Self::Coalesce {
            Coalesce::new(stream, sink, spawner)
        }
    }

    impl<
            U: TryStream<Ok = Vec<u8>>,
            V: Sink<Vec<u8>>,
            T: protocol::Unravel<Transport<S, U, V>>,
            S: Spawn + Unpin,
        > FramedTransportUnravel<T, U, V, S> for ProtocolMveTransport
    where
        T::Target: Unpin,
        T::Finalize: Unpin,
    {
        type Unravel = Unravel<S, U, V, T>;

        fn unravel(item: T, stream: U, sink: V, spawner: S) -> Self::Unravel {
            Unravel::new(stream, sink, spawner, item)
        }
    }
}
