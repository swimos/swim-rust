// Copyright 2015-2020 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::ready;
use futures::Future;
use futures::FutureExt;
use futures_util::task::{Context, Poll};
use tokio::macros::support::Pin;
use tokio::time;

use pin_project::{pin_project, project};
pub use strategy::RetryStrategy;

mod strategy;

#[cfg(test)]
mod tests;

/// A retryable request that will attempt to fulfil the request using the retry strategy provided.
/// Transient errors, such as a connection error will be retried but permanent errors such as sender
/// being closed will cause the request to be cancelled straight away.
#[pin_project]
pub struct RetryableRequest<'fut, S, V>
where
    S: RetrySink<'fut, V> + Unpin,
    V: Send,
{
    #[pin]
    sink: S,
    value: V,
    strategy: RetryStrategy,
    #[pin]
    state: RetryState<S::Future>,
    #[pin]
    ctx: RetryContext,
}

#[pin_project]
pub struct RetryContext {
    recreate: bool,
}

impl<'fut, S, V> RetryableRequest<'fut, S, V>
where
    S: RetrySink<'fut, V> + Unpin,
    V: Send + Clone,
{
    pub fn send(sink: S, value: V, strategy: RetryStrategy) -> RetryableRequest<'fut, S, V> {
        RetryableRequest {
            sink,
            value,
            strategy,
            state: RetryState::NotStarted,
            ctx: RetryContext { recreate: false },
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum RetryErr {
    SenderClosed,
    ConnectionError,
    HostUnavailable,
}

impl<'fut, S, V> Future for RetryableRequest<'fut, S, V>
where
    S: RetrySink<'fut, V, Error = RetryErr> + Unpin,
    V: Send + Clone,
{
    type Output = Result<(), RetryErr>;

    #[project]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut this = self.as_mut().project();

            #[project]
            match this.state.project() {
                RetryState::NotStarted => {
                    let mut sink = this.sink;
                    let value = this.value.clone();
                    let r = sink.send_value(value, &this.ctx);
                    let new_state = RetryState::Pending(r);

                    this.ctx.recreate = false;
                    self.as_mut().project().state.set(new_state);
                }
                RetryState::Pending(mut fut) => {
                    match ready!(fut.poll_unpin(cx)) {
                        Ok(_) => {
                            return Poll::Ready(Ok(()));
                        }
                        Err(e) => {
                            let new_state = match e {
                                // The router has signed that this request should be cancelled. Return with
                                // an error that will be sunk by the router
                                RetryErr::ConnectionError => {
                                    return Poll::Ready(Err(RetryErr::ConnectionError));
                                }
                                // If the maximum number of requests has been reached, then this error
                                // will be returned and forwarded to the downlink for a
                                RetryErr::HostUnavailable | RetryErr::SenderClosed => {
                                    this.ctx.recreate = true;
                                    RetryState::Retrying(RetryErr::HostUnavailable)
                                }
                            };

                            self.as_mut().project().state.set(new_state);
                        }
                    };
                }
                RetryState::Retrying(err) => match this.strategy.next() {
                    Some(duration) => match duration {
                        Some(duration) => {
                            self.as_mut()
                                .project()
                                .state
                                .set(RetryState::Sleeping(time::delay_for(duration)));
                        }
                        None => {
                            self.as_mut().project().state.set(RetryState::NotStarted);
                        }
                    },
                    None => {
                        // Return the previous error and complete
                        return Poll::Ready(Err(err.clone()));
                    }
                },
                RetryState::Sleeping(timer) => {
                    ready!(timer.poll(cx));
                    self.as_mut().project().state.set(RetryState::NotStarted);
                }
            }
        }
    }
}

/// The current state of a retry attempt
#[pin_project]
enum RetryState<F> {
    /// The current retry has not yet been started.
    NotStarted,
    /// A retry has been started and the future [`F`] is currently pending.
    Pending(#[pin] F),
    /// A retry is going to be attempted with the [`RetryStrategy`]. If there are no retries left, then
    /// the [`RetryErr`] is returned
    Retrying(RetryErr),
    /// The given [`RetryStrategy`] needs to sleep between retries for [`Delay`].
    Sleeping(#[pin] time::Delay),
}

pub trait RetrySink<'fut, V>
where
    V: Send,
{
    type Error;
    type Future: Future<Output = Result<(), Self::Error>> + Send + Unpin + 'fut;

    fn send_value(self: &mut Self, value: V, ctx: &RetryContext) -> Self::Future;
}

pub mod boxed_connection_sender {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures::ready;
    use futures::{Future, FutureExt};
    use futures_util::future::BoxFuture;
    use futures_util::task::Waker;
    use tokio::sync::mpsc::error::TrySendError;
    use tokio::sync::oneshot;
    use tokio_tungstenite::tungstenite::protocol::Message;

    use crate::connections::ConnectionSender;
    use crate::router::outgoing::retry::{RetryContext, RetryErr, RetrySink};
    use crate::router::{ConnReqSender, RoutingError};

    /// A boxed [`connections::ConnectionSender`] [`RetrySink`] that is backed by an [`mpsc::Sender`]
    /// Between retry attempts, the sender will attempt to acquire a [`ConnectionSender`] to fulfil
    /// the request using the given [`oneshot::Sender`] instance. If when requesting a new
    /// conenction sender the router returns [`RoutingError::ConnectionError`] then the request is
    /// cancelled as the connection pool has failed to acquire an active connection due to an
    /// unrecoverable error. However, if the router returns [`RoutingError::Transient`] then it is
    /// assumed that the error may resolve.
    pub struct BoxedConnSender {
        sender: ConnReqSender,
        host: url::Url,
    }

    impl BoxedConnSender {
        pub fn new(sender: ConnReqSender, host: url::Url) -> BoxedConnSender {
            BoxedConnSender { sender, host }
        }
    }

    impl<'fut> RetrySink<'fut, Message> for BoxedConnSender {
        type Error = RetryErr;
        type Future = RequestFuture<'fut>;

        fn send_value(&mut self, value: Message, ctx: &RetryContext) -> Self::Future {
            RequestFuture::new(self.sender.clone(), self.host.clone(), value, ctx.recreate)
        }
    }

    impl<'a> RequestFuture<'a> {
        fn new(
            sender: ConnReqSender,
            host: url::Url,
            value: Message,
            recreate: bool,
        ) -> RequestFuture<'a> {
            RequestFuture {
                sender,
                host,
                value,
                state: State::NotStarted,
                recreate,
            }
        }
    }

    pub struct RequestFuture<'a> {
        sender: ConnReqSender,
        host: url::Url,
        value: Message,
        state: State<'a>,
        recreate: bool,
    }

    enum State<'a> {
        NotStarted,
        AcquiringSender(BoxFuture<'a, Result<ConnectionSender, RoutingError>>),
        Sending(ConnectionSender),
    }

    impl<'a> RequestFuture<'a> {
        fn request_connection(
            &mut self,
            recreate: bool,
            waker: Waker,
        ) -> BoxFuture<'a, Result<ConnectionSender, RoutingError>> {
            let mut sender = self.sender.clone();
            let host = self.host.clone();

            let a = async move {
                let (connection_tx, connection_rx) = oneshot::channel();
                sender
                    .send((host, connection_tx, recreate))
                    .await
                    .map_err(|_| RoutingError::ConnectionError)?;

                let result = connection_rx
                    .await
                    .map_err(|_| RoutingError::ConnectionError)?;

                waker.wake();

                result
            };

            // todo: Remove boxing as it is back with `dyn`
            FutureExt::boxed(a)
        }
    }

    impl<'a> Future for RequestFuture<'a> {
        type Output = Result<(), RetryErr>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let recreate = self.recreate;

            let f = self.request_connection(recreate, cx.waker().clone());
            let RequestFuture { state, value, .. } = self.get_mut();

            if let State::NotStarted = state {
                *state = State::AcquiringSender(f);
            }

            match state {
                State::NotStarted => unreachable!(),
                State::AcquiringSender(f) => match ready!((*f).poll_unpin(cx)) {
                    Ok(sender) => {
                        *state = State::Sending(sender);
                        Poll::Pending
                    }
                    Err(e) => {
                        match e {
                            // Router has indicated that the request should be cancelled or an
                            // unrecoverable error occured
                            RoutingError::ConnectionError => {
                                Poll::Ready(Err(RetryErr::ConnectionError))
                            }
                            _ => Poll::Ready(Err(RetryErr::HostUnavailable)),
                        }
                    }
                },
                State::Sending(ref mut sender) => match sender.poll_ready(cx) {
                    Poll::Ready(Ok(_)) => match sender.try_send(value.clone()) {
                        Ok(_) => Poll::Ready(Ok(())),
                        Err(TrySendError::Closed(_)) => Poll::Ready(Err(RetryErr::SenderClosed)),
                        Err(TrySendError::Full(_)) => unreachable!(),
                    },

                    Poll::Ready(Err(_)) => Poll::Ready(Err(RetryErr::SenderClosed)),
                    Poll::Pending => Poll::Pending,
                },
            }
        }
    }
}

#[allow(dead_code)]
pub mod boxedmpsc {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures::Future;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TrySendError;

    use crate::router::outgoing::retry::{RetryContext, RetryErr, RetrySink};

    pub struct BoxedMpscSender<V, P>
    where
        P: Fn() -> mpsc::Sender<V>,
    {
        producer: P,
    }

    impl<V, P> BoxedMpscSender<V, P>
    where
        P: Fn() -> mpsc::Sender<V>,
    {
        pub fn new(producer: P) -> BoxedMpscSender<V, P> {
            BoxedMpscSender { producer }
        }
    }

    impl<'fut, V: 'fut, P> RetrySink<'fut, V> for BoxedMpscSender<V, P>
    where
        V: Send + Clone,
        P: Fn() -> mpsc::Sender<V>,
    {
        type Error = RetryErr;
        type Future = MpscFuture<V>;

        fn send_value(&mut self, value: V, _ctx: &RetryContext) -> Self::Future {
            MpscFuture::new((self.producer)(), value)
        }
    }

    impl<'l, V> MpscFuture<V> {
        fn new(sender: mpsc::Sender<V>, value: V) -> MpscFuture<V> {
            MpscFuture { sender, value }
        }
    }

    pub struct MpscFuture<V> {
        sender: mpsc::Sender<V>,
        value: V,
    }

    impl<V> Unpin for MpscFuture<V> {}

    impl<V> Future for MpscFuture<V>
    where
        V: Clone,
    {
        type Output = Result<(), RetryErr>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let MpscFuture { sender, value } = self.get_mut();

            match sender.poll_ready(cx) {
                Poll::Ready(Ok(_)) => match sender.try_send(value.clone()) {
                    Ok(_) => Poll::Ready(Ok(())),
                    Err(TrySendError::Closed(_)) => Poll::Ready(Err(RetryErr::SenderClosed)),
                    Err(TrySendError::Full(_)) => unreachable!(),
                },
                Poll::Ready(Err(_)) => Poll::Ready(Err(RetryErr::SenderClosed)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    #[tokio::test]
    async fn simple_send() {
        use super::RetryableRequest;

        let payload = 5;
        let (tx, mut rx) = mpsc::channel(5);
        let result = RetryableRequest::send(
            BoxedMpscSender::new(|| tx.clone()),
            payload,
            Default::default(),
        )
        .await;

        assert_eq!(result.is_ok(), true);
        assert_eq!(rx.recv().await.unwrap(), payload);
    }
}
