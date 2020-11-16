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

use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot};

use utilities::future::retryable::ResettableFuture;

use crate::connections::ConnectionSender;
use crate::router::ConnectionRequest;
use futures::task::{Context, Poll};
use futures::Future;
use pin_project::pin_project;
use std::pin::Pin;
use swim_common::routing::RoutingError;
use utilities::future::retryable::request::{RetrySendError, RetryableRequest, SendResult};

use swim_common::ws::WsMessage;
use tracing::trace;

#[pin_project]
struct LoggingRetryable<F> {
    #[pin]
    f: F,
}

impl<F> Future for LoggingRetryable<F>
where
    F: ResettableFuture + Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().f.poll(cx)
    }
}

impl<F> ResettableFuture for LoggingRetryable<F>
where
    F: ResettableFuture,
{
    fn reset(self: Pin<&mut Self>) -> bool {
        let result = self.project().f.reset();
        if result {
            trace!("Request failed. Retrying");
        } else {
            trace!("Request failed. Couldn't reset request. Not retrying");
        }

        result
    }
}

pub(crate) fn new_request(
    sender: mpsc::Sender<ConnectionRequest>,
    payload: WsMessage,
) -> impl ResettableFuture<Output = Result<(), RoutingError>> {
    let retryable = RetryableRequest::new(
        sender,
        payload,
        |sender, payload, is_retry| {
            acquire_sender(sender, is_retry).then(|r| async move {
                match r {
                    Ok(r) => {
                        let mut connection_sender = r.0;
                        let request_sender = r.1;

                        match connection_sender.send_message(payload).await {
                            Ok(result) => Ok((result, request_sender)),
                            Err(e) => {
                                let payload = e.0;
                                Err((
                                    MpscRetryErr {
                                        kind: RoutingError::ConnectionError,
                                        transient: true,
                                        payload: Some(payload),
                                    },
                                    request_sender,
                                ))
                            }
                        }
                    }
                    Err((mut error, sender)) => {
                        error.payload = Some(payload);
                        Err((error, sender))
                    }
                }
            })
        },
        |e| match e.payload {
            Some(payload) => payload,
            None => {
                // The payload is set after the request is reset so this isn't reachable.
                unreachable!()
            }
        },
    );

    LoggingRetryable { f: retryable }
}

async fn acquire_sender(
    sender: mpsc::Sender<ConnectionRequest>,
    is_retry: bool,
) -> SendResult<mpsc::Sender<ConnectionRequest>, ConnectionSender, MpscRetryErr> {
    let (connection_tx, connection_rx) = oneshot::channel();

    if sender
        .send(ConnectionRequest::new(connection_tx, is_retry))
        .await
        .is_err()
    {
        return MpscRetryErr::from(RoutingError::ConnectionError, Some(sender), None);
    }

    match connection_rx.await {
        Ok(r) => match r {
            Ok(r) => Ok((r, Some(sender))),
            Err(e) => MpscRetryErr::from(e, Some(sender), None),
        },
        Err(_) => MpscRetryErr::from(RoutingError::ConnectionError, Some(sender), None),
    }
}

#[derive(Clone)]
struct MpscRetryErr {
    kind: RoutingError,
    transient: bool,
    payload: Option<WsMessage>,
}

impl MpscRetryErr {
    fn from(
        kind: RoutingError,
        sender: Option<mpsc::Sender<ConnectionRequest>>,
        payload: Option<WsMessage>,
    ) -> SendResult<mpsc::Sender<ConnectionRequest>, ConnectionSender, MpscRetryErr> {
        let transient = kind.is_transient();

        Err((
            MpscRetryErr {
                kind,
                transient,
                payload,
            },
            sender,
        ))
    }
}

impl RetrySendError for MpscRetryErr {
    type ErrKind = RoutingError;

    fn is_transient(&self) -> bool {
        self.transient
    }

    fn kind(&self) -> Self::ErrKind {
        self.kind.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use utilities::future::retryable::strategy::RetryStrategy;
    use utilities::future::retryable::RetryableFuture;

    use crate::router::retry::MpscRetryErr;
    use crate::router::RoutingError;
    use futures::Future;
    use swim_common::ws::WsMessage;
    use tokio::sync::mpsc;
    use utilities::future::retryable::request::{RetryableRequest, SendResult};

    #[tokio::test]
    async fn send_ok() {
        let (tx, mut rx) = mpsc::channel(5);
        let payload = WsMessage::Text(String::from("Text"));
        let retryable = new_retryable(
            payload.clone(),
            tx,
            |sender: mpsc::Sender<WsMessage>, payload, _is_retry| async move {
                let _ = sender.send(payload.clone()).await;
                Ok(((), Some(sender)))
            },
        );

        assert_eq!(retryable.await, Ok(()));
        assert_eq!(rx.recv().await.unwrap(), payload);
    }

    #[tokio::test]
    async fn recovers() {
        let (tx, mut rx) = mpsc::channel(5);
        let payload = WsMessage::Text(String::from("Text"));
        let retryable = new_retryable(
            payload.clone(),
            tx,
            |sender: mpsc::Sender<WsMessage>, payload, is_retry| async move {
                if is_retry {
                    let _ = sender.send(payload.clone().into()).await;
                    Ok(((), Some(sender)))
                } else {
                    Err((
                        MpscRetryErr {
                            kind: RoutingError::ConnectionError,
                            transient: true,
                            payload: Some(payload),
                        },
                        Some(sender),
                    ))
                }
            },
        );

        assert_eq!(retryable.await, Ok(()));
        assert_eq!(rx.recv().await.unwrap(), payload);
    }

    #[tokio::test]
    async fn errors() {
        let (tx, _rx) = mpsc::channel(5);
        let message = WsMessage::Text(String::from("Text"));
        let retryable = new_retryable(
            message.clone(),
            tx,
            |sender: mpsc::Sender<WsMessage>, payload, _is_retry| async {
                Err((
                    MpscRetryErr {
                        kind: RoutingError::ConnectionError,
                        transient: true,
                        payload: Some(payload),
                    },
                    Some(sender),
                ))
            },
        );

        assert_eq!(retryable.await, Err(RoutingError::ConnectionError))
    }

    async fn new_retryable<Fac, F>(
        payload: WsMessage,
        tx: mpsc::Sender<WsMessage>,
        fac: Fac,
    ) -> Result<(), RoutingError>
    where
        Fac: FnMut(mpsc::Sender<WsMessage>, WsMessage, bool) -> F,
        F: Future<Output = SendResult<mpsc::Sender<WsMessage>, (), MpscRetryErr>>,
    {
        let retryable =
            RetryableRequest::new(tx, payload, fac, |e| e.payload.expect("Missing payload"));

        RetryableFuture::new(
            retryable,
            RetryStrategy::immediate(NonZeroUsize::new(3).unwrap()),
        )
        .await
    }
}
