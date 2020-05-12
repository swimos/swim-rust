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
use tokio_tungstenite::tungstenite::protocol::Message;

use utilities::future::retryable::ResettableFuture;

use crate::connections::ConnectionSender;
use crate::router::{ConnectionRequest, RoutingError};
use utilities::future::retryable::request::{RetrySendError, RetryableRequest, SendResult};

pub(crate) fn new_request(
    sender: mpsc::Sender<ConnectionRequest>,
    payload: Message,
) -> impl ResettableFuture<Output = Result<(), RoutingError>> {
    RetryableRequest::new(
        sender,
        payload,
        |sender, payload, is_retry| {
            acquire_sender(sender, is_retry).then(|r| async move {
                match r {
                    Ok(r) => {
                        let mut sender = r.0;
                        match sender.send_message(payload).await {
                            Ok(res) => Ok((res, r.1)),
                            Err(e) => Err((
                                MpscRetryErr {
                                    kind: RoutingError::ConnectionError,
                                    transient: true,
                                    payload: Some(e.0),
                                },
                                r.1,
                            )),
                        }
                    }
                    Err((mut e, s)) => {
                        e.payload = Some(payload);
                        Err((e, s))
                    }
                }
            })
        },
        |e| e.payload.expect("Missing payload"),
    )
}

async fn acquire_sender(
    mut sender: mpsc::Sender<ConnectionRequest>,
    is_retry: bool,
) -> SendResult<mpsc::Sender<ConnectionRequest>, ConnectionSender, MpscRetryErr> {
    let (connection_tx, connection_rx) = oneshot::channel();

    if sender.send((connection_tx, is_retry)).await.is_err() {
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
    payload: Option<Message>,
}

impl MpscRetryErr {
    fn from(
        kind: RoutingError,
        sender: Option<mpsc::Sender<ConnectionRequest>>,
        payload: Option<Message>,
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
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::protocol::Message;
    use utilities::future::retryable::request::{RetryableRequest, SendResult};

    #[tokio::test]
    async fn send_ok() {
        let (tx, mut rx) = mpsc::channel(5);
        let payload = Message::Text(String::from("Text"));
        let retryable = new_retryable(
            payload.clone(),
            tx,
            |mut sender: mpsc::Sender<Message>, payload, _is_retry| async move {
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
        let payload = Message::Text(String::from("Text"));
        let retryable = new_retryable(
            payload.clone(),
            tx,
            |mut sender: mpsc::Sender<Message>, payload, is_retry| async move {
                if is_retry {
                    let _ = sender.send(payload.clone()).await;
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
        let message = Message::Text(String::from("Text"));
        let retryable = new_retryable(
            message.clone(),
            tx,
            |sender: mpsc::Sender<Message>, payload, _is_retry| async {
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
        payload: Message,
        tx: mpsc::Sender<Message>,
        fac: Fac,
    ) -> Result<(), RoutingError>
    where
        Fac: FnMut(mpsc::Sender<Message>, Message, bool) -> F,
        F: Future<Output = SendResult<mpsc::Sender<Message>, (), MpscRetryErr>>,
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
