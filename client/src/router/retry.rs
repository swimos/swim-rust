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
use crate::router::ConnectionRequest;
use utilities::err::MaybeTransientErr;
use utilities::future::retryable::request::{RetryableRequest, SendResult};

pub fn new_request(
    sender: mpsc::Sender<ConnectionRequest>,
    payload: Message,
) -> impl ResettableFuture<Output = Result<(), MpscRetryErr<Message>>> {
    RetryableRequest::new(
        sender,
        payload,
        |sender, payload, is_retry| {
            acquire_sender(sender, is_retry).then(|r| async move {
                match r {
                    Ok(r) => {
                        let mut sender = r.0;
                        match sender.send_message(payload).await {
                            Ok(_) => Ok(((), r.1)),
                            Err(e) => {
                                return Err((MpscRetryErr::Transient(Some(e.0)), r.1));
                            }
                        }
                    }
                    Err(mut e) => {
                        if let MpscRetryErr::Transient(ref mut o) = e.0 {
                            o.replace(payload);
                        }

                        Err(e)
                    }
                }
            })
        },
        |e| match e {
            MpscRetryErr::Transient(s) => match s {
                Some(m) => m,
                None => unreachable!("Missing payload"),
            },
            _ => unreachable!("Incorrect routing error returned"),
        },
    )
}

pub(crate) async fn acquire_sender<T>(
    mut sender: mpsc::Sender<ConnectionRequest>,
    is_retry: bool,
) -> SendResult<mpsc::Sender<ConnectionRequest>, ConnectionSender, MpscRetryErr<T>> {
    let (connection_tx, connection_rx) = oneshot::channel();

    if sender.send((connection_tx, is_retry)).await.is_err() {
        return MpscRetryErr::transient(sender);
    }

    match connection_rx.await {
        Ok(r) => match r {
            Ok(r) => Ok((r, Some(sender))),
            Err(e) => {
                if e.is_transient() {
                    MpscRetryErr::transient(sender)
                } else {
                    MpscRetryErr::permanent(sender)
                }
            }
        },
        Err(_) => MpscRetryErr::transient(sender),
    }
}

#[derive(Eq, PartialEq, Debug)]
pub enum MpscRetryErr<T> {
    Transient(Option<T>),
    Permanent,
    Failed,
}

impl<T> MpscRetryErr<T> {
    fn transient(
        sender: mpsc::Sender<ConnectionRequest>,
    ) -> SendResult<mpsc::Sender<ConnectionRequest>, ConnectionSender, MpscRetryErr<T>> {
        Err((MpscRetryErr::Transient(None), Some(sender)))
    }

    fn permanent(
        sender: mpsc::Sender<ConnectionRequest>,
    ) -> SendResult<mpsc::Sender<ConnectionRequest>, ConnectionSender, MpscRetryErr<T>> {
        Err((MpscRetryErr::Permanent, Some(sender)))
    }
}

impl<T> MaybeTransientErr for MpscRetryErr<T> {
    fn is_transient(&self) -> bool {
        match *self {
            MpscRetryErr::Transient(..) => true,
            MpscRetryErr::Permanent => false,
            MpscRetryErr::Failed => false,
        }
    }

    fn permanent(&self) -> Self {
        MpscRetryErr::Failed
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use utilities::future::retryable::strategy::RetryStrategy;
    use utilities::future::retryable::RetryableFuture;

    use crate::router::retry::MpscRetryErr;
    use futures::Future;
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::protocol::Message;
    use utilities::future::retryable::request::{RetryableRequest, SendResult};

    #[tokio::test]
    async fn simple_send() {
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
                    Err((MpscRetryErr::Transient(Some(payload)), Some(sender)))
                }
            },
        );

        assert_eq!(retryable.await, Ok(()));
        assert_eq!(rx.recv().await.unwrap(), payload);
    }

    #[tokio::test]
    async fn permanent_error() {
        let (tx, _rx) = mpsc::channel(5);
        let message = Message::Text(String::from("Text"));
        let retryable = new_retryable(
            message.clone(),
            tx,
            |sender: mpsc::Sender<Message>, payload, _is_retry| async {
                Err((MpscRetryErr::Transient(Some(payload)), Some(sender)))
            },
        );

        assert_eq!(retryable.await, Err(MpscRetryErr::Failed))
    }

    async fn new_retryable<Fac, F, T>(
        payload: T,
        tx: mpsc::Sender<T>,
        fac: Fac,
    ) -> Result<(), MpscRetryErr<T>>
    where
        Fac: FnMut(mpsc::Sender<T>, T, bool) -> F,
        F: Future<Output = SendResult<mpsc::Sender<T>, (), MpscRetryErr<T>>>,
    {
        let retryable = RetryableRequest::new(tx, payload, fac, |e| match e {
            MpscRetryErr::Transient(s) => match s {
                Some(m) => m,
                None => unreachable!("Missing payload"),
            },
            _ => unreachable!("Incorrect routing error returned"),
        });

        RetryableFuture::new(
            retryable,
            RetryStrategy::immediate(NonZeroUsize::new(3).unwrap()),
        )
        .await
    }
}
