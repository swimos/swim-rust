// Copyright 2015-2021 SWIM.AI inc.
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

use crate::configuration::router::RouterParams;
use crate::router::{CloseReceiver, CloseResponseSender, ConnectionRequest, OldConnectionRequest};
use futures::{select, FutureExt, StreamExt};
use swim_common::warp::envelope::Envelope;
use tokio::sync::mpsc;
use tracing::{error, info, span, trace, Level};

use crate::router::retry::new_request;
use swim_common::routing::error::RoutingError;
use swim_common::routing::{RoutingAddr, TaggedEnvelope};
use tokio_stream::wrappers::ReceiverStream;
use utilities::future::retryable::RetryableFuture;
use utilities::FieldKind::Tagged;

//----------------------------------Downlink to Connection Pool---------------------------------

/// Tasks that the outgoing task can handle.
#[derive(Debug)]
enum OutgoingRequest {
    Message(Envelope),
    Close(Option<CloseResponseSender>),
}

/// The outgoing task is responsible for routing messages coming from the
/// subscribers (typically downlinks) or direct messages, to remote hosts.
/// A single outgoing task is responsible for a single remote host.
/// Depending on the retry strategy, the outgoing task may try multiple times to send a message
/// if a non-fatal connection error is encountered.
///
/// Note: The outgoing task *DOES NOT* open connections by default when created.
/// It will only open connections when required.
pub(crate) struct OutgoingHostTask {
    envelope_rx: mpsc::Receiver<Envelope>,
    connection_request_tx: mpsc::Sender<OldConnectionRequest>,
    close_rx: CloseReceiver,
    config: RouterParams,
}

impl OutgoingHostTask {
    pub fn new(
        envelope_rx: mpsc::Receiver<Envelope>,
        connection_request_tx: mpsc::Sender<OldConnectionRequest>,
        close_rx: CloseReceiver,
        config: RouterParams,
    ) -> Self {
        OutgoingHostTask {
            envelope_rx,
            connection_request_tx,
            close_rx,
            config,
        }
    }

    pub async fn run(self) -> Result<(), RoutingError> {
        let OutgoingHostTask {
            envelope_rx,
            connection_request_tx,
            close_rx,
            config,
        } = self;

        let mut close_trigger = close_rx.fuse();
        let mut envelope_rx = ReceiverStream::new(envelope_rx).fuse();

        loop {
            let task = select! {
                closed = &mut close_trigger => {
                    match closed {
                        Ok(tx) => Some(OutgoingRequest::Close(Some((*tx).clone()))),
                        _ => Some(OutgoingRequest::Close(None)),
                    }
                },
                maybe_env = envelope_rx.next() => maybe_env.map(OutgoingRequest::Message),
            }
            .ok_or(RoutingError::ConnectionError)?;

            let span = span!(Level::TRACE, "outgoing_event");
            let _enter = span.enter();

            trace!("Received request {:?}", task);

            match task {
                OutgoingRequest::Message(envelope) => {
                    let message = TaggedEnvelope(RoutingAddr::local(0), envelope);

                    let request = new_request(connection_request_tx.clone(), message.into());

                    RetryableFuture::new(request, config.retry_strategy())
                        .await
                        .map_err(|e| {
                            error!(cause = %e, "Failed to send envelope");
                            e
                        })?;

                    trace!("Completed request");
                }
                OutgoingRequest::Close(close_rx) => {
                    if close_rx.is_some() {
                        info!("Closing");
                        break Ok(());
                    }
                }
            }
            trace!("Completed outgoing request");
        }
    }
}

#[cfg(test)]
mod route_tests {
    use std::num::NonZeroUsize;

    use utilities::future::retryable::strategy::RetryStrategy;

    use super::*;

    use crate::configuration::router::RouterParamBuilder;
    use crate::connections::ConnectionSender;
    use utilities::sync::promise;

    fn router_config(strategy: RetryStrategy) -> RouterParams {
        RouterParamBuilder::new()
            .with_retry_stategy(strategy)
            .build()
    }

    // Test that after a permanent error, the task fails
    #[tokio::test]
    async fn permanent_error() {
        let config = router_config(RetryStrategy::none());
        let (task_request_tx, mut task_request_rx) = mpsc::channel(config.buffer_size().get());
        let (envelope_tx, envelope_rx) = mpsc::channel(config.buffer_size().get());
        let (_close_tx, close_rx) = promise::promise();

        let outgoing_task = OutgoingHostTask::new(envelope_rx, task_request_tx, close_rx, config);
        let handle = tokio::spawn(outgoing_task.run());

        let _ = envelope_tx.send(Envelope::sync("node", "lane")).await;

        let connection_request = task_request_rx.recv().await.unwrap();
        let _ = connection_request
            .request_tx
            .send(Err(RoutingError::ConnectionError));

        let task_result = handle.await.unwrap();
        assert_eq!(task_result, Err(RoutingError::ConnectionError))
    }

    // Test that after a transient error, the retry system attempts the request again and succeeds
    #[tokio::test]
    async fn transient_error() {
        let config = router_config(RetryStrategy::immediate(NonZeroUsize::new(1).unwrap()));
        let (close_tx, close_rx) = promise::promise();

        let (task_request_tx, mut task_request_rx) = mpsc::channel(config.buffer_size().get());
        let (envelope_tx, envelope_rx) = mpsc::channel(config.buffer_size().get());

        let outgoing_task = OutgoingHostTask::new(envelope_rx, task_request_tx, close_rx, config);

        let handle = tokio::spawn(outgoing_task.run());
        let _ = envelope_tx.send(Envelope::sync("node", "lane")).await;

        let connection_request = task_request_rx.recv().await.unwrap();
        let _ = connection_request
            .request_tx
            .send(Err(RoutingError::ConnectionError));

        let connection_request = task_request_rx.recv().await.unwrap();
        let (dummy_tx, _dummy_rx) = mpsc::channel(config.buffer_size().get());

        let _ = connection_request
            .request_tx
            .send(Ok(ConnectionSender::new(dummy_tx)));

        let (response_tx, mut _response_rx) = mpsc::channel(config.buffer_size().get());
        assert!(close_tx.provide(response_tx).is_ok());

        let task_result = handle.await.unwrap();
        assert_eq!(task_result, Ok(()));
    }
}
