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

use futures::StreamExt;
use tokio::sync::mpsc;

use common::warp::envelope::Envelope;

use crate::configuration::router::RouterParams;
use crate::router::retry::new_request;
use crate::router::{ConnectionRequest, RoutingError};
use tokio_tungstenite::tungstenite::protocol::Message;
use utilities::future::retryable::RetryableFuture;

//----------------------------------Downlink to Connection Pool---------------------------------

#[cfg(test)]
mod tests;

enum OutgoingRequest {
    Message(Envelope),
    _Close,
}

pub struct OutgoingHostTask {
    envelope_rx: mpsc::Receiver<Envelope>,
    connection_request_tx: mpsc::Sender<ConnectionRequest>,
    config: RouterParams,
}

impl OutgoingHostTask {
    pub fn new(
        envelope_rx: mpsc::Receiver<Envelope>,
        connection_request_tx: mpsc::Sender<ConnectionRequest>,
        config: RouterParams,
    ) -> Self {
        OutgoingHostTask {
            envelope_rx,
            connection_request_tx,
            config,
        }
    }

    pub async fn run(self) -> Result<(), RoutingError> {
        let OutgoingHostTask {
            envelope_rx,
            connection_request_tx,
            config,
        } = self;

        let mut rx = envelope_rx.map(OutgoingRequest::Message);

        loop {
            let task = rx.next().await.ok_or(RoutingError::ConnectionError)?;
            tracing::trace!("Received request");

            match task {
                OutgoingRequest::Message(envelope) => {
                    let message = Message::Text(envelope.into_value().to_string());
                    let request = new_request(connection_request_tx.clone(), message);
                    let retry = RetryableFuture::new(request, config.retry_strategy());

                    retry.await.map_err(|_| RoutingError::ConnectionError)?;
                    tracing::trace!("Completed request");
                }
                OutgoingRequest::_Close => {
                    break;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod route_tests {
    use std::num::NonZeroUsize;

    use utilities::future::retryable::strategy::RetryStrategy;

    use crate::configuration::router::RouterParamBuilder;
    use crate::connections::ConnectionSender;

    use super::*;

    fn router_config(strategy: RetryStrategy) -> RouterParams {
        RouterParamBuilder::default()
            .with_buffer_size(5)
            .with_idle_timeout(10)
            .with_conn_reaper_frequency(10)
            .with_retry_stategy(strategy)
            .build()
    }

    // Test that after a permanent error, the task fails
    #[tokio::test]
    async fn permanent_error() {
        let config = router_config(RetryStrategy::none());
        let (task_request_tx, mut task_request_rx) = mpsc::channel(config.buffer_size().get());
        let (mut envelope_tx, envelope_rx) = mpsc::channel(config.buffer_size().get());

        let outgoing_task = OutgoingHostTask::new(envelope_rx, task_request_tx, config);
        let handle = tokio::spawn(outgoing_task.run());

        let _ = envelope_tx
            .send(Envelope::sync("node".into(), "lane".into()))
            .await;
        let (tx, _recreate) = task_request_rx.recv().await.unwrap();
        let _ = tx.send(Err(RoutingError::ConnectionError));

        let task_result = handle.await.unwrap();
        assert_eq!(task_result, Err(RoutingError::ConnectionError))
    }

    // Todo broken until close method is fixed.
    // Test that after a transient error, the retry system attempts the request again and succeeds
    #[tokio::test]
    #[ignore]
    async fn transient_error() {
        let config = router_config(RetryStrategy::immediate(NonZeroUsize::new(1).unwrap()));

        let (task_request_tx, mut task_request_rx) = mpsc::channel(config.buffer_size().get());
        let (mut envelope_tx, envelope_rx) = mpsc::channel(config.buffer_size().get());

        let outgoing_task = OutgoingHostTask::new(envelope_rx, task_request_tx, config);

        let handle = tokio::spawn(outgoing_task.run());
        let _ = envelope_tx
            .send(Envelope::sync("node".into(), "lane".into()))
            .await;

        let (tx, _recreate) = task_request_rx.recv().await.unwrap();
        let _ = tx.send(Err(RoutingError::Transient));

        let (tx, _recreate) = task_request_rx.recv().await.unwrap();
        let (dummy_tx, _dummy_rx) = mpsc::channel(config.buffer_size().get());

        let _ = tx.send(Ok(ConnectionSender::new(dummy_tx)));
        // let _ = close_tx.send(()).await;

        let task_result = handle.await.unwrap();
        assert_eq!(task_result, Ok(()))
    }
}
