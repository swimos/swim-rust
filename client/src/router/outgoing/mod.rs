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

use std::collections::HashMap;

use futures::{stream, StreamExt, TryFutureExt};
use futures_util::FutureExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use common::warp::envelope::Envelope;

use crate::configuration::router::RouterParams;
use crate::router::incoming::IncomingTaskReqSender;
use crate::router::retry::RetryableRequest;
use crate::router::{
    CloseRequestReceiver, CloseRequestSender, ConnReqSender, ConnectionResponse, Host, RoutingError,
};
use utilities::future::retryable::RetryableFuture;

//----------------------------------Downlink to Connection Pool---------------------------------

pub struct OutgoingRequest {
    host: Host,
    outgoing_tx: oneshot::Sender<mpsc::Sender<Envelope>>,
}

impl OutgoingRequest {
    pub fn new(host: Host, outgoing_tx: oneshot::Sender<mpsc::Sender<Envelope>>) -> Self {
        OutgoingRequest { host, outgoing_tx }
    }
}

pub type OutgoingTaskReqSender = mpsc::Sender<OutgoingRequest>;
type OutgoingTaskReqReceiver = mpsc::Receiver<OutgoingRequest>;

#[cfg(test)]
mod tests;

pub struct OutgoingTask {
    connection_request_tx: ConnReqSender,
    task_request_rx: OutgoingTaskReqReceiver,
    close_request_rx: CloseRequestReceiver,
    config: RouterParams,
    incoming_task_request_tx: IncomingTaskReqSender,
}

impl OutgoingTask {
    pub fn new(
        connection_request_tx: ConnReqSender,
        incoming_task_request_tx: IncomingTaskReqSender,
        config: RouterParams,
    ) -> (Self, OutgoingTaskReqSender, CloseRequestSender) {
        let (task_request_tx, task_request_rx) = mpsc::channel(config.buffer_size().get());
        let (close_request_tx, close_request_rx) = mpsc::channel(config.buffer_size().get());

        (
            OutgoingTask {
                connection_request_tx,
                task_request_rx,
                close_request_rx,
                config,
                incoming_task_request_tx,
            },
            task_request_tx,
            close_request_tx,
        )
    }

    pub async fn run(self) -> Result<(), RoutingError> {
        let OutgoingTask {
            connection_request_tx,
            task_request_rx,
            close_request_rx,
            config,
            incoming_task_request_tx,
        } = self;
        let mut outgoing_host_tasks: HashMap<String, (mpsc::Sender<Envelope>, mpsc::Sender<()>)> =
            HashMap::new();
        let mut outgoing_host_tasks_handlers: HashMap<String, JoinHandle<()>> = HashMap::new();

        let mut rx = combine_outgoing_streams(task_request_rx, close_request_rx);

        loop {
            let task = rx.next().await.ok_or(RoutingError::ConnectionError)?;

            match task {
                TaskRequestType::NewTask(OutgoingRequest {
                    host,
                    outgoing_tx: task_tx,
                }) => {
                    if !outgoing_host_tasks.contains_key(&host) {
                        let (outgoing_host_task, envelope_tx, close_tx) = OutgoingHostTask::new(
                            host.clone(),
                            connection_request_tx.clone(),
                            config,
                        );

                        outgoing_host_tasks.insert(host.clone(), (envelope_tx, close_tx));

                        let mut complete_sender = incoming_task_request_tx.clone();

                        let host_temp = host.clone();
                        let outgoing_host_task = outgoing_host_task.run().then(|r| async move {
                            // If the task failed, then notify the RouteHostMessagesTask so it can be handled
                            if r.is_err() {
                                let _ = complete_sender
                                    .send(ConnectionResponse::Failure(host_temp))
                                    .await;
                            }
                        });

                        let task_handle = tokio::spawn(outgoing_host_task);
                        outgoing_host_tasks_handlers.insert(host.clone(), task_handle);
                    }

                    let (envelope_tx, _) = outgoing_host_tasks
                        .get(&host.to_string())
                        .ok_or(RoutingError::ConnectionError)?
                        .clone();

                    task_tx
                        .send(envelope_tx)
                        .map_err(|_| RoutingError::ConnectionError)?;
                }

                TaskRequestType::Close => {
                    for (_, (_, mut close_tx)) in outgoing_host_tasks {
                        close_tx
                            .send(())
                            .await
                            .map_err(|_| RoutingError::ConnectionError)?;
                    }

                    for (_, task_handler) in outgoing_host_tasks_handlers {
                        task_handler
                            .await
                            .map_err(|_| RoutingError::ConnectionError)?;
                    }

                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

enum TaskRequestType {
    NewTask(OutgoingRequest),
    NewMessage(Envelope),
    Close,
}

fn combine_outgoing_streams(
    task_request_rx: OutgoingTaskReqReceiver,
    close_requests_rx: CloseRequestReceiver,
) -> impl stream::Stream<Item = TaskRequestType> + Send + 'static {
    let task_request = task_request_rx.map(TaskRequestType::NewTask);
    let close_request = close_requests_rx.map(|_| TaskRequestType::Close);
    stream::select(task_request, close_request)
}

struct OutgoingHostTask {
    host: Host,
    envelope_rx: mpsc::Receiver<Envelope>,
    close_rx: mpsc::Receiver<()>,
    connection_request_tx: ConnReqSender,
    config: RouterParams,
}

impl OutgoingHostTask {
    fn new(
        host: Host,
        connection_request_tx: ConnReqSender,
        config: RouterParams,
    ) -> (Self, mpsc::Sender<Envelope>, mpsc::Sender<()>) {
        let (envelope_tx, envelope_rx) = mpsc::channel(config.buffer_size().get());
        let (close_tx, close_rx) = mpsc::channel(config.buffer_size().get());
        (
            OutgoingHostTask {
                host,
                envelope_rx,
                close_rx,
                connection_request_tx,
                config,
            },
            envelope_tx,
            close_tx,
        )
    }

    async fn run(self) -> Result<(), RoutingError> {
        let OutgoingHostTask {
            host,
            envelope_rx,
            close_rx,
            connection_request_tx,
            config,
        } = self;

        let mut rx = combine_outgoing_host_streams(envelope_rx, close_rx);

        loop {
            let task = rx.next().await.ok_or(RoutingError::ConnectionError)?;
            tracing::trace!("Received request");

            match task {
                TaskRequestType::NewMessage(envelope) => {
                    let message = Message::Text(envelope.into_value().to_string()).to_string();
                    let message = message.as_str();
                    let retryable = RetryableRequest::new(|is_retry| {
                        let mut sender = connection_request_tx.clone();
                        let host = host.clone();

                        async move {
                            let (connection_tx, connection_rx) = oneshot::channel();

                            sender
                                .send((host, connection_tx, is_retry))
                                .await
                                .map_err(|_| RoutingError::ConnectionError)?;

                            connection_rx
                                .await
                                .map_err(|_| RoutingError::ConnectionError)?
                        }
                        .and_then(|mut s| async move {
                            s.send_message(&message)
                                .map_err(|_| RoutingError::ConnectionError)
                                .await
                        })
                    });

                    let retry = RetryableFuture::new(retryable, config.retry_strategy());
                    retry.await.map_err(|_| RoutingError::ConnectionError)?;
                }
                TaskRequestType::Close => {
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn combine_outgoing_host_streams(
    envelope_rx: mpsc::Receiver<Envelope>,
    close_requests_rx: CloseRequestReceiver,
) -> impl stream::Stream<Item = TaskRequestType> + Send + 'static {
    let envelope_request = envelope_rx.map(TaskRequestType::NewMessage);
    let close_request = close_requests_rx.map(|_| TaskRequestType::Close);
    stream::select(envelope_request, close_request)
}

#[cfg(test)]
mod route_tests {
    use super::*;

    use crate::configuration::router::RouterParamBuilder;
    use crate::connections::ConnectionSender;
    use std::num::NonZeroUsize;
    use utilities::future::retryable::strategy::RetryStrategy;

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
        let (host_route_task, mut envelope_tx, _close_tx) =
            OutgoingHostTask::new(String::from("ws://127.0.0.1:9001"), task_request_tx, config);
        let handle = tokio::spawn(host_route_task.run());

        let _ = envelope_tx
            .send(Envelope::sync("node".into(), "lane".into()))
            .await;
        let (_url, tx, _recreate) = task_request_rx.recv().await.unwrap();
        let _ = tx.send(Err(RoutingError::ConnectionError));

        let task_result = handle.await.unwrap();
        assert_eq!(task_result, Err(RoutingError::ConnectionError))
    }

    // Test that after a transient error, the retry system attempts the request again and succeeds
    #[tokio::test]
    async fn transient_error() {
        let config = router_config(RetryStrategy::immediate(NonZeroUsize::new(1).unwrap()));

        let (task_request_tx, mut task_request_rx) = mpsc::channel(config.buffer_size().get());
        let (host_route_task, mut envelope_tx, mut close_tx) =
            OutgoingHostTask::new(String::from("ws://127.0.0.1:9001"), task_request_tx, config);

        let handle = tokio::spawn(host_route_task.run());
        let _ = envelope_tx
            .send(Envelope::sync("node".into(), "lane".into()))
            .await;

        let (_url, tx, _recreate) = task_request_rx.recv().await.unwrap();
        let _ = tx.send(Err(RoutingError::Transient));

        let (_url, tx, _recreate) = task_request_rx.recv().await.unwrap();
        let (dummy_tx, _dummy_rx) = mpsc::channel(config.buffer_size().get());

        let _ = tx.send(Ok(ConnectionSender::new(dummy_tx)));
        let _ = close_tx.send(()).await;

        let task_result = handle.await.unwrap();
        assert_eq!(task_result, Ok(()))
    }
}
