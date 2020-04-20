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

use futures::{stream, StreamExt};
use futures_util::FutureExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use common::warp::envelope::Envelope;

use crate::router::configuration::RouterConfig;
use crate::router::envelope_routing_task::retry::boxed_connection_sender::BoxedConnSender;
use crate::router::envelope_routing_task::retry::RetryableRequest;
use crate::router::message_routing_task::{ConnectionResponse, HostMessageNewTaskRequestSender};
use crate::router::{CloseRequestReceiver, CloseRequestSender, ConnReqSender, RoutingError};

//----------------------------------Downlink to Connection Pool---------------------------------

pub type HostEnvelopeTaskRequestSender =
    mpsc::Sender<(url::Url, oneshot::Sender<mpsc::Sender<Envelope>>)>;

type HostEnvelopeTaskRequestReceiver =
    mpsc::Receiver<(url::Url, oneshot::Sender<mpsc::Sender<Envelope>>)>;

pub(crate) mod retry;

#[cfg(test)]
mod tests;

pub struct RequestEnvelopeRoutingHostTask {
    connection_request_tx: ConnReqSender,
    task_request_rx: HostEnvelopeTaskRequestReceiver,
    close_request_rx: CloseRequestReceiver,
    config: RouterConfig,
    message_routing_new_task_request_tx: HostMessageNewTaskRequestSender,
}

impl RequestEnvelopeRoutingHostTask {
    pub fn new(
        connection_request_tx: ConnReqSender,
        config: RouterConfig,
        message_routing_new_task_request_tx: HostMessageNewTaskRequestSender,
    ) -> (Self, HostEnvelopeTaskRequestSender, CloseRequestSender) {
        let (task_request_tx, task_request_rx) = mpsc::channel(config.buffer_size().get());
        let (close_request_tx, close_request_rx) = mpsc::channel(config.buffer_size().get());

        (
            RequestEnvelopeRoutingHostTask {
                connection_request_tx,
                task_request_rx,
                close_request_rx,
                config,
                message_routing_new_task_request_tx,
            },
            task_request_tx,
            close_request_tx,
        )
    }

    pub async fn run(self) -> Result<(), RoutingError> {
        let RequestEnvelopeRoutingHostTask {
            connection_request_tx,
            task_request_rx,
            close_request_rx,
            config,
            message_routing_new_task_request_tx,
        } = self;
        let mut host_route_tasks: HashMap<String, (mpsc::Sender<Envelope>, mpsc::Sender<()>)> =
            HashMap::new();
        let mut host_tasks_join_handlers: HashMap<String, JoinHandle<()>> = HashMap::new();

        let mut rx = combine_receive_task_requests(task_request_rx, close_request_rx);

        loop {
            let host_task_request = rx.next().await.ok_or(RoutingError::ConnectionError)?;

            match host_task_request {
                RequestTaskType::NewTask((host_url, task_tx)) => {
                    let host = host_url.to_string();

                    if !host_route_tasks.contains_key(&host) {
                        let (host_route_task, envelope_tx, close_tx) = RouteHostEnvelopesTask::new(
                            host_url.clone(),
                            connection_request_tx.clone(),
                            config,
                        );

                        host_route_tasks.insert(host.clone(), (envelope_tx, close_tx));

                        let mut complete_sender = message_routing_new_task_request_tx.clone();
                        let task_handle = host_route_task.run().then(|r| async move {
                            // If the request failed, then notify the RouteHostMessagesTask so it can be handled
                            if r.is_err() {
                                let _ = complete_sender
                                    .send(ConnectionResponse::Failure(host_url))
                                    .await;
                            }
                        });

                        let task_handle = tokio::spawn(task_handle);
                        host_tasks_join_handlers.insert(host.clone(), task_handle);
                    }

                    let (envelope_tx, _) = host_route_tasks
                        .get(&host.to_string())
                        .ok_or(RoutingError::ConnectionError)?
                        .clone();
                    task_tx
                        .send(envelope_tx)
                        .map_err(|_| RoutingError::ConnectionError)?;
                }

                RequestTaskType::Close => {
                    for (_, (_, mut close_tx)) in host_route_tasks {
                        close_tx
                            .send(())
                            .await
                            .map_err(|_| RoutingError::ConnectionError)?;
                    }

                    for (_, task_handler) in host_tasks_join_handlers {
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

enum RequestTaskType {
    NewTask((url::Url, oneshot::Sender<mpsc::Sender<Envelope>>)),
    Envelope(Envelope),
    Close,
}

fn combine_receive_task_requests(
    task_request_rx: HostEnvelopeTaskRequestReceiver,
    close_requests_rx: CloseRequestReceiver,
) -> impl stream::Stream<Item = RequestTaskType> + Send + 'static {
    let task_request = task_request_rx.map(RequestTaskType::NewTask);
    let close_request = close_requests_rx.map(|_| RequestTaskType::Close);
    stream::select(task_request, close_request)
}

struct RouteHostEnvelopesTask {
    host_url: url::Url,
    envelope_rx: mpsc::Receiver<Envelope>,
    close_rx: mpsc::Receiver<()>,
    connection_request_tx: ConnReqSender,
    config: RouterConfig,
}

impl RouteHostEnvelopesTask {
    fn new(
        host_url: url::Url,
        connection_request_tx: ConnReqSender,
        config: RouterConfig,
    ) -> (Self, mpsc::Sender<Envelope>, mpsc::Sender<()>) {
        let (envelope_tx, envelope_rx) = mpsc::channel(config.buffer_size().get());
        let (close_tx, close_rx) = mpsc::channel(config.buffer_size().get());
        (
            RouteHostEnvelopesTask {
                host_url,
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
        let RouteHostEnvelopesTask {
            host_url,
            envelope_rx,
            close_rx,
            connection_request_tx,
            config,
        } = self;

        let mut rx = combine_envelope_requests(envelope_rx, close_rx);

        loop {
            let request = rx.next().await.ok_or(RoutingError::ConnectionError)?;

            match request {
                RequestTaskType::Envelope(_envelope) => RetryableRequest::send(
                    BoxedConnSender::new(connection_request_tx.clone(), host_url.clone()),
                    Message::Text(String::from("@sync(node:\"/unit/foo\", lane:\"info\")")),
                    config.retry_strategy(),
                )
                .await
                .map_err(|_| RoutingError::ConnectionError)?,
                RequestTaskType::Close => {
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn combine_envelope_requests(
    envelope_rx: mpsc::Receiver<Envelope>,
    close_requests_rx: CloseRequestReceiver,
) -> impl stream::Stream<Item = RequestTaskType> + Send + 'static {
    let envelope_request = envelope_rx.map(RequestTaskType::Envelope);
    let close_request = close_requests_rx.map(|_| RequestTaskType::Close);
    stream::select(envelope_request, close_request)
}
