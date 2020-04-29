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

use crate::connections::ConnectionSender;
use crate::router::configuration::RouterConfig;
use crate::router::{
    CloseRequestReceiver, CloseRequestSender, ConnReqSender, ConnectionResponse, Host, RoutingError,
};

//----------------------------------Downlink to Connection Pool---------------------------------

// pub struct OutgoingRequest {
//     host: Host,
//     outgoing_tx: oneshot::Sender<mpsc::Sender<Envelope>>,
// }

// impl OutgoingRequest {
//     pub fn new(host: Host, outgoing_tx: oneshot::Sender<mpsc::Sender<Envelope>>) -> Self {
//         OutgoingRequest { host, outgoing_tx }
//     }
// }

// pub type OutgoingTaskReqSender = mpsc::Sender<OutgoingRequest>;
// type OutgoingTaskReqReceiver = mpsc::Receiver<OutgoingRequest>;
//
pub(crate) mod retry;

// #[cfg(test)]
// mod tests;

// pub struct OutgoingTask {
//     connection_request_tx: ConnReqSender,
//     task_request_rx: OutgoingTaskReqReceiver,
//     close_request_rx: CloseRequestReceiver,
//     config: RouterConfig,
//     incoming_task_request_tx: IncomingTaskReqSender,
// }
//
// impl OutgoingTask {
//     pub fn new(
//         connection_request_tx: ConnReqSender,
//         incoming_task_request_tx: IncomingTaskReqSender,
//         config: RouterConfig,
//     ) -> (Self, OutgoingTaskReqSender, CloseRequestSender) {
//         let (task_request_tx, task_request_rx) = mpsc::channel(config.buffer_size().get());
//         let (close_request_tx, close_request_rx) = mpsc::channel(config.buffer_size().get());
//
//         (
//             OutgoingTask {
//                 connection_request_tx,
//                 task_request_rx,
//                 close_request_rx,
//                 config,
//                 incoming_task_request_tx,
//             },
//             task_request_tx,
//             close_request_tx,
//         )
//     }
//
//     pub async fn run(self) -> Result<(), RoutingError> {
//         let OutgoingTask {
//             connection_request_tx,
//             task_request_rx,
//             close_request_rx,
//             config,
//             incoming_task_request_tx,
//         } = self;
//         let mut outgoing_host_tasks: HashMap<String, (mpsc::Sender<Envelope>, mpsc::Sender<()>)> =
//             HashMap::new();
//         let mut outgoing_host_tasks_handlers: HashMap<String, JoinHandle<()>> = HashMap::new();
//
//         let mut rx = combine_outgoing_streams(task_request_rx, close_request_rx);
//
//         loop {
//             let task = rx.next().await.ok_or(RoutingError::ConnectionError)?;
//
//             match task {
//                 TaskRequestType::NewTask(OutgoingRequest {
//                     host,
//                     outgoing_tx: task_tx,
//                 }) => {
//                     if !outgoing_host_tasks.contains_key(&host) {
//                         let (outgoing_host_task, envelope_tx, close_tx) = OutgoingHostTask::new(
//                             host.clone(),
//                             connection_request_tx.clone(),
//                             config,
//                         );
//
//                         outgoing_host_tasks.insert(host.clone(), (envelope_tx, close_tx));
//
//                         let mut complete_sender = incoming_task_request_tx.clone();
//
//                         let host_temp = host.clone();
//                         let outgoing_host_task = outgoing_host_task.run().then(|r| async move {
//                             // If the task failed, then notify the RouteHostMessagesTask so it can be handled
//                             if r.is_err() {
//                                 let _ = complete_sender
//                                     .send(ConnectionResponse::Failure(host_temp))
//                                     .await;
//                             }
//                         });
//
//                         let task_handle = tokio::spawn(outgoing_host_task);
//                         outgoing_host_tasks_handlers.insert(host.clone(), task_handle);
//                     }
//
//                     let (envelope_tx, _) = outgoing_host_tasks
//                         .get(&host.to_string())
//                         .ok_or(RoutingError::ConnectionError)?
//                         .clone();
//
//                     task_tx
//                         .send(envelope_tx)
//                         .map_err(|_| RoutingError::ConnectionError)?;
//                 }
//
//                 TaskRequestType::Close => {
//                     for (_, (_, mut close_tx)) in outgoing_host_tasks {
//                         close_tx
//                             .send(())
//                             .await
//                             .map_err(|_| RoutingError::ConnectionError)?;
//                     }
//
//                     for (_, task_handler) in outgoing_host_tasks_handlers {
//                         task_handler
//                             .await
//                             .map_err(|_| RoutingError::ConnectionError)?;
//                     }
//
//                     break;
//                 }
//                 _ => {}
//             }
//         }
//         Ok(())
//     }
// }
//
//
// fn combine_outgoing_streams(
//     task_request_rx: OutgoingTaskReqReceiver,
//     close_requests_rx: CloseRequestReceiver,
// ) -> impl stream::Stream<Item = TaskRequestType> + Send + 'static {
//     let task_request = task_request_rx.map(TaskRequestType::NewTask);
//     let close_request = close_requests_rx.map(|_| TaskRequestType::Close);
//     stream::select(task_request, close_request)
// }

enum OutgoingRequest {
    // Connect(ConnectionSender),
    Message(Envelope),
    Close,
}

pub struct OutgoingHostTask {
    envelope_rx: mpsc::Receiver<Envelope>,
    connection_request_tx: mpsc::Sender<(oneshot::Sender<ConnectionSender>, bool)>,
    close_rx: mpsc::Receiver<()>,
    config: RouterConfig,
}

impl OutgoingHostTask {
    pub fn new(
        envelope_rx: mpsc::Receiver<Envelope>,
        connection_request_tx: mpsc::Sender<(oneshot::Sender<ConnectionSender>, bool)>,
        config: RouterConfig,
    ) -> (Self, mpsc::Sender<()>) {
        let (close_tx, close_rx) = mpsc::channel(config.buffer_size().get());
        (
            OutgoingHostTask {
                envelope_rx,
                connection_request_tx,
                close_rx,
                config,
            },
            close_tx,
        )
    }

    pub async fn run(self) -> Result<(), RoutingError> {
        let OutgoingHostTask {
            envelope_rx,
            mut connection_request_tx,
            close_rx,
            config,
        } = self;

        let mut connection = None;
        let mut rx = combine_outgoing_host_streams(envelope_rx, close_rx);

        loop {
            let task = rx.next().await.ok_or(RoutingError::ConnectionError)?;
            tracing::trace!("Received request");

            match task {
                OutgoingRequest::Message(envelope) => {
                    let message = envelope.into_value().to_string();

                    if connection.is_none() {
                        let (connection_tx, connection_rx) = oneshot::channel();

                        connection_request_tx.send((connection_tx, false));

                        connection = Some(
                            connection_rx
                                .await
                                .map_err(|_| RoutingError::ConnectionError)?,
                        );
                    }

                    connection.clone()
                        .ok_or(RoutingError::ConnectionError)?
                        .send_message(&message);

                    //Todo merge with new retry.
                    //
                    // RetryableRequest::send(
                    //     BoxedConnSender::new(connection_request_tx.clone(), host.clone()),
                    //     message,
                    //     config.retry_strategy(),
                    // )
                    // .await
                    // .map_err(|_| RoutingError::ConnectionError)?;

                    tracing::trace!("Completed request");
                }
                OutgoingRequest::Close => {
                    break;
                }
            }
        }
        Ok(())
    }
}

fn combine_outgoing_host_streams(
    envelope_rx: mpsc::Receiver<Envelope>,
    close_requests_rx: CloseRequestReceiver,
) -> impl stream::Stream<Item = OutgoingRequest> + Send + 'static {
    let envelope_request = envelope_rx.map(OutgoingRequest::Message);
    let close_request = close_requests_rx.map(|_| OutgoingRequest::Close);
    stream::select(envelope_request, close_request)
}

// #[cfg(test)]
// mod route_tests {
//     use super::*;
//
//     use crate::router::configuration::RouterConfigBuilder;
//
//     use crate::connections::ConnectionSender;
//     use crate::router::outgoing::retry::RetryStrategy;
//
//     fn router_config(strategy: RetryStrategy) -> RouterConfig {
//         RouterConfigBuilder::default()
//             .with_buffer_size(5)
//             .with_idle_timeout(10)
//             .with_conn_reaper_frequency(10)
//             .with_retry_stategy(strategy)
//             .build()
//     }
//
//     // Test that after a permanent error, the task fails
//     #[tokio::test]
//     async fn permanent_error() {
//         let config = router_config(RetryStrategy::none());
//         let (task_request_tx, mut task_request_rx) = mpsc::channel(config.buffer_size().get());
//         let (host_route_task, mut envelope_tx, _close_tx) =
//             OutgoingHostTask::new(String::from("ws://127.0.0.1:9001"), task_request_tx, config);
//         let handle = tokio::spawn(host_route_task.run());
//
//         let _ = envelope_tx
//             .send(Envelope::sync("node".into(), "lane".into()))
//             .await;
//         let (_url, tx, _recreate) = task_request_rx.recv().await.unwrap();
//         let _ = tx.send(Err(RoutingError::ConnectionError));
//
//         let task_result = handle.await.unwrap();
//         assert_eq!(task_result, Err(RoutingError::ConnectionError))
//     }
//
//     // Test that after a transient error, the retry system attempts the request again and succeeds
//     #[tokio::test]
//     async fn transient_error() {
//         let config = router_config(RetryStrategy::immediate(1));
//
//         let (task_request_tx, mut task_request_rx) = mpsc::channel(config.buffer_size().get());
//         let (host_route_task, mut envelope_tx, mut close_tx) =
//             OutgoingHostTask::new(String::from("ws://127.0.0.1:9001"), task_request_tx, config);
//
//         let handle = tokio::spawn(host_route_task.run());
//         let _ = envelope_tx
//             .send(Envelope::sync("node".into(), "lane".into()))
//             .await;
//
//         let (_url, tx, _recreate) = task_request_rx.recv().await.unwrap();
//         let _ = tx.send(Err(RoutingError::Transient));
//
//         let (_url, tx, _recreate) = task_request_rx.recv().await.unwrap();
//         let (dummy_tx, _dummy_rx) = mpsc::channel(config.buffer_size().get());
//
//         let _ = tx.send(Ok(ConnectionSender::new(dummy_tx)));
//         let _ = close_tx.send(()).await;
//
//         let task_result = handle.await.unwrap();
//         assert_eq!(task_result, Ok(()))
//     }
// }
