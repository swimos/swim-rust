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
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use common::warp::envelope::Envelope;

use crate::router::{
    CloseRequestReceiver, CloseRequestSender, ConnectionResponse, RouterEvent, RoutingError,
};
use common::model::parser::parse_single;
use std::convert::TryFrom;

//-------------------------------Connection Pool to Downlink------------------------------------

pub type IncomingTaskReqSender = mpsc::Sender<ConnectionResponse>;
pub type IncomingTaskReqReceiver = mpsc::Receiver<ConnectionResponse>;

pub struct IncomingRequest {
    node: String,
    lane: String,
    incoming_tx: mpsc::Sender<RouterEvent>,
}

impl IncomingRequest {
    pub fn new(node: String, lane: String, incoming_tx: mpsc::Sender<RouterEvent>) -> Self {
        IncomingRequest {
            node,
            lane,
            incoming_tx,
        }
    }
}

pub type IncomingSubscriberReqSender = mpsc::Sender<(String, IncomingRequest)>;

pub type IncomingSubscriberReqReceiver = mpsc::Receiver<(String, IncomingRequest)>;

pub struct IncomingTask {
    new_task_request_rx: IncomingTaskReqReceiver,
    subscribe_request_rx: IncomingSubscriberReqReceiver,
    close_request_rx: CloseRequestReceiver,
    buffer_size: usize,
}

impl IncomingTask {
    pub fn new(
        buffer_size: usize,
    ) -> (
        IncomingTask,
        IncomingTaskReqSender,
        IncomingSubscriberReqSender,
        CloseRequestSender,
    ) {
        let (new_task_request_tx, new_task_request_rx) = mpsc::channel(buffer_size);
        let (subscribe_request_tx, subscribe_request_rx) = mpsc::channel(buffer_size);
        let (close_request_tx, close_request_rx) = mpsc::channel(buffer_size);

        (
            IncomingTask {
                new_task_request_rx,
                subscribe_request_rx,
                close_request_rx,
                buffer_size,
            },
            new_task_request_tx,
            subscribe_request_tx,
            close_request_tx,
        )
    }

    //Todo refactor this into smaller methods
    pub async fn run(self) -> Result<(), RoutingError> {
        let IncomingTask {
            new_task_request_rx,
            subscribe_request_rx,
            close_request_rx,
            buffer_size,
        } = self;

        let mut incoming_host_tasks: HashMap<String, HostTaskReqSender> = HashMap::new();
        let mut incoming_host_tasks_handlers: HashMap<
            String,
            JoinHandle<Result<(), RoutingError>>,
        > = HashMap::new();

        let mut rx =
            combine_incoming_streams(new_task_request_rx, subscribe_request_rx, close_request_rx);

        loop {
            let task = rx.next().await.ok_or(RoutingError::ConnectionError)?;

            if let HostTaskRequestType::Close = task.task_type {
                for (_, mut task) in incoming_host_tasks {
                    task.send(HostTaskRequestType::Close)
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;
                }

                for (_, task_handler) in incoming_host_tasks_handlers {
                    task_handler
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?
                        .map_err(|_| RoutingError::ConnectionError)?;
                }

                break;
            } else {
                let TaskRequest {
                    host,
                    task_type: task,
                } = task;

                let host = host.ok_or(RoutingError::ConnectionError)?.to_string();

                if let HostTaskRequestType::Unreachable = task {
                    if incoming_host_tasks.contains_key(&host) {
                        let mut task_tx = incoming_host_tasks
                            .remove(&host)
                            .ok_or(RoutingError::ConnectionError)?;

                        task_tx
                            .send(task)
                            .await
                            .map_err(|_| RoutingError::ConnectionError)?;

                        let task_join_handler = incoming_host_tasks_handlers
                            .remove(&host)
                            .ok_or(RoutingError::ConnectionError)?;

                        task_join_handler
                            .await
                            .map_err(|_| RoutingError::ConnectionError)?
                            .map_err(|_| RoutingError::ConnectionError)?;
                    }
                } else {
                    if !incoming_host_tasks.contains_key(&host) {
                        let (task, task_tx) = IncomingHostTask::new(buffer_size);
                        incoming_host_tasks.insert(host.clone(), task_tx);
                        incoming_host_tasks_handlers.insert(host.clone(), tokio::spawn(task.run()));
                    }

                    let task_tx = incoming_host_tasks
                        .get_mut(&host)
                        .ok_or(RoutingError::ConnectionError)?;

                    task_tx
                        .send(task)
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;
                }
            }
        }
        Ok(())
    }
}

fn combine_incoming_streams(
    new_task_request_rx: IncomingTaskReqReceiver,
    subscribe_request_rx: IncomingSubscriberReqReceiver,
    close_request_rx: CloseRequestReceiver,
) -> impl stream::Stream<Item = TaskRequest> + Send + 'static {
    let new_task_request = new_task_request_rx.map(|status| match status {
        ConnectionResponse::Success((url, channel)) => TaskRequest {
            host: Some(url),
            task_type: HostTaskRequestType::Connect(channel),
        },
        ConnectionResponse::Failure(url) => TaskRequest {
            host: Some(url),
            task_type: HostTaskRequestType::Unreachable,
        },
    });

    let subscribe_request = subscribe_request_rx.map(|(host, incoming_request)| TaskRequest {
        host: Some(host),
        task_type: HostTaskRequestType::Subscribe(incoming_request),
    });

    let close_request = close_request_rx.map(|_| TaskRequest {
        host: None,
        task_type: HostTaskRequestType::Close,
    });

    stream::select(
        new_task_request,
        stream::select(subscribe_request, close_request),
    )
}

pub struct TaskRequest {
    host: Option<String>,
    task_type: HostTaskRequestType,
}

pub enum HostTaskRequestType {
    Connect(mpsc::Receiver<Message>),
    Subscribe(IncomingRequest),
    Message(Message),
    Unreachable,
    Disconnect,
    Close,
}

type HostTaskReqSender = mpsc::Sender<HostTaskRequestType>;
type HostTaskReqReceiver = mpsc::Receiver<HostTaskRequestType>;

pub struct IncomingHostTask {
    task_rx: HostTaskReqReceiver,
}

impl IncomingHostTask {
    pub fn new(buffer_size: usize) -> (IncomingHostTask, HostTaskReqSender) {
        let (task_tx, task_rx) = mpsc::channel(buffer_size);

        (IncomingHostTask { task_rx }, task_tx)
    }

    //Todo split into smaller methods
    pub async fn run(self) -> Result<(), RoutingError> {
        let IncomingHostTask { mut task_rx } = self;

        let mut subscribers: HashMap<String, Vec<mpsc::Sender<RouterEvent>>> = HashMap::new();
        let mut connection = None;

        loop {
            if connection.is_none() {
                let task = task_rx.recv().await.ok_or(RoutingError::ConnectionError)?;

                match task {
                    HostTaskRequestType::Connect(message_rx) => {
                        connection = Some(message_rx);
                    }

                    HostTaskRequestType::Subscribe(IncomingRequest {
                        node,
                        lane,
                        incoming_tx: event_tx,
                    }) => {
                        let destination = format!("{}/{}", node, lane);
                        subscribers
                            .entry(destination)
                            .or_insert_with(Vec::new)
                            .push(event_tx);
                    }

                    HostTaskRequestType::Unreachable => {
                        println!("Unreachable Host");
                        for (_, destination) in subscribers.iter_mut() {
                            for subscriber in destination {
                                subscriber
                                    .send(RouterEvent::Unreachable)
                                    .await
                                    .map_err(|_| RoutingError::ConnectionError)?;
                            }
                        }
                        break;
                    }

                    HostTaskRequestType::Close => {
                        println!("Closing Router");
                        for (_, destination) in subscribers.iter_mut() {
                            for subscriber in destination {
                                subscriber
                                    .send(RouterEvent::Stopping)
                                    .await
                                    .map_err(|_| RoutingError::ConnectionError)?;
                            }
                        }
                        break;
                    }

                    _ => {}
                }
            } else {
                let task = tokio::select! {
                    Some(task) = task_rx.recv() => {
                        Some(task)
                    }

                    maybe_message = connection.as_mut().ok_or(RoutingError::ConnectionError)?.recv() => {
                        match maybe_message{
                            Some(message) => Some(HostTaskRequestType::Message(message)),
                            None => Some(HostTaskRequestType::Disconnect),
                        }
                    }

                    else => None,
                };

                let task = task.ok_or(RoutingError::ConnectionError)?;

                match task {
                    HostTaskRequestType::Connect(message_rx) => {
                        connection = Some(message_rx);
                    }

                    HostTaskRequestType::Subscribe(IncomingRequest {
                        node,
                        lane,
                        incoming_tx: event_tx,
                    }) => {
                        let destination = format!("{}/{}", node, lane);
                        subscribers
                            .entry(destination)
                            .or_insert_with(Vec::new)
                            .push(event_tx);
                    }

                    HostTaskRequestType::Message(message) => {
                        let message = message.to_text().unwrap();
                        let value = parse_single(message).unwrap();
                        let envelope = Envelope::try_from(value).unwrap();
                        let destination = envelope.destination();
                        let event = RouterEvent::Envelope(envelope);

                        if let Some(destination) = destination {
                            if subscribers.contains_key(&destination) {
                                //Todo Replace with tracing
                                println!("{:?}", event);
                                let destination_subs = subscribers
                                    .get_mut(&destination)
                                    .ok_or(RoutingError::ConnectionError)?;

                                for subscriber in destination_subs.iter_mut() {
                                    subscriber
                                        .send(event.clone())
                                        .await
                                        .map_err(|_| RoutingError::ConnectionError)?;
                                }
                            } else {
                                //Todo log the messsage
                                println!("No downlink interested in message: {:?}", event);
                            }
                        } else {
                            println!("Host messages are not supported: {:?}", event);
                        }
                    }

                    HostTaskRequestType::Disconnect => {
                        println!("Connection closed");
                        connection = None;

                        for (_, destination) in subscribers.iter_mut() {
                            for subscriber in destination {
                                subscriber
                                    .send(RouterEvent::ConnectionClosed)
                                    .await
                                    .map_err(|_| RoutingError::ConnectionError)?;
                            }
                        }
                    }

                    HostTaskRequestType::Close => {
                        println!("Closing Router");
                        for (_, destination) in subscribers.iter_mut() {
                            for subscriber in destination {
                                subscriber
                                    .send(RouterEvent::Stopping)
                                    .await
                                    .map_err(|_| RoutingError::ConnectionError)?;
                            }
                        }
                        break;
                    }

                    _ => {}
                }
            }
        }
        Ok(())
    }
}
