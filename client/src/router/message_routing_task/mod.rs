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

use crate::router::{CloseRequestReceiver, CloseRequestSender, RouterEvent, RoutingError};

//-------------------------------Connection Pool to Downlink------------------------------------

pub enum ConnectionResponse {
    Success((url::Url, mpsc::Receiver<Message>)),
    Failure(url::Url),
}

pub type HostMessageNewTaskRequestSender = mpsc::Sender<ConnectionResponse>;
pub type HostMessageNewTaskRequestReceiver = mpsc::Receiver<ConnectionResponse>;

pub type HostMessageRegisterTaskRequestSender = mpsc::Sender<(url::Url, mpsc::Sender<RouterEvent>)>;
pub type HostMessageRegisterTaskRequestReceiver =
    mpsc::Receiver<(url::Url, mpsc::Sender<RouterEvent>)>;

pub struct RequestMessageRoutingHostTask {
    new_task_request_rx: HostMessageNewTaskRequestReceiver,
    register_task_request_rx: HostMessageRegisterTaskRequestReceiver,
    close_request_rx: CloseRequestReceiver,
    buffer_size: usize,
}

impl RequestMessageRoutingHostTask {
    pub fn new(
        buffer_size: usize,
    ) -> (
        RequestMessageRoutingHostTask,
        HostMessageNewTaskRequestSender,
        HostMessageRegisterTaskRequestSender,
        CloseRequestSender,
    ) {
        let (new_task_request_tx, new_task_request_rx) = mpsc::channel(buffer_size);
        let (register_task_request_tx, register_task_request_rx) = mpsc::channel(buffer_size);
        let (close_request_tx, close_request_rx) = mpsc::channel(buffer_size);

        (
            RequestMessageRoutingHostTask {
                new_task_request_rx,
                register_task_request_rx,
                close_request_rx,
                buffer_size,
            },
            new_task_request_tx,
            register_task_request_tx,
            close_request_tx,
        )
    }

    //Todo refactor this into smaller methods
    pub async fn run(self) -> Result<(), RoutingError> {
        let RequestMessageRoutingHostTask {
            new_task_request_rx,
            register_task_request_rx,
            close_request_rx,
            buffer_size,
        } = self;

        let mut host_route_tasks: HashMap<String, RequestTaskTypeSender> = HashMap::new();
        let mut host_tasks_join_handlers: HashMap<String, JoinHandle<Result<(), RoutingError>>> =
            HashMap::new();

        let mut rx = combine_receive_task_requests(
            new_task_request_rx,
            register_task_request_rx,
            close_request_rx,
        );

        loop {
            let host_task_request = rx.next().await.ok_or(RoutingError::ConnectionError)?;

            if let RequestTaskType::Close = host_task_request.task_request {
                for (_, mut task) in host_route_tasks {
                    task.send(RequestTaskType::Close)
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;
                }

                for (_, task_handler) in host_tasks_join_handlers {
                    task_handler
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?
                        .map_err(|_| RoutingError::ConnectionError)?;
                }

                break;
            } else {
                let RequestHostTaskType { host, task_request } = host_task_request;
                let host = host.ok_or(RoutingError::ConnectionError)?.to_string();

                if let RequestTaskType::Unreachable = task_request {
                    if host_route_tasks.contains_key(&host) {
                        let mut task_tx = host_route_tasks
                            .remove(&host)
                            .ok_or(RoutingError::ConnectionError)?;

                        task_tx
                            .send(task_request)
                            .await
                            .map_err(|_| RoutingError::ConnectionError)?;

                        let task_join_handler = host_tasks_join_handlers
                            .remove(&host)
                            .ok_or(RoutingError::ConnectionError)?;

                        task_join_handler
                            .await
                            .map_err(|_| RoutingError::ConnectionError)?
                            .map_err(|_| RoutingError::ConnectionError)?;
                    }
                } else {
                    if !host_route_tasks.contains_key(&host) {
                        let (task, task_tx) = RouteHostMessagesTask::new(buffer_size);
                        host_route_tasks.insert(host.clone(), task_tx);
                        host_tasks_join_handlers.insert(host.clone(), tokio::spawn(task.run()));
                    }

                    let task_tx = host_route_tasks
                        .get_mut(&host)
                        .ok_or(RoutingError::ConnectionError)?;

                    task_tx
                        .send(task_request)
                        .await
                        .map_err(|_| RoutingError::ConnectionError)?;
                }
            }
        }
        Ok(())
    }
}

fn combine_receive_task_requests(
    new_task_request_rx: HostMessageNewTaskRequestReceiver,
    register_task_request_rx: HostMessageRegisterTaskRequestReceiver,
    close_request_rx: CloseRequestReceiver,
) -> impl stream::Stream<Item = RequestHostTaskType> + Send + 'static {
    let new_task_request = new_task_request_rx.map(|status| match status {
        ConnectionResponse::Success((url, channel)) => RequestHostTaskType {
            host: Some(url),
            task_request: RequestTaskType::Connection(channel),
        },
        ConnectionResponse::Failure(url) => RequestHostTaskType {
            host: Some(url),
            task_request: RequestTaskType::Unreachable,
        },
    });

    let register_task_request =
        register_task_request_rx.map(|(url, channel)| RequestHostTaskType {
            host: Some(url),
            task_request: RequestTaskType::Subscriber(channel),
        });

    let close_request = close_request_rx.map(|_| RequestHostTaskType {
        host: None,
        task_request: RequestTaskType::Close,
    });

    stream::select(
        new_task_request,
        stream::select(register_task_request, close_request),
    )
}

pub struct RequestHostTaskType {
    host: Option<url::Url>,
    task_request: RequestTaskType,
}

pub enum RequestTaskType {
    Connection(mpsc::Receiver<Message>),
    Subscriber(mpsc::Sender<RouterEvent>),
    Unreachable,
    Message(Message),
    Disconnect,
    Close,
}

type RequestTaskTypeSender = mpsc::Sender<RequestTaskType>;
type RequestTaskTypeReceiver = mpsc::Receiver<RequestTaskType>;

pub struct RouteHostMessagesTask {
    task_rx: RequestTaskTypeReceiver,
}

impl RouteHostMessagesTask {
    pub fn new(buffer_size: usize) -> (RouteHostMessagesTask, RequestTaskTypeSender) {
        let (task_tx, task_rx) = mpsc::channel(buffer_size);

        (RouteHostMessagesTask { task_rx }, task_tx)
    }

    //Todo split into smaller methods
    pub async fn run(self) -> Result<(), RoutingError> {
        let RouteHostMessagesTask { mut task_rx } = self;

        let mut subscribers: Vec<mpsc::Sender<RouterEvent>> = Vec::new();
        let mut connection = None;

        loop {
            if connection.is_none() {
                let task_request = task_rx.recv().await.ok_or(RoutingError::ConnectionError)?;

                match task_request {
                    RequestTaskType::Connection(message_rx) => {
                        connection = Some(message_rx);
                    }

                    RequestTaskType::Subscriber(event_tx) => {
                        subscribers.push(event_tx);
                    }

                    RequestTaskType::Unreachable => {
                        println!("Unreachable Host");
                        for subscriber in subscribers.iter_mut() {
                            subscriber
                                .send(RouterEvent::Unreachable)
                                .await
                                .map_err(|_| RoutingError::ConnectionError)?;
                        }
                        break;
                    }

                    RequestTaskType::Close => {
                        println!("Closing Router");
                        for subscriber in subscribers.iter_mut() {
                            subscriber
                                .send(RouterEvent::Stopping)
                                .await
                                .map_err(|_| RoutingError::ConnectionError)?;
                        }
                        break;
                    }

                    _ => {}
                }
            } else {
                let result = tokio::select! {
                    Some(task_request) = task_rx.recv() => {
                        Some(task_request)
                    }

                    maybe_message = connection.as_mut().ok_or(RoutingError::ConnectionError)?.recv() => {
                        match maybe_message{
                            Some(message) => Some(RequestTaskType::Message(message)),
                            None => Some(RequestTaskType::Disconnect),
                        }
                    }

                    else => None,
                };

                let task_request = result.ok_or(RoutingError::ConnectionError)?;

                match task_request {
                    RequestTaskType::Connection(message_rx) => {
                        connection = Some(message_rx);
                    }

                    RequestTaskType::Subscriber(event_tx) => {
                        subscribers.push(event_tx);
                    }

                    RequestTaskType::Message(message) => {
                        //Todo parse the message to envelope
                        println!("{:?}", message);

                        let synced = RouterEvent::Envelope(Envelope::synced(
                            String::from("node_uri"),
                            String::from("lane_uri"),
                        ));

                        //Todo this should send envelopes based on lane and node
                        for subscriber in subscribers.iter_mut() {
                            subscriber
                                .send(synced.clone())
                                .await
                                .map_err(|_| RoutingError::ConnectionError)?;
                        }
                    }

                    RequestTaskType::Disconnect => {
                        println!("Connection closed");
                        connection = None;

                        for subscriber in subscribers.iter_mut() {
                            subscriber
                                .send(RouterEvent::ConnectionClosed)
                                .await
                                .map_err(|_| RoutingError::ConnectionError)?;
                        }
                    }

                    RequestTaskType::Close => {
                        println!("Closing Router");
                        for subscriber in subscribers.iter_mut() {
                            subscriber
                                .send(RouterEvent::Stopping)
                                .await
                                .map_err(|_| RoutingError::ConnectionError)?;
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
