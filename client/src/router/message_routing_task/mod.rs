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

use crate::router::RoutingError;
use common::warp::envelope::Envelope;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;

//-------------------------------Connection Pool to Downlink------------------------------------

pub type HostMessageNewTaskRequestSender = mpsc::Sender<(url::Url, mpsc::Receiver<Message>)>;
pub type HostMessageNewTaskRequestReceiver = mpsc::Receiver<(url::Url, mpsc::Receiver<Message>)>;

pub type HostMessageRegisterTaskRequestSender = mpsc::Sender<(url::Url, mpsc::Sender<Envelope>)>;
pub type HostMessageRegisterTaskRequestReceiver =
    mpsc::Receiver<(url::Url, mpsc::Sender<Envelope>)>;

pub struct RequestMessageRoutingHostTask {
    new_task_request_rx: HostMessageNewTaskRequestReceiver,
    register_task_request_rx: HostMessageRegisterTaskRequestReceiver,
    buffer_size: usize,
}

impl RequestMessageRoutingHostTask {
    pub fn new(
        buffer_size: usize,
    ) -> (
        RequestMessageRoutingHostTask,
        HostMessageNewTaskRequestSender,
        HostMessageRegisterTaskRequestSender,
    ) {
        let (new_task_request_tx, new_task_request_rx) = mpsc::channel(buffer_size);
        let (register_task_request_tx, register_task_request_rx) = mpsc::channel(buffer_size);

        (
            RequestMessageRoutingHostTask {
                new_task_request_rx,
                register_task_request_rx,
                buffer_size,
            },
            new_task_request_tx,
            register_task_request_tx,
        )
    }

    pub async fn run(self) -> Result<(), RoutingError> {
        let RequestMessageRoutingHostTask {
            mut new_task_request_rx,
            mut register_task_request_rx,
            buffer_size,
        } = self;

        let mut host_route_tasks: HashMap<String, RequestTaskTypeSender> = HashMap::new();

        loop {
            let result = tokio::select! {

                Some((url, channel)) = new_task_request_rx.recv() => {
                    Some((url.to_string(), RequestTaskType::Connection(channel)))
                }

                Some((url, channel)) = register_task_request_rx.recv() => {
                    Some((url.to_string(), RequestTaskType::Subscriber(channel)))
                }

                else => None,
            };

            let (host, task_request) = result.ok_or(RoutingError::ConnectionError)?;

            if !host_route_tasks.contains_key(&host) {
                let (task, task_tx) = RouteHostMessagesTask::new(buffer_size);
                host_route_tasks.insert(host.clone(), task_tx);
                tokio::spawn(task.run());
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

pub enum RequestTaskType {
    Connection(mpsc::Receiver<Message>),
    Subscriber(mpsc::Sender<Envelope>),
    Message(Message),
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

        let mut subscribers: Vec<mpsc::Sender<Envelope>> = Vec::new();
        let mut connection = None;

        loop {
            if connection.is_none() {
                let task_request = task_rx.recv().await.ok_or(RoutingError::ConnectionError)?;

                match task_request {
                    RequestTaskType::Connection(message_rx) => {
                        connection = Some(message_rx);
                    }

                    RequestTaskType::Subscriber(envelope_tx) => {
                        subscribers.push(envelope_tx);
                    }

                    _ => {}
                }
            } else {
                let result = tokio::select! {

                    Some(task_request) = task_rx.recv() => {
                        Some(task_request)
                    }

                    Some(message) =  connection.as_mut().ok_or(RoutingError::ConnectionError)?.recv() => {
                        Some(RequestTaskType::Message(message))
                    }

                    else => None,
                };

                let task_request = result.ok_or(RoutingError::ConnectionError)?;

                match task_request {
                    RequestTaskType::Connection(message_rx) => {
                        connection = Some(message_rx);
                    }

                    RequestTaskType::Subscriber(envelope_tx) => {
                        subscribers.push(envelope_tx);
                    }

                    RequestTaskType::Message(message) => {
                        //Todo parse the message to envelope
                        println!("{:?}", message);

                        let synced =
                            Envelope::synced(String::from("node_uri"), String::from("lane_uri"));

                        for subscriber in subscribers.iter_mut() {
                            subscriber
                                .send(synced.clone())
                                .await
                                .map_err(|_| RoutingError::ConnectionError)?;
                        }
                    }
                }
            }
        }
    }
}
