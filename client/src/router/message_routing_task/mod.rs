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

use crate::connections::{ConnectionError, ConnectionPoolMessage};
use crate::router::RoutingError;
use common::warp::envelope::{Envelope, LaneAddressed};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
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

enum RequestTaskType {
    New((url::Url, mpsc::Receiver<Message>)),
    Register((url::Url, mpsc::Sender<Envelope>)),
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

    //Todo new should happen before register
    pub async fn run(self) -> Result<(), RoutingError> {
        let RequestMessageRoutingHostTask {
            mut new_task_request_rx,
            mut register_task_request_rx,
            buffer_size,
        } = self;

        let mut host_route_tasks: HashMap<String, HostMessagesTaskRequestSender> = HashMap::new();

        loop {
            let task = tokio::select! {

                Some(task_request) = new_task_request_rx.recv() => {
                    Some(RequestTaskType::New(task_request))
                }

                Some(task_request) = register_task_request_rx.recv() => {
                    Some(RequestTaskType::Register(task_request))
                }

                else => None,
            };

            let task = task.ok_or(RoutingError::ConnectionError)?;

            match task {
                RequestTaskType::New((host_url, connection_rx)) => {
                    let (task, task_tx) = RouteHostMessagesTask::new(connection_rx, buffer_size);
                    host_route_tasks.insert(host_url.to_string(), task_tx);

                    tokio::spawn(task.run());

                    println!("1");
                }

                RequestTaskType::Register((_host_url, _envelope_tx)) => {
                    //Todo register the envelope senders with the tasks
                    println!("2");
                }
            };
        }
    }
}

type HostMessagesTaskRequestSender = mpsc::Sender<mpsc::Sender<Envelope>>;
type HostMessagesTaskRequestReceiver = mpsc::Receiver<mpsc::Sender<Envelope>>;

pub struct RouteHostMessagesTask {
    connection_rx: mpsc::Receiver<Message>,
    task_rx: HostMessagesTaskRequestReceiver,
    buffer_size: usize,
}

impl RouteHostMessagesTask {
    pub fn new(
        connection_rx: mpsc::Receiver<Message>,
        buffer_size: usize,
    ) -> (RouteHostMessagesTask, HostMessagesTaskRequestSender) {
        let (task_tx, task_rx) = mpsc::channel(buffer_size);

        (
            RouteHostMessagesTask {
                connection_rx,
                task_rx,
                buffer_size,
            },
            task_tx,
        )
    }

    pub async fn run(self) -> Result<(), RoutingError> {
        let RouteHostMessagesTask {
            connection_rx: mut _connection_rx,
            task_rx: mut _task_rx,
            buffer_size: _buffer_size,
        } = self;

        let mut _subscribers: Vec<mpsc::Sender<Envelope>> = Vec::new();

        loop {
            //Todo add select

            let _message = _connection_rx
                .recv()
                .await
                .ok_or(RoutingError::ConnectionError)?;

            //Todo parse the message
            // let envelope = Envelope::sync(String::from("node_uri"), String::from("lane_uri"));

            //Todo add select for registering downlink channels
            // downlink_channel_rx
        }
    }
}

// rx receives messages directly from every open connection in the pool
async fn _receive_all_messages_from_pool(
    mut router_rx: mpsc::Receiver<Result<ConnectionPoolMessage, ConnectionError>>,
    mut sink_request_tx: mpsc::Sender<(url::Url, oneshot::Sender<mpsc::Sender<String>>)>,
) {
    loop {
        let pool_message = router_rx.recv().await.unwrap().unwrap();
        let ConnectionPoolMessage {
            host,
            message: _message,
        } = pool_message;

        //TODO this needs to be implemented
        //We can have multiple sinks (downlinks) for a given host

        let host_url = url::Url::parse(&host).unwrap();
        let (sink_tx, _sink_rx) = oneshot::channel();

        sink_request_tx.send((host_url, sink_tx)).await.unwrap();

        //Todo This should be sent down to the host tasks.
        // sink.send_item(text);
    }
}

async fn _request_sinks(
    mut sink_request_rx: mpsc::Receiver<(url::Url, oneshot::Sender<mpsc::Sender<String>>)>,
) {
    let mut _sinks: HashMap<String, oneshot::Sender<mpsc::Sender<String>>> = HashMap::new();

    loop {
        let (_host, _sink_tx) = sink_request_rx.recv().await.unwrap();

        // Todo Implement this.
        // let sink = pool.request_sink();

        // sink_tx.send(sink);
    }
}

async fn _receive_host_messages_from_pool(
    mut message_rx: mpsc::Receiver<String>,
    downlinks_rxs: Vec<mpsc::Sender<Envelope>>,
) {
    loop {
        //TODO parse the message to an envelope
        let _message = message_rx.recv().await.unwrap();

        let lane_addressed = LaneAddressed {
            node_uri: String::from("node_uri"),
            lane_uri: String::from("lane_uri"),
            body: None,
        };

        let _envelope = Envelope::EventMessage(lane_addressed);

        for mut _downlink_rx in &downlinks_rxs {
            // Todo need clone for envelope
            // downlink_rx.send_item(envelope.clone());
        }
    }
}
