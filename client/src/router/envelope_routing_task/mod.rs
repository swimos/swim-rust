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

use crate::router::{
    CloseRequestReceiver, CloseRequestSender, ConnectionRequestSender, RoutingError,
};
use common::warp::envelope::Envelope;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

//----------------------------------Downlink to Connection Pool---------------------------------

pub type HostEnvelopeTaskRequestSender =
    mpsc::Sender<(url::Url, oneshot::Sender<mpsc::Sender<Envelope>>)>;

type HostEnvelopeTaskRequestReceiver =
    mpsc::Receiver<(url::Url, oneshot::Sender<mpsc::Sender<Envelope>>)>;

pub struct RequestEnvelopeRoutingHostTask {
    connection_request_tx: ConnectionRequestSender,
    task_request_rx: HostEnvelopeTaskRequestReceiver,
    close_request_rx: CloseRequestReceiver,
    buffer_size: usize,
}

impl RequestEnvelopeRoutingHostTask {
    pub fn new(
        connection_request_tx: ConnectionRequestSender,
        buffer_size: usize,
    ) -> (Self, HostEnvelopeTaskRequestSender, CloseRequestSender) {
        let (task_request_tx, task_request_rx) = mpsc::channel(buffer_size);
        let (close_request_tx, close_request_rx) = mpsc::channel(buffer_size);

        (
            RequestEnvelopeRoutingHostTask {
                connection_request_tx,
                task_request_rx,
                close_request_rx,
                buffer_size,
            },
            task_request_tx,
            close_request_tx,
        )
    }

    pub async fn run(self) -> Result<(), RoutingError> {
        let RequestEnvelopeRoutingHostTask {
            connection_request_tx,
            mut task_request_rx,
            //Todo
            mut close_request_rx,
            buffer_size,
        } = self;
        let mut host_route_tasks: HashMap<String, mpsc::Sender<Envelope>> = HashMap::new();

        loop {
            let (host_url, task_tx) = task_request_rx
                .recv()
                .await
                .ok_or(RoutingError::ConnectionError)?;
            let host = host_url.to_string();

            if !host_route_tasks.contains_key(&host) {
                let (host_route_task, envelope_tx) = RouteHostEnvelopesTask::new(
                    host_url,
                    connection_request_tx.clone(),
                    buffer_size,
                );

                host_route_tasks.insert(host.clone(), envelope_tx);
                //Todo store this handler
                let _host_route_handler = tokio::spawn(host_route_task.run());
            }

            let envelope_tx = host_route_tasks
                .get(&host.to_string())
                .ok_or(RoutingError::ConnectionError)?
                .clone();
            task_tx
                .send(envelope_tx)
                .map_err(|_| RoutingError::ConnectionError)?;
        }
    }
}

struct RouteHostEnvelopesTask {
    host_url: url::Url,
    envelope_rx: mpsc::Receiver<Envelope>,
    connection_request_tx: ConnectionRequestSender,
}

impl RouteHostEnvelopesTask {
    fn new(
        host_url: url::Url,
        connection_request_tx: ConnectionRequestSender,
        buffer_size: usize,
    ) -> (Self, mpsc::Sender<Envelope>) {
        let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size);

        (
            RouteHostEnvelopesTask {
                host_url,
                envelope_rx,
                connection_request_tx,
            },
            envelope_tx,
        )
    }

    async fn run(self) -> Result<(), RoutingError> {
        let RouteHostEnvelopesTask {
            host_url,
            mut envelope_rx,
            mut connection_request_tx,
        } = self;

        loop {
            let _envelope = envelope_rx
                .recv()
                .await
                .ok_or(RoutingError::ConnectionError)?;

            //Todo extract this out of the loop and request new connection only when a failure occurs.
            let (connection_tx, connection_rx) = oneshot::channel();
            connection_request_tx
                .send((host_url.clone(), connection_tx))
                .await
                .map_err(|_| RoutingError::ConnectionError)?;
            let mut connection = connection_rx
                .await
                .map_err(|_| RoutingError::ConnectionError)?;

            println!("{:?}", _envelope);
            //TODO parse the envelope to a message
            let message = "@sync(node:\"/unit/foo\", lane:\"info\")";
            println!("{:?}", host_url.to_string());
            println!("{:?}", message);
            connection
                .send_message(message)
                .await
                .map_err(|_| RoutingError::ConnectionError)?;
        }
    }
}
