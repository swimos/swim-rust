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

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::protocol::Message;

use common::warp::envelope::Envelope;

use crate::router::configuration::RouterConfig;
use crate::router::envelope_routing_task::retry::boxed_connection_sender::BoxedConnSender;
use crate::router::envelope_routing_task::retry::RetryableRequest;
use crate::router::{CloseRequestReceiver, CloseRequestSender, ConnReqSendResult, RoutingError};

//----------------------------------Downlink to Connection Pool---------------------------------

pub type HostEnvelopeTaskRequestSender =
    mpsc::Sender<(url::Url, oneshot::Sender<mpsc::Sender<Envelope>>)>;

type HostEnvelopeTaskRequestReceiver =
    mpsc::Receiver<(url::Url, oneshot::Sender<mpsc::Sender<Envelope>>)>;

pub(crate) mod retry;

#[cfg(test)]
mod tests;

pub struct RequestEnvelopeRoutingHostTask {
    connection_request_tx: ConnReqSendResult,
    task_request_rx: HostEnvelopeTaskRequestReceiver,
    _close_request_rx: CloseRequestReceiver,
    config: RouterConfig,
}

impl RequestEnvelopeRoutingHostTask {
    pub fn new(
        connection_request_tx: ConnReqSendResult,
        config: RouterConfig,
    ) -> (Self, HostEnvelopeTaskRequestSender, CloseRequestSender) {
        let (task_request_tx, task_request_rx) = mpsc::channel(config.buffer_size().get());
        let (close_request_tx, close_request_rx) = mpsc::channel(config.buffer_size().get());

        (
            RequestEnvelopeRoutingHostTask {
                connection_request_tx,
                task_request_rx,
                _close_request_rx: close_request_rx,
                config,
            },
            task_request_tx,
            close_request_tx,
        )
    }

    pub async fn run(self) -> Result<(), RoutingError> {
        let RequestEnvelopeRoutingHostTask {
            connection_request_tx,
            mut task_request_rx,
            config,
            ..
        } = self;
        let mut host_route_tasks: HashMap<String, mpsc::Sender<Envelope>> = HashMap::new();

        loop {
            let (host_url, task_tx) = task_request_rx
                .recv()
                .await
                .ok_or(RoutingError::ConnectionError)?;
            let host = host_url.to_string();

            if !host_route_tasks.contains_key(&host) {
                let (host_route_task, envelope_tx) =
                    RouteHostEnvelopesTask::new(host_url, connection_request_tx.clone(), config);

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
    connection_request_tx: ConnReqSendResult,
    config: RouterConfig,
}

impl RouteHostEnvelopesTask {
    fn new(
        host_url: url::Url,
        connection_request_tx: ConnReqSendResult,
        config: RouterConfig,
    ) -> (Self, mpsc::Sender<Envelope>) {
        let (envelope_tx, envelope_rx) = mpsc::channel(config.buffer_size().get());
        (
            RouteHostEnvelopesTask {
                host_url,
                envelope_rx,
                connection_request_tx,
                config,
            },
            envelope_tx,
        )
    }

    async fn run(mut self) -> Result<(), RoutingError> {
        loop {
            let _envelope = self
                .envelope_rx
                .recv()
                .await
                .ok_or(RoutingError::ConnectionError)?;

            RetryableRequest::send(
                BoxedConnSender::new(self.connection_request_tx.clone(), self.host_url.clone()),
                Message::Text(String::from("@sync(node:\"/unit/foo\", lane:\"info\")")),
                self.config.retry_strategy(),
            )
            .await
            .map_err(|_| RoutingError::ConnectionError)?;
        }
    }
}
