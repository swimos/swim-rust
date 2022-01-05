// Copyright 2015-2021 Swim Inc.
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
use swim_runtime::error::{ConnectionError, Unresolvable};
use swim_runtime::remote::{AttachClientRequest, RawOutRoute, RemoteRoutingRequest};
use swim_runtime::routing::{RoutingAddr, TaggedEnvelope, UnroutableClient};
use swim_utilities::trigger::promise;
use tokio::sync::mpsc;
use url::Url;

pub(crate) struct FakeConnections {
    outgoing_channels: HashMap<Url, mpsc::Sender<TaggedEnvelope>>,
    incoming_channels: HashMap<Url, mpsc::Receiver<TaggedEnvelope>>,
    addr: u32,
}

impl FakeConnections {
    pub(crate) fn new() -> Self {
        FakeConnections {
            outgoing_channels: HashMap::new(),
            incoming_channels: HashMap::new(),
            addr: 0,
        }
    }

    pub(crate) fn add_connection(
        &mut self,
        url: Url,
    ) -> (mpsc::Sender<TaggedEnvelope>, mpsc::Receiver<TaggedEnvelope>) {
        let (outgoing_tx, outgoing_rx) = mpsc::channel(8);
        let (incoming_tx, incoming_rx) = mpsc::channel(8);

        let _ = self.outgoing_channels.insert(url.clone(), outgoing_tx);
        let _ = self.incoming_channels.insert(url, incoming_rx);
        self.addr += 1;

        (incoming_tx, outgoing_rx)
    }

    fn get_connection(
        &mut self,
        url: &Url,
    ) -> Option<(mpsc::Sender<TaggedEnvelope>, mpsc::Receiver<TaggedEnvelope>)> {
        if let Some(conn_sender) = self.outgoing_channels.get(url) {
            Some((
                conn_sender.clone(),
                self.incoming_channels.remove(url).unwrap(),
            ))
        } else {
            None
        }
    }
}

pub(crate) struct MockRemoteRouterTask;

impl MockRemoteRouterTask {
    pub(crate) fn build(mut fake_conns: FakeConnections) -> mpsc::Sender<RemoteRoutingRequest> {
        let (tx, mut rx) = mpsc::channel(32);

        tokio::spawn(async move {
            let mut drops = vec![];

            let mut count = 0;
            let mut url_to_id = HashMap::new();
            let mut id_to_conn = HashMap::new();

            while let Some(request) = rx.recv().await {
                match request {
                    RemoteRoutingRequest::ResolveUrl { host, request } => {
                        if let Some(id) = url_to_id.get(&host) {
                            request.send_ok(*id).unwrap();
                        } else if let Some((sender_tx, receiver_rx)) =
                            fake_conns.get_connection(&host)
                        {
                            let id = RoutingAddr::remote(count);
                            count += 1;
                            url_to_id.insert(host, id);
                            id_to_conn.insert(id, (sender_tx, Some(receiver_rx)));
                            request.send_ok(id).unwrap();
                        } else {
                            request
                                .send_err(ConnectionError::Resolution(host.to_string()))
                                .unwrap();
                        }
                    }
                    RemoteRoutingRequest::AttachClient { request } => {
                        let AttachClientRequest { addr, request, .. } = request;
                        if let Some((sender_tx, receiver_rx)) = id_to_conn.get_mut(&addr) {
                            let (on_drop_tx, on_drop_rx) = promise::promise();

                            let (on_dropped_tx, _on_dropped_rx) = promise::promise();

                            drops.push(on_drop_tx);

                            let route = UnroutableClient::new(
                                RawOutRoute {
                                    sender: sender_tx.clone(),
                                    on_drop: on_drop_rx.clone(),
                                },
                                receiver_rx.take().expect("Receiver taken twice."),
                                on_drop_rx,
                                on_dropped_tx,
                            );

                            request.send(Ok(route)).unwrap();
                        } else {
                            request.send(Err(Unresolvable(addr))).unwrap();
                        }
                    }
                    _ => unreachable!(),
                }
            }
        });

        tx
    }
}
