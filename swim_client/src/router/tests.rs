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

use crate::router::{AddressableWrapper, RoutingPath};
use std::collections::HashMap;
use std::convert::TryFrom;
use swim_model::path::{AbsolutePath, Path, RelativePath};
use swim_runtime::error::{ConnectionError, ResolutionError};
use swim_runtime::remote::router::{RemoteRoutingRequest, RoutingError};
use swim_runtime::remote::table::{BidirectionalRegistrator, SchemeHostPort};
use swim_runtime::remote::Scheme;
use swim_runtime::routing::{RoutingAddr, TaggedEnvelope, TaggedSender};
use swim_utilities::trigger::promise;
use tokio::sync::mpsc;
use url::Url;

pub(crate) struct FakeConnections {
    outgoing_channels: HashMap<Url, TaggedSender>,
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

        let _ = self.outgoing_channels.insert(
            url.clone(),
            TaggedSender::new(RoutingAddr::client(self.addr), outgoing_tx),
        );
        let _ = self.incoming_channels.insert(url, incoming_rx);
        self.addr += 1;

        (incoming_tx, outgoing_rx)
    }

    fn get_connection(
        &mut self,
        url: &Url,
    ) -> Option<(TaggedSender, mpsc::Receiver<TaggedEnvelope>)> {
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

            while let Some(request) = rx.recv().await {
                match request {
                    RemoteRoutingRequest::Bidirectional { host, request } => {
                        if let Some((sender_tx, receiver_rx)) = fake_conns.get_connection(&host) {
                            let (tx, mut rx) = mpsc::channel(8);
                            let (on_drop_tx, on_drop_rx) = promise::promise();

                            drops.push(on_drop_tx);

                            let registrator =
                                BidirectionalRegistrator::new(sender_tx, tx, on_drop_rx);

                            request.send(Ok(registrator)).unwrap();

                            let receiver_request = rx.recv().await.unwrap();
                            receiver_request.send(receiver_rx).unwrap();
                        } else {
                            request
                                .send(Err(RoutingError::Connection(ConnectionError::Resolution(
                                    ResolutionError::unresolvable(host.to_string()),
                                ))))
                                .unwrap();
                        }
                    }
                    _ => unreachable!(),
                }
            }
        });

        tx
    }
}

#[test]
fn test_routing_path_from_relative_path() {
    let path = RelativePath::new("/foo", "/bar");

    let routing_path = RoutingPath::try_from(AddressableWrapper(path)).unwrap();

    assert_eq!(routing_path, RoutingPath::Local("/foo".to_string()))
}

#[test]
fn test_routing_path_from_absolute_path() {
    let url = url::Url::parse(&"warp://127.0.0.1:9001".to_string()).unwrap();
    let path = AbsolutePath::new(url, "/foo", "/bar");

    let routing_path = RoutingPath::try_from(AddressableWrapper(path)).unwrap();

    assert_eq!(
        routing_path,
        RoutingPath::Remote(SchemeHostPort::new(
            Scheme::Ws,
            "127.0.0.1".to_string(),
            9001
        ))
    )
}

#[test]
fn test_routing_path_from_absolute_path_invalid() {
    let url = url::Url::parse(&"carp://127.0.0.1:9001".to_string()).unwrap();
    let path = AbsolutePath::new(url, "/foo", "/bar");

    let routing_path = RoutingPath::try_from(AddressableWrapper(path));

    assert!(routing_path.is_err());
}

#[test]
fn test_routing_path_from_path() {
    let path = Path::Local(RelativePath::new("/foo", "/bar"));
    let routing_path = RoutingPath::try_from(AddressableWrapper(path)).unwrap();

    assert_eq!(routing_path, RoutingPath::Local("/foo".to_string()))
}

#[test]
fn test_routing_path_from_path_invalid() {
    let url = Url::parse("unix:/run/foo.socket").unwrap();
    let path = Path::Remote(AbsolutePath::new(url, "/foo", "/bar"));
    let routing_path = RoutingPath::try_from(AddressableWrapper(path));

    assert!(routing_path.is_err());
}
