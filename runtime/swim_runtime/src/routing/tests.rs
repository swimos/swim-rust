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

use futures::future::BoxFuture;
use futures::io::ErrorKind;
use futures::FutureExt;
use futures_util::future::ready;
use swim_model::path::Path;
use tokio::sync::mpsc;
use url::Url;

use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger::promise;
use swim_warp::envelope::Envelope;

use crate::error::{ConnectionError, IoError, ResolutionError, RoutingError};
use crate::remote::RawRoute;
use crate::routing::fixture::router_fixture;
use crate::routing::fixture::{invalid, RouterCallback};
use crate::routing::{PlaneRoutingRequest, RemoteRoutingRequest, Route};
use crate::routing::{RoutingAddr, TaggedEnvelope, TaggedSender};
use crate::test_fixture::{LocalRoutes, RouteTable};

#[tokio::test]
async fn tagged_sender() {
    let (tx, mut rx) = mpsc::channel(8);
    let mut sender = TaggedSender::new(RoutingAddr::remote(7), tx);

    assert!(sender
        .send_item(Envelope::linked().node_uri("/node").lane_uri("lane").done())
        .await
        .is_ok());

    let received = rx.recv().await;
    assert_eq!(
        received,
        Some(TaggedEnvelope(
            RoutingAddr::remote(7),
            Envelope::linked().node_uri("/node").lane_uri("lane").done()
        ))
    );
}

#[test]
fn routing_addr_discriminate() {
    assert!(RoutingAddr::remote(0x1).is_remote());
    assert!(RoutingAddr::client(0x1).is_client());
    assert!(RoutingAddr::plane(0x1).is_plane());
    assert!(RoutingAddr::remote(u32::MAX).is_remote());
    assert!(RoutingAddr::client(u32::MAX).is_client());
    assert!(RoutingAddr::plane(u32::MAX).is_plane());
}

#[test]
fn routing_addr_display() {
    assert_eq!(
        RoutingAddr::remote(0x1).to_string(),
        "Remote(1)".to_string()
    );
    assert_eq!(
        RoutingAddr::remote(u32::MAX).to_string(),
        "Remote(4294967295)".to_string()
    );
    assert_eq!(RoutingAddr::plane(0x1).to_string(), "Plane(1)".to_string());
    assert_eq!(
        RoutingAddr::plane(u32::MAX).to_string(),
        "Plane(4294967295)".to_string()
    );
    assert_eq!(
        RoutingAddr::client(0x1).to_string(),
        "Client(1)".to_string()
    );
    assert_eq!(
        RoutingAddr::client(u32::MAX).to_string(),
        "Client(4294967295)".to_string()
    );
}

const ADDR: RoutingAddr = RoutingAddr::remote(4);

fn test_url() -> Url {
    "swim://remote:80".parse().unwrap()
}

fn path() -> RelativeUri {
    "/agent/lane".parse().unwrap()
}

fn envelope(body: &str) -> Envelope {
    Envelope::event()
        .node_uri("node")
        .lane_uri("lane")
        .body(body)
        .done()
}

struct RemoteResolver {
    url: Url,
    sender: mpsc::Sender<TaggedEnvelope>,
    resolved: bool,
}

impl RouterCallback<RemoteRoutingRequest> for RemoteResolver {
    fn call(&mut self, request: RemoteRoutingRequest) -> BoxFuture<()> {
        let RemoteResolver {
            url,
            sender,
            resolved,
        } = self;
        let (_drop_tx, drop_rx) = promise::promise();

        match request {
            RemoteRoutingRequest::Endpoint { addr, request } => {
                if *resolved && addr == ADDR {
                    assert!(request
                        .send_ok(RawRoute::new(sender.clone(), drop_rx.clone()))
                        .is_ok());
                } else {
                    assert!(request.send_err(ResolutionError::Addr(addr)).is_ok());
                }
            }
            RemoteRoutingRequest::ResolveUrl { host, request } => {
                if host == *url {
                    *resolved = true;
                    assert!(request.send_ok(ADDR).is_ok());
                } else {
                    assert!(request
                        .send_err(ConnectionError::Io(IoError::new(ErrorKind::NotFound, None)))
                        .is_ok());
                }
            }
            RemoteRoutingRequest::Bidirectional { .. } => {}
        }

        ready(()).boxed()
    }
}

#[tokio::test]
async fn resolve_remote_ok() {
    let our_addr = RoutingAddr::remote(0);

    let (tx, mut rx) = mpsc::channel(8);
    let url = test_url();

    let (router, _task) = router_fixture::<Path, _, _, _>(
        invalid,
        RemoteResolver {
            url: url.clone(),
            sender: tx,
            resolved: false,
        },
        invalid,
    );

    let mut router = router.tagged(our_addr);

    let result = router.lookup((Some(url), path())).await;
    assert_eq!(result, Ok(ADDR));

    let result = router.resolve_sender(ADDR).await;
    assert!(result.is_ok());

    let Route { mut sender, .. } = result.unwrap();
    assert!(sender.send_item(envelope("a")).await.is_ok());

    let result = rx.recv().now_or_never();
    assert_eq!(result, Some(Some(TaggedEnvelope(our_addr, envelope("a")))));
}

#[tokio::test]
async fn resolve_remote_failure() {
    let (tx, _rx) = mpsc::channel(8);
    let url = test_url();

    let (mut router, _task) = router_fixture::<Path, _, _, _>(
        invalid,
        RemoteResolver {
            url: url.clone(),
            sender: tx,
            resolved: false,
        },
        invalid,
    );

    let other_addr = RoutingAddr::remote(56);
    let result = router.resolve_sender(other_addr).await;
    let _expected = ResolutionError::Addr(other_addr);

    assert!(matches!(result, Err(_expected)));
}

#[tokio::test]
async fn lookup_remote_failure() {
    let (tx, _rx) = mpsc::channel(8);
    let url = test_url();

    let (mut router, _task) = router_fixture::<Path, _, _, _>(
        invalid,
        RemoteResolver {
            url: url.clone(),
            sender: tx,
            resolved: false,
        },
        invalid,
    );

    let other_url = "swim://other:80".parse().unwrap();
    let result = router.lookup((Some(other_url), path())).await;
    assert_eq!(
        result,
        Err(RoutingError::Connection(ConnectionError::Io(IoError::new(
            ErrorKind::NotFound,
            None
        ))))
    );
}

#[tokio::test]
async fn delegate_local_ok() {
    let our_addr = RoutingAddr::remote(0);

    let mut table = RouteTable::new(our_addr);
    let mut rx = table.add(path());

    let (router, _task) = LocalRoutes::from_table(table);
    let mut router = router.tagged(our_addr);

    let result = router.lookup(path()).await;
    assert!(result.is_ok());
    let local_addr = result.unwrap();

    let result = router.resolve_sender(local_addr).await;
    assert!(result.is_ok());
    let Route { mut sender, .. } = result.unwrap();
    assert!(sender.send_item(envelope("a")).await.is_ok());

    let result = rx.recv().now_or_never();
    assert_eq!(result, Some(Some(TaggedEnvelope(our_addr, envelope("a")))));
}

#[tokio::test]
async fn resolve_local_err() {
    let our_addr = RoutingAddr::remote(0);
    let (tx, _rx) = mpsc::channel(8);
    let url = test_url();

    let (router, _task) = router_fixture::<Path, _, _, _>(
        |req: PlaneRoutingRequest| async move {
            match req {
                PlaneRoutingRequest::Endpoint { addr, request } => {
                    let _r = request.send(Err(ResolutionError::Addr(addr)));
                }
                req => panic!("Unexpected request: {:?}", req),
            }
        },
        RemoteResolver {
            url: url.clone(),
            sender: tx,
            resolved: false,
        },
        invalid,
    );

    let mut router = router.tagged(our_addr);

    let local_addr = RoutingAddr::plane(0);
    let result = router.resolve_sender(local_addr).await;
    let _expected = ResolutionError::Addr(local_addr);

    assert!(matches!(result, Err(_expected)));
}

#[tokio::test]
async fn lookup_local_err() {
    let (mut router, _task) = crate::routing::fixture::router_fixture::<Path, _, _, _>(
        |req: PlaneRoutingRequest| async move {
            match req {
                PlaneRoutingRequest::Resolve { route, request, .. } => {
                    let _r = request
                        .send(Err(
                            RoutingError::Resolution(ResolutionError::Agent(route)).into()
                        ));
                }
                req => panic!("Unexpected request: {:?}", req),
            }
        },
        invalid,
        invalid,
    );

    let result = router.lookup(path()).await;
    assert_eq!(
        result,
        Err(RoutingError::Connection(ConnectionError::Resolution(
            ResolutionError::Agent(path())
        )))
    );
}
