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

use crate::routing::{RoutingAddr, TaggedEnvelope, TaggedSender};
use swim_warp::envelope::Envelope;
use tokio::sync::mpsc;

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

// use crate::error::{ConnectionError, IoError, ResolutionError};
// use crate::routing::{Router, RoutingError};
// use crate::remote::test_fixture::LocalRoutes;
// use crate::remote::{RawRoute, RemoteRoutingRequest};
// use crate::routing::{Route, RoutingAddr, TaggedEnvelope};
// use futures::future::join;
// use futures::io::ErrorKind;
// use futures::{FutureExt, StreamExt};
// use swim_utilities::routing::uri::RelativeUri;
// use swim_utilities::trigger;
// use swim_utilities::trigger::promise;
// use swim_warp::envelope::Envelope;
// use tokio::sync::mpsc;
// use tokio_stream::wrappers::ReceiverStream;
// use url::Url;
//
// const ADDR: RoutingAddr = RoutingAddr::remote(4);
//
// async fn fake_resolution(
//     rx: mpsc::Receiver<RemoteRoutingRequest>,
//     url: Url,
//     sender: mpsc::Sender<TaggedEnvelope>,
//     stop_trigger: trigger::Receiver,
// ) {
//     let mut rx = ReceiverStream::new(rx).take_until(stop_trigger);
//     let mut resolved = false;
//
//     let (_drop_tx, drop_rx) = promise::promise();
//
//     while let Some(request) = rx.next().await {
//         match request {
//             RemoteRoutingRequest::Endpoint { addr, request } => {
//                 if resolved && addr == ADDR {
//                     assert!(request
//                         .send_ok(RawRoute::new(sender.clone(), drop_rx.clone()))
//                         .is_ok());
//                 } else {
//                     assert!(request.send_err(ResolutionError::Addr(addr)).is_ok());
//                 }
//             }
//             RemoteRoutingRequest::ResolveUrl { host, request } => {
//                 if host == url {
//                     resolved = true;
//                     assert!(request.send_ok(ADDR).is_ok());
//                 } else {
//                     assert!(request
//                         .send_err(ConnectionError::Io(IoError::new(ErrorKind::NotFound, None)))
//                         .is_ok());
//                 }
//             }
//             RemoteRoutingRequest::Bidirectional { .. } => {}
//         }
//     }
// }
//
// fn test_url() -> Url {
//     "swim://remote:80".parse().unwrap()
// }
//
// fn path() -> RelativeUri {
//     "/agent/lane".parse().unwrap()
// }
//
// fn envelope(body: &str) -> Envelope {
//     Envelope::event()
//         .node_uri("node")
//         .lane_uri("lane")
//         .body(body)
//         .done()
// }
//
// #[tokio::test]
// async fn resolve_remote_ok() {
//     let our_addr = RoutingAddr::remote(0);
//     let delegate = LocalRoutes::new(our_addr);
//     let (req_tx, req_rx) = mpsc::channel(8);
//     let (tx, mut rx) = mpsc::channel(8);
//     let (stop_tx, stop_rx) = trigger::trigger();
//     let url = test_url();
//
//     let mut router = Router::server(our_addr, delegate, req_tx);
//     let fake_resolver = fake_resolution(req_rx, url.clone(), tx, stop_rx);
//
//     let task = async move {
//         let result = router.lookup(Some(url), path()).await;
//         assert_eq!(result, Ok(ADDR));
//         let result = router.resolve_sender(ADDR).await;
//         assert!(result.is_ok());
//         let Route { mut sender, .. } = result.unwrap();
//         assert!(sender.send_item(envelope("a")).await.is_ok());
//         drop(stop_tx);
//         let result = rx.recv().now_or_never();
//         assert_eq!(result, Some(Some(TaggedEnvelope(our_addr, envelope("a")))));
//     };
//
//     join(fake_resolver, task).await;
// }
//
// #[tokio::test]
// async fn resolve_remote_failure() {
//     let our_addr = RoutingAddr::remote(0);
//     let delegate = LocalRoutes::new(our_addr);
//     let (req_tx, req_rx) = mpsc::channel(8);
//     let (tx, _rx) = mpsc::channel(8);
//     let (stop_tx, stop_rx) = trigger::trigger();
//     let url = test_url();
//
//     let mut router = Router::server(our_addr, delegate, req_tx);
//     let fake_resolver = fake_resolution(req_rx, url.clone(), tx, stop_rx);
//
//     let task = async move {
//         let other_addr = RoutingAddr::remote(56);
//         let result = router.resolve_sender(other_addr).await;
//         let _expected = ResolutionError::unresolvable(other_addr.to_string());
//
//         assert!(matches!(result, Err(_expected)));
//         drop(stop_tx);
//     };
//
//     join(fake_resolver, task).await;
// }
//
// #[tokio::test]
// async fn lookup_remote_failure() {
//     let our_addr = RoutingAddr::remote(0);
//     let delegate = LocalRoutes::new(our_addr);
//     let (req_tx, req_rx) = mpsc::channel(8);
//     let (tx, _rx) = mpsc::channel(8);
//     let (stop_tx, stop_rx) = trigger::trigger();
//     let url = test_url();
//
//     let mut router = Router::server(our_addr, delegate, req_tx);
//     let fake_resolver = fake_resolution(req_rx, url.clone(), tx, stop_rx);
//
//     let task = async move {
//         let other_url = "swim://other:80".parse().unwrap();
//         let result = router.lookup(Some(other_url), path()).await;
//         assert_eq!(
//             result,
//             Err(RouterError::ConnectionFailure(ConnectionError::Io(
//                 IoError::new(ErrorKind::NotFound, None)
//             )))
//         );
//         drop(stop_tx);
//     };
//
//     join(fake_resolver, task).await;
// }
//
// #[tokio::test]
// async fn delegate_local_ok() {
//     let our_addr = RoutingAddr::remote(0);
//     let delegate = LocalRoutes::new(our_addr);
//     let mut rx = delegate.add(path());
//
//     let (req_tx, req_rx) = mpsc::channel(8);
//     let (tx, _rx) = mpsc::channel(8);
//     let (stop_tx, stop_rx) = trigger::trigger();
//     let url = test_url();
//
//     let mut router = Router::server(our_addr, delegate, req_tx);
//     let fake_resolver = fake_resolution(req_rx, url.clone(), tx, stop_rx);
//
//     let task = async move {
//         let result = router.lookup(None, path()).await;
//         assert!(result.is_ok());
//         let local_addr = result.unwrap();
//
//         let result = router.resolve_sender(local_addr).await;
//         assert!(result.is_ok());
//         let Route { mut sender, .. } = result.unwrap();
//         assert!(sender.send_item(envelope("a")).await.is_ok());
//         drop(stop_tx);
//         let result = rx.recv().now_or_never();
//         assert_eq!(result, Some(Some(TaggedEnvelope(our_addr, envelope("a")))));
//     };
//
//     join(fake_resolver, task).await;
// }
//
// #[tokio::test]
// async fn resolve_local_err() {
//     let our_addr = RoutingAddr::remote(0);
//     let delegate = LocalRoutes::new(our_addr);
//
//     let (req_tx, req_rx) = mpsc::channel(8);
//     let (tx, _rx) = mpsc::channel(8);
//     let (stop_tx, stop_rx) = trigger::trigger();
//     let url = test_url();
//
//     let mut router = Router::server(our_addr, delegate, req_tx);
//     let fake_resolver = fake_resolution(req_rx, url.clone(), tx, stop_rx);
//
//     let task = async move {
//         let local_addr = RoutingAddr::plane(0);
//         let result = router.resolve_sender(local_addr).await;
//         let _expected = ResolutionError::unresolvable(local_addr.to_string());
//
//         assert!(matches!(result, Err(_expected)));
//         drop(stop_tx);
//     };
//
//     join(fake_resolver, task).await;
// }
//
// #[tokio::test]
// async fn lookup_local_err() {
//     let our_addr = RoutingAddr::remote(0);
//     let delegate = LocalRoutes::new(our_addr);
//
//     let (req_tx, req_rx) = mpsc::channel(8);
//     let (tx, _rx) = mpsc::channel(8);
//     let (stop_tx, stop_rx) = trigger::trigger();
//     let url = test_url();
//
//     let mut router = Router::server(our_addr, delegate, req_tx);
//     let fake_resolver = fake_resolution(req_rx, url.clone(), tx, stop_rx);
//
//     let task = async move {
//         let result = router.lookup(None, path()).await;
//         assert_eq!(result, Err(RouterError::NoAgentAtRoute(path())));
//         drop(stop_tx);
//     };
//
//     join(fake_resolver, task).await;
// }
