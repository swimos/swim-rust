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

use crate::routing::error::{ResolutionError, RouterError, Unresolvable};
use crate::routing::remote::router::RemoteRouter;
use crate::routing::remote::test_fixture::LocalRoutes;
use crate::routing::remote::{RawRoute, RoutingRequest};
use crate::routing::{Route, RoutingAddr, ServerRouter, TaggedEnvelope};
use futures::future::join;
use futures::io::ErrorKind;
use futures::{FutureExt, StreamExt};
use swim_common::model::Value;
use swim_common::routing::{ConnectionError, ConnectionErrorKind};
use swim_common::warp::envelope::Envelope;
use tokio::sync::mpsc;
use url::Url;
use utilities::sync::{promise, trigger};
use utilities::uri::RelativeUri;

const ADDR: RoutingAddr = RoutingAddr::remote(4);

async fn fake_resolution(
    rx: mpsc::Receiver<RoutingRequest>,
    url: Url,
    sender: mpsc::Sender<TaggedEnvelope>,
    stop_trigger: trigger::Receiver,
) {
    let mut rx = rx.take_until(stop_trigger);
    let mut resolved = false;

    let (_drop_tx, drop_rx) = promise::promise();

    while let Some(request) = rx.next().await {
        match request {
            RoutingRequest::Endpoint { addr, request } => {
                if resolved && addr == ADDR {
                    assert!(request
                        .send_ok(RawRoute::new(sender.clone(), drop_rx.clone()))
                        .is_ok());
                } else {
                    assert!(request.send_err(Unresolvable(addr)).is_ok());
                }
            }
            RoutingRequest::ResolveUrl { host, request } => {
                if host == url {
                    resolved = true;
                    assert!(request.send_ok(ADDR).is_ok());
                } else {
                    assert!(request
                        .send_err(ConnectionError::new(ConnectionErrorKind::Socket(
                            ErrorKind::NotFound
                        )))
                        .is_ok());
                }
            }
        }
    }
}

fn test_url() -> Url {
    "swim://remote:80".parse().unwrap()
}

fn path() -> RelativeUri {
    "/agent/lane".parse().unwrap()
}

fn envelope(body: &str) -> Envelope {
    Envelope::make_event("node", "lane", Some(Value::text(body)))
}

#[tokio::test]
async fn resolve_remote_ok() {
    let our_addr = RoutingAddr::remote(0);
    let delegate = LocalRoutes::new(our_addr);
    let (req_tx, req_rx) = mpsc::channel(8);
    let (tx, mut rx) = mpsc::channel(8);
    let (stop_tx, stop_rx) = trigger::trigger();
    let url = test_url();

    let mut router = RemoteRouter::new(our_addr, delegate, req_tx);
    let fake_resolver = fake_resolution(req_rx, url.clone(), tx, stop_rx);

    let task = async move {
        let result = router.lookup(Some(url), path()).await;
        assert_eq!(result, Ok(ADDR));
        let result = router.resolve_sender(ADDR).await;
        assert!(result.is_ok());
        let Route { mut sender, .. } = result.unwrap();
        assert!(sender.send_item(envelope("a")).await.is_ok());
        drop(stop_tx);
        let result = rx.next().now_or_never();
        assert_eq!(result, Some(Some(TaggedEnvelope(our_addr, envelope("a")))));
    };

    join(fake_resolver, task).await;
}

#[tokio::test]
async fn resolve_remote_failure() {
    let our_addr = RoutingAddr::remote(0);
    let delegate = LocalRoutes::new(our_addr);
    let (req_tx, req_rx) = mpsc::channel(8);
    let (tx, _rx) = mpsc::channel(8);
    let (stop_tx, stop_rx) = trigger::trigger();
    let url = test_url();

    let mut router = RemoteRouter::new(our_addr, delegate, req_tx);
    let fake_resolver = fake_resolution(req_rx, url.clone(), tx, stop_rx);

    let task = async move {
        let other_addr = RoutingAddr::remote(56);
        let result = router.resolve_sender(other_addr).await;
        assert!(
            matches!(result, Err(ResolutionError::Unresolvable(Unresolvable(a))) if a == other_addr)
        );
        drop(stop_tx);
    };

    join(fake_resolver, task).await;
}

#[tokio::test]
async fn lookup_remote_failure() {
    let our_addr = RoutingAddr::remote(0);
    let delegate = LocalRoutes::new(our_addr);
    let (req_tx, req_rx) = mpsc::channel(8);
    let (tx, _rx) = mpsc::channel(8);
    let (stop_tx, stop_rx) = trigger::trigger();
    let url = test_url();

    let mut router = RemoteRouter::new(our_addr, delegate, req_tx);
    let fake_resolver = fake_resolution(req_rx, url.clone(), tx, stop_rx);

    let task = async move {
        let other_url = "swim://other:80".parse().unwrap();
        let result = router.lookup(Some(other_url), path()).await;
        assert_eq!(
            result,
            Err(RouterError::ConnectionFailure(ConnectionError::new(
                ConnectionErrorKind::Socket(ErrorKind::NotFound)
            )))
        );
        drop(stop_tx);
    };

    join(fake_resolver, task).await;
}

#[tokio::test]
async fn delegate_local_ok() {
    let our_addr = RoutingAddr::remote(0);
    let delegate = LocalRoutes::new(our_addr);
    let mut rx = delegate.add(path());

    let (req_tx, req_rx) = mpsc::channel(8);
    let (tx, _rx) = mpsc::channel(8);
    let (stop_tx, stop_rx) = trigger::trigger();
    let url = test_url();

    let mut router = RemoteRouter::new(our_addr, delegate, req_tx);
    let fake_resolver = fake_resolution(req_rx, url.clone(), tx, stop_rx);

    let task = async move {
        let result = router.lookup(None, path()).await;
        assert!(result.is_ok());
        let local_addr = result.unwrap();

        let result = router.resolve_sender(local_addr).await;
        assert!(result.is_ok());
        let Route { mut sender, .. } = result.unwrap();
        assert!(sender.send_item(envelope("a")).await.is_ok());
        drop(stop_tx);
        let result = rx.next().now_or_never();
        assert_eq!(result, Some(Some(TaggedEnvelope(our_addr, envelope("a")))));
    };

    join(fake_resolver, task).await;
}

#[tokio::test]
async fn resolve_local_err() {
    let our_addr = RoutingAddr::remote(0);
    let delegate = LocalRoutes::new(our_addr);

    let (req_tx, req_rx) = mpsc::channel(8);
    let (tx, _rx) = mpsc::channel(8);
    let (stop_tx, stop_rx) = trigger::trigger();
    let url = test_url();

    let mut router = RemoteRouter::new(our_addr, delegate, req_tx);
    let fake_resolver = fake_resolution(req_rx, url.clone(), tx, stop_rx);

    let task = async move {
        let local_addr = RoutingAddr::local(0);

        let result = router.resolve_sender(local_addr).await;
        assert!(
            matches!(result, Err(ResolutionError::Unresolvable(Unresolvable(a))) if a == local_addr)
        );
        drop(stop_tx);
    };

    join(fake_resolver, task).await;
}

#[tokio::test]
async fn lookup_local_err() {
    let our_addr = RoutingAddr::remote(0);
    let delegate = LocalRoutes::new(our_addr);

    let (req_tx, req_rx) = mpsc::channel(8);
    let (tx, _rx) = mpsc::channel(8);
    let (stop_tx, stop_rx) = trigger::trigger();
    let url = test_url();

    let mut router = RemoteRouter::new(our_addr, delegate, req_tx);
    let fake_resolver = fake_resolution(req_rx, url.clone(), tx, stop_rx);

    let task = async move {
        let result = router.lookup(None, path()).await;
        assert_eq!(result, Err(RouterError::NoAgentAtRoute(path())));
        drop(stop_tx);
    };

    join(fake_resolver, task).await;
}
