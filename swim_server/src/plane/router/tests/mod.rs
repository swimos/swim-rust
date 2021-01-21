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

use crate::plane::router::{PlaneRouter, PlaneRouterFactory};
use crate::plane::PlaneRequest;
use crate::routing::error::{RouterError, Unresolvable};
use crate::routing::remote::RawRoute;
use crate::routing::TaggedAgentEnvelope;
use crate::routing::{
    RoutingAddr, ServerRouter, ServerRouterFactory, TaggedEnvelope, TopLevelRouter,
    TopLevelRouterFactory,
};
use futures::future::join;
use swim_common::routing::{ConnectionError, ProtocolError, ResolutionErrorKind};
use swim_common::warp::envelope::Envelope;
use tokio::sync::mpsc;
use url::Url;
use utilities::sync::promise;

#[tokio::test]
async fn plane_router_get_sender() {
    let addr = RoutingAddr::local(5);

    let (req_tx, mut req_rx) = mpsc::channel(8);
    let (send_tx, mut send_rx) = mpsc::channel(8);
    let (_drop_tx, drop_rx) = promise::promise();

    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let top_level_router = TopLevelRouter::new(addr, req_tx.clone(), remote_tx);

    let mut router = PlaneRouter::new(addr, top_level_router, req_tx);

    let provider_task = async move {
        while let Some(req) = req_rx.recv().await {
            if let PlaneRequest::Endpoint { id, request } = req {
                if id == addr {
                    assert!(request
                        .send_ok(RawRoute::new(send_tx.clone(), drop_rx.clone()))
                        .is_ok());
                } else {
                    assert!(request.send_err(Unresolvable(id)).is_ok());
                }
            } else {
                panic!("Unexpected request {:?}!", req);
            }
        }
    };

    let send_task = async move {
        let result1 = router.resolve_sender(addr).await;
        assert!(result1.is_ok());
        let mut sender = result1.unwrap();
        assert!(sender
            .sender
            .transform_and_send(Envelope::linked("/node", "lane"))
            .await
            .is_ok());
        assert_eq!(
            send_rx.recv().await,
            Some(TaggedEnvelope::agent(TaggedAgentEnvelope(
                addr,
                Envelope::linked("/node", "lane")
            )))
        );

        let result2 = router.resolve_sender(RoutingAddr::local(56)).await;

        assert!(matches!(
            result2.err().unwrap().kind(),
            ResolutionErrorKind::Unresolvable
        ));
    };

    join(provider_task, send_task).await;
}

#[tokio::test]
async fn plane_router_factory() {
    let (req_tx, _req_rx) = mpsc::channel(8);

    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let top_level_router_factory = TopLevelRouterFactory::new(req_tx.clone(), remote_tx);

    let fac = PlaneRouterFactory::new(req_tx, top_level_router_factory);
    let router = fac.create_for(RoutingAddr::local(56));
    assert_eq!(router.tag, RoutingAddr::local(56));
}

#[tokio::test]
async fn plane_router_resolve() {
    let host = Url::parse("warp:://somewhere").unwrap();
    let addr = RoutingAddr::remote(5);

    let (req_tx, mut req_rx) = mpsc::channel(8);

    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let top_level_router = TopLevelRouter::new(addr, req_tx.clone(), remote_tx);

    let mut router = PlaneRouter::new(addr, top_level_router, req_tx);

    let host_cpy = host.clone();

    let provider_task = async move {
        while let Some(req) = req_rx.recv().await {
            if let PlaneRequest::Resolve {
                host,
                name,
                request,
            } = req
            {
                if host == Some(host_cpy.clone()) && name == "/node" {
                    assert!(request.send_ok(addr).is_ok());
                } else if host.is_some() {
                    assert!(request
                        .send_err(RouterError::ConnectionFailure(ConnectionError::Protocol(
                            ProtocolError::warp(Some("Boom!".into()))
                        )))
                        .is_ok());
                } else {
                    assert!(request.send_err(RouterError::NoAgentAtRoute(name)).is_ok());
                }
            } else {
                panic!("Unexpected request {:?}!", req);
            }
        }
    };

    let send_task = async move {
        let result1 = router.lookup(Some(host), "/node".parse().unwrap()).await;
        assert!(matches!(result1, Ok(a) if a == addr));

        let other_host = Url::parse("warp://other").unwrap();

        let result2 = router
            .lookup(Some(other_host), "/node".parse().unwrap())
            .await;

        let _expected = RouterError::ConnectionFailure(ConnectionError::Protocol(
            ProtocolError::warp(Some("Boom!".into())),
        ));

        assert!(matches!(result2, Err(_expected)));

        let result3 = router.lookup(None, "/node".parse().unwrap()).await;
        assert!(matches!(result3, Err(RouterError::NoAgentAtRoute(name)) if name == "/node"));
    };

    join(provider_task, send_task).await;
}
