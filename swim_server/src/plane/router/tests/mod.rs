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

use crate::plane::error::Unresolvable;
use crate::plane::router::{PlaneRouter, PlaneRouterFactory, PlaneRouterSender};
use crate::plane::PlaneRequest;
use crate::routing::{RoutingAddr, ServerRouter, TaggedEnvelope};
use futures::future::join;
use swim_common::routing::RoutingError;
use swim_common::sink::item::ItemSink;
use swim_common::warp::envelope::Envelope;
use tokio::sync::mpsc;

#[tokio::test]
async fn plane_router_sender() {
    let (tx, mut rx) = mpsc::channel(8);
    let mut sender = PlaneRouterSender::new(RoutingAddr::remote(7), tx);

    assert!(sender
        .send_item(Envelope::linked("node", "lane"))
        .await
        .is_ok());

    let received = rx.recv().await;
    assert_eq!(
        received,
        Some(TaggedEnvelope(
            RoutingAddr::remote(7),
            Envelope::linked("node", "lane")
        ))
    );
}

#[tokio::test]
async fn plane_router_get_sender() {
    let addr = RoutingAddr::remote(5);

    let (req_tx, mut req_rx) = mpsc::channel(8);
    let (send_tx, mut send_rx) = mpsc::channel(8);

    let mut router = PlaneRouter::new(addr, req_tx);

    let provider_task = async move {
        while let Some(req) = req_rx.recv().await {
            if let PlaneRequest::Endpoint { id, request } = req {
                if id == addr {
                    assert!(request.send_ok(send_tx.clone()).is_ok());
                } else {
                    assert!(request.send_err(Unresolvable(id)).is_ok());
                }
            } else {
                panic!("Unexpected request {:?}!", req);
            }
        }
    };

    let send_task = async move {
        let result1 = router.get_sender(addr).await;
        assert!(result1.is_ok());
        let mut sender = result1.unwrap();
        assert!(sender
            .send_item(Envelope::linked("node", "lane"))
            .await
            .is_ok());
        assert_eq!(
            send_rx.recv().await,
            Some(TaggedEnvelope(addr, Envelope::linked("node", "lane")))
        );

        let result2 = router.get_sender(RoutingAddr::local(56)).await;

        assert!(matches!(result2, Err(RoutingError::HostUnreachable)));
    };

    join(provider_task, send_task).await;
}

#[tokio::test]
async fn plane_router_factory() {
    let (req_tx, _req_rx) = mpsc::channel(8);
    let fac = PlaneRouterFactory::new(req_tx);
    let router = fac.create(RoutingAddr::local(56));
    assert_eq!(router.tag, RoutingAddr::local(56));
}
