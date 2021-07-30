// Copyright 2015-2021 SWIM.AI inc.
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

use crate::agent::context::AgentExecutionContext;
use crate::agent::Eff;
use futures::future::{join, join3, ready, BoxFuture};
use futures::{FutureExt, StreamExt};
use swim_common::routing::{ConnectionDropped, Origin, Route, Router, RoutingAddr, TaggedEnvelope, TaggedSender, BidirectionalRoute};
use tokio::sync::mpsc;

use std::sync::Arc;
use swim_common::warp::envelope::Envelope;
use swim_common::warp::path::RelativePath;

use crate::agent::lane::channels::uplink::stateless::StatelessUplinks;
use crate::agent::lane::channels::uplink::{AddressedUplinkMessage, UplinkAction, UplinkKind};
use crate::agent::lane::channels::TaggedAction;
use crate::meta::metric::uplink::{
    uplink_observer, TaggedWarpUplinkProfile, UplinkActionObserver, UplinkEventObserver,
    UplinkProfileSender, WarpUplinkProfile,
};
use crate::meta::metric::{aggregator_sink, NodeMetricAggregator};
use std::ops::Add;
use swim_common::routing::error::ResolutionError;
use swim_common::routing::error::RouterError;
use tokio::sync::mpsc::Receiver;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use url::Url;
use utilities::instant::AtomicInstant;
use utilities::sync::promise;
use utilities::uri::RelativeUri;

#[derive(Clone, Debug)]
struct TestRouter {
    router_addr: RoutingAddr,
    sender: mpsc::Sender<TaggedEnvelope>,
    _drop_tx: Arc<promise::Sender<ConnectionDropped>>,
    drop_rx: promise::Receiver<ConnectionDropped>,
}

impl TestRouter {
    fn new(router_addr: RoutingAddr, sender: mpsc::Sender<TaggedEnvelope>) -> Self {
        let (drop_tx, drop_rx) = promise::promise();
        TestRouter {
            router_addr,
            sender,
            _drop_tx: Arc::new(drop_tx),
            drop_rx,
        }
    }
}

impl Router for TestRouter {
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<Result<Route, ResolutionError>> {
        let TestRouter {
            sender, drop_rx, ..
        } = self;
        ready(Ok(Route::new(
            TaggedSender::new(addr, sender.clone()),
            drop_rx.clone(),
        )))
        .boxed()
    }

    fn resolve_bidirectional(&mut self, host: Option<Url>, route: RelativeUri) -> BoxFuture<'_, Result<BidirectionalRoute, ResolutionError>> {
        //Todo dm
        unimplemented!()
    }

    fn lookup(
        &mut self,
        _host: Option<Url>,
        _route: RelativeUri,
    ) -> BoxFuture<'static, Result<RoutingAddr, RouterError>> {
        panic!("Unexpected resolution attempt.")
    }
}

struct TestContext(
    TestRouter,
    mpsc::Sender<Eff>,
    RelativeUri,
    Arc<AtomicInstant>,
);

impl AgentExecutionContext for TestContext {
    type Router = TestRouter;

    fn router_handle(&self) -> Self::Router {
        self.0.clone()
    }

    fn spawner(&self) -> mpsc::Sender<Eff> {
        self.1.clone()
    }

    fn uri(&self) -> &RelativeUri {
        &self.2
    }

    fn metrics(&self) -> NodeMetricAggregator {
        aggregator_sink()
    }

    fn uplinks_idle_since(&self) -> &Arc<AtomicInstant> {
        &self.3
    }
}

async fn check_receive(
    rx: &mut mpsc::Receiver<TaggedEnvelope>,
    expected_addr: RoutingAddr,
    expected: Envelope,
) {
    let TaggedEnvelope(rec_addr, envelope) = rx.recv().await.unwrap();

    assert_eq!(rec_addr, expected_addr);
    assert_eq!(envelope, expected);
}

#[tokio::test]
async fn immediate_unlink_stateless_uplinks() {
    let route = RelativePath::new("node", "lane");
    let (producer_tx, producer_rx) = mpsc::channel::<AddressedUplinkMessage<i32>>(5);
    let (action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks = StatelessUplinks::new(
        ReceiverStream::new(producer_rx),
        route.clone(),
        UplinkKind::Supply,
        stub_action_observer(),
    );

    let router = TestRouter::new(RoutingAddr::local(1024), router_tx);

    let uplinks_task = uplinks.run(ReceiverStream::new(action_rx), router, error_tx, 256);

    let addr = RoutingAddr::remote(7);

    let assertion_task = async move {
        assert!(action_tx
            .send(TaggedAction(addr, UplinkAction::Unlink))
            .await
            .is_ok());

        check_receive(
            &mut router_rx,
            addr,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;

        drop(action_tx);
        drop(producer_tx);
    };

    join(uplinks_task, assertion_task).await;
}

fn stub_action_observer() -> UplinkActionObserver {
    let (tx, _rx) = mpsc::channel(48);
    let sender = UplinkProfileSender::new(RelativePath::new("node", "lane"), tx);

    uplink_observer(Duration::from_secs(1), sender).1
}

#[tokio::test]
async fn sync_with_stateless_uplinks() {
    let route = RelativePath::new("node", "lane");
    let (producer_tx, producer_rx) = mpsc::channel::<AddressedUplinkMessage<i32>>(5);
    let (action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks = StatelessUplinks::new(
        ReceiverStream::new(producer_rx),
        route.clone(),
        UplinkKind::Action,
        stub_action_observer(),
    );

    let router = TestRouter::new(RoutingAddr::local(1024), router_tx);

    let uplinks_task = uplinks.run(ReceiverStream::new(action_rx), router, error_tx, 256);

    let addr = RoutingAddr::remote(7);

    let assertion_task = async move {
        assert!(action_tx
            .send(TaggedAction(addr, UplinkAction::Link))
            .await
            .is_ok());

        check_receive(
            &mut router_rx,
            addr,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        drop(action_tx);
        drop(producer_tx);

        check_receive(
            &mut router_rx,
            addr,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;
    };

    join(uplinks_task, assertion_task).await;
}

#[tokio::test]
async fn sync_with_action_lane() {
    let route = RelativePath::new("node", "lane");
    let (producer_tx, producer_rx) = mpsc::channel::<AddressedUplinkMessage<i32>>(5);
    let (action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks = StatelessUplinks::new(
        ReceiverStream::new(producer_rx),
        route.clone(),
        UplinkKind::Action,
        stub_action_observer(),
    );

    let router = TestRouter::new(RoutingAddr::local(1024), router_tx);

    let uplinks_task = uplinks.run(ReceiverStream::new(action_rx), router, error_tx, 256);

    let addr = RoutingAddr::remote(7);

    let assertion_task = async move {
        assert!(action_tx
            .send(TaggedAction(addr, UplinkAction::Sync))
            .await
            .is_ok());

        check_receive(
            &mut router_rx,
            addr,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;
        check_receive(
            &mut router_rx,
            addr,
            Envelope::synced(&route.node, &route.lane),
        )
        .await;

        drop(action_tx);
        drop(producer_tx);

        check_receive(
            &mut router_rx,
            addr,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;
    };

    join(uplinks_task, assertion_task).await;
}

#[tokio::test]
async fn sync_after_link_on_stateless_uplinks() {
    let route = RelativePath::new("node", "lane");
    let (producer_tx, producer_rx) = mpsc::channel::<AddressedUplinkMessage<i32>>(5);
    let (action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks = StatelessUplinks::new(
        ReceiverStream::new(producer_rx),
        route.clone(),
        UplinkKind::Supply,
        stub_action_observer(),
    );

    let router = TestRouter::new(RoutingAddr::local(1024), router_tx);

    let uplinks_task = uplinks.run(ReceiverStream::new(action_rx), router, error_tx, 256);

    let addr = RoutingAddr::remote(7);

    let assertion_task = async move {
        assert!(action_tx
            .send(TaggedAction(addr, UplinkAction::Link))
            .await
            .is_ok());

        check_receive(
            &mut router_rx,
            addr,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        assert!(action_tx
            .send(TaggedAction(addr, UplinkAction::Sync))
            .await
            .is_ok());

        check_receive(
            &mut router_rx,
            addr,
            Envelope::synced(&route.node, &route.lane),
        )
        .await;

        drop(action_tx);
        drop(producer_tx);

        check_receive(
            &mut router_rx,
            addr,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;
    };

    join(uplinks_task, assertion_task).await;
}

#[tokio::test]
async fn link_to_and_receive_from_broadcast_uplinks() {
    let route = RelativePath::new("node", "lane");
    let (response_tx, response_rx) = mpsc::channel(5);
    let (action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks = StatelessUplinks::new(
        ReceiverStream::new(response_rx),
        route.clone(),
        UplinkKind::Supply,
        stub_action_observer(),
    );

    let router = TestRouter::new(RoutingAddr::local(1024), router_tx);

    let uplinks_task = uplinks.run(ReceiverStream::new(action_rx), router, error_tx, 256);

    let addr = RoutingAddr::remote(7);

    let assertion_task = async move {
        assert!(action_tx
            .send(TaggedAction(addr, UplinkAction::Link))
            .await
            .is_ok());

        check_receive(
            &mut router_rx,
            addr,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        assert!(response_tx
            .send(AddressedUplinkMessage::Broadcast(12))
            .await
            .is_ok());
        assert!(response_tx
            .send(AddressedUplinkMessage::Broadcast(17))
            .await
            .is_ok());

        check_receive(
            &mut router_rx,
            addr,
            Envelope::make_event(&route.node, &route.lane, Some(12.into())),
        )
        .await;
        check_receive(
            &mut router_rx,
            addr,
            Envelope::make_event(&route.node, &route.lane, Some(17.into())),
        )
        .await;

        drop(action_tx);
        drop(response_tx);

        check_receive(
            &mut router_rx,
            addr,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;
    };

    join(uplinks_task, assertion_task).await;
}

#[tokio::test]
async fn link_to_and_receive_from_addressed_uplinks() {
    let route = RelativePath::new("node", "lane");
    let (response_tx, response_rx) = mpsc::channel(5);
    let (action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks = StatelessUplinks::new(
        ReceiverStream::new(response_rx),
        route.clone(),
        UplinkKind::Supply,
        stub_action_observer(),
    );

    let router = TestRouter::new(RoutingAddr::local(1024), router_tx);

    let uplinks_task = uplinks.run(ReceiverStream::new(action_rx), router, error_tx, 256);

    let addr1 = RoutingAddr::remote(7);
    let addr2 = RoutingAddr::remote(13);

    let assertion_task = async move {
        assert!(action_tx
            .send(TaggedAction(addr1, UplinkAction::Link))
            .await
            .is_ok());
        check_receive(
            &mut router_rx,
            addr1,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        assert!(action_tx
            .send(TaggedAction(addr2, UplinkAction::Link))
            .await
            .is_ok());
        check_receive(
            &mut router_rx,
            addr2,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        assert!(response_tx
            .send(AddressedUplinkMessage::addressed(12, addr1))
            .await
            .is_ok());
        assert!(response_tx
            .send(AddressedUplinkMessage::addressed(17, addr2))
            .await
            .is_ok());

        check_receive(
            &mut router_rx,
            addr1,
            Envelope::make_event(&route.node, &route.lane, Some(12.into())),
        )
        .await;
        check_receive(
            &mut router_rx,
            addr2,
            Envelope::make_event(&route.node, &route.lane, Some(17.into())),
        )
        .await;

        drop(action_tx);
        drop(response_tx);

        let addrs = vec![addr1, addr2];

        for _ in &addrs {
            let TaggedEnvelope(rec_addr, envelope) = router_rx.recv().await.unwrap();
            assert_eq!(envelope, Envelope::unlinked(&route.node, &route.lane));
            assert!(addrs.contains(&rec_addr));
        }
    };

    join(uplinks_task, assertion_task).await;
}

#[tokio::test]
async fn link_twice_to_stateless_uplinks() {
    let route = RelativePath::new("node", "lane");
    let (response_tx, response_rx) = mpsc::channel(5);

    let (action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks = StatelessUplinks::new(
        ReceiverStream::new(response_rx),
        route.clone(),
        UplinkKind::Supply,
        stub_action_observer(),
    );

    let router = TestRouter::new(RoutingAddr::local(1024), router_tx);

    let uplinks_task = uplinks.run(ReceiverStream::new(action_rx), router, error_tx, 256);

    let addrs = vec![RoutingAddr::remote(7), RoutingAddr::remote(8)];

    let assertion_task = async move {
        for addr in &addrs {
            assert!(action_tx
                .send(TaggedAction(*addr, UplinkAction::Link))
                .await
                .is_ok());

            check_receive(
                &mut router_rx,
                *addr,
                Envelope::linked(&route.node, &route.lane),
            )
            .await;
        }

        let values: Vec<i32> = vec![1, 2, 3, 4, 5];

        for v in values {
            assert!(response_tx
                .send(AddressedUplinkMessage::Broadcast(v))
                .await
                .is_ok());

            for _ in &addrs {
                let TaggedEnvelope(rec_addr, envelope) = router_rx.recv().await.unwrap();
                let expected = Envelope::make_event(&route.node, &route.lane, Some(v.into()));

                assert!(addrs.contains(&rec_addr));
                assert_eq!(envelope, expected);
            }
        }

        drop(action_tx);
        drop(response_tx);

        for _ in &addrs {
            let TaggedEnvelope(rec_addr, envelope) = router_rx.recv().await.unwrap();
            assert_eq!(envelope, Envelope::unlinked(&route.node, &route.lane));
            assert!(addrs.contains(&rec_addr));
        }
    };

    join(uplinks_task, assertion_task).await;
}

#[tokio::test]
async fn no_messages_after_unlink_from_stateless_uplinks() {
    let route = RelativePath::new("node", "lane");
    let (response_tx, response_rx) = mpsc::channel(5);
    let (action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks = StatelessUplinks::new(
        ReceiverStream::new(response_rx),
        route.clone(),
        UplinkKind::Supply,
        stub_action_observer(),
    );

    let router = TestRouter::new(RoutingAddr::local(1024), router_tx);

    let uplinks_task = uplinks.run(ReceiverStream::new(action_rx), router, error_tx, 256);

    let addr1 = RoutingAddr::remote(7);
    let addr2 = RoutingAddr::remote(8);
    let addrs = vec![addr1, addr2];

    let assertion_task = async move {
        for addr in &addrs {
            assert!(action_tx
                .send(TaggedAction(*addr, UplinkAction::Link))
                .await
                .is_ok());

            check_receive(
                &mut router_rx,
                *addr,
                Envelope::linked(&route.node, &route.lane),
            )
            .await;
        }

        assert!(action_tx
            .send(TaggedAction(addr2, UplinkAction::Unlink))
            .await
            .is_ok());
        check_receive(
            &mut router_rx,
            addr2,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;

        assert!(response_tx
            .send(AddressedUplinkMessage::Broadcast(23))
            .await
            .is_ok());
        assert!(response_tx
            .send(AddressedUplinkMessage::Broadcast(25))
            .await
            .is_ok());

        check_receive(
            &mut router_rx,
            addr1,
            Envelope::make_event(&route.node, &route.lane, Some(23.into())),
        )
        .await;

        check_receive(
            &mut router_rx,
            addr1,
            Envelope::make_event(&route.node, &route.lane, Some(25.into())),
        )
        .await;

        drop(action_tx);
        drop(response_tx);

        check_receive(
            &mut router_rx,
            addr1,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;
    };

    join(uplinks_task, assertion_task).await;
}

// Asserts that any values that are sent to the lane when there are no uplinks are dropped and upon
// uplinking only the newly sent values are received.
#[tokio::test]
async fn send_no_uplink_stateless_uplinks() {
    let route = RelativePath::new("node", "lane");
    let (producer_tx, producer_rx) = mpsc::channel(5);
    let (action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let router = TestRouter::new(RoutingAddr::local(1024), router_tx);

    let uplinks = StatelessUplinks::new(
        ReceiverStream::new(producer_rx),
        route.clone(),
        UplinkKind::Supply,
        stub_action_observer(),
    );
    let uplinks_task = uplinks.run(ReceiverStream::new(action_rx), router, error_tx, 256);

    let addr = RoutingAddr::remote(7);

    let assertion_task = async move {
        let values: Vec<i32> = vec![1, 2, 3, 4, 5];
        for v in values {
            assert!(producer_tx
                .send(AddressedUplinkMessage::Broadcast(v))
                .await
                .is_ok());
        }

        assert!(action_tx
            .send(TaggedAction(addr, UplinkAction::Sync))
            .await
            .is_ok());

        check_receive(
            &mut router_rx,
            addr,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;
        check_receive(
            &mut router_rx,
            addr,
            Envelope::synced(&route.node, &route.lane),
        )
        .await;

        let values: Vec<i32> = vec![6, 7, 8, 9, 10];
        for v in values {
            assert!(producer_tx
                .send(AddressedUplinkMessage::Broadcast(v))
                .await
                .is_ok());

            check_receive(
                &mut router_rx,
                addr,
                Envelope::make_event(&route.node, &route.lane, Some(v.into())),
            )
            .await;
        }

        drop(action_tx);
        drop(producer_tx);

        check_receive(
            &mut router_rx,
            addr,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;
    };

    join(uplinks_task, assertion_task).await;
}

fn tracking_observer() -> (
    UplinkEventObserver,
    UplinkActionObserver,
    Receiver<TaggedWarpUplinkProfile>,
) {
    let (tx, rx) = mpsc::channel(48);
    let sender = UplinkProfileSender::new(RelativePath::new("node", "lane"), tx);

    let (event, action) = uplink_observer(Duration::from_secs(1), sender);
    (event, action, rx)
}

#[tokio::test]
async fn metrics() {
    let route = RelativePath::new("node", "lane");
    let (producer_tx, producer_rx) = mpsc::channel::<AddressedUplinkMessage<i32>>(5);
    let (action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(10);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let (event_observer, action_observer, metric_rx) = tracking_observer();

    let uplinks = StatelessUplinks::new(
        ReceiverStream::new(producer_rx),
        route.clone(),
        UplinkKind::Supply,
        action_observer,
    );

    let router = TestRouter::new(RoutingAddr::local(1024), router_tx);

    let uplinks_task = uplinks.run(ReceiverStream::new(action_rx), router, error_tx, 256);

    let assertion_task = async move {
        for i in 0..5 {
            let addr = RoutingAddr::remote(i);

            assert!(action_tx
                .send(TaggedAction(addr, UplinkAction::Link))
                .await
                .is_ok());

            check_receive(
                &mut router_rx,
                addr,
                Envelope::linked(&route.node, &route.lane),
            )
            .await;

            assert!(action_tx
                .send(TaggedAction(addr, UplinkAction::Sync))
                .await
                .is_ok());

            check_receive(
                &mut router_rx,
                addr,
                Envelope::synced(&route.node, &route.lane),
            )
            .await;

            assert!(action_tx
                .send(TaggedAction(addr, UplinkAction::Unlink))
                .await
                .is_ok());

            check_receive(
                &mut router_rx,
                addr,
                Envelope::unlinked(&route.node, &route.lane),
            )
            .await;
        }

        drop(action_tx);
        drop(producer_tx);

        event_observer.force_flush();
    };

    let receive_task = async move {
        let mut accumulated_profile = WarpUplinkProfile::default();
        let mut metric_stream = ReceiverStream::new(metric_rx);

        while let Some(tagged) = metric_stream.next().await {
            accumulated_profile = accumulated_profile.add(tagged.profile);
        }

        let expected_profile = WarpUplinkProfile {
            event_delta: 0,
            event_rate: 0,
            command_delta: 0,
            command_rate: 0,
            open_delta: 5,
            close_delta: 5,
        };
        assert_eq!(expected_profile, accumulated_profile);
    };

    join3(assertion_task, uplinks_task, receive_task).await;
}
