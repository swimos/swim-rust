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

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::uplink::action::ActionLaneUplinks;
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::{UplinkAction, UplinkError};
use crate::agent::lane::channels::TaggedAction;
use crate::agent::Eff;
use crate::plane::error::ResolutionError;
use crate::routing::{RoutingAddr, ServerRouter, TaggedEnvelope};
use futures::future::{join, ready, BoxFuture};
use futures::FutureExt;
use http::Uri;
use swim_common::routing::RoutingError;
use swim_common::sink::item::ItemSink;
use swim_common::warp::envelope::Envelope;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use url::Url;

struct TestContext(TestRouter, mpsc::Sender<Eff>);
#[derive(Clone, Debug)]
struct TestRouter(mpsc::Sender<TaggedEnvelope>);
#[derive(Clone, Debug)]
struct TestSender(RoutingAddr, mpsc::Sender<TaggedEnvelope>);

impl ServerRouter for TestRouter {
    type Sender = TestSender;

    fn get_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Self::Sender, RoutingError>> {
        ready(Ok(TestSender(addr, self.0.clone()))).boxed()
    }

    fn resolve(
        &mut self,
        _host: Option<Url>,
        _route: Uri,
    ) -> BoxFuture<'static, Result<RoutingAddr, ResolutionError>> {
        panic!("Unexpected resolution attempt.")
    }
}

impl<'a> ItemSink<'a, Envelope> for TestSender {
    type Error = RoutingError;
    type SendFuture = BoxFuture<'a, Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: Envelope) -> Self::SendFuture {
        let tagged = TaggedEnvelope(self.0, value);
        async move {
            self.1
                .send(tagged)
                .await
                .map_err(|_| RoutingError::RouterDropped)
        }
        .boxed()
    }
}

impl AgentExecutionContext for TestContext {
    type Router = TestRouter;

    fn router_handle(&self) -> Self::Router {
        self.0.clone()
    }

    fn spawner(&self) -> mpsc::Sender<Eff> {
        self.1.clone()
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
async fn immediate_unlink_action_lane() {
    let route = RelativePath::new("node", "lane");
    let (response_tx, response_rx) = mpsc::channel(5);
    let (mut action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks: ActionLaneUplinks<i32> = ActionLaneUplinks::new(response_rx, route.clone());

    let router = TestRouter(router_tx);

    let uplinks_task = uplinks.run(action_rx, router, error_tx);

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
        drop(response_tx);
    };

    join(uplinks_task, assertion_task).await;
}

#[tokio::test]
async fn link_to_action_lane() {
    let route = RelativePath::new("node", "lane");
    let (response_tx, response_rx) = mpsc::channel(5);
    let (mut action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks: ActionLaneUplinks<i32> = ActionLaneUplinks::new(response_rx, route.clone());

    let router = TestRouter(router_tx);

    let uplinks_task = uplinks.run(action_rx, router, error_tx);

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
async fn sync_with_action_lane() {
    let route = RelativePath::new("node", "lane");
    let (response_tx, response_rx) = mpsc::channel(5);
    let (mut action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks: ActionLaneUplinks<i32> = ActionLaneUplinks::new(response_rx, route.clone());

    let router = TestRouter(router_tx);

    let uplinks_task = uplinks.run(action_rx, router, error_tx);

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
async fn sync_after_link_on_action_lane() {
    let route = RelativePath::new("node", "lane");
    let (response_tx, response_rx) = mpsc::channel(5);
    let (mut action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks: ActionLaneUplinks<i32> = ActionLaneUplinks::new(response_rx, route.clone());

    let router = TestRouter(router_tx);

    let uplinks_task = uplinks.run(action_rx, router, error_tx);

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
async fn link_to_and_receive_from_action_lane() {
    let route = RelativePath::new("node", "lane");
    let (mut response_tx, response_rx) = mpsc::channel(5);
    let (mut action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks: ActionLaneUplinks<i32> = ActionLaneUplinks::new(response_rx, route.clone());

    let router = TestRouter(router_tx);

    let uplinks_task = uplinks.run(action_rx, router, error_tx);

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

        assert!(response_tx.send((addr, 12)).await.is_ok());
        assert!(response_tx.send((addr, 17)).await.is_ok());

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
async fn link_twice_to_action_lane() {
    let route = RelativePath::new("node", "lane");
    let (mut response_tx, response_rx) = mpsc::channel(5);
    let (mut action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks: ActionLaneUplinks<i32> = ActionLaneUplinks::new(response_rx, route.clone());

    let router = TestRouter(router_tx);

    let uplinks_task = uplinks.run(action_rx, router, error_tx);

    let addr1 = RoutingAddr::remote(7);
    let addr2 = RoutingAddr::remote(8);

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

        assert!(response_tx.send((addr2, 23)).await.is_ok());
        check_receive(
            &mut router_rx,
            addr2,
            Envelope::make_event(&route.node, &route.lane, Some(23.into())),
        )
        .await;

        assert!(response_tx.send((addr1, 25)).await.is_ok());
        check_receive(
            &mut router_rx,
            addr1,
            Envelope::make_event(&route.node, &route.lane, Some(25.into())),
        )
        .await;

        drop(action_tx);
        drop(response_tx);

        let TaggedEnvelope(first_unlinked, envelope) = router_rx.recv().await.unwrap();

        assert_eq!(envelope, Envelope::unlinked(&route.node, &route.lane));
        assert!(first_unlinked == addr1 || first_unlinked == addr2);

        let TaggedEnvelope(second_unlinked, envelope) = router_rx.recv().await.unwrap();

        assert_eq!(envelope, Envelope::unlinked(&route.node, &route.lane));
        assert!(second_unlinked == addr1 || second_unlinked == addr2);
        assert_ne!(first_unlinked, second_unlinked);
    };

    join(uplinks_task, assertion_task).await;
}

#[tokio::test]
async fn no_messages_after_unlink_from_action_lane() {
    let route = RelativePath::new("node", "lane");
    let (mut response_tx, response_rx) = mpsc::channel(5);
    let (mut action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, mut router_rx) = mpsc::channel(5);
    let (error_tx, _error_rx) = mpsc::channel(5);

    let uplinks: ActionLaneUplinks<i32> = ActionLaneUplinks::new(response_rx, route.clone());

    let router = TestRouter(router_tx);

    let uplinks_task = uplinks.run(action_rx, router, error_tx);

    let addr1 = RoutingAddr::remote(7);
    let addr2 = RoutingAddr::remote(8);

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

        assert!(response_tx.send((addr2, 23)).await.is_ok());

        assert!(response_tx.send((addr1, 25)).await.is_ok());
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

#[tokio::test]
async fn report_errors_from_action_lane() {
    let route = RelativePath::new("node", "lane");
    let (response_tx, response_rx) = mpsc::channel(5);
    let (mut action_tx, action_rx) = mpsc::channel(5);
    let (router_tx, router_rx) = mpsc::channel(5);
    let (error_tx, mut error_rx) = mpsc::channel(5);

    let uplinks: ActionLaneUplinks<i32> = ActionLaneUplinks::new(response_rx, route.clone());

    let router = TestRouter(router_tx);

    let uplinks_task = uplinks.run(action_rx, router, error_tx);

    let addr = RoutingAddr::remote(7);

    let assertion_task = async move {
        drop(router_rx);

        assert!(action_tx
            .send(TaggedAction(addr, UplinkAction::Link))
            .await
            .is_ok());

        let err_result = error_rx.recv().await;
        assert!(
            matches!(err_result, Some(UplinkErrorReport { error: UplinkError::ChannelDropped, addr: rec_addr }) if rec_addr == addr)
        );

        drop(action_tx);
        drop(response_tx);
    };

    join(uplinks_task, assertion_task).await;
}
