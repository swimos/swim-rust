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

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::model::map::MapLane;
use crate::agent::lane::model::value::ValueLane;
use crate::agent::{
    agent_lifecycle, map_lifecycle, value_lifecycle, AgentParameters, SwimAgent, TestClock,
};
use crate::interface::ServerDownlinksConfig;
use crate::meta::info::{LaneInfo, LaneKind};
use crate::plane::provider::AgentProvider;
use crate::routing::TopLevelServerRouterFactory;
use futures::future::BoxFuture;
use futures::FutureExt;
use server_store::agent::mock::MockNodeStore;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;
use swim_async_runtime::time::timeout;
use swim_client::connections::SwimConnPool;
use swim_client::downlink::Downlinks;
use swim_client::interface::ClientContext;
use swim_client::router::ClientRouterFactory;
use swim_form::structural::read::ReadError;
use swim_form::Form;
use swim_model::record;
use swim_runtime::configuration::DownlinkConnectionsConfig;
use swim_runtime::error::{ConnectionDropped, ResolutionError, RouterError};
use swim_runtime::routing::{Route, Router, RoutingAddr, TaggedEnvelope, TaggedSender};
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger::promise;
use swim_warp::envelope::{Envelope, EnvelopeKind};
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use url::Url;

mod swim_server {
    pub use crate::*;
}

#[test]
fn lane_info_form_ok() {
    let lane_info = LaneInfo::new("/lane", LaneKind::Map);
    let value = lane_info.clone().into_value();

    let expected = record! {
        attrs => ["LaneInfo"],
        items => [
            ("laneUri", "/lane"),
            ("laneType", "Map")
        ]
    };

    assert_eq!(value, expected);
    assert_eq!(LaneInfo::try_convert(value), Ok(lane_info));
}

#[test]
fn lane_info_form_err() {
    let value = record! {
        attrs => ["LaneInfo"],
        items => [
            ("laneUri", "/lane"),
            ("laneType", "Meta")
        ]
    };
    assert!(matches!(
        LaneInfo::try_convert(value),
        Err(ReadError::Malformatted { .. })
    ));

    let value = record! {
        attrs => ["LaneInfoy"],
        items => [
            ("laneUri", "/lane"),
            ("laneType", "Meta")
        ]
    };

    assert!(matches!(
        LaneInfo::try_convert(value),
        Err(ReadError::UnexpectedAttribute(name)) if name == "LaneInfoy"
    ));

    let value = record! {
        attrs => ["LaneInfo"],
        items => [
            ("laneUriy", "/lane"),
            ("laneType", "Meta")
        ]
    };

    assert!(matches!(
        LaneInfo::try_convert(value),
        Err(ReadError::UnexpectedField(name)) if name == "laneUriy"
    ));
}

#[derive(Default, Debug, Clone)]
struct MockAgentConfig;

#[derive(Debug, SwimAgent)]
#[agent(config = "MockAgentConfig")]
struct MockAgent {
    #[lifecycle(name = "ValueLifecycle")]
    pub value: ValueLane<i32>,
    #[lifecycle(name = "MapLifecycle")]
    pub map: MapLane<String, i32>,
}

#[value_lifecycle(agent = "MockAgent", event_type = "i32")]
struct ValueLifecycle;

#[map_lifecycle(agent = "MockAgent", key_type = "String", value_type = "i32")]
struct MapLifecycle;

#[agent_lifecycle(agent = "MockAgent")]
struct MockAgentLifecycle;

#[derive(Clone)]
struct MockRouter {
    router_addr: RoutingAddr,
    inner: mpsc::Sender<TaggedEnvelope>,
    _drop_tx: Arc<promise::Sender<ConnectionDropped>>,
    drop_rx: promise::Receiver<ConnectionDropped>,
}

impl MockRouter {
    fn new(router_addr: RoutingAddr, inner: mpsc::Sender<TaggedEnvelope>) -> MockRouter {
        let (tx, rx) = promise::promise();

        MockRouter {
            router_addr,
            inner,
            _drop_tx: Arc::new(tx),
            drop_rx: rx,
        }
    }
}

impl Router for MockRouter {
    fn resolve_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Route, ResolutionError>> {
        async move {
            let MockRouter { inner, drop_rx, .. } = self;
            let route = Route::new(TaggedSender::new(addr, inner.clone()), drop_rx.clone());
            Ok(route)
        }
        .boxed()
    }

    fn lookup(
        &mut self,
        _host: Option<Url>,
        _route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
        panic!("Unexpected resolution attempt.")
    }
}

#[tokio::test]
async fn lane_info_sync() {
    let (tx, mut rx) = mpsc::channel(5);
    let uri = RelativeUri::try_from("/test").unwrap();
    let buffer_size = non_zero_usize!(10);
    let clock = TestClock::default();
    let exec_config = AgentExecutionConfig::with(
        buffer_size,
        1,
        0,
        Duration::from_secs(1),
        None,
        Duration::from_secs(60),
    );
    let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size.get());
    let provider = AgentProvider::new(MockAgentConfig, MockAgentLifecycle);

    let (client_tx, client_rx) = mpsc::channel(8);
    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let (plane_tx, _plane_rx) = mpsc::channel(8);
    let (_close_tx, close_rx) = promise::promise();

    let top_level_factory =
        TopLevelServerRouterFactory::new(plane_tx, client_tx.clone(), remote_tx);

    let client_router_fac = ClientRouterFactory::new(client_tx.clone(), top_level_factory);

    let (conn_pool, _pool_task) = SwimConnPool::new(
        DownlinkConnectionsConfig::default(),
        (client_tx, client_rx),
        client_router_fac,
        close_rx.clone(),
    );

    let (downlinks, _downlinks_task) = Downlinks::new(
        non_zero_usize!(8),
        conn_pool,
        Arc::new(ServerDownlinksConfig::default()),
        close_rx,
    );

    let client = ClientContext::new(downlinks);

    let parameters = AgentParameters::new(MockAgentConfig, exec_config, uri, HashMap::new());

    let (_a, agent_proc) = provider.run(
        parameters,
        clock.clone(),
        client,
        ReceiverStream::new(envelope_rx),
        MockRouter::new(RoutingAddr::plane(1024), tx),
        MockNodeStore::mock(),
    );

    let _agent_task = swim_async_runtime::task::spawn(agent_proc);

    assert!(envelope_tx
        .send(TaggedEnvelope(
            RoutingAddr::remote(1),
            Envelope::sync()
                .node_uri("/swim:meta:node/test/")
                .lane_uri("lanes")
                .done(),
        ))
        .await
        .is_ok());

    let assert_task = async move {
        let link = rx.recv().await.expect("No linked envelope received");
        assert_eq!(
            link.1,
            Envelope::linked()
                .node_uri("/test")
                .lane_uri("lanes")
                .done()
        );

        let mut expected_events = HashSet::new();
        expected_events.insert(record! {
            attrs => [
                ("update", record!(items => [("key", "map")])),
                ("LaneInfo")
            ],
            items => [
                ("laneUri", "map"),
                ("laneType", "Map")
            ]
        });

        expected_events.insert(record! {
            attrs => [
                ("update", record!(items => [("key", "value")])),
                ("LaneInfo")
            ],
            items => [
                ("laneUri", "value"),
                ("laneType", "Value")
            ]
        });

        let event = rx.recv().await.expect("Expected an event");
        assert_eq!(event.1.kind(), EnvelopeKind::Event);
        assert!(expected_events.contains(&event.1.body().expect("Missing event body")));

        let event = rx.recv().await.expect("Expected an event");
        assert_eq!(event.1.kind(), EnvelopeKind::Event);
        assert!(expected_events.contains(&event.1.body().expect("Missing event body")));

        let link = rx.recv().await.expect("No synced envelope received");
        assert_eq!(
            link.1,
            Envelope::synced()
                .node_uri("/test")
                .lane_uri("lanes")
                .done()
        );

        let link = rx.recv().await.expect("No unlinked envelope received");
        assert_eq!(
            link.1,
            Envelope::unlinked()
                .node_uri("/test")
                .lane_uri("lanes")
                .done()
        );

        assert!(rx.recv().now_or_never().is_none());
    };

    let _jh = tokio::spawn(timeout::timeout(Duration::from_secs(5), assert_task))
        .await
        .expect("No response received");
}
