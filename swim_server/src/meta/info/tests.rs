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
use crate::agent::store::mock::MockNodeStore;
use crate::agent::{agent_lifecycle, map_lifecycle, value_lifecycle, SwimAgent, TestClock};
use crate::meta::info::{LaneInfo, LaneKind};
use crate::plane::provider::AgentProvider;
use crate::routing::error::RouterError;
use crate::routing::{
    ConnectionDropped, Route, RoutingAddr, ServerRouter, TaggedEnvelope, TaggedSender,
};
use futures::future::BoxFuture;
use futures::FutureExt;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_common::form::{Form, FormErr};
use swim_common::record;
use swim_common::routing::ResolutionError;
use swim_common::warp::envelope::Envelope;
use swim_runtime::time::timeout;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use url::Url;
use utilities::sync::promise;
use utilities::uri::RelativeUri;

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
    assert_eq!(LaneInfo::try_convert(value), Err(FormErr::Malformatted));

    let value = record! {
        attrs => ["LaneInfoy"],
        items => [
            ("laneUri", "/lane"),
            ("laneType", "Meta")
        ]
    };
    assert_eq!(LaneInfo::try_convert(value), Err(FormErr::MismatchedTag));

    let value = record! {
        attrs => ["LaneInfo"],
        items => [
            ("laneUriy", "/lane"),
            ("laneType", "Meta")
        ]
    };
    assert_eq!(LaneInfo::try_convert(value), Err(FormErr::Malformatted));
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

impl ServerRouter for MockRouter {
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
    let buffer_size = NonZeroUsize::new(10).unwrap();
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

    let (_a, agent_proc) = provider.run(
        uri,
        HashMap::new(),
        exec_config,
        clock.clone(),
        ReceiverStream::new(envelope_rx),
        MockRouter::new(RoutingAddr::local(1024), tx),
        MockNodeStore::mock(),
    );

    let _agent_task = swim_runtime::task::spawn(agent_proc);

    assert!(envelope_tx
        .send(TaggedEnvelope(
            RoutingAddr::remote(1),
            Envelope::sync("/swim:meta:node/test/", "lanes"),
        ))
        .await
        .is_ok());

    let assert_task = async move {
        let link = rx.recv().await.expect("No linked envelope received");
        assert_eq!(link.1, Envelope::linked("/test", "lanes"));

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
        assert_eq!(event.1.tag(), "event");
        assert!(expected_events.contains(&event.1.body.expect("Missing event body")));

        let event = rx.recv().await.expect("Expected an event");
        assert_eq!(event.1.tag(), "event");
        assert!(expected_events.contains(&event.1.body.expect("Missing event body")));

        let link = rx.recv().await.expect("No synced envelope received");
        assert_eq!(link.1, Envelope::synced("/test", "lanes"));

        let link = rx.recv().await.expect("No unlinked envelope received");
        assert_eq!(link.1, Envelope::unlinked("/test", "lanes"));

        assert!(rx.recv().now_or_never().is_none());
    };

    let _jh = tokio::spawn(timeout::timeout(Duration::from_secs(5), assert_task))
        .await
        .expect("No response received");
}
