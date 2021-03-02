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
use crate::agent::lane::model::map::{MapLane, MapLaneEvent};
use crate::agent::lane::model::value::ValueLane;
use crate::agent::{
    agent_lifecycle, map_lifecycle, value_lifecycle, AgentContext, SwimAgent, TestClock,
};
use crate::meta::log::config::FlushStrategy;
use crate::meta::log::{LogBuffer, LogEntry, LogLevel};
use crate::plane::provider::AgentProvider;
use crate::routing::error::RouterError;
use crate::routing::{
    ConnectionDropped, Route, RoutingAddr, ServerRouter, TaggedEnvelope, TaggedSender,
};
use futures::future::BoxFuture;
use futures::FutureExt;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use swim_common::form::Form;
use swim_common::model::Value;
use swim_common::routing::ResolutionError;
use swim_common::warp::envelope::{Envelope, OutgoingHeader, OutgoingLinkMessage};
use swim_common::warp::path::RelativePath;
use swim_warp::model::map::MapUpdate;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use url::Url;
use utilities::sync::promise;
use utilities::uri::RelativeUri;

mod swim_server {
    pub use crate::*;
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

#[map_lifecycle(agent = "MockAgent", key_type = "String", value_type = "i32", on_event)]
struct MapLifecycle;

impl MapLifecycle {
    #[allow(dead_code)]
    async fn on_event<Context>(
        &self,
        _event: &MapLaneEvent<String, i32>,
        _model: &MapLane<String, i32>,
        context: &Context,
    ) where
        Context: AgentContext<MockAgent> + Sized + Send + Sync + 'static,
    {
        context
            .logger()
            .log("Map lifecycle on event".to_string(), LogLevel::Info);
    }
}

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

// #[tokio::test]
async fn lane_info_sync() {
    let (tx, mut rx) = mpsc::channel(5);
    let uri = RelativeUri::try_from("/test").unwrap();
    let buffer_size = NonZeroUsize::new(10).unwrap();
    let clock = TestClock::default();
    let exec_config = AgentExecutionConfig::with(buffer_size, 1, 0, Duration::from_secs(1), None);
    let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size.get());
    let provider = AgentProvider::new(MockAgentConfig, MockAgentLifecycle);

    let (_a, agent_proc) = provider.run(
        uri,
        HashMap::new(),
        exec_config,
        clock.clone(),
        ReceiverStream::new(envelope_rx),
        MockRouter::new(RoutingAddr::local(1024), tx),
    );

    let _agent_task = swim_runtime::task::spawn(agent_proc);

    assert!(envelope_tx
        .send(TaggedEnvelope(
            RoutingAddr::remote(1),
            Envelope::sync("/test", "map"),
        ))
        .await
        .is_ok());

    assert!(envelope_tx
        .send(TaggedEnvelope(
            RoutingAddr::remote(1),
            Envelope::sync("/swim:meta:node/test", "infoLog"),
        ))
        .await
        .is_ok());

    let action = MapUpdate::Update(Value::text("key"), Arc::new(Value::Int32Value(1)));

    let map_update = OutgoingLinkMessage {
        header: OutgoingHeader::Command,
        path: RelativePath::new("/test", "map"),
        body: Some(action.into_value()),
    };

    let map_update = Envelope::from(map_update);

    assert!(envelope_tx
        .send(TaggedEnvelope(RoutingAddr::remote(1), map_update))
        .await
        .is_ok());

    while let Some(env) = rx.recv().await {
        println!("{:?}", env);
    }
}

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
            && self.level == other.level
            && self.node == other.node
            && self.lane == other.lane
    }
}

#[test]
fn log_buffer_immediate() {
    let log_entry = |msg: &str| {
        LogEntry::make(
            msg.to_string(),
            LogLevel::Info,
            RelativeUri::from_str("/test").unwrap(),
            "lane",
        )
    };

    let mut buffer = LogBuffer::new(FlushStrategy::Immediate);

    assert_eq!(buffer.next(), None);
    buffer.push(log_entry("1"));

    assert_eq!(buffer.next(), Some(vec![log_entry("1")]));
    assert_eq!(buffer.next(), None);
}

#[test]
fn log_buffer_immediate_multiple() {
    let log_entry = |msg: &str| {
        LogEntry::make(
            msg.to_string(),
            LogLevel::Info,
            RelativeUri::from_str("/test").unwrap(),
            "lane",
        )
    };

    let mut buffer = LogBuffer::new(FlushStrategy::Immediate);

    assert_eq!(buffer.next(), None);

    buffer.push(log_entry("1"));
    buffer.push(log_entry("2"));
    buffer.push(log_entry("3"));
    buffer.push(log_entry("4"));
    buffer.push(log_entry("5"));

    assert_eq!(
        buffer.next(),
        Some(vec![
            log_entry("1"),
            log_entry("2"),
            log_entry("3"),
            log_entry("4"),
            log_entry("5"),
        ])
    );

    assert_eq!(buffer.next(), None);
}

#[test]
fn log_buffer_n() {
    let log_entry = |msg: &str| {
        LogEntry::make(
            msg.to_string(),
            LogLevel::Info,
            RelativeUri::from_str("/test").unwrap(),
            "lane",
        )
    };

    let mut buffer = LogBuffer::new(FlushStrategy::Buffer(NonZeroUsize::new(5).unwrap()));

    assert_eq!(buffer.next(), None);
    buffer.push(log_entry("1"));

    assert_eq!(buffer.next(), None);
    buffer.push(log_entry("2"));
    assert_eq!(buffer.next(), None);

    buffer.push(log_entry("3"));
    buffer.push(log_entry("4"));
    buffer.push(log_entry("5"));

    assert_eq!(
        buffer.next(),
        Some(vec![
            log_entry("1"),
            log_entry("2"),
            log_entry("3"),
            log_entry("4"),
            log_entry("5"),
        ])
    );

    assert_eq!(buffer.next(), None);
    buffer.push(log_entry("6"));
    assert_eq!(buffer.next(), None);
}
