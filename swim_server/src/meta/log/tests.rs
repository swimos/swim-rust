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
use crate::agent::lane::model::supply::SupplyLane;
use crate::agent::lane::model::value::ValueLane;
use crate::agent::{
    agent_lifecycle, map_lifecycle, value_lifecycle, AgentContext, SwimAgent, TestClock,
};
use crate::meta::log::config::FlushStrategy;
use crate::meta::log::{LogBuffer, LogEntry, LogLevel, NodeLogger};
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

const TEST_MSG: &str = "Map lifecycle on event";

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
        context.logger().log(TEST_MSG.to_string(), LogLevel::Info);
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

#[tokio::test]
async fn agent_log() {
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
        body: Some(action.as_value()),
    };

    let map_update = Envelope::from(map_update);

    assert!(envelope_tx
        .send(TaggedEnvelope(RoutingAddr::remote(1), map_update.clone()))
        .await
        .is_ok());

    let link = rx.recv().await.expect("Missing linked envelope");
    assert_eq!(link.1, Envelope::linked("/test", "infoLog"));

    let sync = rx.recv().await.expect("Missing synced envelope");
    assert_eq!(sync.1, Envelope::synced("/test", "infoLog"));

    let link = rx.recv().await.expect("Missing linked envelope");
    assert_eq!(link.1, Envelope::linked("/test", "map"));

    let event = rx.recv().await.expect("Missing event envelope");
    assert_eq!(
        event.1,
        Envelope::make_event("/test", "map", Some(action.as_value()))
    );
    let event = rx.recv().await.expect("Missing event envelope");
    assert_eq!(
        event.1,
        Envelope::make_event("/test", "map", Some(action.as_value()))
    );

    let sync = rx.recv().await.expect("Missing synced envelope");
    assert_eq!(sync.1, Envelope::synced("/test", "map"));

    let event = rx.recv().await.expect("Missing event envelope");
    let body = event.1.body.expect("Missing event body");

    let log_entry = LogEntry::try_convert(body).expect("Failed to convert log entry");
    assert_eq!(
        log_entry,
        LogEntry::make(
            TEST_MSG.to_string(),
            LogLevel::Info,
            RelativeUri::from_str("/test").unwrap(),
            "infoLog"
        )
    );
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
    let buffer = LogBuffer::new(FlushStrategy::Immediate);

    assert_eq!(buffer.next(), None);
    buffer.push(log_entry(LogLevel::Info, "1"));

    assert_eq!(buffer.next(), Some(vec![log_entry(LogLevel::Info, "1")]));
    assert_eq!(buffer.next(), None);
}

#[test]
fn log_buffer_immediate_multiple() {
    let buffer = LogBuffer::new(FlushStrategy::Immediate);

    assert_eq!(buffer.next(), None);

    buffer.push(log_entry(LogLevel::Info, "1"));
    buffer.push(log_entry(LogLevel::Info, "2"));
    buffer.push(log_entry(LogLevel::Info, "3"));
    buffer.push(log_entry(LogLevel::Info, "4"));
    buffer.push(log_entry(LogLevel::Info, "5"));

    assert_eq!(
        buffer.next(),
        Some(vec![
            log_entry(LogLevel::Info, "1"),
            log_entry(LogLevel::Info, "2"),
            log_entry(LogLevel::Info, "3"),
            log_entry(LogLevel::Info, "4"),
            log_entry(LogLevel::Info, "5"),
        ])
    );

    assert_eq!(buffer.next(), None);
}

#[test]
fn log_buffer_n() {
    let buffer = LogBuffer::new(FlushStrategy::Buffer(NonZeroUsize::new(5).unwrap()));

    assert_eq!(buffer.next(), None);
    buffer.push(log_entry(LogLevel::Info, "1"));

    assert_eq!(buffer.next(), None);
    buffer.push(log_entry(LogLevel::Info, "2"));
    assert_eq!(buffer.next(), None);

    buffer.push(log_entry(LogLevel::Info, "3"));
    buffer.push(log_entry(LogLevel::Info, "4"));
    buffer.push(log_entry(LogLevel::Info, "5"));

    assert_eq!(
        buffer.next(),
        Some(vec![
            log_entry(LogLevel::Info, "1"),
            log_entry(LogLevel::Info, "2"),
            log_entry(LogLevel::Info, "3"),
            log_entry(LogLevel::Info, "4"),
            log_entry(LogLevel::Info, "5"),
        ])
    );

    assert_eq!(buffer.next(), None);
    buffer.push(log_entry(LogLevel::Info, "6"));
    assert_eq!(buffer.next(), None);
}

#[test]
fn log_buffer_over_capacity() {
    let buffer = LogBuffer::new(FlushStrategy::Buffer(NonZeroUsize::new(2).unwrap()));

    assert_eq!(buffer.next(), None);
    buffer.push(log_entry(LogLevel::Info, "1"));
    buffer.push(log_entry(LogLevel::Info, "2"));
    buffer.push(log_entry(LogLevel::Info, "3"));
    buffer.push(log_entry(LogLevel::Info, "4"));
    buffer.push(log_entry(LogLevel::Info, "5"));

    assert_eq!(
        buffer.next(),
        Some(vec![
            log_entry(LogLevel::Info, "1"),
            log_entry(LogLevel::Info, "2"),
            log_entry(LogLevel::Info, "3"),
            log_entry(LogLevel::Info, "4"),
            log_entry(LogLevel::Info, "5"),
        ])
    );

    assert_eq!(buffer.next(), None);
    buffer.push(log_entry(LogLevel::Info, "6"));
    assert_eq!(buffer.next(), None);
}

fn log_entry(level: LogLevel, message: &str) -> LogEntry {
    LogEntry::make(
        message.to_string(),
        level,
        RelativeUri::from_str("/test").unwrap(),
        "lane",
    )
}

fn supply_lane() -> (mpsc::Receiver<LogEntry>, SupplyLane<LogEntry>) {
    let (tx, rx) = mpsc::channel(8);
    (rx, SupplyLane::new(tx))
}

#[tokio::test]
async fn node_logger_buffered() {
    let (mut trace_rx, trace_lane) = supply_lane();
    let (mut debug_rx, debug_lane) = supply_lane();
    let (mut info_rx, info_lane) = supply_lane();
    let (mut warn_rx, warn_lane) = supply_lane();
    let (mut error_rx, error_lane) = supply_lane();
    let (mut fail_rx, fail_lane) = supply_lane();

    let arc_buffer = Arc::new(LogBuffer::new(FlushStrategy::Buffer(
        NonZeroUsize::new(6).unwrap(),
    )));

    let node_logger = NodeLogger::new(
        arc_buffer.clone(),
        RelativeUri::from_str("/node").unwrap(),
        Arc::new(trace_lane),
        Arc::new(debug_lane),
        Arc::new(info_lane),
        Arc::new(warn_lane),
        Arc::new(error_lane),
        Arc::new(fail_lane),
    );

    node_logger.log_entry(log_entry(LogLevel::Trace, "Trace"));
    assert_eq!(arc_buffer.next(), None);
    assert!(trace_rx.recv().now_or_never().flatten().is_none());

    node_logger.log_entry(log_entry(LogLevel::Debug, "Debug"));
    assert_eq!(arc_buffer.next(), None);
    assert!(debug_rx.recv().now_or_never().flatten().is_none());

    node_logger.log_entry(log_entry(LogLevel::Info, "Info"));
    assert_eq!(arc_buffer.next(), None);
    assert!(info_rx.recv().now_or_never().flatten().is_none());

    node_logger.log_entry(log_entry(LogLevel::Warn, "Warn"));
    assert_eq!(arc_buffer.next(), None);
    assert!(warn_rx.recv().now_or_never().flatten().is_none());

    node_logger.log_entry(log_entry(LogLevel::Error, "Error"));
    assert_eq!(arc_buffer.next(), None);
    assert!(error_rx.recv().now_or_never().flatten().is_none());

    node_logger.log_entry(log_entry(LogLevel::Fail, "Fail"));

    assert_eq!(
        trace_rx.recv().await,
        Some(log_entry(LogLevel::Trace, "Trace"))
    );
    assert_eq!(
        debug_rx.recv().await,
        Some(log_entry(LogLevel::Debug, "Debug"))
    );
    assert_eq!(
        info_rx.recv().await,
        Some(log_entry(LogLevel::Info, "Info"))
    );
    assert_eq!(
        warn_rx.recv().await,
        Some(log_entry(LogLevel::Warn, "Warn"))
    );
    assert_eq!(
        error_rx.recv().await,
        Some(log_entry(LogLevel::Error, "Error"))
    );
    assert_eq!(
        fail_rx.recv().await,
        Some(log_entry(LogLevel::Fail, "Fail"))
    );

    assert_eq!(arc_buffer.next(), None);
}

#[tokio::test]
async fn node_logger_immediate() {
    let (mut trace_rx, trace_lane) = supply_lane();
    let (mut debug_rx, debug_lane) = supply_lane();
    let (mut info_rx, info_lane) = supply_lane();
    let (mut warn_rx, warn_lane) = supply_lane();
    let (mut error_rx, error_lane) = supply_lane();
    let (mut fail_rx, fail_lane) = supply_lane();

    let arc_buffer = Arc::new(LogBuffer::new(FlushStrategy::Immediate));

    let node_logger = NodeLogger::new(
        arc_buffer.clone(),
        RelativeUri::from_str("/node").unwrap(),
        Arc::new(trace_lane),
        Arc::new(debug_lane),
        Arc::new(info_lane),
        Arc::new(warn_lane),
        Arc::new(error_lane),
        Arc::new(fail_lane),
    );

    node_logger.log_entry(log_entry(LogLevel::Trace, "Trace"));
    assert_eq!(
        trace_rx.recv().await,
        Some(log_entry(LogLevel::Trace, "Trace"))
    );

    node_logger.log_entry(log_entry(LogLevel::Debug, "Debug"));
    assert_eq!(
        debug_rx.recv().await,
        Some(log_entry(LogLevel::Debug, "Debug"))
    );

    node_logger.log_entry(log_entry(LogLevel::Info, "Info"));
    assert_eq!(
        info_rx.recv().await,
        Some(log_entry(LogLevel::Info, "Info"))
    );

    node_logger.log_entry(log_entry(LogLevel::Warn, "Warn"));
    assert_eq!(
        warn_rx.recv().await,
        Some(log_entry(LogLevel::Warn, "Warn"))
    );

    node_logger.log_entry(log_entry(LogLevel::Error, "Error"));
    assert_eq!(
        error_rx.recv().await,
        Some(log_entry(LogLevel::Error, "Error"))
    );

    node_logger.log_entry(log_entry(LogLevel::Fail, "Fail"));
    assert_eq!(
        fail_rx.recv().await,
        Some(log_entry(LogLevel::Fail, "Fail"))
    );

    assert_eq!(arc_buffer.next(), None);
}
