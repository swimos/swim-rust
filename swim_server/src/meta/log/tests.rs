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

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::model::map::{MapLane, MapLaneEvent};
use crate::agent::lane::model::supply::SupplyLane;
use crate::agent::lane::model::value::ValueLane;
use crate::agent::{
    agent_lifecycle, map_lifecycle, value_lifecycle, AgentContext, AgentParameters, SwimAgent,
    TestClock,
};
use crate::interface::ServerDownlinksConfig;
use crate::meta::log::config::{FlushStrategy, LogConfig};
use crate::meta::log::{LogBuffer, LogEntry, LogLanes, LogLevel, NodeLogger};
use crate::plane::provider::AgentProvider;
use crate::routing::TopLevelServerRouterFactory;
use futures::future::BoxFuture;
use futures::FutureExt;
use server_store::agent::mock::MockNodeStore;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::Arc;
use swim_client::downlink::Downlinks;
use swim_client::interface::ClientContext;
use swim_client::router::ClientConnectionFactory;
use swim_form::Form;
use swim_model::Value;
use swim_runtime::configuration::DownlinkConnectionsConfig;
use swim_runtime::error::{ConnectionDropped, ResolutionError, RouterError};
use swim_runtime::routing::{Route, Router, RoutingAddr, TaggedEnvelope, TaggedSender};
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger;
use swim_utilities::trigger::promise;
use swim_warp::envelope::Envelope;
use swim_warp::map::MapUpdate;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::ReceiverStream;
use url::Url;

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
        assert!(context
            .logger()
            .log(TEST_MSG.to_string(), "lane".to_string(), LogLevel::Info)
            .await
            .is_ok());
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
async fn agent_log() {
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

    let parameters = AgentParameters::new(MockAgentConfig, exec_config, uri, HashMap::new());

    let (client_tx, _client_rx) = mpsc::channel(8);
    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let (plane_tx, _plane_rx) = mpsc::channel(8);
    let (_close_tx, close_rx) = promise::promise();

    let top_level_factory =
        TopLevelServerRouterFactory::new(plane_tx, client_tx.clone(), remote_tx.clone());

    let client_connections = ClientConnectionFactory::new(top_level_factory, client_tx, remote_tx);

    let (downlinks, _downlinks_task) = Downlinks::new(
        client_connections,
        DownlinkConnectionsConfig::default(),
        Arc::new(ServerDownlinksConfig::default()),
        close_rx,
    );

    let client = ClientContext::new(downlinks);

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
            Envelope::sync().node_uri("/test").lane_uri("map").done(),
        ))
        .await
        .is_ok());

    assert!(envelope_tx
        .send(TaggedEnvelope(
            RoutingAddr::remote(1),
            Envelope::sync()
                .node_uri("/swim:meta:node/test")
                .lane_uri("infoLog")
                .done(),
        ))
        .await
        .is_ok());

    let action = MapUpdate::Update(Value::text("key"), Arc::new(Value::Int32Value(1)));

    let map_update = Envelope::command()
        .node_uri("/test")
        .lane_uri("map")
        .body(action.clone())
        .done();

    let map_update = map_update;

    assert!(envelope_tx
        .send(TaggedEnvelope(RoutingAddr::remote(1), map_update.clone()))
        .await
        .is_ok());

    let link = rx.recv().await.expect("Missing linked envelope");
    assert_eq!(
        link.1,
        Envelope::linked()
            .node_uri("/test")
            .lane_uri("infoLog")
            .done()
    );

    let sync = rx.recv().await.expect("Missing synced envelope");
    assert_eq!(
        sync.1,
        Envelope::synced()
            .node_uri("/test")
            .lane_uri("infoLog")
            .done()
    );

    let link = rx.recv().await.expect("Missing linked envelope");
    assert_eq!(
        link.1,
        Envelope::linked().node_uri("/test").lane_uri("map").done()
    );

    let event = rx.recv().await.expect("Missing event envelope");
    assert_eq!(
        event.1,
        Envelope::event()
            .node_uri("/test")
            .lane_uri("map")
            .body(action.clone())
            .done()
    );
    let event = rx.recv().await.expect("Missing event envelope");
    assert_eq!(
        event.1,
        Envelope::event()
            .node_uri("/test")
            .lane_uri("map")
            .body(action.clone())
            .done()
    );

    let sync = rx.recv().await.expect("Missing synced envelope");
    assert_eq!(
        sync.1,
        Envelope::synced().node_uri("/test").lane_uri("map").done()
    );

    let event = rx.recv().await.expect("Missing event envelope");
    let body = event.1.body().expect("Missing event body");

    let log_entry = LogEntry::try_from_value(body).expect("Failed to convert log entry");
    assert_eq!(
        log_entry,
        LogEntry::make(
            TEST_MSG.to_string(),
            LogLevel::Info,
            RelativeUri::from_str("/test").unwrap(),
            "lane".to_string()
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

fn log_entry(level: LogLevel, message: &str) -> LogEntry {
    LogEntry::make(
        message.to_string(),
        level,
        RelativeUri::from_str("/node").unwrap(),
        "lane".to_string(),
    )
}

fn supply_lane() -> (mpsc::Receiver<LogEntry>, SupplyLane<LogEntry>) {
    let (tx, rx) = mpsc::channel(8);
    (rx, SupplyLane::new(tx))
}

#[test]
fn no_buffer() {
    let mut buffer = LogBuffer::None;
    let entry = log_entry(LogLevel::Trace, "test");
    assert_eq!(buffer.push(entry.clone()), Some(vec![entry]));
}

#[test]
fn capped_buffer() {
    let mut buffer = LogBuffer::Capped(Vec::with_capacity(5));
    let entries = vec![
        log_entry(LogLevel::Trace, "1"),
        log_entry(LogLevel::Trace, "2"),
        log_entry(LogLevel::Trace, "3"),
        log_entry(LogLevel::Trace, "4"),
        log_entry(LogLevel::Trace, "5"),
    ];

    assert_eq!(buffer.push(log_entry(LogLevel::Trace, "1")), None);
    assert_eq!(buffer.push(log_entry(LogLevel::Trace, "2")), None);
    assert_eq!(buffer.push(log_entry(LogLevel::Trace, "3")), None);
    assert_eq!(buffer.push(log_entry(LogLevel::Trace, "4")), None);
    assert_eq!(buffer.push(log_entry(LogLevel::Trace, "5")), Some(entries));
}

#[tokio::test]
async fn flushes() {
    let (mut trace_rx, trace_lane) = supply_lane();
    let (_debug_rx, debug_lane) = supply_lane();
    let (_info_rx, info_lane) = supply_lane();
    let (_warn_rx, warn_lane) = supply_lane();
    let (_error_rx, error_lane) = supply_lane();
    let (_fail_rx, fail_lane) = supply_lane();

    let log_lanes = LogLanes {
        trace_lane,
        debug_lane,
        info_lane,
        warn_lane,
        error_lane,
        fail_lane,
    };

    let (stop_tx, stop_rx) = trigger::trigger();
    let flush_interval = Duration::from_secs(2);
    let config = make_config(flush_interval, FlushStrategy::Buffer(non_zero_usize!(6)));

    let (node_logger, task) = NodeLogger::new(
        RelativeUri::from_str("/node").unwrap(),
        non_zero_usize!(256),
        stop_rx,
        log_lanes,
        config,
    );

    let jh = tokio::spawn(task);

    send(&node_logger, LogLevel::Trace).await;
    assert!(trace_rx.recv().now_or_never().flatten().is_none());

    sleep(flush_interval).await;

    assert_eq!(
        trace_rx.recv().await,
        Some(log_entry(LogLevel::Trace, "Trace"))
    );

    assert!(stop_tx.trigger());
    assert!(jh.await.is_ok());
}

#[tokio::test]
async fn node_logger_buffered() {
    let (mut trace_rx, trace_lane) = supply_lane();
    let (mut debug_rx, debug_lane) = supply_lane();
    let (mut info_rx, info_lane) = supply_lane();
    let (mut warn_rx, warn_lane) = supply_lane();
    let (mut error_rx, error_lane) = supply_lane();
    let (mut fail_rx, fail_lane) = supply_lane();

    let log_lanes = LogLanes {
        trace_lane,
        debug_lane,
        info_lane,
        warn_lane,
        error_lane,
        fail_lane,
    };

    let flush_interval = Duration::from_secs(30);
    let (stop_tx, stop_rx) = trigger::trigger();
    let config = make_config(flush_interval, FlushStrategy::Buffer(non_zero_usize!(6)));

    let (node_logger, task) = NodeLogger::new(
        RelativeUri::from_str("/node").unwrap(),
        non_zero_usize!(256),
        stop_rx,
        log_lanes,
        config,
    );

    let jh = tokio::spawn(task);

    send(&node_logger, LogLevel::Trace).await;
    assert!(trace_rx.recv().now_or_never().flatten().is_none());

    send(&node_logger, LogLevel::Debug).await;
    assert!(debug_rx.recv().now_or_never().flatten().is_none());

    send(&node_logger, LogLevel::Info).await;
    assert!(info_rx.recv().now_or_never().flatten().is_none());

    send(&node_logger, LogLevel::Warn).await;
    assert!(warn_rx.recv().now_or_never().flatten().is_none());

    send(&node_logger, LogLevel::Error).await;
    assert!(error_rx.recv().now_or_never().flatten().is_none());

    send(&node_logger, LogLevel::Fail).await;

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

    assert!(stop_tx.trigger());
    assert!(jh.await.is_ok());
}

async fn send(node_logger: &NodeLogger, level: LogLevel) {
    assert!(node_logger
        .log(level.to_string(), "lane".to_string(), level)
        .await
        .is_ok());
}

fn make_config(flush_interval: Duration, flush_strategy: FlushStrategy) -> LogConfig {
    LogConfig {
        flush_interval,
        channel_buffer_size: non_zero_usize!(64),
        lane_buffer: non_zero_usize!(2),
        flush_strategy,
        max_pending_messages: non_zero_usize!(64),
    }
}

#[tokio::test]
async fn node_logger_immediate() {
    let (mut trace_rx, trace_lane) = supply_lane();
    let (mut debug_rx, debug_lane) = supply_lane();
    let (mut info_rx, info_lane) = supply_lane();
    let (mut warn_rx, warn_lane) = supply_lane();
    let (mut error_rx, error_lane) = supply_lane();
    let (mut fail_rx, fail_lane) = supply_lane();

    let log_lanes = LogLanes {
        trace_lane,
        debug_lane,
        info_lane,
        warn_lane,
        error_lane,
        fail_lane,
    };

    let flush_interval = Duration::from_secs(5);
    let (stop_tx, stop_rx) = trigger::trigger();
    let config = make_config(flush_interval, FlushStrategy::Immediate);

    let (node_logger, task) = NodeLogger::new(
        RelativeUri::from_str("/node").unwrap(),
        non_zero_usize!(256),
        stop_rx,
        log_lanes,
        config,
    );

    let jh = tokio::spawn(task);

    send(&node_logger, LogLevel::Trace).await;
    assert_eq!(
        trace_rx.recv().await,
        Some(log_entry(LogLevel::Trace, "Trace"))
    );

    send(&node_logger, LogLevel::Debug).await;
    assert_eq!(
        debug_rx.recv().await,
        Some(log_entry(LogLevel::Debug, "Debug"))
    );

    send(&node_logger, LogLevel::Info).await;
    assert_eq!(
        info_rx.recv().await,
        Some(log_entry(LogLevel::Info, "Info"))
    );

    send(&node_logger, LogLevel::Warn).await;
    assert_eq!(
        warn_rx.recv().await,
        Some(log_entry(LogLevel::Warn, "Warn"))
    );

    send(&node_logger, LogLevel::Error).await;
    assert_eq!(
        error_rx.recv().await,
        Some(log_entry(LogLevel::Error, "Error"))
    );

    send(&node_logger, LogLevel::Fail).await;
    assert_eq!(
        fail_rx.recv().await,
        Some(log_entry(LogLevel::Fail, "Fail"))
    );

    assert!(stop_tx.trigger());
    assert!(jh.await.is_ok());
}
