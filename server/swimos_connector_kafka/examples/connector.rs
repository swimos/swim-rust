// Copyright 2015-2024 Swim Inc.
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

use std::{cell::RefCell, collections::HashMap, pin::pin};

use bytes::BytesMut;
use futures::{
    future::{join, BoxFuture},
    stream::FuturesUnordered,
    StreamExt, TryStreamExt,
};
use swimos_agent::{
    agent_model::{downlink::BoxDownlinkChannel, AgentSpec, ItemDescriptor, ItemFlags},
    event_handler::{
        ActionContext, DownlinkSpawner, EventHandler, HandlerFuture, LaneSpawnOnDone, LaneSpawner,
        Spawner, StepResult,
    },
    AgentMetadata,
};
use swimos_api::{
    agent::{
        AgentConfig, AgentContext, DownlinkKind, HttpLaneRequestChannel, LaneConfig, StoreKind,
        WarpLaneKind,
    },
    error::{AgentRuntimeError, DownlinkRuntimeError, DynamicRegistrationError, OpenStoreError},
};
use swimos_connector::{Connector, ConnectorAgent};
use swimos_connector_kafka::{
    DeserializationFormat, Endianness, KafkaConnector, KafkaConnectorConfiguration,
    KafkaConnectorError, KafkaLogLevel, MapLaneSpec, ValueLaneSpec,
};
use swimos_utilities::{
    byte_channel::{ByteReader, ByteWriter},
    routing::RouteUri,
    trigger,
};

fn make_config() -> KafkaConnectorConfiguration {
    KafkaConnectorConfiguration {
        properties: [
            ("bootstrap.servers", "datagen.nstream.cloud:9092"),
            ("message.timeout.ms", "5000"),
            ("group.id", "rust-consumer-test"),
            ("auto.offset.reset", "smallest"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect(),
        log_level: KafkaLogLevel::Debug,
        value_lanes: vec![ValueLaneSpec::new(Some("latest_key"), "$key", true)],
        map_lanes: vec![MapLaneSpec::new(
            "times",
            "$payload.ranLatest.mean_ul_sinr",
            "$payload.ranLatest.recorded_time",
            false,
            true,
        )],
        key_deserializer: DeserializationFormat::Int32(Endianness::LittleEndian),
        payload_deserializer: DeserializationFormat::Json,
        topics: vec!["cellular-integer-json".to_string()],
    }
}

async fn connector_task() -> Result<(), KafkaConnectorError> {
    let config = make_config();
    let connector = KafkaConnector::for_config(config);
    let agent = ConnectorAgent::default();

    let (tx, rx) = trigger::trigger();

    let on_start = run_handler_with_futures(&agent, connector.on_start(tx));

    let (_, result) = join(on_start, rx).await;
    result.expect("Creating lanes failed.");

    let mut stream = connector.create_stream()?;

    let mut stop_signal = pin!(tokio::signal::ctrl_c());
    let spawner = TestSpawner::default();
    loop {
        let handler = tokio::select! {
            biased;
            _ = &mut stop_signal => break,
            result = stream.try_next() => {
                if let Some(handler) = result? {
                    handler
                } else {
                    break;
                }
            },
        };

        run_handler(&agent, &spawner, handler);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    connector_task().await?;
    Ok(())
}

struct LaneRequest {
    name: String,
    is_map: bool,
    on_done: LaneSpawnOnDone<ConnectorAgent>,
}

impl std::fmt::Debug for LaneRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneRequest")
            .field("name", &self.name)
            .field("is_map", &self.is_map)
            .field("on_done", &"...")
            .finish()
    }
}

struct TestContext;

impl AgentContext for TestContext {
    fn ad_hoc_commands(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        panic!("Unexpected call.");
    }

    fn add_lane(
        &self,
        _name: &str,
        _lane_kind: WarpLaneKind,
        _config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Unexpected call.");
    }

    fn add_http_lane(
        &self,
        _name: &str,
    ) -> BoxFuture<'static, Result<HttpLaneRequestChannel, AgentRuntimeError>> {
        panic!("Unexpected call.");
    }

    fn open_downlink(
        &self,
        _host: Option<&str>,
        _node: &str,
        _lane: &str,
        _kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        panic!("Unexpected call.");
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        panic!("Unexpected call.");
    }
}

#[derive(Default, Debug)]
struct TestSpawner {
    suspended: FuturesUnordered<HandlerFuture<ConnectorAgent>>,
    lane_requests: RefCell<Vec<LaneRequest>>,
}

impl Spawner<ConnectorAgent> for TestSpawner {
    fn spawn_suspend(&self, fut: HandlerFuture<ConnectorAgent>) {
        self.suspended.push(fut);
    }
}

impl DownlinkSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_downlink(
        &self,
        _dl_channel: BoxDownlinkChannel<ConnectorAgent>,
    ) -> Result<(), DownlinkRuntimeError> {
        panic!("Opening downlinks not supported.");
    }
}

impl LaneSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_warp_lane(
        &self,
        name: &str,
        kind: WarpLaneKind,
        on_done: LaneSpawnOnDone<ConnectorAgent>,
    ) -> Result<(), DynamicRegistrationError> {
        let is_map = match kind {
            WarpLaneKind::Map => true,
            WarpLaneKind::Value => false,
            _ => panic!("Unexpected lane kind: {}", kind),
        };
        self.lane_requests.borrow_mut().push(LaneRequest {
            name: name.to_string(),
            is_map,
            on_done,
        });
        Ok(())
    }
}

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
}

async fn run_handler_with_futures<H: EventHandler<ConnectorAgent>>(
    agent: &ConnectorAgent,
    handler: H,
) {
    let mut spawner = TestSpawner::default();
    run_handler(agent, &spawner, handler);
    let mut handlers = vec![];
    let reg = move |req: LaneRequest| {
        let LaneRequest {
            name,
            is_map,
            on_done,
        } = req;
        let kind = if is_map {
            WarpLaneKind::Map
        } else {
            WarpLaneKind::Value
        };
        let descriptor = ItemDescriptor::WarpLane {
            kind,
            flags: ItemFlags::TRANSIENT,
        };
        let result = agent.register_dynamic_item(&name, descriptor);
        on_done(result.map_err(Into::into))
    };
    for request in std::mem::take::<Vec<LaneRequest>>(spawner.lane_requests.borrow_mut().as_mut()) {
        handlers.push(reg(request));
    }

    while !(handlers.is_empty() && spawner.suspended.is_empty()) {
        if let Some(h) = handlers.pop() {
            run_handler(agent, &spawner, h);
        } else {
            let h = spawner.suspended.next().await.expect("No handler.");
            run_handler(agent, &spawner, h);
        };
        for request in
            std::mem::take::<Vec<LaneRequest>>(spawner.lane_requests.borrow_mut().as_mut())
        {
            handlers.push(reg(request));
        }
    }
}

fn run_handler<H: EventHandler<ConnectorAgent>>(
    agent: &ConnectorAgent,
    spawner: &TestSpawner,
    mut handler: H,
) {
    let route_params = HashMap::new();
    let uri = make_uri();
    let meta = make_meta(&uri, &route_params);
    let mut join_lane_init = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();
    let agent_context = TestContext;

    let mut action_context = ActionContext::new(
        spawner,
        &agent_context,
        spawner,
        spawner,
        &mut join_lane_init,
        &mut ad_hoc_buffer,
    );

    loop {
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { .. } => {}
            StepResult::Fail(err) => panic!("Handler Failed: {}", err),
            StepResult::Complete { .. } => {
                break;
            }
        }
    }
}
