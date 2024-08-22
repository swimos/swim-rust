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

use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use bytes::BytesMut;
use swimos_agent::agent_model::downlink::BoxDownlinkChannelFactory;
use swimos_agent::event_handler::{
    ActionContext, LinkSpawner, HandlerFuture, LaneSpawnOnDone, LaneSpawner, Spawner,
    StepResult, CommanderSpawnOnDone
};
use swimos_agent::lanes::LaneItem;
use swimos_agent::{
    agent_model::{AgentSpec, ItemDescriptor, ItemFlags, WriteResult},
    event_handler::{DownlinkSpawnOnDone, EventHandler},
    AgentItem,
};
use swimos_agent_protocol::encoding::lane::{MapLaneResponseDecoder, ValueLaneResponseDecoder};
use swimos_agent_protocol::{LaneResponse, MapMessage, MapOperation};
use swimos_api::address::Address;
use swimos_api::{
    agent::{StoreKind, WarpLaneKind},
    error::DynamicRegistrationError,
};
use swimos_model::{Text, Value};
use swimos_recon::print_recon_compact;
use tokio_util::codec::Decoder;
use uuid::Uuid;

use crate::test_support::{make_meta, make_uri};
use crate::ConnectorAgent;

use super::{GenericMapLane, GenericValueLane};

#[test]
fn register_value_lane() {
    let agent = ConnectorAgent::default();
    let descriptor = ItemDescriptor::WarpLane {
        kind: WarpLaneKind::Value,
        flags: ItemFlags::TRANSIENT,
    };
    let result = agent.register_dynamic_item("lane", descriptor);

    let id = result.expect("Registration failed.");
    let guard = agent.value_lanes.borrow();
    let lane = guard.get("lane").expect("Missing registration.");
    assert_eq!(lane.id(), id);
}

#[test]
fn register_map_lane() {
    let agent = ConnectorAgent::default();
    let descriptor = ItemDescriptor::WarpLane {
        kind: WarpLaneKind::Map,
        flags: ItemFlags::TRANSIENT,
    };
    let result = agent.register_dynamic_item("lane", descriptor);

    let id = result.expect("Registration failed.");
    let guard = agent.map_lanes.borrow();
    let lane = guard.get("lane").expect("Missing registration.");
    assert_eq!(lane.id(), id);
}

#[test]
fn register_multiple_lanes() {
    let agent = ConnectorAgent::default();
    let descriptor1 = ItemDescriptor::WarpLane {
        kind: WarpLaneKind::Value,
        flags: ItemFlags::TRANSIENT,
    };
    let descriptor2 = ItemDescriptor::WarpLane {
        kind: WarpLaneKind::Value,
        flags: ItemFlags::TRANSIENT,
    };
    let descriptor3 = ItemDescriptor::WarpLane {
        kind: WarpLaneKind::Map,
        flags: ItemFlags::TRANSIENT,
    };
    let descriptor4 = ItemDescriptor::WarpLane {
        kind: WarpLaneKind::Map,
        flags: ItemFlags::TRANSIENT,
    };

    let id1 = agent
        .register_dynamic_item("lane1", descriptor1)
        .expect("Registration failed.");
    let id2 = agent
        .register_dynamic_item("lane2", descriptor2)
        .expect("Registration failed.");
    let id3 = agent
        .register_dynamic_item("lane3", descriptor3)
        .expect("Registration failed.");
    let id4 = agent
        .register_dynamic_item("lane4", descriptor4)
        .expect("Registration failed.");

    let ids: HashSet<u64> = HashSet::from_iter([id1, id2, id3, id4]);
    assert_eq!(ids.len(), 4);

    let value_lanes = agent.value_lanes.borrow();
    let map_lanes = agent.map_lanes.borrow();

    assert_eq!(
        value_lanes
            .get("lane1")
            .expect("Missing registration.")
            .id(),
        id1
    );
    assert_eq!(
        value_lanes
            .get("lane2")
            .expect("Missing registration.")
            .id(),
        id2
    );
    assert_eq!(
        map_lanes.get("lane3").expect("Missing registration.").id(),
        id3
    );
    assert_eq!(
        map_lanes.get("lane4").expect("Missing registration.").id(),
        id4
    );
}

#[test]
fn unsupported_lane_registrations() {
    let bad_warp = [
        WarpLaneKind::Command,
        WarpLaneKind::Demand,
        WarpLaneKind::DemandMap,
        WarpLaneKind::JoinMap,
        WarpLaneKind::JoinValue,
        WarpLaneKind::Spatial,
        WarpLaneKind::Supply,
    ];

    for kind in bad_warp {
        let agent = ConnectorAgent::default();
        let descriptor = ItemDescriptor::WarpLane {
            kind,
            flags: ItemFlags::TRANSIENT,
        };
        let result = agent.register_dynamic_item("lane", descriptor);

        assert_eq!(
            result,
            Err(DynamicRegistrationError::LaneKindUnsupported(kind))
        );
    }

    for kind in [StoreKind::Value, StoreKind::Map] {
        let agent = ConnectorAgent::default();
        let descriptor = ItemDescriptor::Store {
            kind,
            flags: ItemFlags::empty(),
        };
        let result = agent.register_dynamic_item("store", descriptor);

        assert_eq!(
            result,
            Err(DynamicRegistrationError::StoreKindUnsupported(kind))
        );
    }

    let agent = ConnectorAgent::default();
    let descriptor = ItemDescriptor::Http;
    let result = agent.register_dynamic_item("http", descriptor);
    assert_eq!(result, Err(DynamicRegistrationError::HttpLanesUnsupported));
}

#[test]
fn duplicate_lane_registrations() {
    let agent = ConnectorAgent::default();
    let value_descriptor = ItemDescriptor::WarpLane {
        kind: WarpLaneKind::Value,
        flags: ItemFlags::TRANSIENT,
    };
    let result = agent.register_dynamic_item("lane", value_descriptor);

    let id = result.expect("Registration failed.");
    let guard = agent.value_lanes.borrow();
    let lane = guard.get("lane").expect("Missing registration.");
    assert_eq!(lane.id(), id);
    drop(guard);

    let result2 = agent.register_dynamic_item("lane", value_descriptor);
    assert_eq!(
        result2,
        Err(DynamicRegistrationError::DuplicateName("lane".to_string()))
    );

    let map_descriptor = ItemDescriptor::WarpLane {
        kind: WarpLaneKind::Map,
        flags: ItemFlags::TRANSIENT,
    };
    let result2 = agent.register_dynamic_item("lane", map_descriptor);
    assert_eq!(
        result2,
        Err(DynamicRegistrationError::DuplicateName("lane".to_string()))
    );
}

fn init(agent: &ConnectorAgent) -> (u64, u64) {
    let value_descriptor = ItemDescriptor::WarpLane {
        kind: WarpLaneKind::Value,
        flags: ItemFlags::TRANSIENT,
    };
    let map_descriptor = ItemDescriptor::WarpLane {
        kind: WarpLaneKind::Map,
        flags: ItemFlags::TRANSIENT,
    };
    let id1 = agent
        .register_dynamic_item("value_lane", value_descriptor)
        .expect("Registration failed.");
    let id2 = agent
        .register_dynamic_item("map_lane", map_descriptor)
        .expect("Registration failed.");
    (id1, id2)
}

fn to_buffer(value: Value) -> BytesMut {
    let mut buffer = BytesMut::new();
    write!(buffer, "{}", print_recon_compact(&value)).expect("Write failed.");
    buffer
}

fn from_buffer(buffer: &mut BytesMut) -> LaneResponse<Value> {
    let mut decoder = ValueLaneResponseDecoder::<Value>::default();
    decoder
        .decode_eof(buffer)
        .expect("Bad record.")
        .expect("Incomplete.")
}

fn from_buffer_synced(buffer: &mut BytesMut) -> Vec<LaneResponse<Value>> {
    let mut decoder = ValueLaneResponseDecoder::<Value>::default();
    let mut responses = vec![];
    while !buffer.is_empty() {
        responses.push(
            decoder
                .decode_eof(buffer)
                .expect("Bad record.")
                .expect("Incomplete."),
        );
    }
    responses
}

fn from_buffer_map(buffer: &mut BytesMut) -> LaneResponse<MapOperation<Value, Value>> {
    let mut decoder = MapLaneResponseDecoder::<Value, Value>::default();
    decoder
        .decode_eof(buffer)
        .expect("Bad record.")
        .expect("Incomplete.")
}

fn with_value_lane(agent: &ConnectorAgent, f: impl FnOnce(&GenericValueLane)) {
    let guard = agent.value_lanes.borrow();
    let lane = guard.get("value_lane").expect("Lane not registered.");
    f(lane);
}

fn with_map_lane(agent: &ConnectorAgent, f: impl FnOnce(&GenericMapLane)) {
    let guard = agent.map_lanes.borrow();
    let lane = guard.get("map_lane").expect("Lane not registered.");
    f(lane);
}

#[test]
fn value_lane_command() {
    let agent = ConnectorAgent::default();
    let (val_id, _) = init(&agent);
    let handler = agent
        .on_value_command("value_lane", to_buffer(Value::from(45)))
        .expect("No handler.");
    let ids = run_handler(&agent, handler);
    assert_eq!(ids, [val_id].into_iter().collect());

    with_value_lane(&agent, |lane| {
        lane.read(|v| {
            assert_eq!(v, &Value::from(45));
        });
        let mut buffer = BytesMut::new();
        assert_eq!(lane.write_to_buffer(&mut buffer), WriteResult::Done);
        assert_eq!(
            from_buffer(&mut buffer),
            LaneResponse::event(Value::from(45))
        );
    });
}

#[test]
fn value_lane_sync() {
    let sync_id = Uuid::from_u128(7474);
    let agent = ConnectorAgent::default();
    let (val_id, _) = init(&agent);
    let handler = agent.on_sync("value_lane", sync_id).expect("No handler.");
    let ids = run_handler(&agent, handler);
    assert_eq!(ids, [val_id].into_iter().collect());

    with_value_lane(&agent, move |lane| {
        let mut buffer = BytesMut::new();
        assert_eq!(lane.write_to_buffer(&mut buffer), WriteResult::Done);
        let responses = from_buffer_synced(&mut buffer);
        assert_eq!(
            responses,
            vec![
                LaneResponse::sync_event(sync_id, Value::Extant),
                LaneResponse::Synced(sync_id)
            ]
        );
    });
}

#[test]
fn map_lane_command() {
    let agent = ConnectorAgent::default();
    let (_, map_id) = init(&agent);
    let handler = agent
        .on_map_command(
            "map_lane",
            MapMessage::Update {
                key: to_buffer(Value::text("a")),
                value: to_buffer(Value::from(74)),
            },
        )
        .expect("No handler.");
    let ids = run_handler(&agent, handler);
    assert_eq!(ids, [map_id].into_iter().collect());

    with_map_lane(&agent, |lane| {
        lane.get_map(|contents| {
            assert_eq!(contents.len(), 1);
            let v = contents.get(&Value::text("a")).expect("Entry missing.");
            assert_eq!(v, &Value::from(74));
        });
        let mut buffer = BytesMut::new();
        assert_eq!(lane.write_to_buffer(&mut buffer), WriteResult::Done);

        let response = from_buffer_map(&mut buffer);
        assert_eq!(
            response,
            LaneResponse::event(MapOperation::Update {
                key: Value::text("a"),
                value: Value::from(74)
            })
        );
    });
}

#[test]
fn map_lane_sync() {
    let agent = ConnectorAgent::default();
    let sync_id = Uuid::from_u128(663883846);
    let (_, map_id) = init(&agent);
    let handler = agent.on_sync("map_lane", sync_id).expect("No handler.");
    let ids = run_handler(&agent, handler);
    assert_eq!(ids, [map_id].into_iter().collect());

    with_map_lane(&agent, move |lane| {
        let mut buffer = BytesMut::new();
        assert_eq!(lane.write_to_buffer(&mut buffer), WriteResult::Done);

        let response = from_buffer_map(&mut buffer);
        assert_eq!(response, LaneResponse::synced(sync_id));
    });
}

struct NoSpawn;

impl Spawner<ConnectorAgent> for NoSpawn {
    fn spawn_suspend(&self, _fut: HandlerFuture<ConnectorAgent>) {
        panic!("Spawning futures not supported.");
    }
}

impl LinkSpawner<ConnectorAgent> for NoSpawn {
    fn spawn_downlink(
        &self,
        _path: Address<Text>,
        _make_channel: BoxDownlinkChannelFactory<ConnectorAgent>,
        _on_done: DownlinkSpawnOnDone<ConnectorAgent>,
    ) {
        panic!("Spawning downlinks not supported.");
    }
    
    fn register_commander(&self, _path: Address<Text>, _on_done: CommanderSpawnOnDone<ConnectorAgent>) {
        panic!("Registering commanders not supported.");
    }
}

impl LaneSpawner<ConnectorAgent> for NoSpawn {
    fn spawn_warp_lane(
        &self,
        _name: &str,
        _kind: WarpLaneKind,
        _on_done: LaneSpawnOnDone<ConnectorAgent>,
    ) -> Result<(), DynamicRegistrationError> {
        panic!("Spawning lanes not supported.");
    }
}

fn run_handler<H>(agent: &ConnectorAgent, mut handler: H) -> HashSet<u64>
where
    H: EventHandler<ConnectorAgent>,
{
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let no_spawn = NoSpawn;
    let mut join_lane_init = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();

    let mut action_context = ActionContext::new(
        &no_spawn,
        &no_spawn,
        &no_spawn,
        &mut join_lane_init,
        &mut ad_hoc_buffer,
    );

    let mut ids = HashSet::new();
    loop {
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { modified_item } => {
                if let Some(modification) = modified_item {
                    ids.insert(modification.id());
                }
            }
            StepResult::Fail(err) => panic!("{:?}", err),
            StepResult::Complete { modified_item, .. } => {
                if let Some(modification) = modified_item {
                    ids.insert(modification.id());
                }
                break;
            }
        }
    }
    ids
}
