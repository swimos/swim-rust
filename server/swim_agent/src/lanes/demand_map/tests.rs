// Copyright 2015-2023 Swim Inc.
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

use bytes::BytesMut;
use swim_api::{
    agent::AgentConfig,
    protocol::{
        agent::{LaneResponse, LaneResponseDecoder},
        map::{MapOperation, MapOperationDecoder},
    },
};
use swim_utilities::routing::route_uri::RouteUri;
use tokio_util::codec::Decoder;

use crate::{
    agent_model::WriteResult,
    event_handler::{ConstHandler, EventHandler, HandlerActionExt, Modification, StepResult},
    lanes::{demand_map::DemandMapLaneSync, LaneItem},
    meta::AgentMetadata,
    test_context::dummy_context,
};

use super::{
    lifecycle::{keys::Keys, on_cue_key::OnCueKey, DemandMapLaneLifecycle},
    DemandMapLane, DemandMapLaneCueKey,
};
use uuid::Uuid;

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";
const LANE_ID: u64 = 45;
const SYNC_ID1: Uuid = Uuid::from_u128(8474374);
const SYNC_ID2: Uuid = Uuid::from_u128(1177374);

struct FakeAgent {
    lane: DemandMapLane<i32, i32>,
}

impl FakeAgent {
    const LANE: fn(&FakeAgent) -> &DemandMapLane<i32, i32> = |agent| &agent.lane;
}

impl Default for FakeAgent {
    fn default() -> Self {
        Self {
            lane: DemandMapLane::new(LANE_ID),
        }
    }
}

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
}

const LIMIT: usize = 100;

fn run_demand_map<'a, LC, H>(
    agent: &FakeAgent,
    lifecycle: &'a LC,
    handler: H,
) -> Vec<LaneResponse<MapOperation<i32, i32>>>
where
    LC: DemandMapLaneLifecycle<i32, i32, FakeAgent>,
    H: EventHandler<FakeAgent> + 'a,
{
    let mut buffer = BytesMut::new();
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut result = run_handler(handler, lifecycle, agent, meta, 1);
    let mut write_required = result.write_required();
    let mut counter = 0;

    while result.run_handler() {
        let handler = super::demand_map_handler(agent, FakeAgent::LANE, lifecycle);
        counter += 1;
        result = run_handler(handler, lifecycle, agent, meta, 1);
        write_required = write_required || result.write_required();
    }

    while write_required {
        match agent.lane.write_to_buffer(&mut buffer) {
            WriteResult::DataStillAvailable => {
                write_required = true;
            }
            WriteResult::RequiresEvent => {
                write_required = false;
                loop {
                    let handler = super::demand_map_handler(agent, FakeAgent::LANE, lifecycle);
                    counter += 1;
                    if counter >= LIMIT {
                        panic!("Handlers have likely diverged.");
                    }
                    let result = run_handler(handler, lifecycle, agent, meta, 1);
                    write_required = write_required || result.write_required();
                    if !result.run_handler() {
                        break;
                    }
                }
            }
            _ => {
                write_required = false;
            }
        }
    }

    let mut decoder = LaneResponseDecoder::new(MapOperationDecoder::<i32, i32>::default());
    let mut responses = vec![];
    loop {
        if let Some(response) = decoder.decode(&mut buffer).expect("Decode failed.") {
            responses.push(response);
        } else {
            if let Some(response) = decoder.decode_eof(&mut buffer).expect("Decode failed.") {
                responses.push(response);
            }
            assert!(buffer.is_empty());
            break;
        }
    }

    responses
}

#[derive(Clone, Copy)]
enum HandlerResult {
    NoMod,
    WriteOnly,
    Both,
}

impl HandlerResult {
    fn run_handler(&self) -> bool {
        matches!(self, HandlerResult::Both)
    }

    fn write_required(&self) -> bool {
        matches!(self, HandlerResult::WriteOnly | HandlerResult::Both)
    }

    fn and(self, other: Self) -> Self {
        match (self, other) {
            (HandlerResult::NoMod, r) => r,
            (r, HandlerResult::NoMod) => r,
            (HandlerResult::WriteOnly, HandlerResult::WriteOnly) => HandlerResult::WriteOnly,
            _ => HandlerResult::Both,
        }
    }
}

const DEPTH_LIMIT: usize = 5;

fn run_handler<H, LC>(
    mut handler: H,
    lifecycle: &LC,
    agent: &FakeAgent,
    meta: AgentMetadata<'_>,
    depth: usize,
) -> HandlerResult
where
    H: EventHandler<FakeAgent>,
    LC: DemandMapLaneLifecycle<i32, i32, FakeAgent>,
{
    if depth == DEPTH_LIMIT {
        panic!("Handler probably diverged.");
    }
    let mut join_value_init = Default::default();
    let mut ad_hoc = Default::default();
    let action_context = &mut dummy_context(&mut join_value_init, &mut ad_hoc);
    let mut result = HandlerResult::NoMod;
    loop {
        match handler.step(action_context, meta, agent) {
            StepResult::Continue { modified_item } => {
                if let Some(Modification {
                    item_id,
                    trigger_handler,
                }) = modified_item
                {
                    assert_eq!(item_id, LANE_ID);
                    if trigger_handler {
                        let sub_handler =
                            super::demand_map_handler(agent, FakeAgent::LANE, lifecycle);
                        result =
                            result.and(run_handler(sub_handler, lifecycle, agent, meta, depth + 1));
                    }
                }
            }
            StepResult::Fail(err) => panic!("Failed: {:?}", err),
            StepResult::Complete { modified_item, .. } => {
                result = result.and(match modified_item {
                    Some(Modification {
                        item_id,
                        trigger_handler,
                    }) => {
                        assert_eq!(item_id, LANE_ID);
                        if trigger_handler {
                            HandlerResult::Both
                        } else {
                            HandlerResult::WriteOnly
                        }
                    }
                    None => HandlerResult::NoMod,
                });
                break result;
            }
        }
    }
}

struct TestLifecycle {
    entries: HashMap<i32, i32>,
}

impl Default for TestLifecycle {
    fn default() -> Self {
        let entries = [(1, 2), (2, 4), (3, 6)].into_iter().collect();
        Self { entries }
    }
}

impl Keys<i32, FakeAgent> for TestLifecycle {
    type KeysHandler<'a> = ConstHandler<HashSet<i32>>
    where
        Self: 'a;

    fn keys(&self) -> Self::KeysHandler<'_> {
        ConstHandler::from(self.entries.keys().copied().collect::<HashSet<_>>())
    }
}

impl OnCueKey<i32, i32, FakeAgent> for TestLifecycle {
    type OnCueKeyHandler<'a> = ConstHandler<Option<i32>>
    where
        Self: 'a;

    fn on_cue_key(&self, key: i32) -> Self::OnCueKeyHandler<'_> {
        ConstHandler::from(self.entries.get(&key).copied())
    }
}

#[test]
fn cue_present_key() {
    let agent = FakeAgent::default();
    let lifecycle = TestLifecycle::default();
    let cue = DemandMapLaneCueKey::new(FakeAgent::LANE, 2);

    let responses = run_demand_map(&agent, &lifecycle, cue);
    assert_eq!(
        &responses,
        &[LaneResponse::StandardEvent(MapOperation::Update {
            key: 2,
            value: 4
        })]
    )
}

#[test]
fn cue_absent_key() {
    let agent = FakeAgent::default();
    let lifecycle = TestLifecycle::default();
    let cue = DemandMapLaneCueKey::new(FakeAgent::LANE, 60);

    let responses = run_demand_map(&agent, &lifecycle, cue);
    assert_eq!(
        &responses,
        &[LaneResponse::StandardEvent(MapOperation::Remove {
            key: 60
        })]
    )
}

#[test]
fn cue_two_keys() {
    let agent = FakeAgent::default();
    let lifecycle = TestLifecycle::default();
    let cue1 = DemandMapLaneCueKey::new(FakeAgent::LANE, 2);
    let cue2 = DemandMapLaneCueKey::new(FakeAgent::LANE, 3);

    let responses = run_demand_map(&agent, &lifecycle, cue1.followed_by(cue2));
    assert_eq!(
        &responses,
        &[
            LaneResponse::StandardEvent(MapOperation::Update { key: 2, value: 4 }),
            LaneResponse::StandardEvent(MapOperation::Update { key: 3, value: 6 })
        ]
    )
}

#[test]
fn cue_three_keys() {
    let agent = FakeAgent::default();
    let lifecycle = TestLifecycle::default();
    let cue1 = DemandMapLaneCueKey::new(FakeAgent::LANE, 1);
    let cue2 = DemandMapLaneCueKey::new(FakeAgent::LANE, 2);
    let cue3 = DemandMapLaneCueKey::new(FakeAgent::LANE, 3);

    let responses = run_demand_map(&agent, &lifecycle, cue1.followed_by(cue2).followed_by(cue3));
    assert_eq!(
        &responses,
        &[
            LaneResponse::StandardEvent(MapOperation::Update { key: 1, value: 2 }),
            LaneResponse::StandardEvent(MapOperation::Update { key: 2, value: 4 }),
            LaneResponse::StandardEvent(MapOperation::Update { key: 3, value: 6 })
        ]
    )
}

#[test]
fn sync_demand_map_lane() {
    let agent = FakeAgent::default();
    let lifecycle = TestLifecycle::default();
    let sync = DemandMapLaneSync::new(FakeAgent::LANE, SYNC_ID1);

    let responses = run_demand_map(&agent, &lifecycle, sync);

    let mut map = HashMap::new();
    let mut synced = false;
    for response in responses {
        assert!(!synced);
        match response {
            LaneResponse::SyncEvent(id, MapOperation::Update { key, value }) => {
                assert_eq!(id, SYNC_ID1);
                map.insert(key, value);
            }
            LaneResponse::Synced(id) => {
                assert_eq!(id, SYNC_ID1);
                synced = true;
            }
            ow => panic!("Unexpected response: {:?}", ow),
        }
    }
    assert!(synced);
    assert_eq!(map, lifecycle.entries);
}

#[test]
fn sync_demand_map_lane_twice() {
    let agent = FakeAgent::default();
    let lifecycle = TestLifecycle::default();
    let sync1 = DemandMapLaneSync::new(FakeAgent::LANE, SYNC_ID1);
    let sync2 = DemandMapLaneSync::new(FakeAgent::LANE, SYNC_ID2);

    let responses = run_demand_map(&agent, &lifecycle, sync1.followed_by(sync2));

    let mut map1 = HashMap::new();
    let mut map2 = HashMap::new();
    let mut synced1 = false;
    let mut synced2 = false;
    for response in responses {
        match response {
            LaneResponse::SyncEvent(id, MapOperation::Update { key, value }) if id == SYNC_ID1 => {
                assert!(!synced1);
                map1.insert(key, value);
            }
            LaneResponse::SyncEvent(id, MapOperation::Update { key, value }) if id == SYNC_ID2 => {
                assert!(!synced2);
                map2.insert(key, value);
            }
            LaneResponse::Synced(id) if id == SYNC_ID1 => {
                synced1 = true;
            }
            LaneResponse::Synced(id) if id == SYNC_ID2 => {
                synced2 = true;
            }
            ow => panic!("Unexpected response: {:?}", ow),
        }
    }
    assert!(synced1);
    assert!(synced2);

    assert_eq!(map1, lifecycle.entries);
    assert_eq!(map2, lifecycle.entries);
}

#[test]
fn sync_and_cue_demand_map_lane() {
    let agent = FakeAgent::default();
    let lifecycle = TestLifecycle::default();
    let sync = DemandMapLaneSync::new(FakeAgent::LANE, SYNC_ID1);
    let cue1 = DemandMapLaneCueKey::new(FakeAgent::LANE, 2);
    let cue2 = DemandMapLaneCueKey::new(FakeAgent::LANE, 3);

    let responses = run_demand_map(&agent, &lifecycle, cue1.followed_by(sync).followed_by(cue2));

    let mut map = HashMap::new();
    let mut synced = false;
    let mut first_cued = false;
    let mut second_cued = false;
    for response in responses {
        assert!(!synced);
        match response {
            LaneResponse::StandardEvent(MapOperation::Update { key: 2, value }) => {
                assert!(!first_cued);
                assert_eq!(value, 4);
                first_cued = true;
            }
            LaneResponse::StandardEvent(MapOperation::Update { key: 3, value }) => {
                assert!(!second_cued);
                assert_eq!(value, 6);
                second_cued = true;
                if !synced {
                    map.insert(3, value);
                }
            }
            LaneResponse::SyncEvent(id, MapOperation::Update { key, value }) => {
                assert_eq!(id, SYNC_ID1);
                assert!(!map.contains_key(&key));
                map.insert(key, value);
            }
            LaneResponse::Synced(id) => {
                assert_eq!(id, SYNC_ID1);
                synced = true;
            }
            ow => panic!("Unexpected response: {:?}", ow),
        }
    }
    assert!(synced);
    assert_eq!(map, lifecycle.entries);
    assert!(first_cued);
    assert!(second_cued);
}
