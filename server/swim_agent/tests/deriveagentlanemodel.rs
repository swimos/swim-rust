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

use std::collections::{HashMap, HashSet};

use bytes::BytesMut;
use std::fmt::Write;
use swim_agent::agent_model::{LaneFlags, LaneSpec};
use swim_agent::lanes::{CommandLane, MapLane, ValueLane};
use swim_agent::model::MapMessage;
use swim_agent::AgentLaneModel;
use swim_model::Text;
use uuid::Uuid;

const SYNC_ID: Uuid = Uuid::from_u128(85883);

fn transient(name: &'static str) -> (&'static str, bool) {
    (name, true)
}

fn persistent(name: &'static str) -> (&'static str, bool) {
    (name, false)
}

fn check_agent<A>(val_lanes: Vec<(&'static str, bool)>, map_lanes: Vec<(&'static str, bool)>)
where
    A: AgentLaneModel + Default,
{
    let agent = A::default();

    let mut val_expected = HashMap::new();
    let mut map_expected = HashMap::new();

    for (name, is_transient) in val_lanes {
        let spec = if is_transient {
            LaneSpec::new(LaneFlags::TRANSIENT)
        } else {
            LaneSpec::new(LaneFlags::empty())
        };
        val_expected.insert(name, spec);
    }

    for (name, is_transient) in map_lanes {
        let spec = if is_transient {
            LaneSpec::new(LaneFlags::TRANSIENT)
        } else {
            LaneSpec::new(LaneFlags::empty())
        };
        map_expected.insert(name, spec);
    }

    assert_eq!(A::value_like_lane_specs(), val_expected);
    assert_eq!(A::map_like_lane_specs(), map_expected);

    let id_map = A::lane_ids();
    let expected_len = val_expected.len() + map_expected.len();
    assert_eq!(id_map.len(), expected_len);

    let mut keys = HashSet::new();
    let mut names = HashSet::new();

    for (key, name) in id_map {
        keys.insert(key);
        names.insert(name);
    }

    let expected_keys = (0..expected_len).map(|n| n as u64).collect::<HashSet<_>>();
    let mut expected_names = HashSet::new();
    expected_names.extend(val_expected.keys().map(|s| Text::new(*s)));
    expected_names.extend(map_expected.keys().map(|s| Text::new(*s)));

    assert_eq!(keys, expected_keys);
    assert_eq!(names, expected_names);

    for name in val_expected.keys() {
        assert!(agent.on_value_command(name, get_i32_buffer(4)).is_some());
        assert!(agent.on_sync(name, SYNC_ID).is_some());
    }

    for name in map_expected.keys() {
        assert!(agent.on_map_command(name, MapMessage::Clear).is_some());
        assert!(agent.on_sync(name, SYNC_ID).is_some());
    }
}

fn get_i32_buffer(n: i32) -> BytesMut {
    let mut buf = BytesMut::new();
    write!(&mut buf, "{}", n).expect("Write to buffer failed.");
    buf
}

#[test]
fn single_value_lane() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct SingleValueLane {
        lane: ValueLane<i32>,
    }

    check_agent::<SingleValueLane>(vec![persistent("lane")], vec![]);
}

#[test]
fn single_map_lane() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct SingleMapLane {
        lane: MapLane<i32, i32>,
    }

    check_agent::<SingleMapLane>(vec![], vec![persistent("lane")]);
}

#[test]
fn single_command_lane() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct SingleCommandLane {
        lane: CommandLane<i32>,
    }

    check_agent::<SingleCommandLane>(vec![transient("lane")], vec![]);
}

#[test]
fn two_value_lanes() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct TwoValueLanes {
        first: ValueLane<i32>,
        second: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec![persistent("first"), persistent("second")], vec![]);
}

#[test]
fn two_map_lanes() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct TwoMapLanes {
        first: MapLane<i32, i32>,
        second: MapLane<i32, i32>,
    }

    check_agent::<TwoMapLanes>(vec![], vec![persistent("first"), persistent("second")]);
}

#[test]
fn two_command_lanes() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct TwoCommandLanes {
        first: CommandLane<i32>,
        second: CommandLane<i32>,
    }

    check_agent::<TwoCommandLanes>(vec![transient("first"), transient("second")], vec![]);
}

#[test]
fn mixed_lanes() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct MixedLanes {
        first: ValueLane<i32>,
        second: MapLane<i32, i32>,
    }

    check_agent::<MixedLanes>(vec![persistent("first")], vec![persistent("second")]);
}

#[test]
fn multiple_lanes() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct MultipleLanes {
        first: ValueLane<i32>,
        second: MapLane<i32, i32>,
        third: ValueLane<i32>,
        fourth: MapLane<i32, i32>,
        fifth: CommandLane<i32>,
    }

    check_agent::<MultipleLanes>(
        vec![persistent("first"), persistent("third"), transient("fifth")],
        vec![persistent("second"), persistent("fourth")],
    );
}

#[test]
fn value_laned_tagged_transient() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct TwoValueLanes {
        #[transient]
        first: ValueLane<i32>,
        second: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec![transient("first"), persistent("second")], vec![]);
}

#[test]
fn map_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct TwoMapLanes {
        first: MapLane<i32, i32>,
        #[transient]
        second: MapLane<i32, i32>,
    }

    check_agent::<TwoMapLanes>(vec![], vec![persistent("first"), transient("second")]);
}

#[test]
fn command_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct TwoCommandLanes {
        #[transient]
        first: CommandLane<i32>,
        second: CommandLane<i32>,
    }

    check_agent::<TwoCommandLanes>(vec![transient("first"), transient("second")], vec![]);
}
