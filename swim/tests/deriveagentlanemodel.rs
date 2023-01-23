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

use std::fmt::Write;
use swim::agent::agent_model::{ItemSpec, LaneFlags};
use swim::agent::lanes::{CommandLane, MapLane, ValueLane};
use swim::agent::model::MapMessage;
use swim::agent::model::Text;
use swim::agent::reexport::bytes::BytesMut;
use swim::agent::reexport::uuid::Uuid;
use swim::agent::AgentLaneModel;
use swim_agent::agent_model::ItemKind;

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
            ItemSpec::new(ItemKind::Lane, LaneFlags::TRANSIENT)
        } else {
            ItemSpec::new(ItemKind::Lane, LaneFlags::empty())
        };
        val_expected.insert(name, spec);
    }

    for (name, is_transient) in map_lanes {
        let spec = if is_transient {
            ItemSpec::new(ItemKind::Lane, LaneFlags::TRANSIENT)
        } else {
            ItemSpec::new(ItemKind::Lane, LaneFlags::empty())
        };
        map_expected.insert(name, spec);
    }

    assert_eq!(A::value_like_item_specs(), val_expected);
    assert_eq!(A::map_like_item_specs(), map_expected);

    let id_map = A::item_ids();
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
    expected_names.extend(val_expected.keys().map(|s| Text::new(s)));
    expected_names.extend(map_expected.keys().map(|s| Text::new(s)));

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
    struct SingleValueLane {
        lane: ValueLane<i32>,
    }

    check_agent::<SingleValueLane>(vec![persistent("lane")], vec![]);
}

#[test]
fn single_map_lane() {
    #[derive(AgentLaneModel)]
    struct SingleMapLane {
        lane: MapLane<i32, i32>,
    }

    check_agent::<SingleMapLane>(vec![], vec![persistent("lane")]);
}

#[test]
fn single_command_lane() {
    #[derive(AgentLaneModel)]
    struct SingleCommandLane {
        lane: CommandLane<i32>,
    }

    check_agent::<SingleCommandLane>(vec![transient("lane")], vec![]);
}

#[test]
fn two_value_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoValueLanes {
        first: ValueLane<i32>,
        second: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec![persistent("first"), persistent("second")], vec![]);
}

#[test]
fn two_map_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoMapLanes {
        first: MapLane<i32, i32>,
        second: MapLane<i32, i32>,
    }

    check_agent::<TwoMapLanes>(vec![], vec![persistent("first"), persistent("second")]);
}

#[test]
fn two_command_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoCommandLanes {
        first: CommandLane<i32>,
        second: CommandLane<i32>,
    }

    check_agent::<TwoCommandLanes>(vec![transient("first"), transient("second")], vec![]);
}

#[test]
fn mixed_lanes() {
    #[derive(AgentLaneModel)]
    struct MixedLanes {
        first: ValueLane<i32>,
        second: MapLane<i32, i32>,
    }

    check_agent::<MixedLanes>(vec![persistent("first")], vec![persistent("second")]);
}

#[test]
fn multiple_lanes() {
    #[derive(AgentLaneModel)]
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
fn value_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
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
    struct TwoCommandLanes {
        #[transient]
        first: CommandLane<i32>,
        second: CommandLane<i32>,
    }

    check_agent::<TwoCommandLanes>(vec![transient("first"), transient("second")], vec![]);
}

mod isolated {

    use super::{check_agent, persistent, transient};

    #[test]
    fn multiple_lanes_qualified() {
        #[derive(swim::agent::AgentLaneModel)]
        struct MultipleLanes {
            first: swim::agent::lanes::ValueLane<i32>,
            second: swim::agent::lanes::MapLane<i32, i32>,
            third: swim::agent::lanes::ValueLane<i32>,
            fourth: swim::agent::lanes::MapLane<i32, i32>,
            fifth: swim::agent::lanes::CommandLane<i32>,
        }

        check_agent::<MultipleLanes>(
            vec![persistent("first"), persistent("third"), transient("fifth")],
            vec![persistent("second"), persistent("fourth")],
        );
    }
}

#[test]
fn two_types_single_scope() {
    #[derive(AgentLaneModel)]
    struct First {
        lane: ValueLane<i32>,
    }

    #[derive(AgentLaneModel)]
    struct Second {
        lane: ValueLane<Text>,
    }

    check_agent::<First>(vec![persistent("lane")], vec![]);
    check_agent::<Second>(vec![persistent("lane")], vec![]);
}
