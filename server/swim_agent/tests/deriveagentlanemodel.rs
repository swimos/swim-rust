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

use std::collections::HashSet;

use bytes::BytesMut;
use std::fmt::Write;
use swim_agent::lanes::{CommandLane, MapLane, ValueLane};
use swim_agent::model::MapMessage;
use swim_agent::AgentLaneModel;
use swim_model::Text;
use uuid::Uuid;

const SYNC_ID: Uuid = Uuid::from_u128(85883);

fn check_agent<A>(val_lanes: Vec<&'static str>, map_lanes: Vec<&'static str>)
where
    A: AgentLaneModel + Default,
{
    let agent = A::default();

    let val_set = val_lanes.into_iter().collect::<HashSet<_>>();
    let map_set = map_lanes.into_iter().collect::<HashSet<_>>();

    assert_eq!(A::value_like_lanes(), val_set);
    assert_eq!(A::map_like_lanes(), map_set);

    let id_map = A::lane_ids();
    let expected_len = val_set.len() + map_set.len();
    assert_eq!(id_map.len(), expected_len);

    let mut keys = HashSet::new();
    let mut names = HashSet::new();

    for (key, name) in id_map {
        keys.insert(key);
        names.insert(name);
    }

    let expected_keys = (0..expected_len).map(|n| n as u64).collect::<HashSet<_>>();
    let mut expected_names = HashSet::new();
    expected_names.extend(val_set.iter().map(|s| Text::new(*s)));
    expected_names.extend(map_set.iter().map(|s| Text::new(*s)));

    assert_eq!(keys, expected_keys);
    assert_eq!(names, expected_names);

    for name in val_set {
        assert!(agent.on_value_command(name, get_i32_buffer(4)).is_some());
        assert!(agent.on_sync(name, SYNC_ID).is_some());
    }

    for name in map_set {
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

    check_agent::<SingleValueLane>(vec!["lane"], vec![]);
}

#[test]
fn single_map_lane() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct SingleMapLane {
        lane: MapLane<i32, i32>,
    }

    check_agent::<SingleMapLane>(vec![], vec!["lane"]);
}

#[test]
fn single_command_lane() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct SingleCommandLane {
        lane: CommandLane<i32>,
    }

    check_agent::<SingleCommandLane>(vec!["lane"], vec![]);
}

#[test]
fn two_value_lanes() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct TwoValueLanes {
        first: ValueLane<i32>,
        second: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec!["first", "second"], vec![]);
}

#[test]
fn two_map_lanes() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct TwoMapLanes {
        first: MapLane<i32, i32>,
        second: MapLane<i32, i32>,
    }

    check_agent::<TwoMapLanes>(vec![], vec!["first", "second"]);
}

#[test]
fn two_command_lanes() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct TwoCommandLanes {
        first: CommandLane<i32>,
        second: CommandLane<i32>,
    }

    check_agent::<TwoCommandLanes>(vec!["first", "second"], vec![]);
}

#[test]
fn mixed_lanes() {
    #[derive(AgentLaneModel)]
    #[agent_root(::swim_agent)]
    struct MixedLanes {
        first: ValueLane<i32>,
        second: MapLane<i32, i32>,
    }

    check_agent::<MixedLanes>(vec!["first"], vec!["second"]);
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

    check_agent::<MultipleLanes>(vec!["first", "third", "fifth"], vec!["second", "fourth"]);
}
