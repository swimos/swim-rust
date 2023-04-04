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

use std::fmt::Write;
use swim::agent::agent_model::{ItemSpec, LaneFlags};
use swim::agent::lanes::{CommandLane, MapLane, ValueLane};
use swim::agent::model::MapMessage;
use swim::agent::model::Text;
use swim::agent::reexport::bytes::BytesMut;
use swim::agent::reexport::uuid::Uuid;
use swim::agent::AgentLaneModel;
use swim_agent::agent_model::ItemKind;
use swim_agent::lanes::JoinValueLane;
use swim_agent::stores::{MapStore, ValueStore};
use swim_api::meta::lane::LaneKind;
use swim_api::store::StoreKind;

const SYNC_ID: Uuid = Uuid::from_u128(85883);

fn persistent_lane(name: &'static str, kind: LaneKind) -> (&'static str, ItemSpec) {
    (
        name,
        ItemSpec::new(ItemKind::Lane(kind), LaneFlags::empty()),
    )
}

fn transient_lane(name: &'static str, kind: LaneKind) -> (&'static str, ItemSpec) {
    (
        name,
        ItemSpec::new(ItemKind::Lane(kind), LaneFlags::TRANSIENT),
    )
}

fn persistent_store(name: &'static str, kind: StoreKind) -> (&'static str, ItemSpec) {
    (
        name,
        ItemSpec::new(ItemKind::Store(kind), LaneFlags::empty()),
    )
}

fn transient_store(name: &'static str, kind: StoreKind) -> (&'static str, ItemSpec) {
    (
        name,
        ItemSpec::new(ItemKind::Store(kind), LaneFlags::TRANSIENT),
    )
}

fn check_agent<A>(specs: Vec<(&'static str, ItemSpec)>)
where
    A: AgentLaneModel + Default,
{
    let agent = A::default();
    let expected = specs.into_iter().collect::<HashMap<_, _>>();

    assert_eq!(A::lane_specs(), expected);

    let id_map = A::item_ids();
    let expected_len = expected.len();
    assert_eq!(id_map.len(), expected_len);

    let mut keys = HashSet::new();
    let mut names = HashSet::new();

    for (key, name) in id_map {
        keys.insert(key);
        names.insert(name);
    }

    let expected_keys = (0..expected_len).map(|n| n as u64).collect::<HashSet<_>>();
    let mut expected_names = HashSet::new();
    expected_names.extend(expected.keys().map(|s| Text::new(s)));

    assert_eq!(keys, expected_keys);
    assert_eq!(names, expected_names);

    for (name, spec) in expected {
        let ItemSpec { kind, .. } = spec;

        if kind.map_like() {
            assert_eq!(
                agent.on_map_command(name, MapMessage::Clear).is_some(),
                kind.is_lane()
            );
            assert_eq!(agent.on_sync(name, SYNC_ID).is_some(), kind.is_lane());
        } else {
            assert_eq!(
                agent.on_value_command(name, get_i32_buffer(4)).is_some(),
                kind.is_lane()
            );
            assert_eq!(agent.on_sync(name, SYNC_ID).is_some(), kind.is_lane());
        }
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

    check_agent::<SingleValueLane>(vec![persistent_lane("lane", LaneKind::Value)]);
}

#[test]
fn single_value_store() {
    #[derive(AgentLaneModel)]
    struct SingleValueStore {
        store: ValueStore<i32>,
    }

    check_agent::<SingleValueStore>(vec![persistent_store("store", StoreKind::Value)]);
}

#[test]
fn single_map_lane() {
    #[derive(AgentLaneModel)]
    struct SingleMapLane {
        lane: MapLane<i32, i32>,
    }

    check_agent::<SingleMapLane>(vec![persistent_lane("lane", LaneKind::Map)]);
}

#[test]
fn single_map_store() {
    #[derive(AgentLaneModel)]
    struct SingleMapStore {
        store: MapStore<i32, i32>,
    }

    check_agent::<SingleMapStore>(vec![persistent_store("store", StoreKind::Map)]);
}

#[test]
fn single_command_lane() {
    #[derive(AgentLaneModel)]
    struct SingleCommandLane {
        lane: CommandLane<i32>,
    }

    check_agent::<SingleCommandLane>(vec![transient_lane("lane", LaneKind::Command)]);
}

#[test]
fn two_value_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoValueLanes {
        first: ValueLane<i32>,
        second: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec![
        persistent_lane("first", LaneKind::Value),
        persistent_lane("second", LaneKind::Value),
    ]);
}

#[test]
fn two_value_stores() {
    #[derive(AgentLaneModel)]
    struct TwoValueStores {
        first: ValueStore<i32>,
        second: ValueStore<i32>,
    }

    check_agent::<TwoValueStores>(vec![
        persistent_store("first", StoreKind::Value),
        persistent_store("second", StoreKind::Value),
    ]);
}

#[test]
fn two_map_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoMapLanes {
        first: MapLane<i32, i32>,
        second: MapLane<i32, i32>,
    }

    check_agent::<TwoMapLanes>(vec![
        persistent_lane("first", LaneKind::Map),
        persistent_lane("second", LaneKind::Map),
    ]);
}

#[test]
fn two_map_stores() {
    #[derive(AgentLaneModel)]
    struct TwoMapStores {
        first: MapStore<i32, i32>,
        second: MapStore<i32, i32>,
    }

    check_agent::<TwoMapStores>(vec![
        persistent_store("first", StoreKind::Map),
        persistent_store("second", StoreKind::Map),
    ]);
}

#[test]
fn two_command_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoCommandLanes {
        first: CommandLane<i32>,
        second: CommandLane<i32>,
    }

    check_agent::<TwoCommandLanes>(vec![
        transient_lane("first", LaneKind::Command),
        transient_lane("second", LaneKind::Command),
    ]);
}

#[test]
fn mixed_lanes() {
    #[derive(AgentLaneModel)]
    struct MixedLanes {
        first: ValueLane<i32>,
        second: MapLane<i32, i32>,
    }

    check_agent::<MixedLanes>(vec![
        persistent_lane("first", LaneKind::Value),
        persistent_lane("second", LaneKind::Map),
    ]);
}

#[test]
fn mixed_stores() {
    #[derive(AgentLaneModel)]
    struct MixedStores {
        first: ValueStore<i32>,
        second: MapStore<i32, i32>,
    }

    check_agent::<MixedStores>(vec![
        persistent_store("first", StoreKind::Value),
        persistent_store("second", StoreKind::Map),
    ]);
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
        sixth: JoinValueLane<i32, i32>,
    }

    check_agent::<MultipleLanes>(vec![
        persistent_lane("first", LaneKind::Value),
        persistent_lane("third", LaneKind::Value),
        transient_lane("fifth", LaneKind::Command),
        persistent_lane("second", LaneKind::Map),
        persistent_lane("fourth", LaneKind::Map),
        persistent_lane("sixth", LaneKind::JoinValue),
    ]);
}

#[test]
fn stores_and_lanes() {
    #[derive(AgentLaneModel)]
    struct StoresAndLanes {
        first: ValueStore<i32>,
        second: ValueLane<i32>,
        third: MapStore<i32, i32>,
        fourth: MapLane<i32, i32>,
    }

    check_agent::<StoresAndLanes>(vec![
        persistent_store("first", StoreKind::Value),
        persistent_lane("second", LaneKind::Value),
        persistent_store("third", StoreKind::Map),
        persistent_lane("fourth", LaneKind::Map),
    ]);
}

#[test]
fn value_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoValueLanes {
        #[transient]
        first: ValueLane<i32>,
        second: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec![
        transient_lane("first", LaneKind::Value),
        persistent_lane("second", LaneKind::Value),
    ]);
}

#[test]
fn value_store_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoValueStores {
        #[transient]
        first: ValueStore<i32>,
        second: ValueStore<i32>,
    }

    check_agent::<TwoValueStores>(vec![
        transient_store("first", StoreKind::Value),
        persistent_store("second", StoreKind::Value),
    ]);
}

#[test]
fn map_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoMapLanes {
        first: MapLane<i32, i32>,
        #[transient]
        second: MapLane<i32, i32>,
    }

    check_agent::<TwoMapLanes>(vec![
        persistent_lane("first", LaneKind::Map),
        transient_lane("second", LaneKind::Map),
    ]);
}

#[test]
fn map_store_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoMapStores {
        first: MapStore<i32, i32>,
        #[transient]
        second: MapStore<i32, i32>,
    }

    check_agent::<TwoMapStores>(vec![
        persistent_store("first", StoreKind::Map),
        transient_store("second", StoreKind::Map),
    ]);
}

#[test]
fn command_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoCommandLanes {
        #[transient]
        first: CommandLane<i32>,
        second: CommandLane<i32>,
    }

    check_agent::<TwoCommandLanes>(vec![
        transient_lane("first", LaneKind::Command),
        transient_lane("second", LaneKind::Command),
    ]);
}

#[test]
fn single_join_value_lane() {
    #[derive(AgentLaneModel)]
    struct SingleJoinValueLane {
        lane: JoinValueLane<i32, i32>,
    }

    check_agent::<SingleJoinValueLane>(vec![persistent_lane("lane", LaneKind::JoinValue)]);
}

#[test]
fn two_join_value_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoJoinValueLanes {
        first: JoinValueLane<i32, i32>,
        second: JoinValueLane<i32, i32>,
    }

    check_agent::<TwoJoinValueLanes>(vec![
        persistent_lane("first", LaneKind::JoinValue),
        persistent_lane("second", LaneKind::JoinValue),
    ]);
}

#[test]
fn join_value_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoJoinValueLanes {
        first: JoinValueLane<i32, i32>,
        #[transient]
        second: JoinValueLane<i32, i32>,
    }

    check_agent::<TwoJoinValueLanes>(vec![
        persistent_lane("first", LaneKind::JoinValue),
        transient_lane("second", LaneKind::JoinValue),
    ]);
}

mod isolated {
    use super::{check_agent, persistent_lane, persistent_store, transient_lane};
    use swim_api::meta::lane::LaneKind;
    use swim_api::store::StoreKind;

    #[test]
    fn multiple_items_qualified() {
        #[derive(swim::agent::AgentLaneModel)]
        struct MultipleLanes {
            first: swim::agent::lanes::ValueLane<i32>,
            second: swim::agent::lanes::MapLane<i32, i32>,
            third: swim::agent::lanes::ValueLane<i32>,
            fourth: swim::agent::lanes::MapLane<i32, i32>,
            fifth: swim::agent::lanes::CommandLane<i32>,
            sixth: swim::agent::stores::ValueStore<i32>,
            seventh: swim::agent::stores::MapStore<i32, i32>,
            eighth: swim::agent::lanes::JoinValueLane<i32, i32>,
        }

        check_agent::<MultipleLanes>(vec![
            persistent_lane("first", LaneKind::Value),
            persistent_lane("third", LaneKind::Value),
            transient_lane("fifth", LaneKind::Command),
            persistent_store("sixth", StoreKind::Value),
            persistent_lane("second", LaneKind::Map),
            persistent_lane("fourth", LaneKind::Map),
            persistent_store("seventh", StoreKind::Map),
            persistent_lane("eighth", LaneKind::JoinValue),
        ]);
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

    check_agent::<First>(vec![persistent_lane("lane", LaneKind::Value)]);
    check_agent::<Second>(vec![persistent_lane("lane", LaneKind::Value)]);
}
