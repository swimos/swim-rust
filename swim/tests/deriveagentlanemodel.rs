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
use swim_agent::stores::{MapStore, ValueStore};

const SYNC_ID: Uuid = Uuid::from_u128(85883);

fn transient(name: &'static str) -> (&'static str, bool) {
    (name, true)
}

fn persistent(name: &'static str) -> (&'static str, bool) {
    (name, false)
}

fn persistent_lane(name: &'static str) -> (ItemKind, &'static str, bool) {
    (ItemKind::Lane, name, false)
}

fn transient_lane(name: &'static str) -> (ItemKind, &'static str, bool) {
    (ItemKind::Lane, name, true)
}

fn persistent_store(name: &'static str) -> (ItemKind, &'static str, bool) {
    (ItemKind::Store, name, false)
}

fn transient_store(name: &'static str) -> (ItemKind, &'static str, bool) {
    (ItemKind::Store, name, true)
}

fn check_agent_with_stores<A>(
    val_items: Vec<(ItemKind, &'static str, bool)>,
    map_items: Vec<(ItemKind, &'static str, bool)>,
) where
    A: AgentLaneModel + Default,
{
    let agent = A::default();

    let mut val_expected = HashMap::new();
    let mut map_expected = HashMap::new();

    for (kind, name, is_transient) in &val_items {
        let spec = if *is_transient {
            ItemSpec::new(*kind, LaneFlags::TRANSIENT)
        } else {
            ItemSpec::new(*kind, LaneFlags::empty())
        };
        val_expected.insert(*name, spec);
    }

    for (kind, name, is_transient) in &map_items {
        let spec = if *is_transient {
            ItemSpec::new(*kind, LaneFlags::TRANSIENT)
        } else {
            ItemSpec::new(*kind, LaneFlags::empty())
        };
        map_expected.insert(*name, spec);
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

    for (kind, name, _) in val_items {
        assert_eq!(
            agent.on_value_command(name, get_i32_buffer(4)).is_some(),
            kind == ItemKind::Lane
        );
        assert_eq!(
            agent.on_sync(name, SYNC_ID).is_some(),
            kind == ItemKind::Lane
        );
    }

    for (kind, name, _) in map_items {
        assert_eq!(
            agent.on_map_command(name, MapMessage::Clear).is_some(),
            kind == ItemKind::Lane
        );
        assert_eq!(
            agent.on_sync(name, SYNC_ID).is_some(),
            kind == ItemKind::Lane
        );
    }
}

fn check_agent<A>(val_lanes: Vec<(&'static str, bool)>, map_lanes: Vec<(&'static str, bool)>)
where
    A: AgentLaneModel + Default,
{
    let val_items = val_lanes
        .into_iter()
        .map(|(name, transient)| (ItemKind::Lane, name, transient))
        .collect();
    let map_items = map_lanes
        .into_iter()
        .map(|(name, transient)| (ItemKind::Lane, name, transient))
        .collect();

    check_agent_with_stores::<A>(val_items, map_items)
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
fn single_value_store() {
    #[derive(AgentLaneModel)]
    struct SingleValueStore {
        store: ValueStore<i32>,
    }

    check_agent_with_stores::<SingleValueStore>(vec![persistent_store("store")], vec![]);
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
fn single_map_store() {
    #[derive(AgentLaneModel)]
    struct SingleMapStore {
        store: MapStore<i32, i32>,
    }

    check_agent_with_stores::<SingleMapStore>(vec![], vec![persistent_store("store")]);
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
fn two_value_stores() {
    #[derive(AgentLaneModel)]
    struct TwoValueStores {
        first: ValueStore<i32>,
        second: ValueStore<i32>,
    }

    check_agent_with_stores::<TwoValueStores>(
        vec![persistent_store("first"), persistent_store("second")],
        vec![],
    );
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
fn two_map_stores() {
    #[derive(AgentLaneModel)]
    struct TwoMapStores {
        first: MapStore<i32, i32>,
        second: MapStore<i32, i32>,
    }

    check_agent_with_stores::<TwoMapStores>(
        vec![],
        vec![persistent_store("first"), persistent_store("second")],
    );
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
fn mixed_stores() {
    #[derive(AgentLaneModel)]
    struct MixedStores {
        first: ValueStore<i32>,
        second: MapStore<i32, i32>,
    }

    check_agent_with_stores::<MixedStores>(
        vec![persistent_store("first")],
        vec![persistent_store("second")],
    );
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
fn stores_and_lanes() {
    #[derive(AgentLaneModel)]
    struct StoresAndLanes {
        first: ValueStore<i32>,
        second: ValueLane<i32>,
        third: MapStore<i32, i32>,
        fourth: MapLane<i32, i32>,
    }

    check_agent_with_stores::<StoresAndLanes>(
        vec![persistent_store("first"), persistent_lane("second")],
        vec![persistent_store("third"), persistent_lane("fourth")],
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
fn value_store_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoValueStores {
        #[transient]
        first: ValueStore<i32>,
        second: ValueStore<i32>,
    }

    check_agent_with_stores::<TwoValueStores>(
        vec![transient_store("first"), persistent_store("second")],
        vec![],
    );
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
fn map_store_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoMapStores {
        first: MapStore<i32, i32>,
        #[transient]
        second: MapStore<i32, i32>,
    }

    check_agent_with_stores::<TwoMapStores>(
        vec![],
        vec![persistent_store("first"), transient_store("second")],
    );
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

    use super::{check_agent_with_stores, persistent_lane, persistent_store, transient_lane};

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
        }

        check_agent_with_stores::<MultipleLanes>(
            vec![
                persistent_lane("first"),
                persistent_lane("third"),
                transient_lane("fifth"),
                persistent_store("sixth"),
            ],
            vec![
                persistent_lane("second"),
                persistent_lane("fourth"),
                persistent_store("seventh"),
            ],
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