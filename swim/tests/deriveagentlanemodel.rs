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
use swim::agent::agent_model::{ItemFlags, ItemSpec};
use swim::agent::lanes::{CommandLane, MapLane, ValueLane};
use swim::agent::model::MapMessage;
use swim::agent::model::Text;
use swim::agent::reexport::bytes::BytesMut;
use swim::agent::reexport::uuid::Uuid;
use swim::agent::AgentLaneModel;
use swim_agent::agent_model::ItemKind;
use swim_agent::lanes::http::Recon;
use swim_agent::lanes::{DemandLane, DemandMapLane, HttpLane, JoinValueLane, SimpleHttpLane};
use swim_agent::reexport::bytes::Bytes;
use swim_agent::stores::{MapStore, ValueStore};
use swim_api::agent::HttpLaneRequest;
use swim_model::http::{HttpRequest, Uri};

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
    check_agent_with_stores_and_http::<A>(val_items, map_items, vec![])
}

fn check_agent_with_stores_and_http<A>(
    val_items: Vec<(ItemKind, &'static str, bool)>,
    map_items: Vec<(ItemKind, &'static str, bool)>,
    http_lanes: Vec<&'static str>,
) where
    A: AgentLaneModel + Default,
{
    let agent = A::default();

    let mut val_expected = HashMap::new();
    let mut map_expected = HashMap::new();

    for (kind, name, is_transient) in &val_items {
        let spec = if *is_transient {
            ItemSpec::new(*kind, ItemFlags::TRANSIENT)
        } else {
            ItemSpec::new(*kind, ItemFlags::empty())
        };
        val_expected.insert(*name, spec);
    }

    for (kind, name, is_transient) in &map_items {
        let spec = if *is_transient {
            ItemSpec::new(*kind, ItemFlags::TRANSIENT)
        } else {
            ItemSpec::new(*kind, ItemFlags::empty())
        };
        map_expected.insert(*name, spec);
    }

    let http_expected = http_lanes.iter().copied().collect::<HashSet<_>>();

    assert_eq!(A::value_like_item_specs(), val_expected);
    assert_eq!(A::map_like_item_specs(), map_expected);
    assert_eq!(A::http_lane_names(), http_expected);

    let id_map = A::item_ids();
    let expected_len = val_expected.len() + map_expected.len() + http_expected.len();
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
    expected_names.extend(http_expected.iter().map(|s| Text::new(s)));

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

    for lane in http_lanes {
        let uri = format!("http://example/node?lane={}", lane)
            .parse::<Uri>()
            .unwrap();
        let request_inner = HttpRequest::get(uri).map(|_| Bytes::new());
        let (request, _response_rx) = HttpLaneRequest::new(request_inner);
        assert!(agent.on_http_request(lane, request).is_ok());
    }
}

fn check_agent_with_http<A>(
    val_lanes: Vec<(&'static str, bool)>,
    map_lanes: Vec<(&'static str, bool)>,
    http_lanes: Vec<&'static str>,
) where
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

    check_agent_with_stores_and_http::<A>(val_items, map_items, http_lanes)
}

fn check_agent<A>(val_lanes: Vec<(&'static str, bool)>, map_lanes: Vec<(&'static str, bool)>)
where
    A: AgentLaneModel + Default,
{
    check_agent_with_http::<A>(val_lanes, map_lanes, vec![])
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
fn single_demand_lane() {
    #[derive(AgentLaneModel)]
    struct SingleDemandLane {
        lane: DemandLane<i32>,
    }

    check_agent::<SingleDemandLane>(vec![transient("lane")], vec![]);
}

#[test]
fn single_demand_map_lane() {
    #[derive(AgentLaneModel)]
    struct SingleDemandMapLane {
        lane: DemandMapLane<i32, i32>,
    }

    check_agent::<SingleDemandMapLane>(vec![], vec![transient("lane")]);
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
fn two_demand_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoDemandLanes {
        first: DemandLane<i32>,
        second: DemandLane<i32>,
    }

    check_agent::<TwoDemandLanes>(vec![transient("first"), transient("second")], vec![]);
}

#[test]
fn two_demand_map_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoDemandMapLanes {
        first: DemandMapLane<i32, i32>,
        second: DemandMapLane<i32, i32>,
    }

    check_agent::<TwoDemandMapLanes>(vec![], vec![transient("first"), transient("second")]);
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
        sixth: JoinValueLane<i32, i32>,
        seventh: DemandLane<i32>,
        eighth: DemandMapLane<i32, i32>,
        ninth: SimpleHttpLane<i32>,
    }

    check_agent_with_http::<MultipleLanes>(
        vec![
            persistent("first"),
            persistent("third"),
            transient("fifth"),
            transient("seventh"),
        ],
        vec![
            persistent("second"),
            persistent("fourth"),
            persistent("sixth"),
            transient("eighth"),
        ],
        vec!["ninth"],
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

#[test]
fn demand_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoDemandLanes {
        #[transient]
        first: DemandLane<i32>,
        second: DemandLane<i32>,
    }

    check_agent::<TwoDemandLanes>(vec![transient("first"), transient("second")], vec![]);
}

#[test]
fn demand_map_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoDemandMapLanes {
        #[transient]
        first: DemandMapLane<i32, i32>,
        second: DemandMapLane<i32, i32>,
    }

    check_agent::<TwoDemandMapLanes>(vec![], vec![transient("first"), transient("second")]);
}

#[test]
fn single_join_value_lane() {
    #[derive(AgentLaneModel)]
    struct SingleJoinValueLane {
        lane: JoinValueLane<i32, i32>,
    }

    check_agent::<SingleJoinValueLane>(vec![], vec![persistent("lane")]);
}

#[test]
fn two_join_value_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoJoinValueLanes {
        first: JoinValueLane<i32, i32>,
        second: JoinValueLane<i32, i32>,
    }

    check_agent::<TwoJoinValueLanes>(vec![], vec![persistent("first"), persistent("second")]);
}

#[test]
fn join_value_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoJoinValueLanes {
        first: JoinValueLane<i32, i32>,
        #[transient]
        second: JoinValueLane<i32, i32>,
    }

    check_agent::<TwoJoinValueLanes>(vec![], vec![persistent("first"), transient("second")]);
}

#[test]
fn single_simple_http_lane() {
    #[derive(AgentLaneModel)]
    struct SingleSimpleHttpLane {
        lane: SimpleHttpLane<i32>,
    }

    check_agent_with_http::<SingleSimpleHttpLane>(vec![], vec![], vec!["lane"]);
}

#[test]
fn single_simple_http_lane_explicit_codec() {
    #[derive(AgentLaneModel)]
    struct SingleSimpleHttpLane {
        lane: SimpleHttpLane<i32, Recon>,
    }

    check_agent_with_http::<SingleSimpleHttpLane>(vec![], vec![], vec!["lane"]);
}

#[test]
fn two_simple_http_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoSimpleHttpLanes {
        first: SimpleHttpLane<i32>,
        second: SimpleHttpLane<i32, Recon>,
    }

    check_agent_with_http::<TwoSimpleHttpLanes>(vec![], vec![], vec!["first", "second"]);
}

#[test]
fn get_and_post_http_lane() {
    #[derive(AgentLaneModel)]
    struct GetAndPostHttpLane {
        lane: HttpLane<i32, String>,
    }

    check_agent_with_http::<GetAndPostHttpLane>(vec![], vec![], vec!["lane"]);
}

#[test]
fn get_post_and_put_http_lane() {
    #[derive(AgentLaneModel)]
    struct GetPostAndPutHttpLane {
        lane: HttpLane<i32, String, i32>,
    }

    check_agent_with_http::<GetPostAndPutHttpLane>(vec![], vec![], vec!["lane"]);
}

#[test]
fn general_http_lane_explicit_codec() {
    #[derive(AgentLaneModel)]
    struct GeneralHttpLane {
        lane: HttpLane<i32, String, i32, Recon>,
    }

    check_agent_with_http::<GeneralHttpLane>(vec![], vec![], vec!["lane"]);
}

#[test]
fn two_general_http_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoGeneralHttpLanes {
        first: HttpLane<i32, i32>,
        second: HttpLane<i32, String, i32>,
    }

    check_agent_with_http::<TwoGeneralHttpLanes>(vec![], vec![], vec!["first", "second"]);
}

mod isolated {

    use super::{
        check_agent_with_stores_and_http, persistent_lane, persistent_store, transient_lane,
    };

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
            ninth: swim::agent::lanes::DemandLane<i32>,
            tenth: swim::agent::lanes::DemandMapLane<i32, i32>,
            eleventh: swim::agent::lanes::SimpleHttpLane<i32>,
            twelfth: swim::agent::lanes::HttpLane<i32, i32>,
        }

        check_agent_with_stores_and_http::<MultipleLanes>(
            vec![
                persistent_lane("first"),
                persistent_lane("third"),
                transient_lane("fifth"),
                persistent_store("sixth"),
                transient_lane("ninth"),
            ],
            vec![
                persistent_lane("second"),
                persistent_lane("fourth"),
                persistent_store("seventh"),
                persistent_lane("eighth"),
                transient_lane("tenth"),
                transient_lane("tenth"),
            ],
            vec!["eleventh", "twelfth"],
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
