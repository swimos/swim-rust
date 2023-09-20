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
use swim::agent::agent_model::ItemFlags;
use swim::agent::lanes::{CommandLane, MapLane, ValueLane};
use swim::agent::model::MapMessage;
use swim::agent::model::Text;
use swim::agent::reexport::bytes::BytesMut;
use swim::agent::reexport::uuid::Uuid;
use swim::agent::AgentLaneModel;
use swim_agent::agent_model::ItemSpec;
use swim_agent::lanes::http::Recon;
use swim_agent::lanes::{DemandLane, DemandMapLane, HttpLane, JoinValueLane, SimpleHttpLane};
use swim_agent::reexport::bytes::Bytes;
use swim_agent::stores::{MapStore, ValueStore};
use swim_api::agent::HttpLaneRequest;
use swim_api::lane::WarpLaneKind;
use swim_api::store::StoreKind;
use swim_model::http::{HttpRequest, Uri};

const SYNC_ID: Uuid = Uuid::from_u128(85883);

fn persistent_lane(name: &'static str, kind: WarpLaneKind) -> (&'static str, ItemSpec) {
    (
        name,
        ItemSpec::WarpLane {
            kind,
            flags: ItemFlags::empty(),
        },
    )
}

fn transient_lane(name: &'static str, kind: WarpLaneKind) -> (&'static str, ItemSpec) {
    (
        name,
        ItemSpec::WarpLane {
            kind,
            flags: ItemFlags::TRANSIENT,
        },
    )
}

fn persistent_store(name: &'static str, kind: StoreKind) -> (&'static str, ItemSpec) {
    (
        name,
        ItemSpec::Store {
            kind,
            flags: ItemFlags::empty(),
        },
    )
}

fn transient_store(name: &'static str, kind: StoreKind) -> (&'static str, ItemSpec) {
    (
        name,
        ItemSpec::Store {
            kind,
            flags: ItemFlags::TRANSIENT,
        },
    )
}

fn http_lane(name: &'static str) -> (&'static str, ItemSpec) {
    (name, ItemSpec::Http)
}

fn check_agent<A>(specs: Vec<(&'static str, ItemSpec)>)
where
    A: AgentLaneModel + Default,
{
    let agent = A::default();
    let expected = specs.into_iter().collect::<HashMap<_, _>>();

    assert_eq!(A::item_specs2(), expected);

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
        match spec {
            ItemSpec::WarpLane { kind, .. } => {
                if kind.map_like() {
                    assert!(agent.on_map_command(name, MapMessage::Clear).is_some());
                    assert!(agent.on_sync(name, SYNC_ID).is_some());
                } else {
                    assert!(agent.on_value_command(name, get_i32_buffer(4)).is_some());
                    assert!(agent.on_sync(name, SYNC_ID).is_some());
                }
            }
            ItemSpec::Store { .. } => {
                assert!(agent.on_map_command(name, MapMessage::Clear).is_none());
                assert!(agent.on_sync(name, SYNC_ID).is_none());
                assert!(agent.on_value_command(name, get_i32_buffer(4)).is_none());
                assert!(agent.on_sync(name, SYNC_ID).is_none());
            }
            ItemSpec::Http => {
                let uri = format!("http://example/node?lane={}", name)
                    .parse::<Uri>()
                    .unwrap();
                let request_inner = HttpRequest::get(uri).map(|_| Bytes::new());
                let (request, _response_rx) = HttpLaneRequest::new(request_inner);
                assert!(agent.on_http_request(name, request).is_ok());
            }
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

    check_agent::<SingleValueLane>(vec![persistent_lane("lane", WarpLaneKind::Value)]);
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

    check_agent::<SingleMapLane>(vec![persistent_lane("lane", WarpLaneKind::Map)]);
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

    check_agent::<SingleCommandLane>(vec![transient_lane("lane", WarpLaneKind::Command)]);
}

#[test]
fn single_demand_lane() {
    #[derive(AgentLaneModel)]
    struct SingleDemandLane {
        lane: DemandLane<i32>,
    }

    check_agent::<SingleDemandLane>(vec![transient_lane("lane", WarpLaneKind::Demand)]);
}

#[test]
fn single_demand_map_lane() {
    #[derive(AgentLaneModel)]
    struct SingleDemandMapLane {
        lane: DemandMapLane<i32, i32>,
    }

    check_agent::<SingleDemandMapLane>(vec![transient_lane("lane", WarpLaneKind::DemandMap)]);
}

#[test]
fn two_value_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoValueLanes {
        first: ValueLane<i32>,
        second: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec![
        persistent_lane("first", WarpLaneKind::Value),
        persistent_lane("second", WarpLaneKind::Value),
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
        persistent_lane("first", WarpLaneKind::Map),
        persistent_lane("second", WarpLaneKind::Map),
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
        transient_lane("first", WarpLaneKind::Command),
        transient_lane("second", WarpLaneKind::Command),
    ]);
}

#[test]
fn two_demand_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoDemandLanes {
        first: DemandLane<i32>,
        second: DemandLane<i32>,
    }

    check_agent::<TwoDemandLanes>(vec![
        transient_lane("first", WarpLaneKind::Demand),
        transient_lane("second", WarpLaneKind::Demand),
    ]);
}

#[test]
fn two_demand_map_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoDemandMapLanes {
        first: DemandMapLane<i32, i32>,
        second: DemandMapLane<i32, i32>,
    }

    check_agent::<TwoDemandMapLanes>(vec![
        transient_lane("first", WarpLaneKind::DemandMap),
        transient_lane("second", WarpLaneKind::DemandMap),
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
        persistent_lane("first", WarpLaneKind::Value),
        persistent_lane("second", WarpLaneKind::Map),
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
        seventh: DemandLane<i32>,
        eighth: DemandMapLane<i32, i32>,
        ninth: SimpleHttpLane<i32>,
    }

    check_agent::<MultipleLanes>(vec![
        persistent_lane("first", WarpLaneKind::Value),
        persistent_lane("third", WarpLaneKind::Value),
        transient_lane("fifth", WarpLaneKind::Command),
        transient_lane("seventh", WarpLaneKind::Demand),
        persistent_lane("second", WarpLaneKind::Map),
        persistent_lane("fourth", WarpLaneKind::Map),
        persistent_lane("sixth", WarpLaneKind::JoinValue),
        transient_lane("eighth", WarpLaneKind::DemandMap),
        http_lane("ninth"),
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
        persistent_lane("second", WarpLaneKind::Value),
        persistent_store("third", StoreKind::Map),
        persistent_lane("fourth", WarpLaneKind::Map),
    ]);
}

#[test]
fn value_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoValueLanes {
        #[lane(transient)]
        first: ValueLane<i32>,
        second: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec![
        transient_lane("first", WarpLaneKind::Value),
        persistent_lane("second", WarpLaneKind::Value),
    ]);
}

#[test]
fn value_store_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoValueStores {
        #[lane(transient)]
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
        #[lane(transient)]
        second: MapLane<i32, i32>,
    }

    check_agent::<TwoMapLanes>(vec![
        persistent_lane("first", WarpLaneKind::Map),
        transient_lane("second", WarpLaneKind::Map),
    ]);
}

#[test]
fn map_store_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoMapStores {
        first: MapStore<i32, i32>,
        #[lane(transient)]
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
        #[lane(transient)]
        first: CommandLane<i32>,
        second: CommandLane<i32>,
    }

    check_agent::<TwoCommandLanes>(vec![
        transient_lane("first", WarpLaneKind::Command),
        transient_lane("second", WarpLaneKind::Command),
    ]);
}

#[test]
fn demand_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoDemandLanes {
        #[lane(transient)]
        first: DemandLane<i32>,
        second: DemandLane<i32>,
    }

    check_agent::<TwoDemandLanes>(vec![
        transient_lane("first", WarpLaneKind::Demand),
        transient_lane("second", WarpLaneKind::Demand),
    ]);
}

#[test]
fn demand_map_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoDemandMapLanes {
        #[lane(transient)]
        first: DemandMapLane<i32, i32>,
        second: DemandMapLane<i32, i32>,
    }

    check_agent::<TwoDemandMapLanes>(vec![
        transient_lane("first", WarpLaneKind::DemandMap),
        transient_lane("second", WarpLaneKind::DemandMap),
    ]);
}

#[test]
fn single_join_value_lane() {
    #[derive(AgentLaneModel)]
    struct SingleJoinValueLane {
        lane: JoinValueLane<i32, i32>,
    }

    check_agent::<SingleJoinValueLane>(vec![persistent_lane("lane", WarpLaneKind::JoinValue)]);
}

#[test]
fn two_join_value_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoJoinValueLanes {
        first: JoinValueLane<i32, i32>,
        second: JoinValueLane<i32, i32>,
    }

    check_agent::<TwoJoinValueLanes>(vec![
        persistent_lane("first", WarpLaneKind::JoinValue),
        persistent_lane("second", WarpLaneKind::JoinValue),
    ]);
}

#[test]
fn join_value_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoJoinValueLanes {
        first: JoinValueLane<i32, i32>,
        #[lane(transient)]
        second: JoinValueLane<i32, i32>,
    }

    check_agent::<TwoJoinValueLanes>(vec![
        persistent_lane("first", WarpLaneKind::JoinValue),
        transient_lane("second", WarpLaneKind::JoinValue),
    ]);
}

#[test]
fn single_simple_http_lane() {
    #[derive(AgentLaneModel)]
    struct SingleSimpleHttpLane {
        lane: SimpleHttpLane<i32>,
    }

    check_agent::<SingleSimpleHttpLane>(vec![http_lane("lane")]);
}

#[test]
fn single_simple_http_lane_explicit_codec() {
    #[derive(AgentLaneModel)]
    struct SingleSimpleHttpLane {
        lane: SimpleHttpLane<i32, Recon>,
    }

    check_agent::<SingleSimpleHttpLane>(vec![http_lane("lane")]);
}

#[test]
fn two_simple_http_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoSimpleHttpLanes {
        first: SimpleHttpLane<i32>,
        second: SimpleHttpLane<i32, Recon>,
    }

    check_agent::<TwoSimpleHttpLanes>(vec![http_lane("first"), http_lane("second")]);
}

#[test]
fn get_and_post_http_lane() {
    #[derive(AgentLaneModel)]
    struct GetAndPostHttpLane {
        lane: HttpLane<i32, String>,
    }

    check_agent::<GetAndPostHttpLane>(vec![http_lane("lane")]);
}

#[test]
fn get_post_and_put_http_lane() {
    #[derive(AgentLaneModel)]
    struct GetPostAndPutHttpLane {
        lane: HttpLane<i32, String, i32>,
    }

    check_agent::<GetPostAndPutHttpLane>(vec![http_lane("lane")]);
}

#[test]
fn general_http_lane_explicit_codec() {
    #[derive(AgentLaneModel)]
    struct GeneralHttpLane {
        lane: HttpLane<i32, String, i32, Recon>,
    }

    check_agent::<GeneralHttpLane>(vec![http_lane("lane")]);
}

#[test]
fn two_general_http_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoGeneralHttpLanes {
        first: HttpLane<i32, i32>,
        second: HttpLane<i32, String, i32>,
    }

    check_agent::<TwoGeneralHttpLanes>(vec![http_lane("first"), http_lane("second")]);
}

mod isolated {
    use crate::check_agent;

    use super::{http_lane, persistent_lane, persistent_store, transient_lane};
    use swim_api::lane::WarpLaneKind;
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
            ninth: swim::agent::lanes::DemandLane<i32>,
            tenth: swim::agent::lanes::DemandMapLane<i32, i32>,
            eleventh: swim::agent::lanes::SimpleHttpLane<i32>,
            twelfth: swim::agent::lanes::HttpLane<i32, i32>,
        }

        check_agent::<MultipleLanes>(vec![
            persistent_lane("first", WarpLaneKind::Value),
            persistent_lane("third", WarpLaneKind::Value),
            transient_lane("fifth", WarpLaneKind::Command),
            persistent_store("sixth", StoreKind::Value),
            persistent_lane("second", WarpLaneKind::Map),
            persistent_lane("fourth", WarpLaneKind::Map),
            persistent_store("seventh", StoreKind::Map),
            persistent_lane("eighth", WarpLaneKind::JoinValue),
            transient_lane("ninth", WarpLaneKind::Demand),
            transient_lane("tenth", WarpLaneKind::DemandMap),
            http_lane("eleventh"),
            http_lane("twelfth"),
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

    check_agent::<First>(vec![persistent_lane("lane", WarpLaneKind::Value)]);
    check_agent::<Second>(vec![persistent_lane("lane", WarpLaneKind::Value)]);
}

#[test]
fn rename_lane() {
    #[derive(AgentLaneModel)]
    struct TwoValueLanes {
        #[lane(name = "renamed")]
        first: ValueLane<i32>,
        second: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec![
        persistent_lane("renamed", WarpLaneKind::Value),
        persistent_lane("second", WarpLaneKind::Value),
    ]);
}

#[test]
fn rename_lane_with_convention() {
    #[derive(AgentLaneModel)]
    struct TwoValueLanes {
        #[lane(convention = "camel")]
        first_lane: ValueLane<i32>,
        second_lane: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec![
        persistent_lane("firstLane", WarpLaneKind::Value),
        persistent_lane("second_lane", WarpLaneKind::Value),
    ]);
}

#[test]
fn rename_all_lanes_with_convention() {
    #[derive(AgentLaneModel)]
    #[agent(convention = "camel")]
    struct TwoValueLanes {
        first_lane: ValueLane<i32>,
        second_lane: ValueLane<i32>,
        third_lane: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec![
        persistent_lane("firstLane", WarpLaneKind::Value),
        persistent_lane("secondLane", WarpLaneKind::Value),
        persistent_lane("thirdLane", WarpLaneKind::Value),
    ]);
}

#[test]
fn override_top_level_convention() {
    #[derive(AgentLaneModel)]
    #[agent(convention = "camel")]
    struct TwoValueLanes {
        first_lane: ValueLane<i32>,
        #[lane(name = "renamed")]
        second_lane: ValueLane<i32>,
        third_lane: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec![
        persistent_lane("firstLane", WarpLaneKind::Value),
        persistent_lane("renamed", WarpLaneKind::Value),
        persistent_lane("thirdLane", WarpLaneKind::Value),
    ]);
}

#[test]
fn agent_level_transient_flag() {
    #[derive(AgentLaneModel)]
    #[agent(transient)]
    struct EverythingTransient {
        first: ValueLane<i32>,
        second: MapLane<i32, i32>,
        third: CommandLane<i32>,
        fourth: ValueStore<i32>,
    }

    check_agent::<EverythingTransient>(vec![
        transient_lane("first", WarpLaneKind::Value),
        transient_lane("second", WarpLaneKind::Map),
        transient_lane("third", WarpLaneKind::Command),
        transient_store("fourth", StoreKind::Value),
    ]);
}
