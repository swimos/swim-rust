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

use std::collections::HashMap;

use std::fmt::Write;
use swim::agent::agent_model::ItemFlags;
use swim::agent::lanes::{CommandLane, MapLane, ValueLane};
use swim::agent::model::MapMessage;
use swim::agent::model::Text;
use swim::agent::reexport::bytes::BytesMut;
use swim::agent::reexport::uuid::Uuid;
use swim::agent::AgentLaneModel;
use swim_agent::agent_model::{ItemDescriptor, ItemSpec};
use swim_agent::lanes::http::Recon;
use swim_agent::lanes::{DemandLane, DemandMapLane, HttpLane, JoinValueLane, SimpleHttpLane};
use swim_agent::reexport::bytes::Bytes;
use swim_agent::stores::{MapStore, ValueStore};
use swim_api::agent::HttpLaneRequest;
use swim_api::lane::WarpLaneKind;
use swim_api::store::StoreKind;
use swim_model::http::{HttpRequest, Uri};

const SYNC_ID: Uuid = Uuid::from_u128(85883);

fn persistent_lane(id: u64, name: &'static str, kind: WarpLaneKind) -> (&'static str, ItemSpec) {
    (
        name,
        ItemSpec::new(
            id,
            ItemDescriptor::WarpLane {
                kind,
                flags: ItemFlags::empty(),
            },
        ),
    )
}

fn transient_lane(id: u64, name: &'static str, kind: WarpLaneKind) -> (&'static str, ItemSpec) {
    (
        name,
        ItemSpec::new(
            id,
            ItemDescriptor::WarpLane {
                kind,
                flags: ItemFlags::TRANSIENT,
            },
        ),
    )
}

fn persistent_store(id: u64, name: &'static str, kind: StoreKind) -> (&'static str, ItemSpec) {
    (
        name,
        ItemSpec::new(
            id,
            ItemDescriptor::Store {
                kind,
                flags: ItemFlags::empty(),
            },
        ),
    )
}

fn transient_store(id: u64, name: &'static str, kind: StoreKind) -> (&'static str, ItemSpec) {
    (
        name,
        ItemSpec::new(
            id,
            ItemDescriptor::Store {
                kind,
                flags: ItemFlags::TRANSIENT,
            },
        ),
    )
}

fn http_lane(id: u64, name: &'static str) -> (&'static str, ItemSpec) {
    (name, ItemSpec::new(id, ItemDescriptor::Http))
}

fn check_agent<A>(specs: Vec<(&'static str, ItemSpec)>)
where
    A: AgentLaneModel + Default,
{
    let agent = A::default();
    let expected = specs.into_iter().collect::<HashMap<_, _>>();

    assert_eq!(A::item_specs(), expected);

    for (name, ItemSpec { descriptor, .. }) in expected {
        match descriptor {
            ItemDescriptor::WarpLane { kind, .. } => {
                if kind.map_like() {
                    assert!(agent.on_map_command(name, MapMessage::Clear).is_some());
                    assert!(agent.on_sync(name, SYNC_ID).is_some());
                } else {
                    assert!(agent.on_value_command(name, get_i32_buffer(4)).is_some());
                    assert!(agent.on_sync(name, SYNC_ID).is_some());
                }
            }
            ItemDescriptor::Store { .. } => {
                assert!(agent.on_map_command(name, MapMessage::Clear).is_none());
                assert!(agent.on_sync(name, SYNC_ID).is_none());
                assert!(agent.on_value_command(name, get_i32_buffer(4)).is_none());
                assert!(agent.on_sync(name, SYNC_ID).is_none());
            }
            ItemDescriptor::Http => {
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

    check_agent::<SingleValueLane>(vec![persistent_lane(0, "lane", WarpLaneKind::Value)]);
}

#[test]
fn single_value_store() {
    #[derive(AgentLaneModel)]
    struct SingleValueStore {
        store: ValueStore<i32>,
    }

    check_agent::<SingleValueStore>(vec![persistent_store(0, "store", StoreKind::Value)]);
}

#[test]
fn single_map_lane() {
    #[derive(AgentLaneModel)]
    struct SingleMapLane {
        lane: MapLane<i32, i32>,
    }

    check_agent::<SingleMapLane>(vec![persistent_lane(0, "lane", WarpLaneKind::Map)]);
}

#[test]
fn single_map_store() {
    #[derive(AgentLaneModel)]
    struct SingleMapStore {
        store: MapStore<i32, i32>,
    }

    check_agent::<SingleMapStore>(vec![persistent_store(0, "store", StoreKind::Map)]);
}

#[test]
fn single_command_lane() {
    #[derive(AgentLaneModel)]
    struct SingleCommandLane {
        lane: CommandLane<i32>,
    }

    check_agent::<SingleCommandLane>(vec![transient_lane(0, "lane", WarpLaneKind::Command)]);
}

#[test]
fn single_demand_lane() {
    #[derive(AgentLaneModel)]
    struct SingleDemandLane {
        lane: DemandLane<i32>,
    }

    check_agent::<SingleDemandLane>(vec![transient_lane(0, "lane", WarpLaneKind::Demand)]);
}

#[test]
fn single_demand_map_lane() {
    #[derive(AgentLaneModel)]
    struct SingleDemandMapLane {
        lane: DemandMapLane<i32, i32>,
    }

    check_agent::<SingleDemandMapLane>(vec![transient_lane(0, "lane", WarpLaneKind::DemandMap)]);
}

#[test]
fn two_value_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoValueLanes {
        first: ValueLane<i32>,
        second: ValueLane<i32>,
    }

    check_agent::<TwoValueLanes>(vec![
        persistent_lane(0, "first", WarpLaneKind::Value),
        persistent_lane(1, "second", WarpLaneKind::Value),
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
        persistent_store(0, "first", StoreKind::Value),
        persistent_store(1, "second", StoreKind::Value),
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
        persistent_lane(0, "first", WarpLaneKind::Map),
        persistent_lane(1, "second", WarpLaneKind::Map),
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
        persistent_store(0, "first", StoreKind::Map),
        persistent_store(1, "second", StoreKind::Map),
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
        transient_lane(0, "first", WarpLaneKind::Command),
        transient_lane(1, "second", WarpLaneKind::Command),
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
        transient_lane(0, "first", WarpLaneKind::Demand),
        transient_lane(1, "second", WarpLaneKind::Demand),
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
        transient_lane(0, "first", WarpLaneKind::DemandMap),
        transient_lane(1, "second", WarpLaneKind::DemandMap),
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
        persistent_lane(0, "first", WarpLaneKind::Value),
        persistent_lane(1, "second", WarpLaneKind::Map),
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
        persistent_store(0, "first", StoreKind::Value),
        persistent_store(1, "second", StoreKind::Map),
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
        persistent_lane(0, "first", WarpLaneKind::Value),
        persistent_lane(2, "third", WarpLaneKind::Value),
        transient_lane(4, "fifth", WarpLaneKind::Command),
        transient_lane(6, "seventh", WarpLaneKind::Demand),
        persistent_lane(1, "second", WarpLaneKind::Map),
        persistent_lane(3, "fourth", WarpLaneKind::Map),
        persistent_lane(5, "sixth", WarpLaneKind::JoinValue),
        transient_lane(7, "eighth", WarpLaneKind::DemandMap),
        http_lane(8, "ninth"),
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
        persistent_store(0, "first", StoreKind::Value),
        persistent_lane(1, "second", WarpLaneKind::Value),
        persistent_store(2, "third", StoreKind::Map),
        persistent_lane(3, "fourth", WarpLaneKind::Map),
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
        transient_lane(0, "first", WarpLaneKind::Value),
        persistent_lane(1, "second", WarpLaneKind::Value),
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
        transient_store(0, "first", StoreKind::Value),
        persistent_store(1, "second", StoreKind::Value),
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
        persistent_lane(0, "first", WarpLaneKind::Map),
        transient_lane(1, "second", WarpLaneKind::Map),
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
        persistent_store(0, "first", StoreKind::Map),
        transient_store(1, "second", StoreKind::Map),
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
        transient_lane(0, "first", WarpLaneKind::Command),
        transient_lane(1, "second", WarpLaneKind::Command),
    ]);
}

#[test]
fn demand_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoDemandLanes {
        #[transient]
        first: DemandLane<i32>,
        second: DemandLane<i32>,
    }

    check_agent::<TwoDemandLanes>(vec![
        transient_lane(0, "first", WarpLaneKind::Demand),
        transient_lane(1, "second", WarpLaneKind::Demand),
    ]);
}

#[test]
fn demand_map_lane_tagged_transient() {
    #[derive(AgentLaneModel)]
    struct TwoDemandMapLanes {
        #[transient]
        first: DemandMapLane<i32, i32>,
        second: DemandMapLane<i32, i32>,
    }

    check_agent::<TwoDemandMapLanes>(vec![
        transient_lane(0, "first", WarpLaneKind::DemandMap),
        transient_lane(1, "second", WarpLaneKind::DemandMap),
    ]);
}

#[test]
fn single_join_value_lane() {
    #[derive(AgentLaneModel)]
    struct SingleJoinValueLane {
        lane: JoinValueLane<i32, i32>,
    }

    check_agent::<SingleJoinValueLane>(vec![persistent_lane(0, "lane", WarpLaneKind::JoinValue)]);
}

#[test]
fn two_join_value_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoJoinValueLanes {
        first: JoinValueLane<i32, i32>,
        second: JoinValueLane<i32, i32>,
    }

    check_agent::<TwoJoinValueLanes>(vec![
        persistent_lane(0, "first", WarpLaneKind::JoinValue),
        persistent_lane(1, "second", WarpLaneKind::JoinValue),
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
        persistent_lane(0, "first", WarpLaneKind::JoinValue),
        transient_lane(1, "second", WarpLaneKind::JoinValue),
    ]);
}

#[test]
fn single_simple_http_lane() {
    #[derive(AgentLaneModel)]
    struct SingleSimpleHttpLane {
        lane: SimpleHttpLane<i32>,
    }

    check_agent::<SingleSimpleHttpLane>(vec![http_lane(0, "lane")]);
}

#[test]
fn single_simple_http_lane_explicit_codec() {
    #[derive(AgentLaneModel)]
    struct SingleSimpleHttpLane {
        lane: SimpleHttpLane<i32, Recon>,
    }

    check_agent::<SingleSimpleHttpLane>(vec![http_lane(0, "lane")]);
}

#[test]
fn two_simple_http_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoSimpleHttpLanes {
        first: SimpleHttpLane<i32>,
        second: SimpleHttpLane<i32, Recon>,
    }

    check_agent::<TwoSimpleHttpLanes>(vec![http_lane(0, "first"), http_lane(1, "second")]);
}

#[test]
fn get_and_post_http_lane() {
    #[derive(AgentLaneModel)]
    struct GetAndPostHttpLane {
        lane: HttpLane<i32, String>,
    }

    check_agent::<GetAndPostHttpLane>(vec![http_lane(0, "lane")]);
}

#[test]
fn get_post_and_put_http_lane() {
    #[derive(AgentLaneModel)]
    struct GetPostAndPutHttpLane {
        lane: HttpLane<i32, String, i32>,
    }

    check_agent::<GetPostAndPutHttpLane>(vec![http_lane(0, "lane")]);
}

#[test]
fn general_http_lane_explicit_codec() {
    #[derive(AgentLaneModel)]
    struct GeneralHttpLane {
        lane: HttpLane<i32, String, i32, Recon>,
    }

    check_agent::<GeneralHttpLane>(vec![http_lane(0, "lane")]);
}

#[test]
fn two_general_http_lanes() {
    #[derive(AgentLaneModel)]
    struct TwoGeneralHttpLanes {
        first: HttpLane<i32, i32>,
        second: HttpLane<i32, String, i32>,
    }

    check_agent::<TwoGeneralHttpLanes>(vec![http_lane(0, "first"), http_lane(1, "second")]);
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
            persistent_lane(0, "first", WarpLaneKind::Value),
            persistent_lane(2, "third", WarpLaneKind::Value),
            transient_lane(4, "fifth", WarpLaneKind::Command),
            persistent_store(5, "sixth", StoreKind::Value),
            persistent_lane(1, "second", WarpLaneKind::Map),
            persistent_lane(3, "fourth", WarpLaneKind::Map),
            persistent_store(6, "seventh", StoreKind::Map),
            persistent_lane(7, "eighth", WarpLaneKind::JoinValue),
            transient_lane(8, "ninth", WarpLaneKind::Demand),
            transient_lane(9, "tenth", WarpLaneKind::DemandMap),
            http_lane(10, "eleventh"),
            http_lane(11, "twelfth"),
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

    check_agent::<First>(vec![persistent_lane(0, "lane", WarpLaneKind::Value)]);
    check_agent::<Second>(vec![persistent_lane(0, "lane", WarpLaneKind::Value)]);
}
