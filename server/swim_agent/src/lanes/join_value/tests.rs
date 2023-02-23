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

use std::{collections::HashMap, fmt::Debug};

use swim_api::agent::AgentConfig;
use swim_utilities::routing::route_uri::RouteUri;

use crate::{
    event_handler::{EventHandlerError, HandlerAction, Modification, StepResult},
    item::MapItem,
    lanes::join_value::{JoinValueLaneGet, JoinValueLaneGetMap},
    meta::AgentMetadata,
    test_context::dummy_context,
};

use super::JoinValueLane;

const ID: u64 = 857383;

const K1: i32 = 5;
const K2: i32 = 78;
const K3: i32 = -4;

const ABSENT: i32 = 93;

const V1: &str = "first";
const V2: &str = "second";
const V3: &str = "third";

fn init() -> HashMap<i32, String> {
    [(K1, V1), (K2, V2), (K3, V3)]
        .into_iter()
        .map(|(k, v)| (k, v.to_string()))
        .collect()
}

fn make_lane(contents: impl IntoIterator<Item = (i32, String)>) -> JoinValueLane<i32, String> {
    let lane = JoinValueLane::new(ID);
    lane.init(contents.into_iter().collect());
    lane
}

#[test]
fn get_from_join_value_lane() {
    let lane = make_lane(init());

    let value = lane.get(&K1, |v| v.cloned());
    assert_eq!(value, Some(V1.to_string()));

    let value = lane.get(&ABSENT, |v| v.cloned());
    assert!(value.is_none());
}

#[test]
fn get_join_value_lane() {
    let lane = make_lane(init());

    let value = lane.get_map(Clone::clone);
    assert_eq!(value, init());
}

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta(uri: &RouteUri) -> AgentMetadata<'_> {
    AgentMetadata::new(uri, &CONFIG)
}

struct TestAgent {
    lane: JoinValueLane<i32, String>,
}

impl TestAgent {
    pub const LANE: fn(&TestAgent) -> &JoinValueLane<i32, String> = |agent| &agent.lane;

    fn with_init() -> Self {
        TestAgent {
            lane: make_lane(init()),
        }
    }
}

fn check_result<T: Eq + Debug>(
    result: StepResult<T>,
    written: bool,
    trigger_handler: bool,
    complete: Option<T>,
) {
    let expected_mod = if written {
        if trigger_handler {
            Some(Modification::of(ID))
        } else {
            Some(Modification::no_trigger(ID))
        }
    } else {
        None
    };
    match (result, complete) {
        (
            StepResult::Complete {
                modified_item,
                result,
            },
            Some(expected),
        ) => {
            assert_eq!(modified_item, expected_mod);
            assert_eq!(result, expected);
        }
        (StepResult::Continue { modified_item }, None) => {
            assert_eq!(modified_item, expected_mod);
        }
        ow => {
            panic!("Unexpected result: {:?}", ow);
        }
    }
}

#[test]
fn join_value_lane_get_event_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let agent = TestAgent::with_init();

    let mut handler = JoinValueLaneGet::new(TestAgent::LANE, K1);

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    check_result(result, false, false, Some(Some(V1.to_string())));

    let mut handler = JoinValueLaneGet::new(TestAgent::LANE, ABSENT);

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    check_result(result, false, false, Some(None));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn map_lane_get_map_event_handler() {
    let uri = make_uri();
    let meta = make_meta(&uri);
    let agent = TestAgent::with_init();

    let mut handler = JoinValueLaneGetMap::new(TestAgent::LANE);

    let expected = init();

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    check_result(result, false, false, Some(expected));

    let result = handler.step(&mut dummy_context(&mut HashMap::new()), meta, &agent);
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}
