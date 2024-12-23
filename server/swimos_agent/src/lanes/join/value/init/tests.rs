// Copyright 2015-2024 Swim Inc.
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

use std::{
    any::TypeId,
    borrow::Cow,
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use bytes::BytesMut;
use swimos_api::{
    address::Address,
    agent::{AgentConfig, DownlinkKind},
};
use swimos_utilities::routing::RouteUri;

use crate::{
    agent_model::AgentDescription,
    event_handler::{DowncastError, JoinLaneInitializer},
    lanes::{join_value::default_lifecycle::DefaultJoinValueLifecycle, JoinValueLane},
    meta::AgentMetadata,
    test_context::run_with_futures,
    test_util::TestDownlinkContext,
};

use super::LifecycleInitializer;

struct TestAgent {
    lane: JoinValueLane<i32, String>,
}

impl AgentDescription for TestAgent {
    fn item_name(&self, id: u64) -> Option<Cow<'_, str>> {
        if id == ID {
            Some(Cow::Borrowed("lane"))
        } else {
            None
        }
    }
}

impl TestAgent {
    const LANE: fn(&TestAgent) -> &JoinValueLane<i32, String> = |agent| &agent.lane;
}
const ID: u64 = 1;

impl Default for TestAgent {
    fn default() -> Self {
        Self {
            lane: JoinValueLane::new(ID),
        }
    }
}

const NODE: &str = "/node";
const LANE: &str = "lane";

#[test]
fn bad_key_type() {
    let init = LifecycleInitializer::new(TestAgent::LANE, || DefaultJoinValueLifecycle);

    let address = Address::text(None, NODE, LANE);

    let result = init.try_create_action(
        Box::new("a".to_string()),
        TypeId::of::<String>(),
        TypeId::of::<String>(),
        address,
    );

    match result.err().expect("Expected failure.") {
        DowncastError::LinkKey { key, expected_type } => {
            assert_eq!(
                key.downcast_ref::<String>().expect("Key should be string."),
                "a"
            );
            assert_eq!(expected_type, TypeId::of::<i32>());
        }
        _ => panic!("Incorrect error kind."),
    }
}

#[test]
fn bad_value_type() {
    let init = LifecycleInitializer::new(TestAgent::LANE, || DefaultJoinValueLifecycle);

    let address = Address::text(None, NODE, LANE);

    let result = init.try_create_action(
        Box::new(1i32),
        TypeId::of::<i32>(),
        TypeId::of::<i32>(),
        address,
    );

    match result.err().expect("Expected failure.") {
        DowncastError::Value {
            actual_type,
            expected_type,
        } => {
            assert_eq!(actual_type, TypeId::of::<i32>());
            assert_eq!(expected_type, TypeId::of::<String>());
        }
        _ => panic!("Incorrect error kind."),
    }
}

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
}

#[tokio::test]
async fn successfully_create_action() {
    let count = Arc::new(AtomicUsize::new(0));
    let count_cpy = count.clone();
    let init = LifecycleInitializer::new(TestAgent::LANE, move || {
        count_cpy.fetch_add(1, Ordering::Relaxed);
        DefaultJoinValueLifecycle
    });

    let address = Address::text(None, NODE, LANE);

    let result = init.try_create_action(
        Box::new(1i32),
        TypeId::of::<i32>(),
        TypeId::of::<String>(),
        address.clone(),
    );

    let handler = result.expect("Should succeed.");

    let context = TestDownlinkContext::default();

    let agent = TestAgent::default();
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let mut inits = HashMap::new();
    let mut command_buffer = BytesMut::new();

    run_with_futures(
        &context,
        &agent,
        meta,
        &mut inits,
        &mut command_buffer,
        handler,
    )
    .await;
    assert_eq!(count.load(Ordering::Relaxed), 1);

    let channels = context.take_channels();
    assert_eq!(channels.len(), 1);
    assert!(channels.contains_key(&address));

    let downlinks = context.take_downlinks();
    match downlinks.as_slice() {
        [downlink] => {
            assert_eq!(downlink.kind(), DownlinkKind::Event);
        }
        _ => panic!("Incorrect number of downlinks."),
    }
}
