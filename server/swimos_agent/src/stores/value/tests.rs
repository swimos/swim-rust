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

use bytes::BytesMut;
use std::{collections::HashMap, fmt::Debug};
use swimos_api::{
    agent::AgentConfig,
    protocol::agent::{StoreResponse, ValueStoreResponseDecoder},
};
use swimos_utilities::routing::route_uri::RouteUri;
use tokio_util::codec::Decoder;

use crate::{
    agent_model::WriteResult,
    event_handler::{EventHandlerError, HandlerAction, Modification, StepResult},
    meta::AgentMetadata,
    stores::{
        value::{ValueStore, ValueStoreGet, ValueStoreSet, ValueStoreWithValue},
        StoreItem,
    },
    test_context::dummy_context,
};

const ID: u64 = 66;

#[test]
fn value_store_not_dirty_initially() {
    let store = ValueStore::new(ID, 123);

    assert!(!store.has_data_to_write());
}

#[test]
fn read_from_value_store() {
    let store = ValueStore::new(ID, 123);

    let result = store.read(|n| *n);

    assert_eq!(result, 123);
}

#[test]
fn read_from_value_store_with_no_prev() {
    let store = ValueStore::new(ID, 123);

    let (prev, result) = store.read_with_prev(|prev, n| (prev, *n));

    assert!(prev.is_none());
    assert_eq!(result, 123);
}

#[test]
fn write_to_value_store() {
    let store = ValueStore::new(ID, 123);

    store.set(89);
    assert!(store.has_data_to_write());
    assert_eq!(store.read(|n| *n), 89);
    assert_eq!(store.read_with_prev(|prev, n| (prev, *n)), (Some(123), 89));
}

#[test]
fn value_store_write_to_buffer_not_dirty() {
    let store = ValueStore::new(ID, 123);
    let mut buffer = BytesMut::new();

    let result = store.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::NoData);
    assert!(buffer.is_empty());
}

#[test]
fn value_store_write_to_buffer_dirty() {
    let store = ValueStore::new(ID, 123);
    store.set(6373);
    let mut buffer = BytesMut::new();

    let result = store.write_to_buffer(&mut buffer);
    assert_eq!(result, WriteResult::Done);
    assert!(!store.has_data_to_write());

    let mut decoder = ValueStoreResponseDecoder::default();
    let content = decoder
        .decode(&mut buffer)
        .expect("Invalid frame.")
        .expect("Incomplete frame.");

    let StoreResponse { message } = content;
    assert_eq!(message.as_ref(), b"6373");
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

struct TestAgent {
    store: ValueStore<i32>,
    str_store: ValueStore<String>,
}

const STORE_ID: u64 = 9;
const STR_STORE_ID: u64 = 3;

impl Default for TestAgent {
    fn default() -> Self {
        Self {
            store: ValueStore::new(STORE_ID, 0),
            str_store: ValueStore::new(STR_STORE_ID, "world".to_owned()),
        }
    }
}

impl TestAgent {
    const STORE: fn(&TestAgent) -> &ValueStore<i32> = |agent| &agent.store;
    const STR_STORE: fn(&TestAgent) -> &ValueStore<String> = |agent| &agent.str_store;
}

fn check_result_for<T: Eq + Debug>(
    store_id: u64,
    result: StepResult<T>,
    written: bool,
    trigger_handler: bool,
    complete: Option<T>,
) {
    let expected_mod = if written {
        if trigger_handler {
            Some(Modification::of(store_id))
        } else {
            Some(Modification::no_trigger(store_id))
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

fn check_result<T: Eq + Debug>(
    result: StepResult<T>,
    written: bool,
    trigger_handler: bool,
    complete: Option<T>,
) {
    check_result_for(STORE_ID, result, written, trigger_handler, complete)
}

#[test]
fn value_store_set_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();

    let mut handler = ValueStoreSet::new(TestAgent::STORE, 84);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, true, true, Some(()));

    assert!(agent.store.has_data_to_write());
    assert_eq!(agent.store.read(|n| *n), 84);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn value_store_get_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();

    let mut handler = ValueStoreGet::new(TestAgent::STORE);

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, false, false, Some(0));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}

#[test]
fn value_store_with_value_event_handler() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();

    let mut handler = ValueStoreWithValue::new(TestAgent::STR_STORE, |s: &str| s.len());

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    check_result(result, false, false, Some(5));

    let result = handler.step(
        &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
        meta,
        &agent,
    );
    assert!(matches!(
        result,
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    ));
}
