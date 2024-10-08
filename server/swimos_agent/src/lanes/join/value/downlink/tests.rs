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

use std::{cell::RefCell, collections::HashMap};

use bytes::BytesMut;

use swimos_api::{address::Address, agent::AgentConfig};
use swimos_model::Text;
use swimos_utilities::routing::RouteUri;

use crate::event_handler::LocalBoxHandlerAction;
use crate::lanes::join_value::Link;
use crate::{
    agent_lifecycle::HandlerContext,
    downlink_lifecycle::{OnConsumeEvent, OnFailed, OnLinked, OnSynced, OnUnlinked},
    event_handler::{
        EventHandler, HandlerActionExt, LocalBoxEventHandler, Modification, ModificationFlags,
        SideEffect, StepResult,
    },
    lanes::{
        join::LinkClosedResponse,
        join_value::{
            lifecycle::{
                on_failed::OnJoinValueFailed, on_linked::OnJoinValueLinked,
                on_synced::OnJoinValueSynced, on_unlinked::OnJoinValueUnlinked,
            },
            DownlinkStatus,
        },
        JoinValueLane,
    },
    meta::AgentMetadata,
    test_context::dummy_context,
};

use super::JoinValueDownlink;

struct TestAgent {
    lane: JoinValueLane<i32, String>,
}

const ID: u64 = 12;

impl Default for TestAgent {
    fn default() -> Self {
        Self {
            lane: JoinValueLane::new(ID),
        }
    }
}

impl TestAgent {
    const LANE: fn(&TestAgent) -> &JoinValueLane<i32, String> = |agent| &agent.lane;
}

#[derive(Debug)]
enum Event {
    Linked {
        key: i32,
        remote: Address<Text>,
    },
    Synced {
        key: i32,
        remote: Address<Text>,
        value: Option<String>,
    },
    Unlinked {
        key: i32,
        remote: Address<Text>,
    },
    Failed {
        key: i32,
        remote: Address<Text>,
    },
}

struct TestLifecycle {
    inner: RefCell<Vec<Event>>,
    response: LinkClosedResponse,
}

impl TestLifecycle {
    pub fn new(response: LinkClosedResponse) -> Self {
        TestLifecycle {
            inner: Default::default(),
            response,
        }
    }

    pub fn push(&self, event: Event) {
        self.inner.borrow_mut().push(event);
    }

    pub fn take(&self) -> Vec<Event> {
        let mut guard = self.inner.borrow_mut();
        std::mem::take(&mut *guard)
    }
}

impl OnJoinValueLinked<i32, TestAgent> for TestLifecycle {
    type OnJoinValueLinkedHandler<'a> = LocalBoxEventHandler<'a, TestAgent>
    where
        Self: 'a;

    fn on_linked<'a>(
        &'a self,
        key: i32,
        remote: Address<&str>,
    ) -> Self::OnJoinValueLinkedHandler<'a> {
        let address = remote.to_text();
        SideEffect::from(move || {
            self.push(Event::Linked {
                key,
                remote: address,
            });
        })
        .boxed_local()
    }
}

impl OnJoinValueSynced<i32, String, TestAgent> for TestLifecycle {
    type OnJoinValueSyncedHandler<'a> = LocalBoxEventHandler<'a, TestAgent>
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        key: i32,
        remote: Address<&str>,
        value: Option<&String>,
    ) -> Self::OnJoinValueSyncedHandler<'a> {
        let address = remote.to_text();
        let value = value.cloned();
        SideEffect::from(move || {
            self.push(Event::Synced {
                key,
                remote: address,
                value,
            });
        })
        .boxed_local()
    }
}

impl OnJoinValueUnlinked<i32, TestAgent> for TestLifecycle {
    type OnJoinValueUnlinkedHandler<'a> = LocalBoxHandlerAction<'a, TestAgent, LinkClosedResponse>
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        key: i32,
        remote: Address<&str>,
    ) -> Self::OnJoinValueUnlinkedHandler<'a> {
        let response = self.response;
        let address = remote.to_text();
        let context: HandlerContext<TestAgent> = Default::default();
        context
            .effect(move || {
                self.push(Event::Unlinked {
                    key,
                    remote: address,
                });
            })
            .map(move |_| response)
            .boxed_local()
    }
}

impl OnJoinValueFailed<i32, TestAgent> for TestLifecycle {
    type OnJoinValueFailedHandler<'a> = LocalBoxHandlerAction<'a, TestAgent, LinkClosedResponse>
    where
        Self: 'a;

    fn on_failed<'a>(
        &'a self,
        key: i32,
        remote: Address<&str>,
    ) -> Self::OnJoinValueFailedHandler<'a> {
        let response = self.response;
        let address = remote.to_text();
        let context: HandlerContext<TestAgent> = Default::default();
        context
            .effect(move || {
                self.push(Event::Failed {
                    key,
                    remote: address,
                });
            })
            .map(move |_| response)
            .boxed_local()
    }
}

const NODE: &str = "node";
const LANE: &str = "lane";

fn make_address() -> Address<Text> {
    Address::text(None, NODE, LANE)
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

fn run_handler<H>(mut handler: H, meta: AgentMetadata<'_>, agent: &TestAgent) -> Vec<Modification>
where
    H: EventHandler<TestAgent>,
{
    let mut modifications = vec![];
    loop {
        match handler.step(
            &mut dummy_context(&mut HashMap::new(), &mut BytesMut::new()),
            meta,
            agent,
        ) {
            StepResult::Continue {
                modified_item: Some(modified),
            } => modifications.push(modified),
            StepResult::Fail(err) => panic!("Handler failed: {:?}", err),
            StepResult::Complete { modified_item, .. } => {
                if let Some(modified) = modified_item {
                    modifications.push(modified)
                }
                break modifications;
            }
            _ => {}
        }
    }
}

fn state_for(lane: &JoinValueLane<i32, String>, key: i32) -> Option<DownlinkStatus> {
    let JoinValueLane { keys, .. } = lane;
    let guard = keys.borrow();
    guard.get(&key).map(|link| link.status)
}

fn set_state_for(
    lane: &JoinValueLane<i32, String>,
    key: i32,
    status: DownlinkStatus,
    value: Option<String>,
) {
    let JoinValueLane { inner, keys } = lane;
    let mut guard = keys.borrow_mut();
    guard.insert(key, Link::new(status));
    if let Some(value) = value {
        inner.update(key, value);
    } else {
        inner.remove(&key);
    }
}

#[test]
fn run_on_linked() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinValueDownlink::new(TestAgent::LANE, 4, make_address(), lifecycle);

    let on_linked = downlink_lifecycle.on_linked();
    assert!(run_handler(on_linked, meta, &agent).is_empty());
    let events = downlink_lifecycle.lifecycle.take();
    if let [Event::Linked { key, remote }] = events.as_slice() {
        assert_eq!(*key, 4);
        assert_eq!(remote, &make_address());
    } else {
        panic!("Events incorrect: {:?}", events);
    }
    let state = state_for(&agent.lane, 4);
    assert_eq!(state, Some(DownlinkStatus::Linked));
}

#[test]
fn run_on_event() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinValueDownlink::new(TestAgent::LANE, 4, make_address(), lifecycle);

    let on_event = downlink_lifecycle.on_event("a".to_string());
    let modifications = run_handler(on_event, meta, &agent);

    if let [Modification { item_id, flags }] = modifications.as_slice() {
        assert_eq!(*item_id, ID);
        assert_eq!(*flags, ModificationFlags::all());
    } else {
        panic!("Modifications incorrect: {:?}", modifications);
    }

    let events = downlink_lifecycle.lifecycle.take();
    assert!(events.is_empty());

    let value = agent.lane.get(&4, |v| v.cloned());
    assert_eq!(value, Some("a".to_string()));
}

#[test]
fn run_on_unlinked_abandon() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    set_state_for(
        &agent.lane,
        4,
        DownlinkStatus::Linked,
        Some("a".to_string()),
    );
    let lifecycle = TestLifecycle::new(LinkClosedResponse::Abandon);
    let downlink_lifecycle = JoinValueDownlink::new(TestAgent::LANE, 4, make_address(), lifecycle);

    let on_unlinked = downlink_lifecycle.on_unlinked();
    assert!(run_handler(on_unlinked, meta, &agent).is_empty());
    let events = downlink_lifecycle.lifecycle.take();
    if let [Event::Unlinked { key, remote }] = events.as_slice() {
        assert_eq!(*key, 4);
        assert_eq!(remote, &make_address());
    } else {
        panic!("Events incorrect: {:?}", events);
    }
    let state = state_for(&agent.lane, 4);
    assert!(state.is_none());

    let value = agent.lane.get(&4, |v| v.cloned());
    assert_eq!(value, Some("a".to_string()));
}

#[test]
fn run_on_unlinked_delete() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    set_state_for(
        &agent.lane,
        4,
        DownlinkStatus::Linked,
        Some("a".to_string()),
    );
    let lifecycle = TestLifecycle::new(LinkClosedResponse::Delete);
    let downlink_lifecycle = JoinValueDownlink::new(TestAgent::LANE, 4, make_address(), lifecycle);

    let on_unlinked = downlink_lifecycle.on_unlinked();
    let modifications = run_handler(on_unlinked, meta, &agent);

    if let [Modification { item_id, flags }] = modifications.as_slice() {
        assert_eq!(*item_id, ID);
        assert_eq!(*flags, ModificationFlags::all());
    } else {
        panic!("Modifications incorrect: {:?}", modifications);
    }

    let events = downlink_lifecycle.lifecycle.take();
    if let [Event::Unlinked { key, remote }] = events.as_slice() {
        assert_eq!(*key, 4);
        assert_eq!(remote, &make_address());
    } else {
        panic!("Events incorrect: {:?}", events);
    }
    let state = state_for(&agent.lane, 4);
    assert!(state.is_none());

    let value = agent.lane.get(&4, |v| v.cloned());
    assert!(value.is_none());
}

#[test]
fn run_on_failed_abandon() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    set_state_for(
        &agent.lane,
        4,
        DownlinkStatus::Linked,
        Some("a".to_string()),
    );
    let lifecycle = TestLifecycle::new(LinkClosedResponse::Abandon);
    let downlink_lifecycle = JoinValueDownlink::new(TestAgent::LANE, 4, make_address(), lifecycle);

    let on_failed = downlink_lifecycle.on_failed();
    assert!(run_handler(on_failed, meta, &agent).is_empty());
    let events = downlink_lifecycle.lifecycle.take();
    if let [Event::Failed { key, remote }] = events.as_slice() {
        assert_eq!(*key, 4);
        assert_eq!(remote, &make_address());
    } else {
        panic!("Events incorrect: {:?}", events);
    }
    let state = state_for(&agent.lane, 4);
    assert!(state.is_none());

    let value = agent.lane.get(&4, |v| v.cloned());
    assert_eq!(value, Some("a".to_string()));
}

#[test]
#[ignore]
fn run_on_failed_delete() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    set_state_for(
        &agent.lane,
        4,
        DownlinkStatus::Linked,
        Some("a".to_string()),
    );
    let lifecycle = TestLifecycle::new(LinkClosedResponse::Delete);
    let downlink_lifecycle = JoinValueDownlink::new(TestAgent::LANE, 4, make_address(), lifecycle);

    let on_failed = downlink_lifecycle.on_failed();
    let modifications = run_handler(on_failed, meta, &agent);

    if let [Modification { item_id, flags }] = modifications.as_slice() {
        assert_eq!(*item_id, ID);
        assert_eq!(*flags, ModificationFlags::all());
    } else {
        panic!("Modifications incorrect: {:?}", modifications);
    }

    let events = downlink_lifecycle.lifecycle.take();
    if let [Event::Failed { key, remote }] = events.as_slice() {
        assert_eq!(*key, 4);
        assert_eq!(remote, &make_address());
    } else {
        panic!("Events incorrect: {:?}", events);
    }
    let state = state_for(&agent.lane, 4);
    assert!(state.is_none());

    let value = agent.lane.get(&4, |v| v.cloned());
    assert!(value.is_none());
}

#[test]
fn run_on_synced() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    set_state_for(
        &agent.lane,
        4,
        DownlinkStatus::Linked,
        Some("a".to_string()),
    );
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinValueDownlink::new(TestAgent::LANE, 4, make_address(), lifecycle);

    let on_synced = downlink_lifecycle.on_synced(&());
    assert!(run_handler(on_synced, meta, &agent).is_empty());
    let events = downlink_lifecycle.lifecycle.take();
    if let [Event::Synced { key, remote, value }] = events.as_slice() {
        assert_eq!(*key, 4);
        assert_eq!(remote, &make_address());
        assert_eq!(*value, Some("a".to_string()));
    } else {
        panic!("Events incorrect: {:?}", events);
    }
}
