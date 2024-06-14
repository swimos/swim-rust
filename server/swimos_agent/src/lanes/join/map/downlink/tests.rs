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

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
};

use bytes::BytesMut;
use swimos_agent_protocol::MapMessage;
use swimos_api::{address::Address, agent::AgentConfig};
use swimos_model::Text;
use swimos_utilities::routing::RouteUri;

use crate::{
    agent_lifecycle::HandlerContext,
    downlink_lifecycle::{OnConsumeEvent, OnFailed, OnLinked, OnSynced, OnUnlinked},
    event_handler::{
        BoxEventHandler, BoxHandlerAction, EventHandler, HandlerActionExt, Modification,
        ModificationFlags, SideEffect, StepResult,
    },
    lanes::{
        join::DownlinkStatus,
        join_map::{
            downlink::JoinMapDownlink,
            lifecycle::{
                on_failed::OnJoinMapFailed, on_linked::OnJoinMapLinked, on_synced::OnJoinMapSynced,
                on_unlinked::OnJoinMapUnlinked,
            },
            Link,
        },
        JoinMapLane, LinkClosedResponse,
    },
    meta::AgentMetadata,
    test_context::dummy_context,
};

struct TestAgent {
    lane: JoinMapLane<String, i32, String>,
}

const ID: u64 = 12;

impl Default for TestAgent {
    fn default() -> Self {
        Self {
            lane: JoinMapLane::new(ID),
        }
    }
}

impl TestAgent {
    const LANE: fn(&TestAgent) -> &JoinMapLane<String, i32, String> = |agent| &agent.lane;
}

#[derive(Debug)]
enum Event {
    Linked {
        link_key: String,
        remote: Address<Text>,
    },
    Synced {
        link_key: String,
        remote: Address<Text>,
        keys: HashSet<i32>,
    },
    Unlinked {
        link_key: String,
        remote: Address<Text>,
        keys: HashSet<i32>,
    },
    Failed {
        link_key: String,
        remote: Address<Text>,
        keys: HashSet<i32>,
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

impl OnJoinMapLinked<String, TestAgent> for TestLifecycle {
    type OnJoinMapLinkedHandler<'a> = BoxEventHandler<'a, TestAgent>
    where
        Self: 'a;

    fn on_linked<'a>(
        &'a self,
        link_key: String,
        remote: Address<&str>,
    ) -> Self::OnJoinMapLinkedHandler<'a> {
        let address = remote.to_text();
        SideEffect::from(move || {
            self.push(Event::Linked {
                link_key,
                remote: address,
            });
        })
        .boxed()
    }
}

impl OnJoinMapSynced<String, i32, TestAgent> for TestLifecycle {
    type OnJoinMapSyncedHandler<'a> = BoxEventHandler<'a, TestAgent>
    where
        Self: 'a;

    fn on_synced<'a>(
        &'a self,
        link_key: String,
        remote: Address<&str>,
        keys: &HashSet<i32>,
    ) -> Self::OnJoinMapSyncedHandler<'a> {
        let address = remote.to_text();
        let keys = keys.clone();
        SideEffect::from(move || {
            self.push(Event::Synced {
                link_key,
                remote: address,
                keys,
            });
        })
        .boxed()
    }
}

impl OnJoinMapUnlinked<String, i32, TestAgent> for TestLifecycle {
    type OnJoinMapUnlinkedHandler<'a> = BoxHandlerAction<'a, TestAgent, LinkClosedResponse>
    where
        Self: 'a;

    fn on_unlinked<'a>(
        &'a self,
        link_key: String,
        remote: Address<&str>,
        keys: HashSet<i32>,
    ) -> Self::OnJoinMapUnlinkedHandler<'a> {
        let response = self.response;
        let address = remote.to_text();
        let context: HandlerContext<TestAgent> = Default::default();
        let keys = keys.clone();
        context
            .effect(move || {
                self.push(Event::Unlinked {
                    link_key,
                    remote: address,
                    keys,
                });
            })
            .map(move |_| response)
            .boxed()
    }
}

impl OnJoinMapFailed<String, i32, TestAgent> for TestLifecycle {
    type OnJoinMapFailedHandler<'a> = BoxHandlerAction<'a, TestAgent, LinkClosedResponse>
    where
        Self: 'a;

    fn on_failed<'a>(
        &'a self,
        link_key: String,
        remote: Address<&str>,
        keys: HashSet<i32>,
    ) -> Self::OnJoinMapFailedHandler<'a> {
        let response = self.response;
        let address = remote.to_text();
        let context: HandlerContext<TestAgent> = Default::default();
        let keys = keys.clone();
        context
            .effect(move || {
                self.push(Event::Failed {
                    link_key,
                    remote: address,
                    keys,
                });
            })
            .map(move |_| response)
            .boxed()
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

fn state_for(
    lane: &JoinMapLane<String, i32, String>,
    link_key: &str,
) -> (Option<Link<i32>>, HashSet<i32>) {
    let JoinMapLane { link_tracker, .. } = lane;
    let guard = link_tracker.borrow();
    let owned = guard
        .ownership
        .iter()
        .filter_map(|(k, v)| if v == link_key { Some(*k) } else { None })
        .collect();
    (guard.links.get(link_key).cloned(), owned)
}

fn set_state_for(
    lane: &JoinMapLane<String, i32, String>,
    link_key: &str,
    status: DownlinkStatus,
    entries: HashMap<i32, String>,
) {
    let JoinMapLane {
        inner,
        link_tracker,
    } = lane;
    let mut guard = link_tracker.borrow_mut();
    let link = Link {
        status,
        ..Default::default()
    };
    if let Some(Link { keys, .. }) = guard.links.insert(link_key.to_string(), link) {
        for k in keys {
            inner.remove(&k);
        }
    }
    for (k, v) in entries.into_iter() {
        guard.insert(link_key.to_string(), k);
        inner.update(k, v);
    }
}

#[test]
fn run_on_linked() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    let on_linked = downlink_lifecycle.on_linked();
    assert!(run_handler(on_linked, meta, &agent).is_empty());
    let events = downlink_lifecycle.lifecycle.take();
    if let [Event::Linked { remote, link_key }] = events.as_slice() {
        assert_eq!(link_key, "link");
        assert_eq!(remote, &make_address());
    } else {
        panic!("Events incorrect: {:?}", events);
    }
    if let (Some(Link { status, keys }), owned) = state_for(&agent.lane, "link") {
        assert_eq!(status, DownlinkStatus::Linked);
        assert!(keys.is_empty());
        assert!(owned.is_empty());
    } else {
        panic!("Incorrect state.");
    }
}

fn upd(k: i32, v: &str) -> MapMessage<i32, String> {
    MapMessage::Update {
        key: k,
        value: v.to_string(),
    }
}

fn rem(k: i32) -> MapMessage<i32, String> {
    MapMessage::Remove { key: k }
}

#[test]
fn run_update_not_linked() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    let on_event = downlink_lifecycle.on_event(upd(1, "a"));
    let modifications = run_handler(on_event, meta, &agent);

    assert!(modifications.is_empty());

    let events = downlink_lifecycle.lifecycle.take();
    assert!(events.is_empty());

    let value = agent.lane.get(&1, |v| v.cloned());
    assert!(value.is_none());
    let (state, owned) = state_for(&agent.lane, "link");
    assert!(state.is_none());
    assert!(owned.is_empty());
}

#[test]
fn run_remove_not_linked() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    let on_event = downlink_lifecycle.on_event(rem(1));
    let modifications = run_handler(on_event, meta, &agent);

    assert!(modifications.is_empty());

    let events = downlink_lifecycle.lifecycle.take();
    assert!(events.is_empty());

    let value = agent.lane.get(&1, |v| v.cloned());
    assert!(value.is_none());
    let (state, owned) = state_for(&agent.lane, "link");
    assert!(state.is_none());
    assert!(owned.is_empty());
}

#[test]
fn run_clear_not_linked() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    let on_event = downlink_lifecycle.on_event(MapMessage::Clear);
    let modifications = run_handler(on_event, meta, &agent);

    assert!(modifications.is_empty());

    let events = downlink_lifecycle.lifecycle.take();
    assert!(events.is_empty());

    let value = agent.lane.get(&1, |v| v.cloned());
    assert!(value.is_none());
    let (state, owned) = state_for(&agent.lane, "link");
    assert!(state.is_none());
    assert!(owned.is_empty());
}

#[test]
fn run_take_not_linked() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    let on_event = downlink_lifecycle.on_event(MapMessage::Take(1));
    let modifications = run_handler(on_event, meta, &agent);

    assert!(modifications.is_empty());

    let events = downlink_lifecycle.lifecycle.take();
    assert!(events.is_empty());

    let value = agent.lane.get(&1, |v| v.cloned());
    assert!(value.is_none());
    let (state, owned) = state_for(&agent.lane, "link");
    assert!(state.is_none());
    assert!(owned.is_empty());
}

#[test]
fn run_drop_not_linked() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    let on_event = downlink_lifecycle.on_event(MapMessage::Drop(1));
    let modifications = run_handler(on_event, meta, &agent);

    assert!(modifications.is_empty());

    let events = downlink_lifecycle.lifecycle.take();
    assert!(events.is_empty());

    let value = agent.lane.get(&1, |v| v.cloned());
    assert!(value.is_none());
    let (state, owned) = state_for(&agent.lane, "link");
    assert!(state.is_none());
    assert!(owned.is_empty());
}

#[test]
fn run_update_linked() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    set_state_for(&agent.lane, "link", DownlinkStatus::Linked, HashMap::new());

    let on_event = downlink_lifecycle.on_event(upd(1, "a"));
    let modifications = run_handler(on_event, meta, &agent);

    if let [Modification { item_id, flags }] = modifications.as_slice() {
        assert_eq!(*item_id, ID);
        assert_eq!(*flags, ModificationFlags::all());
    } else {
        panic!("Modifications incorrect: {:?}", modifications);
    }

    let events = downlink_lifecycle.lifecycle.take();
    assert!(events.is_empty());

    let value = agent.lane.get(&1, |v| v.cloned());
    assert_eq!(value, Some("a".to_string()));
    let (state, owned) = state_for(&agent.lane, "link");
    let expected = [1].into_iter().collect();
    if let Some(Link { status, keys }) = state {
        assert_eq!(status, DownlinkStatus::Linked);
        assert_eq!(keys, expected);
    }
    assert_eq!(owned, expected);
}

#[test]
fn run_remove_linked() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    set_state_for(
        &agent.lane,
        "link",
        DownlinkStatus::Linked,
        [(1, "a".to_string()), (2, "b".to_string())]
            .into_iter()
            .collect(),
    );

    let on_event = downlink_lifecycle.on_event(rem(1));
    let modifications = run_handler(on_event, meta, &agent);

    if let [Modification { item_id, flags }] = modifications.as_slice() {
        assert_eq!(*item_id, ID);
        assert_eq!(*flags, ModificationFlags::all());
    } else {
        panic!("Modifications incorrect: {:?}", modifications);
    }

    let events = downlink_lifecycle.lifecycle.take();
    assert!(events.is_empty());

    let value = agent.lane.get(&1, |v| v.cloned());
    assert!(value.is_none());
    let value = agent.lane.get(&2, |v| v.cloned());
    assert_eq!(value, Some("b".to_string()));

    let (state, owned) = state_for(&agent.lane, "link");
    let expected = [2].into_iter().collect();
    if let Some(Link { status, keys }) = state {
        assert_eq!(status, DownlinkStatus::Linked);
        assert_eq!(keys, expected);
    }
    assert_eq!(owned, expected);
}

#[test]
fn run_clear_linked() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    set_state_for(
        &agent.lane,
        "link",
        DownlinkStatus::Linked,
        [(1, "a".to_string()), (2, "b".to_string())]
            .into_iter()
            .collect(),
    );

    let on_event = downlink_lifecycle.on_event(MapMessage::Clear);
    let modifications = run_handler(on_event, meta, &agent);

    if let [Modification { item_id, flags }] = modifications.as_slice() {
        assert_eq!(*item_id, ID);
        assert_eq!(*flags, ModificationFlags::all());
    } else {
        panic!("Modifications incorrect: {:?}", modifications);
    }

    let events = downlink_lifecycle.lifecycle.take();
    assert!(events.is_empty());

    let value = agent.lane.get(&1, |v| v.cloned());
    assert!(value.is_none());
    let value = agent.lane.get(&2, |v| v.cloned());
    assert!(value.is_none());
    let (state, owned) = state_for(&agent.lane, "link");

    if let Some(Link { status, keys }) = state {
        assert_eq!(status, DownlinkStatus::Linked);
        assert!(keys.is_empty());
    }
    assert!(owned.is_empty());
}

#[test]
fn run_take_linked() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    set_state_for(
        &agent.lane,
        "link",
        DownlinkStatus::Linked,
        [(1, "a".to_string()), (2, "b".to_string())]
            .into_iter()
            .collect(),
    );

    let on_event = downlink_lifecycle.on_event(MapMessage::Take(1));
    let modifications = run_handler(on_event, meta, &agent);

    if let [Modification { item_id, flags }] = modifications.as_slice() {
        assert_eq!(*item_id, ID);
        assert_eq!(*flags, ModificationFlags::all());
    } else {
        panic!("Modifications incorrect: {:?}", modifications);
    }

    let events = downlink_lifecycle.lifecycle.take();
    assert!(events.is_empty());

    let value = agent.lane.get(&1, |v| v.cloned());
    assert_eq!(value, Some("a".to_string()));
    let value = agent.lane.get(&2, |v| v.cloned());
    assert!(value.is_none());

    let expected = [1].into_iter().collect();
    let (state, owned) = state_for(&agent.lane, "link");

    if let Some(Link { status, keys }) = state {
        assert_eq!(status, DownlinkStatus::Linked);
        assert_eq!(keys, expected);
    }
    assert_eq!(owned, expected);
}

#[test]
fn run_drop_linked() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());
    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    set_state_for(
        &agent.lane,
        "link",
        DownlinkStatus::Linked,
        [(1, "a".to_string()), (2, "b".to_string())]
            .into_iter()
            .collect(),
    );

    let on_event = downlink_lifecycle.on_event(MapMessage::Drop(1));
    let modifications = run_handler(on_event, meta, &agent);

    if let [Modification { item_id, flags }] = modifications.as_slice() {
        assert_eq!(*item_id, ID);
        assert_eq!(*flags, ModificationFlags::all());
    } else {
        panic!("Modifications incorrect: {:?}", modifications);
    }

    let events = downlink_lifecycle.lifecycle.take();
    assert!(events.is_empty());

    let value = agent.lane.get(&1, |v| v.cloned());
    assert!(value.is_none());
    let value = agent.lane.get(&2, |v| v.cloned());
    assert_eq!(value, Some("b".to_string()));

    let expected = [2].into_iter().collect();
    let (state, owned) = state_for(&agent.lane, "link");

    if let Some(Link { status, keys }) = state {
        assert_eq!(status, DownlinkStatus::Linked);
        assert_eq!(keys, expected);
    }
    assert_eq!(owned, expected);
}

#[test]
fn run_on_synced() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(Default::default());

    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    set_state_for(
        &agent.lane,
        "link",
        DownlinkStatus::Linked,
        [(1, "a".to_string()), (2, "b".to_string())]
            .into_iter()
            .collect(),
    );

    let on_synced = downlink_lifecycle.on_synced(&());
    let modifications = run_handler(on_synced, meta, &agent);

    assert!(modifications.is_empty());

    let expected = [1, 2].into_iter().collect();

    let events = downlink_lifecycle.lifecycle.take();
    if let [Event::Synced {
        remote,
        link_key,
        keys,
    }] = events.as_slice()
    {
        assert_eq!(link_key, "link");
        assert_eq!(remote, &make_address());
        assert_eq!(keys, &expected)
    } else {
        panic!("Events incorrect: {:?}", events);
    }

    let value = agent.lane.get(&1, |v| v.cloned());
    assert_eq!(value, Some("a".to_string()));
    let value = agent.lane.get(&2, |v| v.cloned());
    assert_eq!(value, Some("b".to_string()));

    let (state, owned) = state_for(&agent.lane, "link");

    if let Some(Link { status, keys }) = state {
        assert_eq!(status, DownlinkStatus::Linked);
        assert_eq!(keys, expected);
    }
    assert_eq!(owned, expected);
}

#[test]
fn run_on_unlinked_abandon() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(LinkClosedResponse::Abandon);

    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    set_state_for(
        &agent.lane,
        "link",
        DownlinkStatus::Linked,
        [(1, "a".to_string()), (2, "b".to_string())]
            .into_iter()
            .collect(),
    );

    let on_unlinked = downlink_lifecycle.on_unlinked();
    let modifications = run_handler(on_unlinked, meta, &agent);

    assert!(modifications.is_empty());

    let expected = [1, 2].into_iter().collect();

    let events = downlink_lifecycle.lifecycle.take();
    if let [Event::Unlinked {
        link_key,
        remote,
        keys,
    }] = events.as_slice()
    {
        assert_eq!(link_key, "link");
        assert_eq!(remote, &make_address());
        assert_eq!(keys, &expected)
    } else {
        panic!("Events incorrect: {:?}", events);
    }

    let value = agent.lane.get(&1, |v| v.cloned());
    assert_eq!(value, Some("a".to_string()));
    let value = agent.lane.get(&2, |v| v.cloned());
    assert_eq!(value, Some("b".to_string()));

    let (state, owned) = state_for(&agent.lane, "link");

    assert!(state.is_none());
    assert!(owned.is_empty());
}

#[test]
fn run_on_unlinked_delete() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(LinkClosedResponse::Delete);

    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    set_state_for(
        &agent.lane,
        "link",
        DownlinkStatus::Linked,
        [(1, "a".to_string()), (2, "b".to_string())]
            .into_iter()
            .collect(),
    );

    let on_unlinked = downlink_lifecycle.on_unlinked();
    let modifications = run_handler(on_unlinked, meta, &agent);

    if let [Modification { item_id, flags }] = modifications.as_slice() {
        assert_eq!(*item_id, ID);
        assert_eq!(*flags, ModificationFlags::all());
    } else {
        panic!("Modifications incorrect: {:?}", modifications);
    }

    let expected = [1, 2].into_iter().collect();

    let events = downlink_lifecycle.lifecycle.take();
    if let [Event::Unlinked {
        link_key,
        remote,
        keys,
    }] = events.as_slice()
    {
        assert_eq!(link_key, "link");
        assert_eq!(remote, &make_address());
        assert_eq!(keys, &expected)
    } else {
        panic!("Events incorrect: {:?}", events);
    }

    let value = agent.lane.get(&1, |v| v.cloned());
    assert!(value.is_none());
    let value = agent.lane.get(&2, |v| v.cloned());
    assert!(value.is_none());

    let (state, owned) = state_for(&agent.lane, "link");

    assert!(state.is_none());
    assert!(owned.is_empty());
}

#[test]
fn run_on_failed_abandon() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(LinkClosedResponse::Abandon);

    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    set_state_for(
        &agent.lane,
        "link",
        DownlinkStatus::Linked,
        [(1, "a".to_string()), (2, "b".to_string())]
            .into_iter()
            .collect(),
    );

    let on_failed = downlink_lifecycle.on_failed();
    let modifications = run_handler(on_failed, meta, &agent);

    assert!(modifications.is_empty());

    let expected = [1, 2].into_iter().collect();

    let events = downlink_lifecycle.lifecycle.take();
    if let [Event::Failed {
        link_key,
        remote,
        keys,
    }] = events.as_slice()
    {
        assert_eq!(link_key, "link");
        assert_eq!(remote, &make_address());
        assert_eq!(keys, &expected)
    } else {
        panic!("Events incorrect: {:?}", events);
    }

    let value = agent.lane.get(&1, |v| v.cloned());
    assert_eq!(value, Some("a".to_string()));
    let value = agent.lane.get(&2, |v| v.cloned());
    assert_eq!(value, Some("b".to_string()));

    let (state, owned) = state_for(&agent.lane, "link");

    assert!(state.is_none());
    assert!(owned.is_empty());
}

#[test]
fn run_on_failed_delete() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let agent = TestAgent::default();
    let lifecycle = TestLifecycle::new(LinkClosedResponse::Delete);

    let downlink_lifecycle = JoinMapDownlink::new(
        TestAgent::LANE,
        "link".to_string(),
        make_address(),
        lifecycle,
    );

    set_state_for(
        &agent.lane,
        "link",
        DownlinkStatus::Linked,
        [(1, "a".to_string()), (2, "b".to_string())]
            .into_iter()
            .collect(),
    );

    let on_failed = downlink_lifecycle.on_failed();
    let modifications = run_handler(on_failed, meta, &agent);

    if let [Modification { item_id, flags }] = modifications.as_slice() {
        assert_eq!(*item_id, ID);
        assert_eq!(*flags, ModificationFlags::all());
    } else {
        panic!("Modifications incorrect: {:?}", modifications);
    }

    let expected = [1, 2].into_iter().collect();

    let events = downlink_lifecycle.lifecycle.take();
    if let [Event::Failed {
        link_key,
        remote,
        keys,
    }] = events.as_slice()
    {
        assert_eq!(link_key, "link");
        assert_eq!(remote, &make_address());
        assert_eq!(keys, &expected)
    } else {
        panic!("Events incorrect: {:?}", events);
    }

    let value = agent.lane.get(&1, |v| v.cloned());
    assert!(value.is_none());
    let value = agent.lane.get(&2, |v| v.cloned());
    assert!(value.is_none());

    let (state, owned) = state_for(&agent.lane, "link");

    assert!(state.is_none());
    assert!(owned.is_empty());
}
