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

use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;
use swimos_api::{
    agent::{response_channel, AgentConfig},
    http::{HttpResponse, StatusCode, Uri},
};
use swimos_utilities::routing::RouteUri;

use crate::agent_lifecycle::item_event::{HLeaf, HttpBranch};
use crate::lanes::http::Request;
use crate::{
    agent_lifecycle::{
        item_event::{tests::run_handler, ItemEventShared},
        utility::HandlerContext,
    },
    event_handler::{ActionContext, HandlerAction, StepResult},
    lanes::{
        http::{
            lifecycle::{
                on_delete::OnDeleteShared, on_get::OnGetShared, on_post::OnPostShared,
                on_put::OnPutShared, HttpRequestContext,
            },
            MethodAndPayload, Recon, RequestAndChannel, Response, UnitResponse,
        },
        SimpleHttpLane,
    },
    meta::AgentMetadata,
};

use super::HttpLeaf;
struct TestAgent {
    first: SimpleHttpLane<i32, Recon>,
    second: SimpleHttpLane<String, Recon>,
    third: SimpleHttpLane<bool, Recon>,
}

const LANE_ID1: u64 = 0;
const LANE_ID2: u64 = 1;
const LANE_ID3: u64 = 2;

impl Default for TestAgent {
    fn default() -> Self {
        TestAgent {
            first: SimpleHttpLane::new(LANE_ID1),
            second: SimpleHttpLane::new(LANE_ID2),
            third: SimpleHttpLane::new(LANE_ID3),
        }
    }
}

impl TestAgent {
    const FIRST: fn(&TestAgent) -> &SimpleHttpLane<i32, Recon> = |agent| &agent.first;
    const SECOND: fn(&TestAgent) -> &SimpleHttpLane<String, Recon> = |agent| &agent.second;
    const THIRD: fn(&TestAgent) -> &SimpleHttpLane<bool, Recon> = |agent| &agent.third;
}

const FIRST_NAME: &str = "first";
const SECOND_NAME: &str = "second";
const THIRD_NAME: &str = "third";

#[derive(Debug)]
struct FakeLifecycleState<T> {
    post_values: Vec<T>,
    put_value: Vec<T>,
    deleted: bool,
}

impl<T> Default for FakeLifecycleState<T> {
    fn default() -> Self {
        Self {
            post_values: Default::default(),
            put_value: Default::default(),
            deleted: false,
        }
    }
}

#[derive(Debug, Clone)]
struct FakeLifecycle<T> {
    get_value: T,
    inner: Arc<Mutex<FakeLifecycleState<T>>>,
}

impl<T> FakeLifecycle<T> {
    fn new(get_value: T) -> Self {
        FakeLifecycle {
            get_value,
            inner: Default::default(),
        }
    }
}

struct GetHandler<T> {
    value: Option<T>,
}

struct OtherHandler<T> {
    value: Option<Option<(T, bool)>>,
    inner: Arc<Mutex<FakeLifecycleState<T>>>,
}

impl<T> HandlerAction<TestAgent> for GetHandler<T> {
    type Completion = Response<T>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        if let Some(t) = self.value.take() {
            StepResult::done(Response::from(t))
        } else {
            StepResult::after_done()
        }
    }
}

impl<T> HandlerAction<TestAgent> for OtherHandler<T> {
    type Completion = UnitResponse;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let inner = &mut *self.inner.lock();
        match self.value.take() {
            Some(Some((v, false))) => inner.post_values.push(v),
            Some(Some((v, true))) => inner.put_value.push(v),
            Some(None) => inner.deleted = true,
            None => return StepResult::after_done(),
        }
        StepResult::done(Default::default())
    }
}

impl<T, Shared> OnGetShared<T, TestAgent, Shared> for FakeLifecycle<T>
where
    T: Clone + Send,
{
    type OnGetHandler<'a> = GetHandler<T>
    where
        Self: 'a,
        Shared: 'a;

    fn on_get<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<TestAgent>,
        _http_context: HttpRequestContext,
    ) -> Self::OnGetHandler<'a> {
        GetHandler {
            value: Some(self.get_value.clone()),
        }
    }
}

impl<T, Shared> OnPostShared<T, TestAgent, Shared> for FakeLifecycle<T>
where
    T: Send,
{
    type OnPostHandler<'a> = OtherHandler<T>
    where
        Self: 'a,
        Shared: 'a;

    fn on_post<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<TestAgent>,
        _http_context: HttpRequestContext,
        value: T,
    ) -> Self::OnPostHandler<'a> {
        OtherHandler {
            value: Some(Some((value, false))),
            inner: self.inner.clone(),
        }
    }
}

impl<T, Shared> OnPutShared<T, TestAgent, Shared> for FakeLifecycle<T>
where
    T: Send,
{
    type OnPutHandler<'a> = OtherHandler<T>
    where
        Self: 'a,
        Shared: 'a;

    fn on_put<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<TestAgent>,
        _http_context: HttpRequestContext,
        value: T,
    ) -> Self::OnPutHandler<'a> {
        OtherHandler {
            value: Some(Some((value, true))),
            inner: self.inner.clone(),
        }
    }
}

impl<T, Shared> OnDeleteShared<TestAgent, Shared> for FakeLifecycle<T>
where
    T: Send,
{
    type OnDeleteHandler<'a> = OtherHandler<T>
    where
        Self: 'a,
        Shared: 'a;

    fn on_delete<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<TestAgent>,
        _http_context: HttpRequestContext,
    ) -> Self::OnDeleteHandler<'a> {
        OtherHandler {
            value: Some(None),
            inner: self.inner.clone(),
        }
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

fn lane_uri(name: &str) -> Uri {
    let uri_str = format!("http://example/node?lane={}", name);
    uri_str.parse().unwrap()
}

#[tokio::test]
async fn http_lane_leaf() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let agent = TestAgent::default();

    let request = Request {
        method_and_payload: MethodAndPayload::Get,
        uri: lane_uri(FIRST_NAME),
        headers: vec![],
    };
    let (tx, rx) = response_channel();

    let req = RequestAndChannel::new(request, tx);
    agent.first.replace(req);
    let lifecycle = FakeLifecycle::new(12);

    let leaf = HttpLeaf::leaf(FIRST_NAME, TestAgent::FIRST, lifecycle.clone());

    assert!(leaf
        .item_event(&(), Default::default(), &agent, "other")
        .is_none());

    if let Some(handler) = leaf.item_event(&(), Default::default(), &agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);
        let HttpResponse {
            status_code,
            payload,
            ..
        } = rx.await.expect("No response.");
        assert_eq!(status_code, StatusCode::OK);
        assert_eq!(payload.as_ref(), b"12");
    } else {
        panic!("Expected an event handler.");
    }
}

#[tokio::test]
async fn http_lane_left_branch() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::new(167);
    let second_lifecycle = FakeLifecycle::new("name".to_string());
    let leaf = HttpLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());

    let branch = HttpBranch::new(
        SECOND_NAME,
        TestAgent::SECOND,
        second_lifecycle.clone(),
        leaf,
        HLeaf,
    );

    //Before first lane.
    assert!(branch
        .item_event(&(), Default::default(), &agent, "a")
        .is_none());
    //Between first and second lanes.
    assert!(branch
        .item_event(&(), Default::default(), &agent, "g")
        .is_none());
    //After second lane.
    assert!(branch
        .item_event(&(), Default::default(), &agent, "u")
        .is_none());

    let request1 = Request {
        method_and_payload: MethodAndPayload::Get,
        uri: lane_uri(FIRST_NAME),
        headers: vec![],
    };
    let (tx1, rx1) = response_channel();

    let req1 = RequestAndChannel::new(request1, tx1);
    agent.first.replace(req1);

    let request2 = Request {
        method_and_payload: MethodAndPayload::Delete,
        uri: lane_uri(SECOND_NAME),
        headers: vec![],
    };
    let (tx2, rx2) = response_channel();

    let req2 = RequestAndChannel::new(request2, tx2);
    agent.second.replace(req2);

    if let Some(handler) = branch.item_event(&(), Default::default(), &agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);
        let HttpResponse {
            status_code,
            payload,
            ..
        } = rx1.await.expect("No response.");
        assert_eq!(status_code, StatusCode::OK);
        assert_eq!(payload.as_ref(), b"167");
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.item_event(&(), Default::default(), &agent, SECOND_NAME) {
        run_handler(meta, &agent, handler);
        let HttpResponse {
            status_code,
            payload,
            ..
        } = rx2.await.expect("No response.");
        assert_eq!(status_code, StatusCode::OK);
        assert!(payload.is_empty());
        assert!(second_lifecycle.inner.lock().deleted);
    } else {
        panic!("Expected an event handler.");
    }
}

#[tokio::test]
async fn http_lane_right_branch() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::new(167);
    let second_lifecycle = FakeLifecycle::new("name".to_string());
    let leaf = HttpLeaf::leaf(SECOND_NAME, TestAgent::SECOND, second_lifecycle.clone());

    let branch = HttpBranch::new(
        FIRST_NAME,
        TestAgent::FIRST,
        first_lifecycle.clone(),
        HLeaf,
        leaf,
    );

    //Before first lane.
    assert!(branch
        .item_event(&(), Default::default(), &agent, "a")
        .is_none());
    //Between first and second lanes.
    assert!(branch
        .item_event(&(), Default::default(), &agent, "g")
        .is_none());
    //After second lane.
    assert!(branch
        .item_event(&(), Default::default(), &agent, "u")
        .is_none());

    let request1 = Request {
        method_and_payload: MethodAndPayload::Get,
        uri: lane_uri(FIRST_NAME),
        headers: vec![],
    };
    let (tx1, rx1) = response_channel();

    let req1 = RequestAndChannel::new(request1, tx1);
    agent.first.replace(req1);

    let request2 = Request {
        method_and_payload: MethodAndPayload::Delete,
        uri: lane_uri(SECOND_NAME),
        headers: vec![],
    };
    let (tx2, rx2) = response_channel();

    let req2 = RequestAndChannel::new(request2, tx2);
    agent.second.replace(req2);

    if let Some(handler) = branch.item_event(&(), Default::default(), &agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);
        let HttpResponse {
            status_code,
            payload,
            ..
        } = rx1.await.expect("No response.");
        assert_eq!(status_code, StatusCode::OK);
        assert_eq!(payload.as_ref(), b"167");
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.item_event(&(), Default::default(), &agent, SECOND_NAME) {
        run_handler(meta, &agent, handler);
        let HttpResponse {
            status_code,
            payload,
            ..
        } = rx2.await.expect("No response.");
        assert_eq!(status_code, StatusCode::OK);
        assert!(payload.is_empty());
        assert!(second_lifecycle.inner.lock().deleted);
    } else {
        panic!("Expected an event handler.");
    }
}

#[tokio::test]
async fn http_lane_two_branches() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let agent = TestAgent::default();

    let first_lifecycle = FakeLifecycle::new(167);
    let second_lifecycle = FakeLifecycle::new("name".to_string());
    let third_lifecycle = FakeLifecycle::new(true);
    let leaf_left = HttpLeaf::leaf(FIRST_NAME, TestAgent::FIRST, first_lifecycle.clone());
    let leaf_right = HttpLeaf::leaf(THIRD_NAME, TestAgent::THIRD, third_lifecycle.clone());

    let branch = HttpBranch::new(
        SECOND_NAME,
        TestAgent::SECOND,
        second_lifecycle.clone(),
        leaf_left,
        leaf_right,
    );

    //Before first lane.
    assert!(branch
        .item_event(&(), Default::default(), &agent, "a")
        .is_none());
    //Between first and second lanes.
    assert!(branch
        .item_event(&(), Default::default(), &agent, "g")
        .is_none());
    //Between second and third lanes.
    assert!(branch
        .item_event(&(), Default::default(), &agent, "sf")
        .is_none());
    //After second lane.
    assert!(branch
        .item_event(&(), Default::default(), &agent, "u")
        .is_none());

    let request1 = Request {
        method_and_payload: MethodAndPayload::Get,
        uri: lane_uri(FIRST_NAME),
        headers: vec![],
    };
    let (tx1, rx1) = response_channel();

    let req1 = RequestAndChannel::new(request1, tx1);
    agent.first.replace(req1);

    let request2 = Request {
        method_and_payload: MethodAndPayload::Delete,
        uri: lane_uri(SECOND_NAME),
        headers: vec![],
    };
    let (tx2, rx2) = response_channel();

    let req2 = RequestAndChannel::new(request2, tx2);
    agent.second.replace(req2);

    let request3 = Request {
        method_and_payload: MethodAndPayload::Post(false),
        uri: lane_uri(THIRD_NAME),
        headers: vec![],
    };
    let (tx3, rx3) = response_channel();

    let req3 = RequestAndChannel::new(request3, tx3);
    agent.third.replace(req3);

    if let Some(handler) = branch.item_event(&(), Default::default(), &agent, FIRST_NAME) {
        run_handler(meta, &agent, handler);
        let HttpResponse {
            status_code,
            payload,
            ..
        } = rx1.await.expect("No response.");
        assert_eq!(status_code, StatusCode::OK);
        assert_eq!(payload.as_ref(), b"167");
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.item_event(&(), Default::default(), &agent, SECOND_NAME) {
        run_handler(meta, &agent, handler);
        let HttpResponse {
            status_code,
            payload,
            ..
        } = rx2.await.expect("No response.");
        assert_eq!(status_code, StatusCode::OK);
        assert!(payload.is_empty());
        assert!(second_lifecycle.inner.lock().deleted);
    } else {
        panic!("Expected an event handler.");
    }

    if let Some(handler) = branch.item_event(&(), Default::default(), &agent, THIRD_NAME) {
        run_handler(meta, &agent, handler);
        let HttpResponse {
            status_code,
            payload,
            ..
        } = rx3.await.expect("No response.");
        assert_eq!(status_code, StatusCode::OK);
        assert!(payload.is_empty());
        let guard = third_lifecycle.inner.lock();
        assert!(matches!(guard.post_values.as_slice(), [false]));
    } else {
        panic!("Expected an event handler.");
    }
}
