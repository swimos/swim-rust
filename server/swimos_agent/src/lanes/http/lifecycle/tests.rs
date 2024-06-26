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

use bytes::BytesMut;
use parking_lot::Mutex;
use swimos_api::{
    agent::{response_channel, AgentConfig},
    http::{Header, HttpResponse, StandardHeaderName, StatusCode},
};
use swimos_recon::print_recon_compact;
use swimos_utilities::routing::RouteUri;

use crate::{
    agent_lifecycle::HandlerContext,
    event_handler::{ActionContext, HandlerAction, StepResult},
    lanes::http::{
        content_type::recon,
        headers::Headers,
        model::{MethodAndPayload, Request},
        Recon, RequestAndChannel, Response, UnitResponse,
    },
    meta::AgentMetadata,
    test_context::dummy_context,
};

use super::{
    on_delete::OnDeleteShared, on_get::OnGetShared, on_post::OnPostShared, on_put::OnPutShared,
    HttpLifecycleHandlerShared, HttpRequestContext, StatefulHttpLaneLifecycle,
};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Get,
    Post(i32),
    Put(i32),
    Delete,
}

struct TestAgent;

struct TestLifecycle;

#[derive(Default, Clone)]
struct TestLifecycleState {
    events: Arc<Mutex<Vec<Event>>>,
}

struct TestGetHandler<Get> {
    value: Option<Get>,
    events: Arc<Mutex<Vec<Event>>>,
}
struct TestOtherHandler {
    value: Option<Option<(i32, bool)>>,
    events: Arc<Mutex<Vec<Event>>>,
}

const GET_VALUE: i32 = 56;

impl OnGetShared<i32, TestAgent, TestLifecycleState> for TestLifecycle {
    type OnGetHandler<'a> = TestGetHandler<i32>
    where
        Self: 'a,
        TestLifecycleState: 'a;

    fn on_get<'a>(
        &'a self,
        shared: &'a TestLifecycleState,
        _handler_context: HandlerContext<TestAgent>,
        _http_context: HttpRequestContext,
    ) -> Self::OnGetHandler<'a> {
        TestGetHandler {
            value: Some(GET_VALUE),
            events: shared.events.clone(),
        }
    }
}

impl OnPostShared<i32, TestAgent, TestLifecycleState> for TestLifecycle {
    type OnPostHandler<'a> = TestOtherHandler
    where
        Self: 'a,
        TestLifecycleState: 'a;

    fn on_post<'a>(
        &'a self,
        shared: &'a TestLifecycleState,
        _handler_context: HandlerContext<TestAgent>,
        _http_context: HttpRequestContext,
        value: i32,
    ) -> Self::OnPostHandler<'a> {
        TestOtherHandler {
            value: Some(Some((value, false))),
            events: shared.events.clone(),
        }
    }
}

impl OnPutShared<i32, TestAgent, TestLifecycleState> for TestLifecycle {
    type OnPutHandler<'a> = TestOtherHandler
    where
        Self: 'a,
        TestLifecycleState: 'a;

    fn on_put<'a>(
        &'a self,
        shared: &'a TestLifecycleState,
        _handler_context: HandlerContext<TestAgent>,
        _http_context: HttpRequestContext,
        value: i32,
    ) -> Self::OnPutHandler<'a> {
        TestOtherHandler {
            value: Some(Some((value, true))),
            events: shared.events.clone(),
        }
    }
}

impl OnDeleteShared<TestAgent, TestLifecycleState> for TestLifecycle {
    type OnDeleteHandler<'a> = TestOtherHandler
    where
        Self: 'a,
        TestLifecycleState: 'a;

    fn on_delete<'a>(
        &'a self,
        shared: &'a TestLifecycleState,
        _handler_context: HandlerContext<TestAgent>,
        _http_context: HttpRequestContext,
    ) -> Self::OnDeleteHandler<'a> {
        TestOtherHandler {
            value: Some(None),
            events: shared.events.clone(),
        }
    }
}

impl HandlerAction<TestAgent> for TestGetHandler<i32> {
    type Completion = Response<i32>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let TestGetHandler { value, events } = self;
        if let Some(v) = value.take() {
            events.lock().push(Event::Get);
            StepResult::done(Response::from(v))
        } else {
            StepResult::after_done()
        }
    }
}

impl HandlerAction<TestAgent> for TestOtherHandler {
    type Completion = UnitResponse;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let TestOtherHandler { value, events } = self;
        events.lock().push(match value.take() {
            Some(Some((v, true))) => Event::Put(v),
            Some(Some((v, false))) => Event::Post(v),
            Some(None) => Event::Delete,
            _ => return StepResult::after_done(),
        });
        StepResult::done(Default::default())
    }
}

const URI: &str = "http://example/node?lane=name";

fn request_headers() -> Vec<Header> {
    vec![
        Header::new(StandardHeaderName::ContentType, recon().to_string()),
        Header::new(StandardHeaderName::Accept, recon().to_string()),
    ]
}

fn bad_request_headers() -> Vec<Header> {
    vec![Header::new(
        StandardHeaderName::Accept,
        mime::APPLICATION_JSON.to_string(),
    )]
}

const NODE_URI: &str = "/node";
const CONFIG: AgentConfig = AgentConfig::DEFAULT;

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn run_handler<H>(agent: &TestAgent, mut handler: H) -> H::Completion
where
    H: HandlerAction<TestAgent>,
{
    let mut join_lane_init = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();
    let mut action_context = dummy_context(&mut join_lane_init, &mut ad_hoc_buffer);
    let route_uri = make_uri();
    let route_params = HashMap::new();
    let meta = AgentMetadata::new(&route_uri, &route_params, &CONFIG);
    loop {
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { modified_item } => {
                assert!(modified_item.is_none())
            }
            StepResult::Fail(err) => panic!("Failed: {}", err),
            StepResult::Complete {
                modified_item,
                result,
            } => {
                assert!(modified_item.is_none());
                break result;
            }
        }
    }
}

#[tokio::test]
async fn run_get_handler() {
    let request = Request {
        method_and_payload: MethodAndPayload::Get,
        uri: URI.parse().unwrap(),
        headers: request_headers(),
    };
    let (tx, rx) = response_channel();
    let req = RequestAndChannel::new(request, tx);
    let agent = TestAgent;
    let shared = TestLifecycleState::default();
    let lifecycle = TestLifecycle;

    let handler = HttpLifecycleHandlerShared::new(req, &shared, Recon, &lifecycle);

    run_handler(&agent, handler);

    let HttpResponse {
        status_code,
        headers,
        payload,
        ..
    } = rx.await.expect("No response provided.");

    assert_eq!(status_code, StatusCode::OK);

    let headers = Headers::new(&headers);
    let ct = headers
        .content_type()
        .expect("Invalid content type.")
        .expect("No content type.");
    assert_eq!(&ct, recon());

    let expected_content = format!("{}", print_recon_compact(&GET_VALUE));
    assert_eq!(payload.as_ref(), expected_content.as_bytes());

    let events = std::mem::take(&mut *shared.events.lock());
    assert_eq!(events, vec![Event::Get]);
}

#[tokio::test]
async fn run_head_handler() {
    let request = Request {
        method_and_payload: MethodAndPayload::Head,
        uri: URI.parse().unwrap(),
        headers: request_headers(),
    };
    let (tx, rx) = response_channel();
    let req = RequestAndChannel::new(request, tx);
    let agent = TestAgent;
    let shared = TestLifecycleState::default();
    let lifecycle = TestLifecycle;

    let handler = HttpLifecycleHandlerShared::new(req, &shared, Recon, &lifecycle);

    run_handler(&agent, handler);

    let HttpResponse {
        status_code,
        headers,
        payload,
        ..
    } = rx.await.expect("No response provided.");

    assert_eq!(status_code, StatusCode::OK);

    let headers = Headers::new(&headers);
    let ct = headers
        .content_type()
        .expect("Invalid content type.")
        .expect("No content type.");
    assert_eq!(&ct, recon());

    assert!(payload.is_empty());
    let events = std::mem::take(&mut *shared.events.lock());
    assert_eq!(events, vec![Event::Get]);
}

#[tokio::test]
async fn run_post_handler() {
    let request = Request {
        method_and_payload: MethodAndPayload::Post(69),
        uri: URI.parse().unwrap(),
        headers: request_headers(),
    };
    let (tx, rx) = response_channel();
    let req = RequestAndChannel::new(request, tx);
    let agent = TestAgent;
    let shared = TestLifecycleState::default();
    let lifecycle = TestLifecycle;

    let handler = HttpLifecycleHandlerShared::new(req, &shared, Recon, &lifecycle);

    run_handler(&agent, handler);

    let HttpResponse {
        status_code,
        headers,
        payload,
        ..
    } = rx.await.expect("No response provided.");

    assert_eq!(status_code, StatusCode::OK);

    let headers = Headers::new(&headers);
    assert!(matches!(headers.content_type(), Ok(None)));

    assert!(payload.is_empty());

    let events = std::mem::take(&mut *shared.events.lock());
    assert_eq!(events, vec![Event::Post(69)]);
}

#[tokio::test]
async fn run_put_handler() {
    let request = Request {
        method_and_payload: MethodAndPayload::Put(848),
        uri: URI.parse().unwrap(),
        headers: request_headers(),
    };
    let (tx, rx) = response_channel();
    let req = RequestAndChannel::new(request, tx);
    let agent = TestAgent;
    let shared = TestLifecycleState::default();
    let lifecycle = TestLifecycle;

    let handler = HttpLifecycleHandlerShared::new(req, &shared, Recon, &lifecycle);

    run_handler(&agent, handler);

    let HttpResponse {
        status_code,
        headers,
        payload,
        ..
    } = rx.await.expect("No response provided.");

    assert_eq!(status_code, StatusCode::OK);

    let headers = Headers::new(&headers);
    assert!(matches!(headers.content_type(), Ok(None)));

    assert!(payload.is_empty());

    let events = std::mem::take(&mut *shared.events.lock());
    assert_eq!(events, vec![Event::Put(848)]);
}

#[tokio::test]
async fn run_delete_handler() {
    let request = Request {
        method_and_payload: MethodAndPayload::Delete,
        uri: URI.parse().unwrap(),
        headers: request_headers(),
    };
    let (tx, rx) = response_channel();
    let req = RequestAndChannel::new(request, tx);
    let agent = TestAgent;
    let shared = TestLifecycleState::default();
    let lifecycle = TestLifecycle;

    let handler = HttpLifecycleHandlerShared::new(req, &shared, Recon, &lifecycle);

    run_handler(&agent, handler);

    let HttpResponse {
        status_code,
        headers,
        payload,
        ..
    } = rx.await.expect("No response provided.");

    assert_eq!(status_code, StatusCode::OK);

    let headers = Headers::new(&headers);
    assert!(matches!(headers.content_type(), Ok(None)));

    assert!(payload.is_empty());

    let events = std::mem::take(&mut *shared.events.lock());
    assert_eq!(events, vec![Event::Delete]);
}

#[tokio::test]
async fn run_unsatisfiable_get_handler() {
    let request = Request {
        method_and_payload: MethodAndPayload::Get,
        uri: URI.parse().unwrap(),
        headers: bad_request_headers(),
    };
    let (tx, rx) = response_channel();
    let req = RequestAndChannel::new(request, tx);
    let agent = TestAgent;
    let shared = TestLifecycleState::default();
    let lifecycle = TestLifecycle;

    let handler = HttpLifecycleHandlerShared::new(req, &shared, Recon, &lifecycle);

    run_handler(&agent, handler);

    let HttpResponse { status_code, .. } = rx.await.expect("No response provided.");

    assert_eq!(status_code, StatusCode::UNSUPPORTED_MEDIA_TYPE);

    let events = std::mem::take(&mut *shared.events.lock());
    assert_eq!(events, vec![Event::Get]);
}

#[tokio::test]
async fn get_method_unsupported_handler() {
    let request = Request {
        method_and_payload: MethodAndPayload::Get,
        uri: URI.parse().unwrap(),
        headers: bad_request_headers(),
    };
    let (tx, rx) = response_channel();
    let req = RequestAndChannel::new(request, tx);

    let agent = TestAgent;

    let lifecycle = StatefulHttpLaneLifecycle::<TestAgent, (), i32, i32, i32>::default();

    let handler = HttpLifecycleHandlerShared::new(req, &(), Recon, &lifecycle);

    run_handler(&agent, handler);

    let HttpResponse { status_code, .. } = rx.await.expect("No response provided.");

    assert_eq!(status_code, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn post_method_unsupported_handler() {
    let request = Request {
        method_and_payload: MethodAndPayload::Post(12),
        uri: URI.parse().unwrap(),
        headers: bad_request_headers(),
    };
    let (tx, rx) = response_channel();
    let req = RequestAndChannel::new(request, tx);

    let agent = TestAgent;

    let lifecycle = StatefulHttpLaneLifecycle::<TestAgent, (), i32, i32, i32>::default();

    let handler = HttpLifecycleHandlerShared::new(req, &(), Recon, &lifecycle);

    run_handler(&agent, handler);

    let HttpResponse { status_code, .. } = rx.await.expect("No response provided.");

    assert_eq!(status_code, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn put_method_unsupported_handler() {
    let request = Request {
        method_and_payload: MethodAndPayload::Put(17),
        uri: URI.parse().unwrap(),
        headers: bad_request_headers(),
    };
    let (tx, rx) = response_channel();
    let req = RequestAndChannel::new(request, tx);

    let agent = TestAgent;

    let lifecycle = StatefulHttpLaneLifecycle::<TestAgent, (), i32, i32, i32>::default();

    let handler = HttpLifecycleHandlerShared::new(req, &(), Recon, &lifecycle);

    run_handler(&agent, handler);

    let HttpResponse { status_code, .. } = rx.await.expect("No response provided.");

    assert_eq!(status_code, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn delete_method_unsupported_handler() {
    let request = Request {
        method_and_payload: MethodAndPayload::Delete,
        uri: URI.parse().unwrap(),
        headers: bad_request_headers(),
    };
    let (tx, rx) = response_channel();
    let req = RequestAndChannel::new(request, tx);

    let agent = TestAgent;

    let lifecycle = StatefulHttpLaneLifecycle::<TestAgent, (), i32, i32, i32>::default();

    let handler = HttpLifecycleHandlerShared::new(req, &(), Recon, &lifecycle);

    run_handler(&agent, handler);

    let HttpResponse { status_code, .. } = rx.await.expect("No response provided.");

    assert_eq!(status_code, StatusCode::METHOD_NOT_ALLOWED);
}
