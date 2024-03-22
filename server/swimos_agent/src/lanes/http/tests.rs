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

use bytes::{Bytes, BytesMut};
use mime::Mime;
use swimos_api::agent::{AgentConfig, HttpLaneRequest, HttpResponseReceiver};
use swimos_model::http::{Header, HttpRequest, Method, StandardHeaderName, StatusCode, Version};
use swimos_utilities::routing::route_uri::RouteUri;

use crate::event_handler::{EventHandler, Modification, ModificationFlags, StepResult};
use crate::lanes::http::model::MethodAndPayload;
use crate::meta::AgentMetadata;
use crate::test_context::dummy_context;

use super::headers::content_type_header;
use super::model::Request;
use super::RequestAndChannel;
use super::{content_type::recon, HttpLaneAccept, Recon, SimpleHttpLane};

struct FakeAgent {
    lane: SimpleHttpLane<i32, Recon>,
}

const LANE_ID: u64 = 6;

impl Default for FakeAgent {
    fn default() -> Self {
        Self {
            lane: SimpleHttpLane::new(LANE_ID),
        }
    }
}

const URI: &str = "http://example/node?lane=lane";

fn make_raw_handler(
    method: Method,
    content_type: Option<&Mime>,
    payload: Bytes,
) -> (
    HttpLaneAccept<FakeAgent, i32, i32, i32, Recon>,
    HttpResponseReceiver,
    Vec<Header>,
) {
    let accept = Header::new(StandardHeaderName::Accept, recon().to_string());
    let extra = Header::new("extra-header", "value");
    let ct = if let Some(content_type) = content_type {
        content_type
    } else {
        recon()
    };
    let headers = vec![content_type_header(ct), accept, extra];
    let request = HttpRequest {
        method,
        version: Version::HTTP_1_1,
        uri: URI.parse().unwrap(),
        headers: headers.clone(),
        payload,
    };
    let (request, rx) = HttpLaneRequest::new(request);
    (
        HttpLaneAccept::new(|agent: &FakeAgent| &agent.lane, request),
        rx,
        headers,
    )
}

fn make_accept_handler(
    method: Method,
    body: Option<i32>,
) -> (
    HttpLaneAccept<FakeAgent, i32, i32, i32, Recon>,
    HttpResponseReceiver,
    Vec<Header>,
) {
    make_raw_handler(
        method,
        None,
        body.map(|n| Bytes::from(n.to_string())).unwrap_or_default(),
    )
}

#[test]
fn accept_get_request() {
    let agent = FakeAgent::default();
    let (handler, _rx, expected_headers) = make_accept_handler(Method::GET, None);
    run_handler(&agent, handler, true);
    let RequestAndChannel {
        request:
            Request {
                method_and_payload,
                uri,
                headers,
            },
        response_tx: _response_tx,
    } = agent.lane.take_request().expect("No request registered.");

    assert_eq!(uri.to_string(), URI);
    assert_eq!(method_and_payload, MethodAndPayload::Get);
    assert_eq!(headers, expected_headers);
}

#[test]
fn accept_post_request() {
    let agent = FakeAgent::default();
    let (handler, _rx, expected_headers) = make_accept_handler(Method::POST, Some(25));
    run_handler(&agent, handler, true);
    let RequestAndChannel {
        request:
            Request {
                method_and_payload,
                uri,
                headers,
            },
        response_tx: _response_tx,
    } = agent.lane.take_request().expect("No request registered.");

    assert_eq!(uri.to_string(), URI);
    assert_eq!(method_and_payload, MethodAndPayload::Post(25));
    assert_eq!(headers, expected_headers);
}

#[test]
fn accept_put_request() {
    let agent = FakeAgent::default();
    let (handler, _rx, expected_headers) = make_accept_handler(Method::PUT, Some(25));
    run_handler(&agent, handler, true);
    let RequestAndChannel {
        request:
            Request {
                method_and_payload,
                uri,
                headers,
            },
        response_tx: _response_tx,
    } = agent.lane.take_request().expect("No request registered.");

    assert_eq!(uri.to_string(), URI);
    assert_eq!(method_and_payload, MethodAndPayload::Put(25));
    assert_eq!(headers, expected_headers);
}

#[test]
fn accept_delete_request() {
    let agent = FakeAgent::default();
    let (handler, _rx, expected_headers) = make_accept_handler(Method::DELETE, None);
    run_handler(&agent, handler, true);
    let RequestAndChannel {
        request:
            Request {
                method_and_payload,
                uri,
                headers,
            },
        response_tx: _response_tx,
    } = agent.lane.take_request().expect("No request registered.");

    assert_eq!(uri.to_string(), URI);
    assert_eq!(method_and_payload, MethodAndPayload::Delete);
    assert_eq!(headers, expected_headers);
}

#[tokio::test]
async fn reject_post_request_bad_payload() {
    let agent = FakeAgent::default();
    let (handler, rx, ..) =
        make_raw_handler(Method::POST, None, Bytes::from(b"invalid".as_slice()));
    run_handler(&agent, handler, false);
    assert!(agent.lane.take_request().is_none());

    let response = rx.await.expect("No response.");

    assert_eq!(response.status_code, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn reject_post_request_unsupported_content_type() {
    let agent = FakeAgent::default();
    let (handler, rx, ..) = make_raw_handler(
        Method::POST,
        Some(&mime::APPLICATION_JSON),
        Bytes::from(b"7".as_slice()),
    );
    run_handler(&agent, handler, false);
    assert!(agent.lane.take_request().is_none());

    let response = rx.await.expect("No response.");

    assert_eq!(response.status_code, StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

const NODE_URI: &str = "/node";
const CONFIG: AgentConfig = AgentConfig::DEFAULT;

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn run_handler<H>(agent: &FakeAgent, mut handler: H, should_succeed: bool)
where
    H: EventHandler<FakeAgent>,
{
    let mut join_lane_init = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();
    let mut action_context = dummy_context(&mut join_lane_init, &mut ad_hoc_buffer);
    let route_uri = make_uri();
    let route_params = HashMap::new();
    let meta = AgentMetadata::new(&route_uri, &route_params, &CONFIG);
    let mut mods = vec![];
    loop {
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue {
                modified_item: Some(modification),
            } => {
                mods.push(modification);
            }
            StepResult::Fail(err) => panic!("Failed: {}", err),
            StepResult::Complete { modified_item, .. } => {
                if let Some(modification) = modified_item {
                    mods.push(modification);
                }
                if should_succeed {
                    check_mods(mods);
                } else {
                    assert!(mods.is_empty());
                }
                break;
            }
            _ => {}
        }
    }
}

fn check_mods(mods: Vec<Modification>) {
    match mods.as_slice() {
        [Modification { item_id, flags }] => {
            assert_eq!(*item_id, LANE_ID);
            assert_eq!(*flags, ModificationFlags::TRIGGER_HANDLER);
        }
        _ => panic!("Incorrect number of modifications: {}", mods.len()),
    }
}
