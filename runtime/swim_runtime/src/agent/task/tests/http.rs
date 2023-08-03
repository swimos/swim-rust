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

use std::{num::NonZeroUsize, time::Duration};

use bytes::Bytes;
use futures::future::join;
use futures::Future;
use http::Uri;
use swim_api::agent::HttpLaneRequest;
use swim_model::{
    http::{HttpRequest, HttpResponse, Method, StatusCode, Version},
    Text,
};
use swim_utilities::{non_zero_usize, trigger};
use tokio::sync::{mpsc, oneshot};

use crate::agent::{
    task::{
        timeout_coord::{self, multi_party_coordinator, VoteResult, Voter},
        HttpLaneEndpoint, HttpLaneRuntimeSpec,
    },
    AgentRuntimeConfig,
};

use super::super::http_task;

const CHAN_SIZE: NonZeroUsize = non_zero_usize!(8);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct TestContext {
    stop: Option<trigger::Sender>,
    requests_tx: mpsc::Sender<HttpLaneRequest>,
    lanes_tx: mpsc::Sender<HttpLaneRuntimeSpec>,
    voter: Voter,
    vote_receiver: timeout_coord::Receiver,
}

impl TestContext {
    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop.trigger();
        }
    }
}

async fn run_test_case<F, Fut>(
    config: AgentRuntimeConfig,
    initial_endpoints: Vec<HttpLaneEndpoint>,
    test_case: F,
) -> Fut::Output
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future,
{
    let (stopping_tx, stopping_rx) = trigger::trigger();
    let (requests_tx, requests_rx) = mpsc::channel(CHAN_SIZE.get());
    let (lanes_tx, lanes_rx) = mpsc::channel(CHAN_SIZE.get());

    let ([voter1, voter2], vote_receiver) = multi_party_coordinator::<2>();
    let task = http_task(
        stopping_rx,
        config,
        requests_rx,
        initial_endpoints,
        lanes_rx,
        voter1,
    );

    let context = TestContext {
        stop: Some(stopping_tx),
        requests_tx,
        lanes_tx,
        voter: voter2,
        vote_receiver,
    };

    let test_task = test_case(context);

    let (_, result) = tokio::time::timeout(TEST_TIMEOUT, join(task, test_task))
        .await
        .expect("Test timed out.");
    result
}

#[tokio::test]
async fn explicit_stop() {
    run_test_case(Default::default(), vec![], |mut context| async move {
        context.stop();
        context
    })
    .await;
}

#[tokio::test]
async fn stop_on_requests_terminated() {
    run_test_case(Default::default(), vec![], |context| async move {
        let TestContext {
            stop,
            requests_tx,
            lanes_tx,
            voter,
            vote_receiver,
        } = context;
        drop(requests_tx);
        (stop, lanes_tx, voter, vote_receiver)
    })
    .await;
}

const URI1: &str = "http://example:8080/path/to_agent?lane=name";
const PAYLOAD: &str = "body";

fn make_request() -> (HttpLaneRequest, oneshot::Receiver<HttpResponse<Bytes>>) {
    let request = HttpRequest {
        method: Method::GET,
        version: Version::HTTP_1_1,
        uri: Uri::from_static(URI1),
        headers: vec![],
        payload: Bytes::from(PAYLOAD),
    };
    let (response_tx, response_rx) = oneshot::channel();
    (
        HttpLaneRequest {
            request,
            response_tx,
        },
        response_rx,
    )
}

#[tokio::test]
async fn request_for_non_existent_lane() {
    run_test_case(Default::default(), vec![], |mut context| async move {
        let TestContext { requests_tx, .. } = &mut context;
        let (request, response_rx) = make_request();
        requests_tx.send(request).await.expect("Channel dropped.");
        let response = response_rx.await.expect("Response not sent.");
        assert_eq!(response.status_code, StatusCode::NOT_FOUND);
        context.stop();
        context
    })
    .await;
}

fn response_body(body: &[u8]) -> Bytes {
    Bytes::from(format!(
        "Request Body: {}",
        std::str::from_utf8(body).expect("Bad UTF")
    ))
}

fn satisfy_request(request: HttpLaneRequest, expected_uri: &'static str, expected_body: &str) {
    let HttpLaneRequest {
        request:
            HttpRequest {
                method,
                uri,
                payload,
                ..
            },
        response_tx,
    } = request;
    assert_eq!(method, Method::GET);
    assert_eq!(payload.as_ref(), expected_body.as_bytes());
    assert_eq!(uri, Uri::from_static(expected_uri));

    response_tx
        .send(HttpResponse {
            status_code: StatusCode::OK,
            version: Version::HTTP_1_1,
            headers: vec![],
            payload: response_body(payload.as_ref()),
        })
        .expect("Request dropped.");
}

fn check_response(response: HttpResponse<Bytes>, expected_body: Bytes) {
    let HttpResponse {
        status_code,
        payload,
        ..
    } = response;
    assert_eq!(status_code, StatusCode::OK);
    assert_eq!(payload, expected_body);
}

#[tokio::test]
async fn request_for_preregistered_lane() {
    let (lane_tx, mut lane_rx) = mpsc::channel(CHAN_SIZE.get());
    let endpoint = HttpLaneEndpoint::new(Text::new("name"), lane_tx);

    run_test_case(
        Default::default(),
        vec![endpoint],
        |mut context| async move {
            let TestContext { requests_tx, .. } = &mut context;
            let (request, response_rx) = make_request();
            requests_tx.send(request).await.expect("Channel dropped.");
            let request = lane_rx.recv().await.expect("Expected request.");
            satisfy_request(request, URI1, PAYLOAD);

            let response = response_rx.await.expect("Response not sent.");
            check_response(response, response_body(PAYLOAD.as_bytes()));

            context.stop();
            context
        },
    )
    .await;
}

#[tokio::test]
async fn request_for_new_lane() {
    run_test_case(Default::default(), vec![], |mut context| async move {
        let TestContext {
            requests_tx,
            lanes_tx,
            ..
        } = &mut context;

        let (reg_tx, reg_rx) = oneshot::channel();
        lanes_tx
            .send(HttpLaneRuntimeSpec {
                name: Text::new("name"),
                promise: reg_tx,
            })
            .await
            .expect("Channel dropped.");
        let mut lane_rx = reg_rx
            .await
            .expect("Lane registration dropped.")
            .expect("Lane registration failed.");

        let (request, response_rx) = make_request();
        requests_tx.send(request).await.expect("Channel dropped.");
        let request = lane_rx.recv().await.expect("Expected request.");
        satisfy_request(request, URI1, PAYLOAD);

        let response = response_rx.await.expect("Response not sent.");
        check_response(response, response_body(PAYLOAD.as_bytes()));

        context.stop();
        context
    })
    .await;
}

const TEST_INACTIVE_TIMEOUT: Duration = Duration::from_millis(100);

#[tokio::test]
async fn votes_to_stop() {
    let config = AgentRuntimeConfig {
        inactive_timeout: TEST_INACTIVE_TIMEOUT,
        ..Default::default()
    };
    run_test_case(config, vec![], |context| async move {
        let TestContext {
            stop,
            requests_tx,
            lanes_tx,
            voter,
            vote_receiver,
        } = context;

        assert_eq!(voter.vote(), VoteResult::UnanimityPending);
        vote_receiver.await;
        (stop, requests_tx, lanes_tx, voter)
    })
    .await;
}

#[tokio::test]
async fn rescinds_stop_vote_on_request() {
    let config = AgentRuntimeConfig {
        inactive_timeout: TEST_INACTIVE_TIMEOUT,
        ..Default::default()
    };
    run_test_case(config, vec![], |mut context| async move {
        let TestContext {
            requests_tx, voter, ..
        } = &mut context;

        tokio::time::sleep(2 * TEST_INACTIVE_TIMEOUT).await;

        let (request, response_rx) = make_request();
        requests_tx.send(request).await.expect("Channel dropped.");
        let response = response_rx.await.expect("Response not sent.");
        assert_eq!(response.status_code, StatusCode::NOT_FOUND);
        assert_eq!(voter.vote(), VoteResult::UnanimityPending);

        context.stop();
        context
    })
    .await;
}
