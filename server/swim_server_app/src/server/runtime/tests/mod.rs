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

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use futures::{future::join3, Future};
use swim_utilities::routing::route_pattern::RoutePattern;

use tokio::{
    io::DuplexStream,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};

use crate::{plane::PlaneBuilder, Server, ServerHandle, SwimServerConfig};

use self::{
    agent::TestAgent,
    connections::{TestConnections, TestWs},
};

use super::SwimServer;

mod agent;
mod connections;

struct TestContext {
    report_rx: UnboundedReceiver<i32>,
    incoming_tx: UnboundedSender<(SocketAddr, DuplexStream)>,
    handle: ServerHandle,
}

const NODE: &str = "/node";
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn run_server<F, Fut>(test_case: F) -> (Result<(), std::io::Error>, Fut::Output)
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future,
{
    let mut plane_builder = PlaneBuilder::default();
    let pattern = RoutePattern::parse_str(NODE).expect("Invalid route.");

    let (report_tx, report_rx) = mpsc::unbounded_channel();

    plane_builder.add_route(
        pattern,
        TestAgent::new(report_tx, |uri, _conf| {
            assert_eq!(uri, "/node");
        }),
    );

    let plane = plane_builder.build().expect("Invalid plane definition.");
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8080);

    let resolve = HashMap::new();
    let remotes = HashMap::new();

    let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();

    let (networking, networking_task) = TestConnections::new(resolve, remotes, incoming_rx);
    let websockets = TestWs::default();
    let config = SwimServerConfig::default();

    let server = SwimServer::new(plane, addr, networking, websockets, config);

    let (server_task, handle) = server.run();
    let context = TestContext {
        report_rx,
        handle,
        incoming_tx,
    };

    let net = networking_task.run();

    let test_task = test_case(context);

    let (_, task_result, result) =
        tokio::time::timeout(TEST_TIMEOUT, join3(net, server_task, test_task))
            .await
            .expect("Test timed out.");
    (task_result, result)
}

#[tokio::test]
async fn server_clean_shutdown() {
    let (result, _) = run_server(|mut context| async move {
        context.handle.stop();
    })
    .await;
    assert!(result.is_ok());
}
