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

use crate::interface::ServerDownlinksConfig;
use crate::plane::lifecycle::PlaneLifecycle;
use crate::plane::spec::RouteSpec;
use crate::plane::tests::fixture::{ReceiveAgentRoute, SendAgentRoute, TestLifecycle};
use crate::plane::{
    AgentRoute, ContextImpl, EnvChannel, PlaneActiveRoutes, PlaneSpec, RouteResolver,
};
use futures::future::join;
use server_store::plane::mock::MockPlaneStore;
use std::sync::Arc;
use std::time::Duration;
use swim_async_runtime::time::clock::Clock;
use swim_async_runtime::time::timeout;
use swim_client::connections::SwimConnPool;
use swim_client::downlink::Downlinks;
use swim_client::interface::ClientContext;
use swim_runtime::configuration::DownlinkConnectionsConfig;
use swim_runtime::remote::router::Router;
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::future::open_ended::OpenEndedFutures;
use swim_utilities::routing::route_pattern::RoutePattern;
use swim_utilities::trigger;
use swim_utilities::trigger::promise;
use tokio::sync::mpsc;

mod fixture;

fn make_spec<Clk: Clock>() -> (
    PlaneSpec<Clk, EnvChannel, MockPlaneStore>,
    trigger::Receiver,
) {
    let send_pattern = RoutePattern::parse(
        format!("/{}/:{}", fixture::SENDER_PREFIX, fixture::PARAM_NAME).chars(),
    )
    .unwrap();
    let receive_pattern = RoutePattern::parse(
        format!("/{}/:{}", fixture::RECEIVER_PREFIX, fixture::PARAM_NAME).chars(),
    )
    .unwrap();

    let sender = RouteSpec::new(
        send_pattern,
        SendAgentRoute::new("target".to_string()).boxed(),
    );

    let (tx, rx) = trigger::trigger();

    let reciever = RouteSpec::new(
        receive_pattern,
        ReceiveAgentRoute::new("target".to_string(), tx).boxed(),
    );

    let lifecycle = TestLifecycle;
    (
        PlaneSpec {
            routes: vec![sender, reciever],
            lifecycle: Some(lifecycle.boxed()),
            store: MockPlaneStore,
        },
        rx,
    )
}

#[tokio::test]
async fn plane_event_loop() {
    let (mut spec, done_rx) = make_spec();
    let (context_tx, context_rx) = mpsc::channel(8);

    let (stop_tx, stop_rx) = promise::promise();
    let config = fixture::make_config();
    let context = ContextImpl::new(context_tx.clone(), spec.routes());
    let lifecycle = spec.take_lifecycle();

    let (client_tx, client_rx) = mpsc::channel(8);
    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let (plane_tx, _plane_rx) = mpsc::channel(8);
    let (_close_tx, close_rx) = promise::promise();

    let router = Router::server(client_tx.clone(), plane_tx.clone(), remote_tx);

    let (conn_pool, _pool_task) = SwimConnPool::new(
        DownlinkConnectionsConfig::default(),
        (client_tx, client_rx),
        router.clone(),
        close_rx.clone(),
    );

    let (downlinks, _downlinks_task) = Downlinks::new(
        non_zero_usize!(8),
        conn_pool,
        Arc::new(ServerDownlinksConfig::default()),
        close_rx,
    );

    let client = ClientContext::new(downlinks);

    let resolver = RouteResolver::new(
        swim_async_runtime::time::clock::runtime_clock(),
        client,
        config,
        spec,
        router,
        stop_rx.clone(),
        PlaneActiveRoutes::default(),
    );

    let plane_task = super::run_plane(
        resolver,
        lifecycle,
        context,
        stop_rx,
        OpenEndedFutures::new(),
        context_rx,
    );

    let completion_task = async move {
        let result = timeout::timeout(Duration::from_secs(10), done_rx).await;
        match result {
            Ok(Err(_)) => panic!("Completion trigger dropped."),
            Err(_) => panic!("Plane timeout out."),
            _ => {}
        }

        let (result_tx, _result_rx) = mpsc::channel(8);
        stop_tx.provide(result_tx).unwrap();
    };

    join(plane_task, completion_task).await;
}
