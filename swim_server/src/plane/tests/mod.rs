// Copyright 2015-2021 SWIM.AI inc.
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

use crate::plane::lifecycle::PlaneLifecycle;
use crate::plane::router::{PlaneRouter, PlaneRouterFactory};
use crate::plane::spec::{PlaneSpec, RouteSpec};
use crate::plane::tests::fixture::{ReceiveAgentRoute, SendAgentRoute, TestLifecycle};
use crate::plane::{AgentRoute, ContextImpl, EnvChannel, PlaneActiveRoutes, RouteResolver};
use crate::routing::TopLevelServerRouterFactory;
use futures::future::join;
use std::sync::Arc;
use std::time::Duration;
use swim_client::configuration::downlink::{ClientParams, ConfigHierarchy};
use swim_client::connections::SwimConnPool;
use swim_client::downlink::Downlinks;
use swim_client::interface::DownlinksContext;
use swim_client::router::ClientRouterFactory;
use swim_common::routing::Router;
use swim_runtime::time::clock::Clock;
use swim_runtime::time::timeout;
use tokio::sync::mpsc;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::route_pattern::RoutePattern;
use utilities::sync::{promise, trigger};

mod fixture;

fn make_spec<Clk: Clock, Delegate: Router + 'static>() -> (
    PlaneSpec<Clk, EnvChannel, PlaneRouter<Delegate>>,
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
        },
        rx,
    )
}

#[tokio::test]
async fn plane_event_loop() {
    let (spec, done_rx) = make_spec();
    let (context_tx, context_rx) = mpsc::channel(8);

    let (stop_tx, stop_rx) = promise::promise();
    let config = fixture::make_config();

    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let (client_tx, _client_rx) = mpsc::channel(8);
    let top_level_router_fac =
        TopLevelServerRouterFactory::new(context_tx.clone(), client_tx, remote_tx);

    let context = ContextImpl::new(context_tx.clone(), spec.routes());

    let PlaneSpec { routes, lifecycle } = spec;

    let (client_tx, client_rx) = mpsc::channel(8);
    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let (plane_tx, _plane_rx) = mpsc::channel(8);
    let (_close_tx, close_rx) = promise::promise();

    let top_level_factory =
        TopLevelServerRouterFactory::new(plane_tx, client_tx.clone(), remote_tx);

    let client_router_fac = ClientRouterFactory::new(client_tx.clone(), top_level_factory);

    let (conn_pool, _pool_task) = SwimConnPool::new(
        ClientParams::default(),
        (client_tx, client_rx),
        client_router_fac,
        close_rx.clone(),
    );

    let (downlinks, _downlinks_task) =
        Downlinks::new(conn_pool, Arc::new(ConfigHierarchy::default()), close_rx);

    let client = DownlinksContext::new(downlinks);

    let resolver = RouteResolver::new(
        swim_runtime::time::clock::runtime_clock(),
        client,
        config,
        routes,
        PlaneRouterFactory::new(context_tx, top_level_router_fac.clone()),
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
