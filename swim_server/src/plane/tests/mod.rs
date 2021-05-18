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
use crate::plane::router::PlaneRouter;
use crate::plane::spec::{PlaneSpec, RouteSpec};
use crate::plane::store::mock::MockPlaneStore;
use crate::plane::tests::fixture::{ReceiveAgentRoute, SendAgentRoute, TestLifecycle};
use crate::plane::{AgentRoute, EnvChannel};
use crate::routing::{ServerRouter, TopLevelRouterFactory};
use futures::future::join;
use std::time::Duration;
use swim_runtime::time::clock::Clock;
use swim_runtime::time::timeout;
use tokio::sync::mpsc;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::route_pattern::RoutePattern;
use utilities::sync::trigger;

mod fixture;

fn make_spec<Clk: Clock, Delegate: ServerRouter + 'static>() -> (
    PlaneSpec<Clk, EnvChannel, PlaneRouter<Delegate>, MockPlaneStore>,
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
    let (spec, done_rx) = make_spec();
    let (context_tx, context_rx) = mpsc::channel(8);

    let (stop_tx, stop_rx) = trigger::trigger();
    let config = fixture::make_config();

    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let top_level_router_fac = TopLevelRouterFactory::new(context_tx.clone(), remote_tx);

    let plane_task = super::run_plane(
        config,
        swim_runtime::time::clock::runtime_clock(),
        spec,
        stop_rx,
        OpenEndedFutures::new(),
        (context_tx, context_rx),
        top_level_router_fac,
    );

    let completion_task = async move {
        let result = timeout::timeout(Duration::from_secs(10), done_rx).await;
        match result {
            Ok(Err(_)) => panic!("Completion trigger dropped."),
            Err(_) => panic!("Plane timeout out."),
            _ => {}
        }
        stop_tx.trigger();
    };

    join(plane_task, completion_task).await;
}
