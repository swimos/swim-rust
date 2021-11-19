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

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::{AgentResult, AgentTaskResult};
use crate::plane::context::PlaneContext;
use crate::plane::lifecycle::PlaneLifecycle;
use crate::plane::router::PlaneRouter;
use crate::plane::{AgentInternals, AgentRoute, EnvChannel, RouteAndParameters};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use http::Uri;
use parking_lot::Mutex;
use pin_utils::pin_mut;
use server_store::agent::SwimNodeStore;
use server_store::plane::mock::MockPlaneStore;
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;
use swim_async_runtime::time::clock::Clock;
use swim_runtime::routing::{Router, TaggedEnvelope};
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger;
use swim_warp::envelope::Envelope;

#[derive(Debug)]
pub struct SendAgent(String);
#[derive(Debug)]
pub struct ReceiveAgent(String);

#[derive(Debug)]
pub struct SendAgentRoute(String);

impl SendAgentRoute {
    pub fn new(target: String) -> Self {
        SendAgentRoute(target)
    }
}

#[derive(Debug)]
pub struct ReceiveAgentRoute {
    expected_id: String,
    done: Arc<Mutex<Option<trigger::Sender>>>,
}

impl ReceiveAgentRoute {
    pub fn new(expected_id: String, done: trigger::Sender) -> Self {
        ReceiveAgentRoute {
            expected_id,
            done: Arc::new(Mutex::new(Some(done))),
        }
    }
}

#[derive(Debug)]
pub struct TestLifecycle;

pub fn make_config() -> AgentExecutionConfig {
    let buffer_size = non_zero_usize!(8);
    AgentExecutionConfig {
        max_pending_envelopes: 1,
        action_buffer: buffer_size,
        update_buffer: buffer_size,
        feedback_buffer: buffer_size,
        uplink_err_buffer: buffer_size,
        max_fatal_uplink_errors: 0,
        max_uplink_start_attempts: non_zero_usize!(1),
        lane_buffer: buffer_size,
        observation_buffer: buffer_size,
        lane_attachment_buffer: buffer_size,
        yield_after: non_zero_usize!(2048),
        retry_strategy: Default::default(),
        cleanup_timeout: Duration::from_secs(1),
        scheduler_buffer: buffer_size,
        value_lane_backpressure: None,
        map_lane_backpressure: None,
        node_log: Default::default(),
        metrics: Default::default(),
        max_idle_time: Duration::from_secs(60),
        max_store_errors: 0,
    }
}

pub const PARAM_NAME: &str = "id";
pub const SENDER_PREFIX: &str = "sender";
pub const RECEIVER_PREFIX: &str = "receiver";
const LANE_NAME: &str = "receiver_lane";
const MESSAGE: &str = "ping!";

impl<Clk: Clock, Delegate: Router + 'static>
    AgentRoute<Clk, EnvChannel, PlaneRouter<Delegate>, SwimNodeStore<MockPlaneStore>>
    for SendAgentRoute
{
    fn run_agent(
        &self,
        route: RouteAndParameters,
        execution_config: AgentExecutionConfig,
        agent_internals: AgentInternals<
            Clk,
            EnvChannel,
            PlaneRouter<Delegate>,
            SwimNodeStore<MockPlaneStore>,
        >,
    ) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>) {
        let AgentInternals {
            incoming_envelopes,
            mut router,
            ..
        } = agent_internals;

        let RouteAndParameters { route, parameters } = route;

        let id = parameters[PARAM_NAME].clone();
        let target = self.0.clone();
        let agent = Arc::new(SendAgent(id.clone()));
        let expected_route: RelativeUri = format!("/{}/{}", SENDER_PREFIX, id).parse().unwrap();
        assert_eq!(route, expected_route);
        assert_eq!(execution_config, make_config());

        let task = async move {
            let target_node: RelativeUri =
                format!("/{}/{}", RECEIVER_PREFIX, target).parse().unwrap();
            let addr = router.lookup(None, target_node.clone()).await.unwrap();
            let mut tx = router.resolve_sender(addr).await.unwrap().sender;
            assert!(tx
                .send_item(
                    Envelope::event()
                        .node_uri(target_node.to_string())
                        .lane_uri(LANE_NAME)
                        .body(MESSAGE)
                        .done()
                )
                .await
                .is_ok());
            pin_mut!(incoming_envelopes);
            while incoming_envelopes.next().await.is_some() {}
            AgentResult {
                route,
                dispatcher_task: AgentTaskResult::default(),
                store_task: AgentTaskResult::default(),
            }
        }
        .boxed();
        (agent, task)
    }
}

impl<Clk: Clock, Delegate>
    AgentRoute<Clk, EnvChannel, PlaneRouter<Delegate>, SwimNodeStore<MockPlaneStore>>
    for ReceiveAgentRoute
{
    fn run_agent(
        &self,
        route: RouteAndParameters,
        execution_config: AgentExecutionConfig,
        agent_internals: AgentInternals<
            Clk,
            EnvChannel,
            PlaneRouter<Delegate>,
            SwimNodeStore<MockPlaneStore>,
        >,
    ) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>) {
        let AgentInternals {
            incoming_envelopes, ..
        } = agent_internals;

        let RouteAndParameters { route, parameters } = route;

        let ReceiveAgentRoute { expected_id, done } = self;
        let mut done_sender = done.lock().take();
        assert!(done_sender.is_some());

        let id = parameters[PARAM_NAME].clone();
        let expected_target = expected_id.clone();
        assert_eq!(id, expected_target);
        let agent = Arc::new(ReceiveAgent(id.clone()));
        let expected_route: Uri = format!("/{}/{}", RECEIVER_PREFIX, id).parse().unwrap();
        assert_eq!(route, expected_route);
        assert_eq!(execution_config, make_config());
        let task = async move {
            pin_mut!(incoming_envelopes);

            let expected_envelope = Envelope::event()
                .node_uri(expected_route.to_string())
                .lane_uri(LANE_NAME)
                .body(MESSAGE)
                .done();

            let mut times_seen = 0;

            while let Some(TaggedEnvelope(_, env)) = incoming_envelopes.next().await {
                if env == expected_envelope {
                    times_seen += 1;
                    if let Some(tx) = done_sender.take() {
                        tx.trigger();
                    }
                } else {
                    panic!("Expected {:?}, received {:?}.", expected_envelope, env);
                }
            }
            AgentResult {
                route,
                dispatcher_task: AgentTaskResult {
                    errors: Default::default(),
                    failed: times_seen != 1,
                },
                store_task: AgentTaskResult::default(),
            }
        }
        .boxed();
        (agent, task)
    }
}

impl PlaneLifecycle for TestLifecycle {
    fn on_start<'a>(&'a mut self, context: &'a mut dyn PlaneContext) -> BoxFuture<'a, ()> {
        async move {
            let result = context
                .get_agent_ref(format!("/{}/hello", SENDER_PREFIX).parse().unwrap())
                .await;
            assert!(result.is_ok());
            let agent = result.unwrap();
            if let Ok(agent) = agent.downcast::<SendAgent>() {
                assert_eq!(agent.0, "hello");
            } else {
                panic!("Unexpected agent type.");
            }
        }
        .boxed()
    }
}
