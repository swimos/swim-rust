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

use crate::agent::dispatch::error::DispatcherErrors;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::AgentResult;
use crate::plane::context::PlaneContext;
use crate::plane::lifecycle::PlaneLifecycle;
use crate::plane::router::PlaneRouter;
use crate::plane::{AgentRoute, EnvChannel};
use crate::routing::{ServerRouter, TaggedEnvelope};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use http::Uri;
use parking_lot::Mutex;
use pin_utils::pin_mut;
use std::any::Any;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use swim_common::warp::envelope::Envelope;
use swim_runtime::time::clock::Clock;
use utilities::sync::trigger;
use utilities::uri::RelativeUri;

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
    let buffer_size = NonZeroUsize::new(8).unwrap();
    AgentExecutionConfig {
        max_pending_envelopes: 1,
        action_buffer: buffer_size,
        update_buffer: buffer_size,
        feedback_buffer: buffer_size,
        uplink_err_buffer: buffer_size,
        max_fatal_uplink_errors: 0,
        max_uplink_start_attempts: NonZeroUsize::new(1).unwrap(),
        lane_buffer: buffer_size,
        observation_buffer: buffer_size,
        lane_attachment_buffer: buffer_size,
        yield_after: NonZeroUsize::new(2048).unwrap(),
        retry_strategy: Default::default(),
        cleanup_timeout: Duration::from_secs(1),
        scheduler_buffer: buffer_size,
        value_lane_backpressure: None,
        map_lane_backpressure: None,
        max_idle_time: Duration::from_secs(60),
    }
}

pub const PARAM_NAME: &str = "id";
pub const SENDER_PREFIX: &str = "sender";
pub const RECEIVER_PREFIX: &str = "receiver";
const LANE_NAME: &str = "receiver_lane";
const MESSAGE: &str = "ping!";

impl<Clk: Clock, Delegate: ServerRouter + 'static>
    AgentRoute<Clk, EnvChannel, PlaneRouter<Delegate>> for SendAgentRoute
{
    fn run_agent(
        &self,
        uri: RelativeUri,
        parameters: HashMap<String, String>,
        execution_config: AgentExecutionConfig,
        _clock: Clk,
        incoming_envelopes: EnvChannel,
        mut router: PlaneRouter<Delegate>,
    ) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>) {
        let id = parameters[PARAM_NAME].clone();
        let target = self.0.clone();
        let agent = Arc::new(SendAgent(id.clone()));
        let expected_route: RelativeUri = format!("/{}/{}", SENDER_PREFIX, id).parse().unwrap();
        assert_eq!(uri, expected_route);
        assert_eq!(execution_config, make_config());
        let task = async move {
            let target_node: RelativeUri =
                format!("/{}/{}", RECEIVER_PREFIX, target).parse().unwrap();
            let addr = router.lookup(None, target_node.clone()).await.unwrap();
            let mut tx = router.resolve_sender(addr).await.unwrap().sender;
            assert!(tx
                .send_item(Envelope::make_event(
                    target_node.to_string(),
                    LANE_NAME.to_string(),
                    Some(MESSAGE.into())
                ))
                .await
                .is_ok());
            pin_mut!(incoming_envelopes);
            while incoming_envelopes.next().await.is_some() {}
            AgentResult {
                route: uri,
                dispatcher_errors: DispatcherErrors::default(),
                failed: false,
            }
        }
        .boxed();
        (agent, task)
    }
}

impl<Clk: Clock, Delegate> AgentRoute<Clk, EnvChannel, PlaneRouter<Delegate>>
    for ReceiveAgentRoute
{
    fn run_agent(
        &self,
        uri: RelativeUri,
        parameters: HashMap<String, String>,
        execution_config: AgentExecutionConfig,
        _clock: Clk,
        incoming_envelopes: EnvChannel,
        _router: PlaneRouter<Delegate>,
    ) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>) {
        let ReceiveAgentRoute { expected_id, done } = self;
        let mut done_sender = done.lock().take();
        assert!(done_sender.is_some());

        let id = parameters[PARAM_NAME].clone();
        let expected_target = expected_id.clone();
        assert_eq!(id, expected_target);
        let agent = Arc::new(ReceiveAgent(id.clone()));
        let expected_route: Uri = format!("/{}/{}", RECEIVER_PREFIX, id).parse().unwrap();
        assert_eq!(uri, expected_route);
        assert_eq!(execution_config, make_config());
        let task = async move {
            pin_mut!(incoming_envelopes);

            let expected_envelope = Envelope::make_event(
                expected_route.to_string(),
                LANE_NAME.to_string(),
                Some(MESSAGE.into()),
            );

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
                route: uri,
                dispatcher_errors: DispatcherErrors::default(),
                failed: times_seen != 1,
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
