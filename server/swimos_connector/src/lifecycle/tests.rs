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

use std::{collections::HashMap, sync::Arc};

use bytes::BytesMut;
use futures::{
    stream::{unfold, FuturesUnordered},
    StreamExt,
};
use parking_lot::Mutex;
use swimos_agent::{
    agent_lifecycle::{on_start::OnStart, on_stop::OnStop},
    agent_model::downlink::BoxDownlinkChannelFactory,
    event_handler::{
        ActionContext, DownlinkSpawnOnDone, LinkSpawner, EventHandler, EventHandlerError,
        HandlerAction, HandlerActionExt, HandlerFuture, LaneSpawnOnDone, LaneSpawner, SideEffect,
        Spawner, StepResult
    },
    AgentMetadata,
};
use swimos_api::{address::Address, agent::WarpLaneKind, error::{CommanderRegistrationError, DynamicRegistrationError}};
use swimos_model::Text;
use swimos_utilities::trigger;
use thiserror::Error;

use crate::{
    test_support::{make_meta, make_uri},
    Connector, ConnectorAgent, ConnectorInitError, ConnectorLifecycle, ConnectorStream,
};

#[derive(Default)]
struct TestSpawner {
    futures: FuturesUnordered<HandlerFuture<ConnectorAgent>>,
}

impl Spawner<ConnectorAgent> for TestSpawner {
    fn spawn_suspend(&self, fut: HandlerFuture<ConnectorAgent>) {
        self.futures.push(fut);
    }
}

impl LinkSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_downlink(
        &self,
        _path: Address<Text>,
        _make_channel: BoxDownlinkChannelFactory<ConnectorAgent>,
        _on_done: DownlinkSpawnOnDone<ConnectorAgent>,
    ) {
        panic!("Spawning downlinks not supported.");
    }

    fn register_commander(&self, _path: Address<Text> ) -> Result<u16, CommanderRegistrationError> {
        panic!("Registering commanders not supported.");
    }
}

impl LaneSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_warp_lane(
        &self,
        _name: &str,
        _kind: WarpLaneKind,
        _on_done: LaneSpawnOnDone<ConnectorAgent>,
    ) -> Result<(), DynamicRegistrationError> {
        panic!("Spawning lanes not supported.");
    }
}

async fn run_handle_with_futs<H>(
    agent: &ConnectorAgent,
    handler: H,
) -> Result<(), Box<dyn std::error::Error + Send>>
where
    H: EventHandler<ConnectorAgent>,
{
    let mut spawner = TestSpawner::default();
    run_handler(&spawner, agent, handler)?;
    while !spawner.futures.is_empty() {
        match spawner.futures.next().await {
            Some(h) => {
                run_handler(&spawner, agent, h)?;
            }
            None => break,
        }
    }
    Ok(())
}

fn run_handler<H>(
    spawner: &TestSpawner,
    agent: &ConnectorAgent,
    mut handler: H,
) -> Result<(), Box<dyn std::error::Error + Send>>
where
    H: EventHandler<ConnectorAgent>,
{
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut join_lane_init = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();

    let mut action_context = ActionContext::new(
        spawner,
        spawner,
        spawner,
        &mut join_lane_init,
        &mut ad_hoc_buffer,
    );

    loop {
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { .. } => {}
            StepResult::Fail(EventHandlerError::EffectError(err)) => return Err(err),
            StepResult::Fail(err) => panic!("{:?}", err),
            StepResult::Complete { .. } => {
                break;
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Event {
    Start,
    Stop,
    Item(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    DropTrigger,
    StreamInit,
    StreamRun,
}

#[derive(Clone, Default)]
struct TestConnector {
    failure: Option<Failure>,
    inner: Arc<Mutex<Vec<Event>>>,
}

impl TestConnector {
    fn make_handler(&self, event: Event) -> TestHandler {
        TestHandler {
            events: self.inner.clone(),
            event,
        }
    }
}

#[derive(Copy, Clone, Default, Debug, Error)]
#[error("Failed")]
struct TestError;

struct TestHandler {
    events: Arc<Mutex<Vec<Event>>>,
    event: Event,
}

impl HandlerAction<ConnectorAgent> for TestHandler {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<ConnectorAgent>,
        _meta: AgentMetadata,
        _context: &ConnectorAgent,
    ) -> StepResult<Self::Completion> {
        self.events.lock().push(self.event);
        StepResult::done(())
    }
}

impl Connector for TestConnector {
    type StreamError = TestError;

    fn create_stream(&self) -> Result<impl ConnectorStream<Self::StreamError>, Self::StreamError> {
        if self.failure == Some(Failure::StreamInit) {
            Err(TestError)
        } else {
            let fail = self.failure == Some(Failure::StreamRun);
            Ok(Box::pin(unfold(
                (self.clone(), 0),
                move |(c, i)| async move {
                    if fail {
                        Some((Err(TestError), (c, i)))
                    } else if i < 4 {
                        Some((Ok(c.make_handler(Event::Item(i))), (c, i + 1)))
                    } else {
                        None
                    }
                },
            )))
        }
    }

    fn on_start(&self, init_complete: trigger::Sender) -> impl EventHandler<ConnectorAgent> + '_ {
        let drop_trigger = self.failure == Some(Failure::DropTrigger);
        self.make_handler(Event::Start)
            .followed_by(SideEffect::from(move || {
                if drop_trigger {
                    drop(init_complete);
                } else {
                    let _ = init_complete.trigger();
                }
            }))
    }

    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        self.make_handler(Event::Stop)
    }
}

#[tokio::test]
async fn connector_lifecycle_start() {
    let connector = TestConnector::default();
    let lifecycle = ConnectorLifecycle::new(connector.clone());
    let handler = lifecycle.on_start();
    let agent = ConnectorAgent::default();
    assert!(run_handle_with_futs(&agent, handler).await.is_ok());

    let events = connector.inner.lock().clone();
    assert_eq!(
        events,
        vec![
            Event::Start,
            Event::Item(0),
            Event::Item(1),
            Event::Item(2),
            Event::Item(3)
        ]
    );
}

#[tokio::test]
async fn connector_lifecycle_stop() {
    let connector = TestConnector::default();
    let lifecycle = ConnectorLifecycle::new(connector.clone());
    let handler = lifecycle.on_stop();
    let agent = ConnectorAgent::default();
    assert!(run_handle_with_futs(&agent, handler).await.is_ok());

    let events = connector.inner.lock().clone();
    assert_eq!(events, vec![Event::Stop]);
}

#[tokio::test]
async fn connector_lifecycle_drop_trigger() {
    let connector = TestConnector {
        failure: Some(Failure::DropTrigger),
        ..Default::default()
    };

    let lifecycle = ConnectorLifecycle::new(connector.clone());
    let handler = lifecycle.on_start();
    let agent = ConnectorAgent::default();
    let result = run_handle_with_futs(&agent, handler).await;

    let err = result.expect_err("Should fail.");
    err.downcast::<ConnectorInitError>()
        .expect("Unexpected error type.");
}

#[tokio::test]
async fn connector_lifecycle_fail_init() {
    let connector = TestConnector {
        failure: Some(Failure::StreamInit),
        ..Default::default()
    };

    let lifecycle = ConnectorLifecycle::new(connector.clone());
    let handler = lifecycle.on_start();
    let agent = ConnectorAgent::default();
    let result = run_handle_with_futs(&agent, handler).await;

    let err = result.expect_err("Should fail.");
    err.downcast::<TestError>().expect("Unexpected error type.");
}

#[tokio::test]
async fn connector_lifecycle_fail_stream() {
    let connector = TestConnector {
        failure: Some(Failure::StreamRun),
        ..Default::default()
    };

    let lifecycle = ConnectorLifecycle::new(connector.clone());
    let handler = lifecycle.on_start();
    let agent = ConnectorAgent::default();
    let result = run_handle_with_futs(&agent, handler).await;

    let err = result.expect_err("Should fail.");
    err.downcast::<TestError>().expect("Unexpected error type.");
}
