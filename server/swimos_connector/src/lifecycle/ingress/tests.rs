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

use std::sync::Arc;

use futures::stream::unfold;
use parking_lot::Mutex;
use swimos_agent::{
    agent_lifecycle::{on_start::OnStart, on_stop::OnStop},
    event_handler::{
        ActionContext, EventHandler, HandlerAction, HandlerActionExt, SideEffect, StepResult,
    },
    AgentMetadata,
};
use swimos_utilities::trigger;
use thiserror::Error;

use crate::{
    connector::BaseConnector, lifecycle::fixture::run_handle_with_futs, ConnectorAgent,
    ConnectorInitError, ConnectorStream, IngressConnector, IngressConnectorLifecycle,
    IngressContext,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Event {
    Start,
    Init,
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

impl BaseConnector for TestConnector {
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

impl IngressConnector for TestConnector {
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

    fn initialize(&self, _context: &mut dyn IngressContext) {
        self.inner.lock().push(Event::Init);
    }
}

#[tokio::test]
async fn connector_lifecycle_start() {
    let connector = TestConnector::default();
    let lifecycle = IngressConnectorLifecycle::new(connector.clone());
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
    let lifecycle = IngressConnectorLifecycle::new(connector.clone());
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

    let lifecycle = IngressConnectorLifecycle::new(connector.clone());
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

    let lifecycle = IngressConnectorLifecycle::new(connector.clone());
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

    let lifecycle = IngressConnectorLifecycle::new(connector.clone());
    let handler = lifecycle.on_start();
    let agent = ConnectorAgent::default();
    let result = run_handle_with_futs(&agent, handler).await;

    let err = result.expect_err("Should fail.");
    err.downcast::<TestError>().expect("Unexpected error type.");
}
