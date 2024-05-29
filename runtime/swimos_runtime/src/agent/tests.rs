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

use std::{collections::HashMap, time::Duration};

use futures::{future::BoxFuture, Future, FutureExt};
use swimos_api::{
    agent::{Agent, AgentConfig, AgentContext, AgentInitResult, LaneConfig, WarpLaneKind},
    error::{AgentInitError, AgentTaskError},
    persistence::StoreDisabled,
};
use swimos_form::structural::read::ReadError;
use swimos_model::Text;
use swimos_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    routing::route_uri::RouteUri,
    trigger,
};
use tokio::sync::mpsc;

use crate::agent::{AgentExecError, AgentRoute, AgentRouteChannels};

use super::AgentRouteTask;

use uuid::Uuid;

use super::DisconnectionReason;

#[test]
fn disconnection_reason_display() {
    assert_eq!(
        DisconnectionReason::AgentStoppedExternally.to_string(),
        "Agent stopped externally."
    );
    assert_eq!(
        DisconnectionReason::AgentTimedOut.to_string(),
        "Agent stopped after a period of inactivity."
    );
    assert_eq!(
        DisconnectionReason::RemoteTimedOut.to_string(),
        "The remote was pruned due to inactivity."
    );
    assert_eq!(
        DisconnectionReason::ChannelClosed.to_string(),
        "The remote stopped listening."
    );
    assert_eq!(
        DisconnectionReason::Failed.to_string(),
        "The agent task was dropped or the connection was never established."
    );
    assert_eq!(
        DisconnectionReason::DuplicateRegistration(Uuid::from_u128(84772)).to_string(),
        "The remote registration for 00000000-0000-0000-0000-000000014b24 was replaced."
    );
}

#[derive(Debug)]
enum TestAgent {
    Init,
    Running,
}

const LANE_NAME: &str = "lane";

impl Agent for TestAgent {
    fn run(
        &self,
        _route: RouteUri,
        _route_params: HashMap<String, String>,
        _config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        match self {
            TestAgent::Init => {
                async move { Err(AgentInitError::DuplicateLane(Text::new("Oh dear."))) }.boxed()
            }
            TestAgent::Running => async move {
                let config = LaneConfig {
                    transient: true,
                    ..Default::default()
                };
                let io = context
                    .add_lane(LANE_NAME, WarpLaneKind::Value, config)
                    .await
                    .expect("Registering lane failed.");
                Ok(agent_task(context, io).boxed())
            }
            .boxed(),
        }
    }
}

async fn agent_task(
    _context: Box<dyn AgentContext + Send>,
    _lane: (ByteWriter, ByteReader),
) -> Result<(), AgentTaskError> {
    Err(AgentTaskError::DeserializationFailed(
        ReadError::IncompleteRecord,
    ))
}

async fn with_timeout<F>(f: F) -> F::Output
where
    F: Future,
{
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .expect("Test timed out.")
}

#[tokio::test]
async fn test_agent_failure() {
    with_timeout(async {
        let agent = TestAgent::Running;
        let identity = AgentRoute {
            identity: Uuid::from_u128(1),
            route: "/node".parse().unwrap(),
            route_params: HashMap::new(),
        };
        let (_attachment_tx, attachment_rx) = mpsc::channel(16);
        let (_http_tx, http_rx) = mpsc::channel(16);
        let (downlink_tx, _downlink_rx) = mpsc::channel(16);
        let (_stopping_tx, stopping_rx) = trigger::trigger();

        let task = AgentRouteTask::new(
            &agent,
            identity,
            AgentRouteChannels::new(attachment_rx, http_rx, downlink_tx),
            stopping_rx,
            Default::default(),
            None,
        );

        assert!(matches!(
            task.run_agent().await,
            Err(AgentExecError::FailedTask(
                AgentTaskError::DeserializationFailed(_)
            ))
        ));
    })
    .await
}

#[tokio::test]
async fn test_agent_failure_with_store() {
    with_timeout(async {
        let agent = TestAgent::Running;
        let identity = AgentRoute {
            identity: Uuid::from_u128(1),
            route: "/node".parse().unwrap(),
            route_params: HashMap::new(),
        };
        let (_attachment_tx, attachment_rx) = mpsc::channel(16);
        let (_http_tx, http_rx) = mpsc::channel(16);
        let (downlink_tx, _downlink_rx) = mpsc::channel(16);
        let (_stopping_tx, stopping_rx) = trigger::trigger();

        let task = AgentRouteTask::new(
            &agent,
            identity,
            AgentRouteChannels::new(attachment_rx, http_rx, downlink_tx),
            stopping_rx,
            Default::default(),
            None,
        );

        let store = async { Ok(StoreDisabled) };

        assert!(matches!(
            task.run_agent_with_store(store).await,
            Err(AgentExecError::FailedTask(
                AgentTaskError::DeserializationFailed(_)
            ))
        ));
    })
    .await
}

#[tokio::test]
async fn test_agent_init_failure() {
    with_timeout(async {
        let agent = TestAgent::Init;
        let identity = AgentRoute {
            identity: Uuid::from_u128(1),
            route: "/node".parse().unwrap(),
            route_params: HashMap::new(),
        };
        let (_attachment_tx, attachment_rx) = mpsc::channel(16);
        let (_http_tx, http_rx) = mpsc::channel(16);
        let (downlink_tx, _downlink_rx) = mpsc::channel(16);
        let (_stopping_tx, stopping_rx) = trigger::trigger();

        let task = AgentRouteTask::new(
            &agent,
            identity,
            AgentRouteChannels::new(attachment_rx, http_rx, downlink_tx),
            stopping_rx,
            Default::default(),
            None,
        );

        let result = task.run_agent().await;
        assert!(matches!(
            result,
            Err(AgentExecError::FailedInit(AgentInitError::DuplicateLane(_)))
        ));
    })
    .await
}
