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

use crate::{
    event_handler::{ActionContext, EventHandlerError, HandlerAction, SideEffect, StepResult},
    meta::AgentMetadata,
    test_context::{no_downlink, DummyAgentContext},
};
use futures::{stream::FuturesUnordered, StreamExt};
use swim_api::agent::AgentConfig;
use swim_utilities::{routing::route_uri::RouteUri, trigger};
use tokio::sync::mpsc;

use super::{HandlerFuture, Spawner, Suspend};

const CONFIG: AgentConfig = AgentConfig::DEFAULT;
const NODE_URI: &str = "/node";

fn make_uri() -> RouteUri {
    RouteUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta<'a>(
    uri: &'a RouteUri,
    route_params: &'a HashMap<String, String>,
) -> AgentMetadata<'a> {
    AgentMetadata::new(uri, route_params, &CONFIG)
}

struct NoSpawn;

impl<Context> Spawner<Context> for NoSpawn {
    fn spawn_suspend(&self, _: HandlerFuture<Context>) {
        panic!("No suspended futures expected.");
    }
}

struct DummyAgent;

#[tokio::test]
async fn suspend_future() {
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let mut join_value_init = HashMap::new();

    let (tx, mut rx) = mpsc::channel(4);
    let (done_tx, done_rx) = trigger::trigger();

    let mut suspend = Suspend::new(async move {
        let _ = tx.send(45).await;
        SideEffect::from(move || {
            let _ = done_tx.trigger();
        })
    });

    let mut spawner = FuturesUnordered::new();

    let result = suspend.step(
        &mut ActionContext::new(
            &spawner,
            &DummyAgentContext,
            &no_downlink,
            &mut join_value_init,
        ),
        meta,
        &DummyAgent,
    );

    assert!(matches!(
        result,
        StepResult::Complete {
            modified_item: None,
            ..
        }
    ));

    tokio::time::timeout(Duration::from_secs(5), async move {
        let mut handler = spawner.next().await.expect("Future was not suspended.");
        assert!(spawner.is_empty());

        assert_eq!(rx.recv().await, Some(45));

        let result = handler.step(
            &mut ActionContext::new(
                &spawner,
                &DummyAgentContext,
                &no_downlink,
                &mut join_value_init,
            ),
            meta,
            &DummyAgent,
        );
        assert!(matches!(
            result,
            StepResult::Complete {
                modified_item: None,
                ..
            }
        ));
        assert!(spawner.is_empty());
        assert!(done_rx.await.is_ok());

        let result = suspend.step(
            &mut ActionContext::new(
                &spawner,
                &DummyAgentContext,
                &no_downlink,
                &mut join_value_init,
            ),
            meta,
            &DummyAgent,
        );
        assert!(matches!(
            result,
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        ));

        assert!(spawner.is_empty());
    })
    .await
    .expect("Timed out.");
}
