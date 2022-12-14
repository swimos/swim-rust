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

use std::time::Duration;

use crate::{
    event_handler::{ActionContext, EventHandlerError, HandlerAction, SideEffect, StepResult},
    meta::AgentMetadata,
    test_context::{no_downlink, DummyAgentContext},
};
use futures::{stream::FuturesUnordered, StreamExt};
use swim_api::agent::AgentConfig;
use swim_utilities::{routing::uri::RelativeUri, trigger};
use tokio::sync::mpsc;

use super::{HandlerFuture, Spawner, Suspend};

const CONFIG: AgentConfig = AgentConfig {};
const NODE_URI: &str = "/node";

fn make_uri() -> RelativeUri {
    RelativeUri::try_from(NODE_URI).expect("Bad URI.")
}

fn make_meta(uri: &RelativeUri) -> AgentMetadata<'_> {
    AgentMetadata::new(uri, &CONFIG)
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
    let meta = make_meta(&uri);

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
        ActionContext::new(&spawner, &DummyAgentContext, &no_downlink),
        meta,
        &DummyAgent,
    );

    assert!(matches!(
        result,
        StepResult::Complete {
            modified_lane: None,
            ..
        }
    ));

    tokio::time::timeout(Duration::from_secs(5), async move {
        let mut handler = spawner.next().await.expect("Future was not suspended.");
        assert!(spawner.is_empty());

        assert_eq!(rx.recv().await, Some(45));

        let result = handler.step(
            ActionContext::new(&spawner, &DummyAgentContext, &no_downlink),
            meta,
            &DummyAgent,
        );
        assert!(matches!(
            result,
            StepResult::Complete {
                modified_lane: None,
                ..
            }
        ));
        assert!(spawner.is_empty());
        assert!(done_rx.await.is_ok());

        let result = suspend.step(
            ActionContext::new(&spawner, &DummyAgentContext, &no_downlink),
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
