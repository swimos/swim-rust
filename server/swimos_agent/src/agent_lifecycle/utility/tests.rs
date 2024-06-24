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

use std::{collections::HashMap, sync::Arc, time::Duration};

use bytes::BytesMut;
use futures::{future::ready, stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use swimos_api::agent::AgentConfig;
use swimos_utilities::routing::RouteUri;

use crate::{
    event_handler::{ActionContext, HandlerAction, LocalBoxEventHandler, StepResult},
    meta::AgentMetadata,
    test_context::{no_downlink, DummyAgentContext},
};

use super::HandlerContext;

struct Fake;

struct FakeHandler {
    n: i32,
    inner: Option<Arc<Mutex<Vec<i32>>>>,
}

impl HandlerAction<Fake> for FakeHandler {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Fake>,
        _meta: AgentMetadata,
        _context: &Fake,
    ) -> StepResult<Self::Completion> {
        if let Some(state) = self.inner.take() {
            state.lock().push(self.n);
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}

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

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn suspend_repeatedly() {
    let state = Arc::new(Mutex::new(vec![]));
    let state_cpy = state.clone();

    let context: HandlerContext<Fake> = HandlerContext::default();

    let mut i = 0;
    let mut handler: LocalBoxEventHandler<'static, Fake> =
        Box::new(context.suspend_repeatedly(Duration::from_secs(1), move || {
            let n = i;
            i += 1;
            Some(ready(if n < 10 {
                Some(FakeHandler {
                    n,
                    inner: Some(state.clone()),
                })
            } else {
                None
            }))
        }));

    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);
    let mut spawner = FuturesUnordered::new();
    let mut join_lane_init = HashMap::new();
    let mut ad_hoc_buffer = BytesMut::new();

    loop {
        loop {
            match handler.step(
                &mut ActionContext::new(
                    &spawner,
                    &DummyAgentContext,
                    &no_downlink,
                    &mut join_lane_init,
                    &mut ad_hoc_buffer,
                ),
                meta,
                &Fake,
            ) {
                StepResult::Continue { .. } => {}
                StepResult::Fail(err) => panic!("Failed: {}", err),
                StepResult::Complete { .. } => break,
            }
        }

        if !spawner.is_empty() {
            if let Some(h) = spawner.next().await {
                handler = h;
            } else {
                break;
            }
        } else {
            break;
        }
    }
    let values = std::mem::take(&mut *state_cpy.lock());
    let expected: Vec<i32> = (0..10).collect();
    assert_eq!(values, expected);
}
