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

use crate::test_support::{fail, run_handler, TestSpawner};
use crate::{ConnectorAgent, ConnectorStream};
use bytes::BytesMut;
use futures::StreamExt;
use parking_lot::Mutex;
use std::{convert::Infallible, sync::Arc};
use swimos_agent::event_handler::{ActionContext, HandlerAction, StepResult};
use swimos_agent::AgentMetadata;

#[derive(Debug)]
struct Handler {
    collector: Arc<Mutex<Vec<usize>>>,
    n: usize,
}

impl Handler {
    fn new(collector: &Arc<Mutex<Vec<usize>>>, n: usize) -> Self {
        Handler {
            collector: collector.clone(),
            n,
        }
    }
}

impl HandlerAction<ConnectorAgent> for Handler {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<ConnectorAgent>,
        _meta: AgentMetadata,
        _context: &ConnectorAgent,
    ) -> StepResult<Self::Completion> {
        self.collector.lock().push(self.n);
        StepResult::done(())
    }
}

fn make_stream(state: &Arc<Mutex<Vec<usize>>>) -> impl ConnectorStream<Infallible> + 'static {
    let handlers = vec![
        Ok(Handler::new(state, 1)),
        Ok(Handler::new(state, 2)),
        Ok(Handler::new(state, 3)),
    ];
    futures::stream::iter(handlers)
}

#[tokio::test]
async fn drive_connector_stream() {
    let state = Arc::new(Mutex::new(vec![]));
    let mut spawner = TestSpawner::default();
    let agent = ConnectorAgent::default();
    let handler = super::suspend_connector(make_stream(&state));

    run_handler(&spawner, &mut BytesMut::new(), &agent, handler, fail);

    let mut n = 0;
    while !spawner.is_empty() {
        n += 1;
        let h = spawner.next().await.expect("Expected future.");
        run_handler(&spawner, &mut BytesMut::new(), &agent, h, fail);
    }

    assert_eq!(n, 4);
    let guard = state.lock();
    assert_eq!(guard.as_ref(), vec![1, 2, 3])
}
