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

use super::AgentLifecycle;
use crate::agent::AgentContext;
use crate::meta::log::NodeLogger;
use futures::future::BoxFuture;
use futures::Stream;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use swim_client::interface::SwimClient;
use swim_common::warp::path::Path;
use utilities::sync::trigger::Receiver;
use utilities::uri::RelativeUri;

struct TestAgent;

struct TestContext;

impl AgentContext<TestAgent> for TestContext {
    fn client(&self) -> SwimClient<Path> {
        unimplemented!()
    }

    fn schedule<Effect, Str, Sch>(&self, _effects: Str, _schedule: Sch) -> BoxFuture<'_, ()>
    where
        Effect: Future<Output = ()> + Send + 'static,
        Str: Stream<Item = Effect> + Send + 'static,
        Sch: Stream<Item = Duration> + Send + 'static,
    {
        panic!("Default lifecycles should do nothing.")
    }

    fn agent(&self) -> &TestAgent {
        panic!("Default lifecycles should do nothing.")
    }

    fn node_uri(&self) -> &RelativeUri {
        panic!("Default lifecycles should do nothing.")
    }

    fn agent_stop_event(&self) -> Receiver {
        panic!("Default lifecycles should do nothing.")
    }

    fn parameter(&self, _key: &str) -> Option<&String> {
        None
    }

    fn parameters(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    fn logger(&self) -> NodeLogger {
        panic!("Unexpected log event")
    }
}

#[tokio::test]
async fn default_agent_lifecycle() {
    let context = TestContext;
    let lifecycle = ();

    //We just check the life-cycle events don't generate panics.
    lifecycle.starting(&context).await;
}
