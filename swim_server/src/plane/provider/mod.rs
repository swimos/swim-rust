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

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::{AgentParameters, AgentResult, SwimAgent};
use crate::plane::AgentRoute;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use swim_client::interface::SwimClient;
use swim_common::routing::{Router, TaggedEnvelope};
use swim_common::warp::path::Path;
use swim_runtime::time::clock::Clock;
use utilities::uri::RelativeUri;

/// [`AgentRoute`] implementation that spawns agents with a fixed configuration.
pub struct AgentProvider<Agent, Config, Lifecycle> {
    _agent_type: PhantomData<fn(Config) -> Agent>,
    configuration: Config,
    lifecycle: Lifecycle,
}

impl<Agent, Config: Debug, Lifecycle: Debug> Debug for AgentProvider<Agent, Config, Lifecycle> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let AgentProvider {
            configuration,
            lifecycle,
            ..
        } = self;
        f.debug_struct("AgentProvider")
            .field("configuration", configuration)
            .field("lifecycle", lifecycle)
            .finish()
    }
}

impl<Agent, Config, Lifecycle> AgentProvider<Agent, Config, Lifecycle>
where
    Agent: SwimAgent<Config> + Send + Sync + Debug + 'static,
    Config: Send + Sync + Clone + Debug + 'static,
    Lifecycle: AgentLifecycle<Agent> + Send + Sync + Clone + Debug + 'static,
{
    pub fn new(configuration: Config, lifecycle: Lifecycle) -> Self {
        AgentProvider {
            _agent_type: PhantomData,
            configuration,
            lifecycle,
        }
    }

    pub fn run<Clk, Envelopes, R>(
        &self,
        uri: RelativeUri,
        parameters: HashMap<String, String>,
        execution_config: AgentExecutionConfig,
        clock: Clk,
        client: SwimClient<Path>,
        incoming_envelopes: Envelopes,
        router: R,
    ) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>)
    where
        Clk: Clock,
        Envelopes: Stream<Item = TaggedEnvelope> + Send + 'static,
        R: Router + Clone + 'static,
    {
        let AgentProvider {
            configuration,
            lifecycle,
            ..
        } = self;

        let parameters =
            AgentParameters::new(configuration.clone(), execution_config, uri, parameters);

        let (agent, task) = crate::agent::run_agent(
            lifecycle.clone(),
            clock,
            client,
            parameters,
            incoming_envelopes,
            router,
        );
        (agent, task.boxed())
    }
}

impl<Clk, Envelopes, R, Agent, Config, Lifecycle> AgentRoute<Clk, Envelopes, R>
    for AgentProvider<Agent, Config, Lifecycle>
where
    Clk: Clock,
    Envelopes: Stream<Item = TaggedEnvelope> + Send + 'static,
    R: Router + Clone + 'static,
    Agent: SwimAgent<Config> + Send + Sync + Debug + 'static,
    Config: Send + Sync + Clone + Debug + 'static,
    Lifecycle: AgentLifecycle<Agent> + Send + Sync + Clone + Debug + 'static,
{
    fn run_agent(
        &self,
        uri: RelativeUri,
        parameters: HashMap<String, String>,
        execution_config: AgentExecutionConfig,
        clock: Clk,
        client: SwimClient<Path>,
        incoming_envelopes: Envelopes,
        router: R,
    ) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>) {
        self.run(
            uri,
            parameters,
            execution_config,
            clock,
            client,
            incoming_envelopes,
            router,
        )
    }
}
