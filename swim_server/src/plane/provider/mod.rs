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

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::{AgentParameters, AgentResult, SwimAgent};
use crate::plane::{AgentInternals, AgentRoute, RouteAndParameters};
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use server_store::agent::NodeStore;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use swim_async_runtime::time::clock::Clock;
use swim_client::interface::ClientContext;
use swim_model::path::Path;
use swim_runtime::routing::TaggedEnvelope;
use swim_runtime::routing::TaggedRouter;

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

    pub fn run<Clk, Envelopes, Store>(
        &self,
        agent_parameters: AgentParameters<Config>,
        clock: Clk,
        client_context: ClientContext<Path>,
        incoming_envelopes: Envelopes,
        router: TaggedRouter<Path>,
        store: Store,
    ) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>)
    where
        Clk: Clock,
        Envelopes: Stream<Item = TaggedEnvelope> + Send + Unpin + 'static,
        Store: NodeStore,
    {
        let AgentProvider { lifecycle, .. } = self;
        use swim_runtime::compat;
        let messages =
            compat::stop_on_failed(compat::messages_from_envelopes(incoming_envelopes), None);

        let (agent, task) = crate::agent::run_agent(
            lifecycle.clone(),
            clock,
            client_context,
            agent_parameters,
            messages,
            router,
            store,
        );
        (agent, task.boxed())
    }
}

impl<Clk, Envelopes, Agent, Config, Lifecycle, Store> AgentRoute<Clk, Envelopes, Store>
    for AgentProvider<Agent, Config, Lifecycle>
where
    Clk: Clock,
    Envelopes: Stream<Item = TaggedEnvelope> + Send + Unpin + 'static,
    Agent: SwimAgent<Config> + Send + Sync + Debug + 'static,
    Config: Send + Sync + Clone + Debug + 'static,
    Lifecycle: AgentLifecycle<Agent> + Send + Sync + Clone + Debug + 'static,
    Store: NodeStore,
{
    fn run_agent(
        &self,
        route: RouteAndParameters,
        execution_config: AgentExecutionConfig,
        agent_internals: AgentInternals<Clk, Envelopes, Store>,
    ) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>) {
        let AgentInternals {
            clock,
            client_context,
            incoming_envelopes,
            router,
            store,
        } = agent_internals;

        let RouteAndParameters {
            route: uri,
            parameters,
        } = route;

        let parameters = AgentParameters::new(
            self.configuration.clone(),
            execution_config,
            uri,
            parameters,
        );

        self.run(
            parameters,
            clock,
            client_context,
            incoming_envelopes,
            router,
            store,
        )
    }
}
