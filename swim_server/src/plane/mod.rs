// Copyright 2015-2020 SWIM.AI inc.
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

pub mod context;
pub mod lifecycle;

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::{AgentParameters, AgentResult, SwimAgent};
use crate::plane::lifecycle::PlaneLifecycle;
use crate::routing::{ServerRouter, TaggedEnvelope};
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use pin_utils::core_reexport::fmt::Formatter;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use swim_runtime::time::clock::Clock;
use url::Url;
use utilities::route_pattern::RoutePattern;

pub trait AgentRoute<Clk, Envelopes, Router>: Debug {
    fn run_agent(
        &self,
        url: Url,
        parameters: HashMap<String, String>,
        execution_config: AgentExecutionConfig,
        clock: Clk,
        incoming_envelopes: Envelopes,
        router: Router,
    ) -> BoxFuture<'static, AgentResult>;

    fn boxed(self) -> BoxAgentRoute<Clk, Envelopes, Router>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

type BoxAgentRoute<Clk, Envelopes, Router> = Box<dyn AgentRoute<Clk, Envelopes, Router>>;

#[derive(Debug)]
struct AgentProvider<Agent, Config, Lifecycle> {
    _agent_type: PhantomData<fn(Config) -> Agent>,
    configuration: Config,
    lifecycle: Lifecycle,
}

impl<Agent, Config, Lifecycle> AgentProvider<Agent, Config, Lifecycle>
where
    Config: Debug,
    Agent: SwimAgent<Config> + Debug,
    Lifecycle: AgentLifecycle<Agent> + Debug,
{
    fn new(configuration: Config, lifecycle: Lifecycle) -> Self {
        AgentProvider {
            _agent_type: PhantomData,
            configuration,
            lifecycle,
        }
    }
}

impl<Clk, Envelopes, Router, Agent, Config, Lifecycle> AgentRoute<Clk, Envelopes, Router>
    for AgentProvider<Agent, Config, Lifecycle>
where
    Clk: Clock,
    Envelopes: Stream<Item = TaggedEnvelope> + Send + 'static,
    Router: ServerRouter + Clone + 'static,
    Agent: SwimAgent<Config> + Send + Sync + Debug + 'static,
    Config: Send + Sync + Clone + Debug + 'static,
    Lifecycle: AgentLifecycle<Agent> + Send + Sync + Clone + Debug + 'static,
{
    fn run_agent(
        &self,
        url: Url,
        parameters: HashMap<String, String>,
        execution_config: AgentExecutionConfig,
        clock: Clk,
        incoming_envelopes: Envelopes,
        router: Router,
    ) -> BoxFuture<'static, AgentResult> {
        let AgentProvider {
            configuration,
            lifecycle,
            ..
        } = self;

        let parameters =
            AgentParameters::new(configuration.clone(), execution_config, url, parameters);

        crate::agent::run_agent(
            lifecycle.clone(),
            clock,
            parameters,
            incoming_envelopes,
            router,
        )
        .boxed()
    }
}

#[derive(Debug)]
pub struct PlaneSpec<Clk, Envelopes, Router> {
    routes: Vec<(RoutePattern, BoxAgentRoute<Clk, Envelopes, Router>)>,
    lifecycle: Option<Box<dyn PlaneLifecycle>>,
}

#[derive(Debug)]
pub struct PlaneBuilder<Clk, Envelopes, Router>(PlaneSpec<Clk, Envelopes, Router>);

impl<Clk, Envelopes, Router> Default for PlaneBuilder<Clk, Envelopes, Router> {
    fn default() -> Self {
        PlaneBuilder(PlaneSpec {
            routes: vec![],
            lifecycle: None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AmbiguousRoutes(RoutePattern, RoutePattern);

impl Display for AmbiguousRoutes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Routes '{}' and '{}' are ambiguous.", &self.0, &self.1)
    }
}

impl Error for AmbiguousRoutes {}

impl<Clk, Envelopes, Router> PlaneBuilder<Clk, Envelopes, Router>
where
    Clk: Clock,
    Envelopes: Stream<Item = TaggedEnvelope> + Send + 'static,
    Router: ServerRouter + Clone + 'static,
{
    pub fn add_route<Agent, Config, Lifecycle>(
        &mut self,
        route: RoutePattern,
        config: Config,
        lifecycle: Lifecycle,
    ) -> Result<(), AmbiguousRoutes>
    where
        Agent: SwimAgent<Config> + Send + Sync + Debug + 'static,
        Config: Send + Sync + Clone + Debug + 'static,
        Lifecycle: AgentLifecycle<Agent> + Send + Sync + Clone + Debug + 'static,
    {
        let PlaneBuilder(PlaneSpec { routes, .. }) = self;
        for (existing_route, _) in routes.iter() {
            if RoutePattern::are_ambiguous(existing_route, &route) {
                return Err(AmbiguousRoutes(existing_route.clone(), route));
            }
        }
        routes.push((route, AgentProvider::new(config, lifecycle).boxed()));
        Ok(())
    }

    pub fn build(self) -> PlaneSpec<Clk, Envelopes, Router> {
        self.0
    }

    pub fn build_with_lifecycle(
        mut self,
        custom_lc: Box<dyn PlaneLifecycle>,
    ) -> PlaneSpec<Clk, Envelopes, Router> {
        self.0.lifecycle = Some(custom_lc);
        self.0
    }
}
