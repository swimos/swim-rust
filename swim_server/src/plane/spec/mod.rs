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

use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::SwimAgent;
use crate::plane::lifecycle::PlaneLifecycle;
use crate::plane::provider::AgentProvider;
use crate::plane::{AgentRoute, BoxAgentRoute};
use crate::routing::{ServerRouter, TaggedEnvelope};
use futures::Stream;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use swim_runtime::time::clock::Clock;
use utilities::route_pattern::RoutePattern;

#[derive(Debug)]
pub(super) struct RouteSpec<Clk, Envelopes, Router> {
    pub pattern: RoutePattern,
    pub agent_route: BoxAgentRoute<Clk, Envelopes, Router>,
}

impl<Clk, Envelopes, Router> RouteSpec<Clk, Envelopes, Router> {
    fn new(pattern: RoutePattern, agent_route: BoxAgentRoute<Clk, Envelopes, Router>) -> Self {
        RouteSpec {
            pattern,
            agent_route,
        }
    }
}

#[derive(Debug)]
pub struct PlaneSpec<Clk, Envelopes, Router> {
    pub(super) routes: Vec<RouteSpec<Clk, Envelopes, Router>>,
    pub(super) lifecycle: Option<Box<dyn PlaneLifecycle>>,
}

impl<Clk, Envelopes, Router> PlaneSpec<Clk, Envelopes, Router> {
    pub fn routes(&self) -> Vec<RoutePattern> {
        self.routes
            .iter()
            .map(|RouteSpec { pattern, .. }| pattern.clone())
            .collect()
    }
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
        for RouteSpec {
            pattern: existing_route,
            ..
        } in routes.iter()
        {
            if RoutePattern::are_ambiguous(existing_route, &route) {
                return Err(AmbiguousRoutes(existing_route.clone(), route));
            }
        }
        routes.push(RouteSpec::new(
            route,
            AgentProvider::new(config, lifecycle).boxed(),
        ));
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
