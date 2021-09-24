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

use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::SwimAgent;
use crate::plane::error::AmbiguousRoutes;
use crate::plane::lifecycle::PlaneLifecycle;
use crate::plane::provider::AgentProvider;
use crate::plane::store::PlaneStore;
use crate::plane::{AgentRoute, BoxAgentRoute};
use crate::routing::{ServerRouter, TaggedEnvelope};
use futures::Stream;
use std::fmt::Debug;
use swim_route::route_pattern::RoutePattern;
use swim_runtime::time::clock::Clock;

#[cfg(test)]
mod tests;

/// Specification of an agent route in a plane. The route pattern describes a parameterized
/// family of agents, all of which share an implementation.
#[derive(Debug)]
pub(super) struct RouteSpec<Clk, Envelopes, Router, Store> {
    pub pattern: RoutePattern,
    pub agent_route: BoxAgentRoute<Clk, Envelopes, Router, Store>,
}

impl<Clk, Envelopes, Router, Store> RouteSpec<Clk, Envelopes, Router, Store> {
    pub(super) fn new(
        pattern: RoutePattern,
        agent_route: BoxAgentRoute<Clk, Envelopes, Router, Store>,
    ) -> Self {
        RouteSpec {
            pattern,
            agent_route,
        }
    }
}

/// A specification of a plane, consisting of the defined routes and an optional custom lifecycle
/// for the plane.
#[derive(Debug)]
pub struct PlaneSpec<Clk, Envelopes, Router, Store>
where
    Store: PlaneStore,
{
    pub(super) routes: Vec<RouteSpec<Clk, Envelopes, Router, Store::NodeStore>>,
    pub(super) lifecycle: Option<Box<dyn PlaneLifecycle>>,
    pub(super) store: Store,
}

impl<Clk, Envelopes, Router, Store> PlaneSpec<Clk, Envelopes, Router, Store>
where
    Store: PlaneStore,
{
    pub fn routes(&self) -> Vec<RoutePattern> {
        self.routes
            .iter()
            .map(|RouteSpec { pattern, .. }| pattern.clone())
            .collect()
    }
}

/// Builder to create a valid plane specification. Agent routes are added successively with
/// ambiguous routes generating an error. The specification can then be constructed with or
/// without a place lifecycle.
#[derive(Debug)]
pub struct PlaneBuilder<Clk, Envelopes, Router, Store>(PlaneSpec<Clk, Envelopes, Router, Store>)
where
    Store: PlaneStore;

impl<Clk, Envelopes, Router, Store> PlaneBuilder<Clk, Envelopes, Router, Store>
where
    Clk: Clock,
    Envelopes: Stream<Item = TaggedEnvelope> + Send + 'static,
    Router: ServerRouter + Clone + 'static,
    Store: PlaneStore,
{
    pub fn new(store: Store) -> Self {
        PlaneBuilder(PlaneSpec {
            routes: vec![],
            lifecycle: None,
            store,
        })
    }

    /// Attempt to add a new agent route to the plane.
    ///
    /// #Arguments
    ///
    /// * `route` - The parameterized route pattern.
    /// * `config` - Configuration to instantiate the agent.
    /// * `lifecycle` - The agent lifecycle.
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
                return Err(AmbiguousRoutes::new(existing_route.clone(), route));
            }
        }
        routes.push(RouteSpec::new(
            route,
            AgentProvider::new(config, lifecycle).boxed(),
        ));
        Ok(())
    }

    /// Construct the specification without adding a plane lifecycle.
    pub fn build(self) -> PlaneSpec<Clk, Envelopes, Router, Store> {
        self.0
    }

    /// Construct the specification adding a plane lifecycle.
    pub fn build_with_lifecycle(
        mut self,
        custom_lc: Box<dyn PlaneLifecycle>,
    ) -> PlaneSpec<Clk, Envelopes, Router, Store> {
        self.0.lifecycle = Some(custom_lc);
        self.0
    }
}
