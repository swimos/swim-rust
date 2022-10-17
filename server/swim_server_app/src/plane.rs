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

use std::collections::HashSet;

use swim_api::agent::{Agent, BoxAgent};
use swim_model::Text;
use swim_utilities::routing::route_pattern::RoutePattern;

use crate::{error::AmbiguousRoutes, util::AgentExt};

/// A plane is a collection of agents which are all served by a single TCP listener. This mode
/// describes all of the kinds of agents that are defined in the lane and maps them to URI routes.
pub struct PlaneModel {
    pub(crate) name: Text,
    pub(crate) routes: Vec<(RoutePattern, BoxAgent)>,
}

/// A builder that will construct a [`PlaneModel`]. The consistency of the routes that are supplied
/// is only checked when the `build` method is called.
pub struct PlaneBuilder {
    model: PlaneModel,
}

impl PlaneBuilder {
    pub fn with_name(name: &str) -> Self {
        PlaneBuilder {
            model: PlaneModel {
                name: Text::new(name),
                routes: Default::default(),
            },
        }
    }

    /// Attempt to construct the model. If any of the routes that were provided are ambiguous,
    /// this will fail.
    pub fn build(self) -> Result<PlaneModel, AmbiguousRoutes> {
        let PlaneBuilder {
            model: PlaneModel { name, routes },
        } = self;
        let template = routes.iter().map(|(r, _)| r).enumerate();

        let left = template.clone();

        let mut ambiguous = HashSet::new();

        for (i, p) in left {
            let right = template.clone().skip(i + 1);
            for (j, q) in right {
                if RoutePattern::are_ambiguous(p, q) {
                    ambiguous.insert(i);
                    ambiguous.insert(j);
                }
            }
        }

        if !ambiguous.is_empty() {
            let bad = routes
                .into_iter()
                .enumerate()
                .filter_map(|(i, (r, _))| {
                    if ambiguous.contains(&i) {
                        Some(r)
                    } else {
                        None
                    }
                })
                .collect();
            Err(AmbiguousRoutes::new(bad))
        } else {
            Ok(PlaneModel { name, routes })
        }
    }

    /// Add a new route to the builder. This does not check that the route is not ambiguous
    /// with respect to the already added routes.
    ///
    /// #Arguments
    /// * `pattern` - The route pattern for matching the node URI of incoming envelopes.
    /// * `agent` - The agent type to be started each time the route matches.
    pub fn add_route<A: Agent + Send + 'static>(&mut self, pattern: RoutePattern, agent: A) {
        self.model.routes.push((pattern, agent.boxed()));
    }
}

#[cfg(test)]
mod tests {
    use futures::future::BoxFuture;
    use swim_api::agent::{Agent, AgentConfig, AgentContext, AgentInitResult};
    use swim_utilities::routing::{route_pattern::RoutePattern, uri::RelativeUri};

    use crate::error::AmbiguousRoutes;

    use super::PlaneModel;

    struct DummyAgent;

    impl Agent for DummyAgent {
        fn run(
            &self,
            _route: RelativeUri,
            _config: AgentConfig,
            _context: Box<dyn AgentContext + Send>,
        ) -> BoxFuture<'static, AgentInitResult> {
            panic!("Not runnable.");
        }
    }

    #[test]
    fn single_route() {
        let mut builder = super::PlaneBuilder::with_name("plane");
        let route = RoutePattern::parse_str("/node").expect("Bad route.");
        builder.add_route(route.clone(), DummyAgent);

        let PlaneModel { name, routes } = builder.build().expect("Building plane failed.");

        assert_eq!(name, "plane");
        match routes.as_slice() {
            [(pattern, _)] => {
                assert_eq!(pattern, &route);
            }
            _ => panic!("Wrong number of routes."),
        }
    }

    #[test]
    fn two_disjoint_routes() {
        let mut builder = super::PlaneBuilder::with_name("plane");
        let route1 = RoutePattern::parse_str("/node").expect("Bad route.");
        let route2 = RoutePattern::parse_str("/other/:id").expect("Bad route.");
        builder.add_route(route1.clone(), DummyAgent);
        builder.add_route(route2.clone(), DummyAgent);

        let PlaneModel { name, routes } = builder.build().expect("Building plane failed.");

        assert_eq!(name, "plane");
        match routes.as_slice() {
            [(pattern1, _), (pattern2, _)] => {
                assert!(
                    (pattern1 == &route1 && pattern2 == &route2)
                        || (pattern1 == &route2 && pattern2 == &route1)
                );
            }
            _ => panic!("Wrong number of routes."),
        }
    }

    #[test]
    fn two_ambiguous_routes() {
        let mut builder = super::PlaneBuilder::with_name("plane");
        let route1 = RoutePattern::parse_str("/node").expect("Bad route.");
        let route2 = RoutePattern::parse_str("/:id").expect("Bad route.");
        builder.add_route(route1.clone(), DummyAgent);
        builder.add_route(route2.clone(), DummyAgent);

        let AmbiguousRoutes { routes } = builder.build().err().expect("Building plane succeeded.");

        match routes.as_slice() {
            [pattern1, pattern2] => {
                assert!(
                    (pattern1 == &route1 && pattern2 == &route2)
                        || (pattern1 == &route2 && pattern2 == &route1)
                );
            }
            _ => panic!("Wrong number of routes."),
        }
    }

    #[test]
    fn two_ambiguous_with_good_routes() {
        let mut builder = super::PlaneBuilder::with_name("plane");
        let route1 = RoutePattern::parse_str("/first/node").expect("Bad route.");
        let route2 = RoutePattern::parse_str("/first/:id").expect("Bad route.");
        let route3 = RoutePattern::parse_str("/second").expect("Bad route.");
        builder.add_route(route1.clone(), DummyAgent);
        builder.add_route(route2.clone(), DummyAgent);
        builder.add_route(route3, DummyAgent);

        let AmbiguousRoutes { routes } = builder.build().err().expect("Building plane succeeded.");

        match routes.as_slice() {
            [pattern1, pattern2] => {
                assert!(
                    (pattern1 == &route1 && pattern2 == &route2)
                        || (pattern1 == &route2 && pattern2 == &route1)
                );
            }
            _ => panic!("Wrong number of routes."),
        }
    }
}
