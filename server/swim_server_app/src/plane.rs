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
use swim_utilities::routing::route_pattern::RoutePattern;

use crate::{error::AmbiguousRoutes, util::AgentExt};

#[derive(Default)]
pub struct PlaneModel {
    pub(crate) routes: Vec<(RoutePattern, BoxAgent)>,
}

#[derive(Default)]
pub struct PlaneBuilder {
    model: PlaneModel,
}

impl PlaneBuilder {
    pub fn build(self) -> Result<PlaneModel, AmbiguousRoutes> {
        let PlaneBuilder {
            model: PlaneModel { routes },
        } = self;
        let template = routes.iter().map(|(r, _)| r).enumerate();

        let left = template.clone();

        let mut ambiguous = HashSet::new();

        for (i, p) in left {
            let right = template.clone().skip(i);
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
            Ok(PlaneModel { routes })
        }
    }

    pub fn add_route<A: Agent + Send + 'static>(&mut self, pattern: RoutePattern, agent: A) {
        self.model.routes.push((pattern, agent.boxed()));
    }
}
