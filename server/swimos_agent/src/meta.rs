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

use std::collections::HashMap;

use swimos_api::agent::AgentConfig;
use swimos_utilities::routing::route_uri::RouteUri;

/// Metadata to describe a running agent instance.
#[derive(Clone, Copy, Debug)]
pub struct AgentMetadata<'a> {
    // The URI of the instance.
    path: &'a RouteUri,
    // Parameters extracted from the route URI.
    route_params: &'a HashMap<String, String>,
    // Specific configuration for the instance.
    configuration: &'a AgentConfig,
}

impl<'a> AgentMetadata<'a> {
    pub fn new(
        path: &'a RouteUri,
        route_params: &'a HashMap<String, String>,
        configuration: &'a AgentConfig,
    ) -> Self {
        AgentMetadata {
            path,
            route_params,
            configuration,
        }
    }

    pub fn agent_uri(&self) -> &'a RouteUri {
        self.path
    }

    pub fn get_param(&self, name: &str) -> Option<&'a str> {
        self.route_params.get(name).map(String::as_str)
    }

    pub fn agent_configuration(&self) -> &'a AgentConfig {
        self.configuration
    }
}
