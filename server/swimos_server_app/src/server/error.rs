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

use swimos_utilities::routing::route_uri::RouteUri;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UnresolvableRoute {
    #[error("No agent at route: {uri}")]
    NoRoute { uri: RouteUri },
    #[error("Server is stopped or stopping.")]
    Stopped,
}

impl UnresolvableRoute {
    pub fn new(uri: RouteUri) -> Self {
        UnresolvableRoute::NoRoute { uri }
    }
}
