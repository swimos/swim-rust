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

use crate::plane::error::NoAgentAtRoute;
use futures::future::BoxFuture;
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;
use utilities::route_pattern::RoutePattern;
use utilities::uri::RelativeUri;

/// The context that is available in [`PlaneLifecycle`] event handlers.
pub trait PlaneContext: Send + Sync {
    /// Get a reference to an agent in this plane.
    ///
    /// #Notes
    ///
    /// This will cause the agent to start if has not already.
    fn get_agent<'a>(
        &'a mut self,
        route: RelativeUri,
    ) -> BoxFuture<'a, Result<Arc<dyn Any + Send + Sync>, NoAgentAtRoute>>;

    /// Get all of the routes in this plane.
    fn routes(&self) -> &Vec<RoutePattern>;

    /// Get an instantaneous snapshot of the routes currently active on this plane.
    fn active_routes(&mut self) -> BoxFuture<HashSet<RelativeUri>>;
}
