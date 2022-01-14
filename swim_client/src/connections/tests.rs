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

use futures::FutureExt;
use futures_util::future::BoxFuture;
use std::time::Duration;
use url::Url;

use swim_runtime::error::{ConnectionError, ResolutionError, RouterError};
use swim_runtime::routing::RouterFactory;
use swim_runtime::routing::{Route, Router, RoutingAddr};
use swim_utilities::routing::uri::RelativeUri;

struct MockRouterFactory;
impl RouterFactory for MockRouterFactory {
    type Router = MockRouter;

    fn create_for(&self, _addr: RoutingAddr) -> Self::Router {
        MockRouter
    }

    fn lookup(
        &mut self,
        _host: Option<Url>,
        _route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
        async {
            Err(RouterError::ConnectionFailure(
                ConnectionError::WriteTimeout(Duration::from_secs(10)),
            ))
        }
        .boxed()
    }
}

struct MockRouter;

impl Router for MockRouter {
    fn resolve_sender(&mut self, _addr: RoutingAddr) -> BoxFuture<Result<Route, ResolutionError>> {
        unimplemented!()
    }

    fn lookup(
        &mut self,
        _host: Option<Url>,
        _route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
        async {
            Err(RouterError::ConnectionFailure(
                ConnectionError::WriteTimeout(Duration::from_secs(10)),
            ))
        }
        .boxed()
    }
}
