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

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::{AgentContext, DynamicAgentIo, DynamicLaneTasks, SwimAgent};
use crate::plane::context::PlaneContext;
use crate::plane::error::AmbiguousRoutes;
use crate::plane::lifecycle::PlaneLifecycle;
use crate::plane::router::PlaneRouter;
use crate::plane::spec::{PlaneBuilder, PlaneSpec, RouteSpec};
use futures::future::{ready, BoxFuture, Ready};
use futures::FutureExt;
use server_store::agent::NodeStore;
use server_store::plane::mock::MockPlaneStore;
use std::time::Duration;
use swim_async_runtime::time::clock::Clock;
use swim_runtime::error::ResolutionError;
use swim_utilities::routing::route_pattern::RoutePattern;
use swim_utilities::routing::uri::RelativeUri;
use tokio_stream::wrappers::ReceiverStream;
use url::Url;

#[derive(Default, Clone, Debug)]
struct DummyClock;

impl Clock for DummyClock {
    type DelayFuture = Ready<()>;

    fn delay(&self, _duration: Duration) -> Self::DelayFuture {
        ready(())
    }
}

type BuilderType = PlaneBuilder<
    DummyClock,
    ReceiverStream<TaggedEnvelope>,
    PlaneRouter<DummyDelegate>,
    MockPlaneStore,
>;

#[derive(Debug)]
struct DummyAgent;

#[derive(Clone, Debug)]
struct DummyConfig(i32);

#[derive(Clone, Debug)]
struct DummyLifecycle(i32);

#[derive(Clone, Debug)]
struct DummyPlaneLifecycle(i32);

#[derive(Clone, Debug)]
struct DummyDelegate;

impl Router for DummyDelegate {
    fn resolve_sender(
        &mut self,
        _addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        unimplemented!()
    }

    fn lookup(
        &mut self,
        _host: Option<Url>,
        _route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        unimplemented!()
    }
}

impl SwimAgent<DummyConfig> for DummyAgent {
    fn instantiate<Context, Store>(
        _configuration: &DummyConfig,
        _exec_conf: &AgentExecutionConfig,
        _store: Store,
    ) -> (
        Self,
        DynamicLaneTasks<Self, Context>,
        DynamicAgentIo<Context>,
    )
    where
        Context: AgentContext<Self> + AgentExecutionContext + Send + Sync + 'static,
        Store: NodeStore,
    {
        panic!("Called unexpectedly.");
    }
}

impl AgentLifecycle<DummyAgent> for DummyLifecycle {
    fn starting<'a, C>(&'a self, _context: &'a C) -> BoxFuture<'a, ()>
    where
        C: AgentContext<DummyAgent> + Send + Sync + 'a,
    {
        ready(()).boxed()
    }
}

impl PlaneLifecycle for DummyPlaneLifecycle {
    fn on_start<'a>(&'a mut self, _context: &'a mut dyn PlaneContext) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn on_stop(&mut self) -> BoxFuture<()> {
        ready(()).boxed()
    }
}

#[test]
fn plane_builder_single_route() {
    let pat = RoutePattern::parse_str("/:id").unwrap();

    let mut builder: BuilderType = PlaneBuilder::new(MockPlaneStore);
    assert!(builder
        .add_route(pat.clone(), DummyConfig(1), DummyLifecycle(1))
        .is_ok());

    let PlaneSpec {
        routes, lifecycle, ..
    } = builder.build();
    assert!(lifecycle.is_none());
    assert_eq!(routes.len(), 1);
    let RouteSpec {
        pattern,
        agent_route,
    } = &routes[0];
    assert_eq!(pattern, &pat);
    let debug = format!("{:?}", agent_route);
    assert_eq!(
        debug,
        "AgentProvider { configuration: DummyConfig(1), lifecycle: DummyLifecycle(1) }"
    );
}

#[test]
fn plane_builder_two_routes() {
    let pat1 = RoutePattern::parse_str("/a").unwrap();
    let pat2 = RoutePattern::parse_str("/b").unwrap();

    let mut builder: BuilderType = PlaneBuilder::new(MockPlaneStore);
    assert!(builder
        .add_route(pat1.clone(), DummyConfig(1), DummyLifecycle(1))
        .is_ok());
    assert!(builder
        .add_route(pat2.clone(), DummyConfig(2), DummyLifecycle(2))
        .is_ok());

    let PlaneSpec {
        routes, lifecycle, ..
    } = builder.build();
    assert!(lifecycle.is_none());
    assert_eq!(routes.len(), 2);
    let RouteSpec {
        pattern,
        agent_route,
    } = &routes[0];
    assert_eq!(pattern, &pat1);
    let debug = format!("{:?}", agent_route);
    assert_eq!(
        debug,
        "AgentProvider { configuration: DummyConfig(1), lifecycle: DummyLifecycle(1) }"
    );

    let RouteSpec {
        pattern,
        agent_route,
    } = &routes[1];
    assert_eq!(pattern, &pat2);
    let debug = format!("{:?}", agent_route);
    assert_eq!(
        debug,
        "AgentProvider { configuration: DummyConfig(2), lifecycle: DummyLifecycle(2) }"
    );
}

#[test]
fn plane_builder_route_collision() {
    let pat1 = RoutePattern::parse_str("/:id").unwrap();
    let pat2 = RoutePattern::parse_str("/b").unwrap();

    let mut builder: BuilderType = PlaneBuilder::new(MockPlaneStore);
    assert!(builder
        .add_route(pat1.clone(), DummyConfig(1), DummyLifecycle(1))
        .is_ok());
    let result = builder.add_route(pat2.clone(), DummyConfig(2), DummyLifecycle(2));
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(
        err == AmbiguousRoutes::new(pat1.clone(), pat2.clone())
            || err == AmbiguousRoutes::new(pat2, pat1)
    );
}

#[test]
fn add_plane_lifecycle() {
    let pat = RoutePattern::parse_str("/:id").unwrap();

    let mut builder: BuilderType = PlaneBuilder::new(MockPlaneStore);
    assert!(builder
        .add_route(pat.clone(), DummyConfig(1), DummyLifecycle(1))
        .is_ok());

    let plane_lifecycle = DummyPlaneLifecycle(5);

    let PlaneSpec {
        routes, lifecycle, ..
    } = builder.build_with_lifecycle(plane_lifecycle.boxed());
    assert!(lifecycle.is_some());

    let lc = lifecycle.unwrap();

    let debug = format!("{:?}", lc);
    assert_eq!(debug, "DummyPlaneLifecycle(5)");

    assert_eq!(routes.len(), 1);
    let RouteSpec {
        pattern,
        agent_route,
    } = &routes[0];
    assert_eq!(pattern, &pat);
    let debug = format!("{:?}", agent_route);
    assert_eq!(
        debug,
        "AgentProvider { configuration: DummyConfig(1), lifecycle: DummyLifecycle(1) }"
    );
}
