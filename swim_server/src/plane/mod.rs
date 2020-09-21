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
pub mod error;
pub mod lifecycle;
pub(crate) mod provider;
mod router;
pub mod spec;

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::AgentResult;
use crate::plane::context::PlaneContext;
use crate::plane::error::{NoAgentAtRoute, ResolutionError, Unresolvable};
use crate::plane::router::{PlaneRouter, PlaneRouterFactory};
use crate::plane::spec::{PlaneSpec, RouteSpec};
use crate::routing::{RoutingAddr, TaggedEnvelope};
use either::Either;
use futures::future::{join, BoxFuture};
use futures::{select_biased, FutureExt, StreamExt};
use futures_util::stream::TakeUntil;
use pin_utils::pin_mut;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::{Arc, Weak};
use swim_common::request::Request;
use swim_common::routing::RoutingError;
use swim_runtime::time::clock::Clock;
use tokio::sync::{mpsc, oneshot};
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use url::Url;
use utilities::route_pattern::RoutePattern;
use utilities::sync::trigger;
use utilities::task::Spawner;

#[cfg(test)]
mod tests;

trait AgentRoute<Clk, Envelopes, Router>: Debug {
    fn run_agent(
        &self,
        uri: String,
        parameters: HashMap<String, String>,
        execution_config: AgentExecutionConfig,
        clock: Clk,
        incoming_envelopes: Envelopes,
        router: Router,
    ) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>);

    fn boxed(self) -> BoxAgentRoute<Clk, Envelopes, Router>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

type BoxAgentRoute<Clk, Envelopes, Router> = Box<dyn AgentRoute<Clk, Envelopes, Router>>;

#[derive(Debug)]
struct LocalEndpoint {
    agent_handle: Weak<dyn Any + Send + Sync>,
    channel: mpsc::Sender<TaggedEnvelope>,
}

impl LocalEndpoint {
    fn new(
        agent_handle: Weak<dyn Any + Send + Sync>,
        channel: mpsc::Sender<TaggedEnvelope>,
    ) -> Self {
        LocalEndpoint {
            agent_handle,
            channel,
        }
    }
}

type EnvChannel = TakeUntil<mpsc::Receiver<TaggedEnvelope>, trigger::Receiver>;

#[derive(Debug, Default)]
struct PlaneActiveRoutes {
    local_endpoints: HashMap<RoutingAddr, LocalEndpoint>,
    local_routes: HashMap<String, RoutingAddr>,
}

impl PlaneActiveRoutes {
    fn get_endpoint(&self, addr: &RoutingAddr) -> Option<&LocalEndpoint> {
        self.local_endpoints.get(&addr)
    }

    fn get_endpoint_for_route(&self, route: &str) -> Option<&LocalEndpoint> {
        let PlaneActiveRoutes {
            local_endpoints,
            local_routes,
            ..
        } = self;
        local_routes
            .get(route)
            .and_then(|addr| local_endpoints.get(addr))
    }

    fn add_endpoint(&mut self, addr: RoutingAddr, route: String, endpoint: LocalEndpoint) {
        let PlaneActiveRoutes {
            local_endpoints,
            local_routes,
        } = self;

        local_routes.insert(route, addr);
        local_endpoints.insert(addr, endpoint);
    }

    fn routes<'a>(&'a self) -> impl Iterator<Item = &'a String> + 'a {
        self.local_routes.keys()
    }

    fn addr_for_route(&self, route: &str) -> Option<RoutingAddr> {
        self.local_routes.get(route).copied()
    }
}

type AgentRequest = Request<Result<Arc<dyn Any + Send + Sync>, NoAgentAtRoute>>;
type EndpointRequest = Request<Result<mpsc::Sender<TaggedEnvelope>, Unresolvable>>;
type RoutesRequest = Request<HashSet<String>>;
type ResolutionRequest = Request<Result<RoutingAddr, ResolutionError>>;

#[derive(Debug)]
enum PlaneRequest {
    Agent {
        name: String,
        request: AgentRequest,
    },
    Endpoint {
        id: RoutingAddr,
        request: EndpointRequest,
    },
    Resolve {
        host: Option<Url>,
        name: String,
        request: ResolutionRequest,
    },
    Routes(RoutesRequest),
}

struct ContextImpl {
    request_tx: mpsc::Sender<PlaneRequest>,
    routes: Vec<RoutePattern>,
}

impl ContextImpl {
    fn new(request_tx: mpsc::Sender<PlaneRequest>, routes: Vec<RoutePattern>) -> Self {
        ContextImpl { request_tx, routes }
    }
}

impl PlaneContext for ContextImpl {
    fn get_agent(
        &self,
        route: String,
    ) -> BoxFuture<'static, Result<Arc<dyn Any + Send + Sync>, NoAgentAtRoute>> {
        let mut req_tx = self.request_tx.clone();
        let (tx, rx) = oneshot::channel();
        async move {
            if req_tx
                .send(PlaneRequest::Agent {
                    name: route.to_string(),
                    request: Request::new(tx),
                })
                .await
                .is_err()
            {
                Err(NoAgentAtRoute(route))
            } else if let Ok(result) = rx.await {
                result
            } else {
                Err(NoAgentAtRoute(route))
            }
        }
        .boxed()
    }

    fn routes(&self) -> &Vec<RoutePattern> {
        &self.routes
    }

    fn active_routes(&self) -> BoxFuture<'static, HashSet<String>> {
        let mut req_tx = self.request_tx.clone();
        let (tx, rx) = oneshot::channel();
        async move {
            if req_tx
                .send(PlaneRequest::Routes(Request::new(tx)))
                .await
                .is_err()
            {
                HashSet::default()
            } else if let Ok(result) = rx.await {
                result
            } else {
                HashSet::default()
            }
        }
        .boxed()
    }
}

struct RouteResolver<Clk> {
    clock: Clk,
    execution_config: AgentExecutionConfig,
    routes: Vec<RouteSpec<Clk, EnvChannel, PlaneRouter>>,
    router_fac: PlaneRouterFactory,
    stop_trigger: trigger::Receiver,
    active_routes: PlaneActiveRoutes,
    counter: u32,
}

impl<Clk: Clock> RouteResolver<Clk> {
    fn try_open_route<S>(
        &mut self,
        route: String,
        spawner: &S,
    ) -> Result<(Arc<dyn Any + Send + Sync>, RoutingAddr), NoAgentAtRoute>
    where
        S: Spawner<BoxFuture<'static, AgentResult>>,
    {
        let RouteResolver {
            clock,
            execution_config,
            routes,
            router_fac,
            stop_trigger,
            active_routes,
            counter,
        } = self;
        let (agent_route, params) = route_for(route.as_str(), routes)?;
        let (tx, rx) = mpsc::channel(8);
        *counter += 1;
        let addr = RoutingAddr::local(*counter);
        let (agent, task) = agent_route.run_agent(
            route.clone(),
            params,
            execution_config.clone(),
            clock.clone(),
            rx.take_until(stop_trigger.clone()),
            router_fac.create(addr),
        );
        active_routes.add_endpoint(addr, route, LocalEndpoint::new(Arc::downgrade(&agent), tx));
        spawner.add(task);
        Ok((agent, addr))
    }
}

const DROPPED_REQUEST: &str = "A plane request was dropped before it could be fulfilled.";
const AGENT_TASK_FAILED: &str = "An agent task failed.";
const PLANE_START_TASK: &str = "Plane on_start event task.";
const PLANE_EVENT_LOOP: &str = "Plane main event loop.";

pub async fn run_plane<Clk, S>(
    execution_config: AgentExecutionConfig,
    clock: Clk,
    spec: PlaneSpec<Clk, EnvChannel, PlaneRouter>,
    stop_trigger: trigger::Receiver,
    spawner: S,
) where
    Clk: Clock,
    S: Spawner<BoxFuture<'static, AgentResult>>,
{
    pin_mut!(spawner);

    let (context_tx, context_rx) = mpsc::channel(8);
    let context = ContextImpl::new(context_tx.clone(), spec.routes());

    let mut requests = context_rx.fuse();

    let PlaneSpec {
        routes,
        mut lifecycle,
    } = spec;

    let start_task = async move {
        if let Some(lc) = &mut lifecycle {
            lc.on_start(&context).await;
        }
        lifecycle
    }
    .instrument(span!(Level::DEBUG, PLANE_START_TASK));

    let event_loop = async move {
        let mut resolver = RouteResolver {
            clock,
            execution_config,
            routes,
            router_fac: PlaneRouterFactory::new(context_tx),
            stop_trigger,
            active_routes: PlaneActiveRoutes::default(),
            counter: 0,
        };

        loop {
            let req_or_res = if spawner.is_empty() {
                requests.next().await.map(Either::Left)
            } else {
                select_biased! {
                    request = requests.next() => request.map(Either::Left),
                    result = spawner.next() => result.map(Either::Right),
                }
            };

            match req_or_res {
                Some(Either::Left(PlaneRequest::Agent { name, request })) => {
                    let result = if let Some(agent) = resolver
                        .active_routes
                        .get_endpoint_for_route(name.as_str())
                        .and_then(|ep| ep.agent_handle.upgrade())
                    {
                        Ok(agent)
                    } else {
                        resolver
                            .try_open_route(name.clone(), spawner.deref())
                            .map(|(agent, _)| agent)
                    };
                    if request.send(result).is_err() {
                        event!(Level::WARN, DROPPED_REQUEST);
                    }
                }
                Some(Either::Left(PlaneRequest::Endpoint { id, request })) => {
                    if id.is_local() {
                        let result = if let Some(tx) = resolver
                            .active_routes
                            .get_endpoint(&id)
                            .map(|ep| ep.channel.clone())
                        {
                            Ok(tx)
                        } else {
                            Err(Unresolvable(id))
                        };
                        if request.send(result).is_err() {
                            event!(Level::WARN, DROPPED_REQUEST);
                        }
                    } else {
                        //TODO Attach external routing here.
                        if request.send_err(Unresolvable(id)).is_err() {
                            event!(Level::WARN, DROPPED_REQUEST);
                        }
                    }
                }
                Some(Either::Left(PlaneRequest::Resolve {
                    host: None,
                    name,
                    request,
                })) => {
                    let result =
                        if let Some(addr) = resolver.active_routes.addr_for_route(name.as_str()) {
                            Ok(addr)
                        } else {
                            match resolver.try_open_route(name.clone(), spawner.deref()) {
                                Ok((_, addr)) => Ok(addr),
                                Err(err) => Err(ResolutionError::NoAgent(err)),
                            }
                        };
                    if request.send(result).is_err() {
                        event!(Level::WARN, DROPPED_REQUEST);
                    }
                }
                Some(Either::Left(PlaneRequest::Resolve {
                    host: Some(_host_url),
                    name: _,
                    request,
                })) => {
                    //TODO Attach external resolution here.
                    if request
                        .send_err(ResolutionError::NoRoute(RoutingError::HostUnreachable))
                        .is_err()
                    {
                        event!(Level::WARN, DROPPED_REQUEST);
                    }
                }
                Some(Either::Left(PlaneRequest::Routes(request))) => {
                    if request
                        .send(resolver.active_routes.routes().map(Clone::clone).collect())
                        .is_err()
                    {
                        event!(Level::WARN, DROPPED_REQUEST);
                    }
                }
                Some(Either::Right(AgentResult {
                    route,
                    dispatcher_errors,
                    failed,
                })) => {
                    if failed {
                        event!(Level::ERROR, AGENT_TASK_FAILED, ?route, ?dispatcher_errors);
                    }
                }
                _ => {
                    break;
                }
            }
        }
    }
    .instrument(span!(Level::DEBUG, PLANE_EVENT_LOOP));

    let (lifecycle, _) = join(start_task, event_loop).await;
    if let Some(mut lc) = lifecycle {
        lc.on_stop().await;
    }
}

type PlaneAgentRoute<Clk> = BoxAgentRoute<Clk, EnvChannel, PlaneRouter>;

fn route_for<'a, Clk>(
    route: &str,
    routes: &'a [RouteSpec<Clk, EnvChannel, PlaneRouter>],
) -> Result<(&'a PlaneAgentRoute<Clk>, HashMap<String, String>), NoAgentAtRoute> {
    //TODO This could be a lot more efficient though it would probably only matter for planes with a large number of routes.
    let matched = routes
        .iter()
        .filter_map(
            |RouteSpec {
                 pattern,
                 agent_route,
             }| {
                if let Ok(params) = pattern.unapply_str(route) {
                    Some((agent_route, params))
                } else {
                    None
                }
            },
        )
        .next();
    if let Some(route_and_params) = matched {
        Ok(route_and_params)
    } else {
        Err(NoAgentAtRoute(route.to_string()))
    }
}
