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

pub mod context;
pub mod error;
pub mod lifecycle;
pub(crate) mod provider;
pub(crate) mod router;
pub mod spec;
#[cfg(test)]
mod tests;

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::AgentResult;
use crate::meta::get_route;
use crate::plane::context::PlaneContext;
use crate::plane::error::NoAgentAtRoute;
use crate::plane::router::{PlaneRouter, PlaneRouterFactory};
use crate::plane::spec::{PlaneSpec, RouteSpec};
use crate::routing::error::{RouterError, Unresolvable};
use crate::routing::remote::RawRoute;
use crate::routing::{ConnectionDropped, RoutingAddr, ServerRouterFactory, TaggedEnvelope};
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
use swim_common::routing::{ConnectionError, ProtocolError, ProtocolErrorKind};
use swim_runtime::time::clock::Clock;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use url::Url;
use utilities::route_pattern::RoutePattern;
use utilities::sync::{promise, trigger};
use utilities::task::Spawner;
use utilities::uri::RelativeUri;

/// Trait for agent routes. An agent route can construct and run any number of instances of a
/// [`SwimAgent`] type.
trait AgentRoute<Clk, Envelopes, Router>: Debug {
    /// Run an instance of the agent.
    ///
    /// #Arguments
    ///
    /// * `uri` The specific URI of the agent instance.
    /// * `parameters` - Named parameters extracted from the agent URI with the route pattern.
    /// * `execution_config` - Configuration parameters controlling how the agent runs.
    /// * `clock` - Clock for scheduling events.
    /// * `incoming_envelopes`- The stream of envelopes routed to the agent.
    /// * `router` - The router by which the agent can send messages.
    fn run_agent(
        &self,
        uri: RelativeUri,
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

/// Endpoint connected to an agent within the plane.
#[derive(Debug)]
struct LocalEndpoint {
    agent_handle: Weak<dyn Any + Send + Sync>,
    channel: mpsc::Sender<TaggedEnvelope>,
    drop_tx: promise::Sender<ConnectionDropped>,
    drop_rx: promise::Receiver<ConnectionDropped>,
}

impl LocalEndpoint {
    fn new(
        agent_handle: Weak<dyn Any + Send + Sync>,
        channel: mpsc::Sender<TaggedEnvelope>,
    ) -> Self {
        let (drop_tx, drop_rx) = promise::promise();
        LocalEndpoint {
            agent_handle,
            channel,
            drop_tx,
            drop_rx,
        }
    }

    fn route(&self) -> RawRoute {
        let LocalEndpoint {
            channel, drop_rx, ..
        } = self;
        RawRoute::new(channel.clone(), drop_rx.clone())
    }
}

pub(in crate) type EnvChannel = TakeUntil<ReceiverStream<TaggedEnvelope>, trigger::Receiver>;

/// Container for the running routes within a plane.
#[derive(Debug, Default)]
struct PlaneActiveRoutes {
    local_endpoints: HashMap<RoutingAddr, LocalEndpoint>,
    local_routes: HashMap<RelativeUri, RoutingAddr>,
}

impl PlaneActiveRoutes {
    fn get_endpoint(&self, addr: &RoutingAddr) -> Option<&LocalEndpoint> {
        self.local_endpoints.get(&addr)
    }

    fn get_endpoint_for_route(&self, route: &RelativeUri) -> Option<&LocalEndpoint> {
        let PlaneActiveRoutes {
            local_endpoints,
            local_routes,
            ..
        } = self;
        local_routes
            .get(route)
            .and_then(|addr| local_endpoints.get(addr))
    }

    fn add_endpoint(&mut self, addr: RoutingAddr, route: RelativeUri, endpoint: LocalEndpoint) {
        let PlaneActiveRoutes {
            local_endpoints,
            local_routes,
        } = self;

        local_routes.insert(route, addr);
        local_endpoints.insert(addr, endpoint);
    }

    fn routes(&self) -> impl Iterator<Item = &RelativeUri> {
        self.local_routes.keys()
    }

    fn addr_for_route(&self, route: &RelativeUri) -> Option<RoutingAddr> {
        self.local_routes.get(route).copied()
    }

    fn remove_endpoint(&mut self, route: &RelativeUri) -> Option<LocalEndpoint> {
        let PlaneActiveRoutes {
            local_endpoints,
            local_routes,
        } = self;
        if let Some(addr) = local_routes.remove(route) {
            local_endpoints.remove(&addr)
        } else {
            None
        }
    }
}

type AgentRequest = Request<Result<Arc<dyn Any + Send + Sync>, NoAgentAtRoute>>;
type EndpointRequest = Request<Result<RawRoute, Unresolvable>>;
type RoutesRequest = Request<HashSet<RelativeUri>>;
type ResolutionRequest = Request<Result<RoutingAddr, RouterError>>;

/// Requests that can be serviced by the plane event loop.
#[derive(Debug)]
pub(crate) enum PlaneRequest {
    /// Get a handle to an agent (starting it where necessary).
    Agent {
        name: RelativeUri,
        request: AgentRequest,
    },
    /// Get channel to route messages to a specified routing address.
    Endpoint {
        id: RoutingAddr,
        request: EndpointRequest,
    },
    /// Resolve the routing address for an agent.
    Resolve {
        host: Option<Url>,
        name: RelativeUri,
        request: ResolutionRequest,
    },
    /// Get all of the active routes for the plane.
    Routes(RoutesRequest),
}

/// Plane context implementation.
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
    fn get_agent_ref(
        &mut self,
        route: RelativeUri,
    ) -> BoxFuture<'_, Result<Arc<dyn Any + Send + Sync>, NoAgentAtRoute>> {
        let (tx, rx) = oneshot::channel();
        async move {
            if self
                .request_tx
                .send(PlaneRequest::Agent {
                    name: route.clone(),
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

    fn active_routes(&mut self) -> BoxFuture<HashSet<RelativeUri>> {
        let (tx, rx) = oneshot::channel();
        async move {
            if self
                .request_tx
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
/// Contains the specifications of all routes that are within a plane and maintains the map of
/// currently active routes.
struct RouteResolver<Clk, DelegateFac: ServerRouterFactory> {
    /// Clock for scheduling tasks.
    clock: Clk,
    /// The configuration for the agent routes that are opened.
    execution_config: AgentExecutionConfig,
    /// The routes for the plane.
    routes: Vec<RouteSpec<Clk, EnvChannel, PlaneRouter<DelegateFac::Router>>>,
    /// Factory to create handles to the plane router when an agent is opened.
    router_fac: PlaneRouterFactory<DelegateFac>,
    /// External trigger that is fired when the plane should stop.
    stop_trigger: trigger::Receiver,
    /// The map of currently active routes.
    active_routes: PlaneActiveRoutes,
    /// Monotonically increasing counter for assigning local routing addresses.
    counter: u32,
}

impl<Clk: Clock, DelegateFac: ServerRouterFactory> RouteResolver<Clk, DelegateFac> {
    /// Attempts to open an agent at a specified route.
    fn try_open_route<S>(
        &mut self,
        route: RelativeUri,
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
        let (agent_route, params) = route_for(&route, routes)?;
        let (tx, rx) = mpsc::channel::<TaggedEnvelope>(8);
        *counter = counter
            .checked_add(1)
            .expect("Local endpoint counter overflow.");
        let addr = RoutingAddr::local(*counter);
        let (agent, task) = agent_route.run_agent(
            route.clone(),
            params,
            execution_config.clone(),
            clock.clone(),
            ReceiverStream::new(rx).take_until(stop_trigger.clone()),
            router_fac.create_for(addr),
        );
        active_routes.add_endpoint(addr, route, LocalEndpoint::new(Arc::downgrade(&agent), tx));
        if spawner.try_add(task).is_err() {
            panic!("Task spawner terminated unexpectedly.");
        }
        Ok((agent, addr))
    }
}

const DROPPED_REQUEST: &str = "A plane request was dropped before it could be fulfilled.";
const AGENT_TASK_FAILED: &str = "An agent task failed.";
const AGENT_TASK_COMPLETE: &str = "An agent task completed normally.";
const PLANE_START_TASK: &str = "Plane on_start event task.";
const PLANE_EVENT_LOOP: &str = "Plane main event loop.";
const STARTING: &str = "Plane starting.";
const ON_START_EVENT: &str = "Running plane on_start handler.";
const GETTING_HANDLE: &str = "Attempting to provide agent handle.";
const GETTING_LOCAL_ENDPOINT: &str = "Attempting to get a local endpoint.";
const GETTING_REMOTE_ENDPOINT: &str = "Attempting to get a remote endpoint.";
const RESOLVING: &str = "Attempting to resolve a target.";
const PROVIDING_ROUTES: &str = "Providing all running routes in the plane.";
const PLANE_STOPPING: &str = "The plane is stopping.";
const ON_STOP_EVENT: &str = "Running plane on_stop handler.";
const PLANE_STOPPED: &str = "The plane has stopped.";

/// The main event loop for a plane. Handles `PlaneRequest`s until the external stop trigger is
/// fired. This task is infallible and will merely report if one of its agents fails rather than
/// stopping.
///
/// #Arguments
/// * `execution_config` - The configuration for agents that belong to this plane.
/// * `clock` - The clock to use for scheduling tasks.
/// * `spec` - The specification for the plane.
/// * `stop_trigger` - Trigger to fire externally when the plane should stop.
/// * `spawner` - Spawns tasks to run the agents for the plane.
/// * `context_channel` - Transmitter and receiver for plane requests.
/// * `delegate_fac` - Factory for creating delegate routers.
pub(crate) async fn run_plane<Clk, S, DelegateFac: ServerRouterFactory>(
    execution_config: AgentExecutionConfig,
    clock: Clk,
    spec: PlaneSpec<Clk, EnvChannel, PlaneRouter<DelegateFac::Router>>,
    stop_trigger: trigger::Receiver,
    spawner: S,
    context_channel: (mpsc::Sender<PlaneRequest>, mpsc::Receiver<PlaneRequest>),
    delegate_fac: DelegateFac,
) where
    Clk: Clock,
    S: Spawner<BoxFuture<'static, AgentResult>>,
{
    event!(Level::DEBUG, STARTING);
    pin_mut!(spawner);

    let (context_tx, context_rx) = context_channel;
    let mut context = ContextImpl::new(context_tx.clone(), spec.routes());

    let mut requests = ReceiverStream::new(context_rx)
        .take_until(stop_trigger.clone())
        .fuse();

    let PlaneSpec {
        routes,
        mut lifecycle,
    } = spec;

    let start_task = async move {
        if let Some(lc) = &mut lifecycle {
            event!(Level::TRACE, ON_START_EVENT);
            lc.on_start(&mut context).await;
        }
        lifecycle
    }
    .instrument(span!(Level::DEBUG, PLANE_START_TASK));

    let event_loop = async move {
        let mut resolver = RouteResolver {
            clock,
            execution_config,
            routes,
            router_fac: PlaneRouterFactory::new(context_tx, delegate_fac),
            stop_trigger,
            active_routes: PlaneActiveRoutes::default(),
            counter: 0,
        };

        let mut stopping = false;

        loop {
            let req_or_res = if stopping {
                if spawner.is_empty() {
                    Either::Right(None)
                } else {
                    Either::Right(spawner.next().await)
                }
            } else if spawner.is_empty() {
                Either::Left(requests.next().await)
            } else {
                select_biased! {
                    request = requests.next() => Either::Left(request),
                    result = spawner.next() => Either::Right(result),
                }
            };

            match req_or_res {
                Either::Left(Some(PlaneRequest::Agent { name, request })) => {
                    event!(Level::TRACE, GETTING_HANDLE, ?name);

                    let route = get_route(name);

                    let result = if let Some(agent) = resolver
                        .active_routes
                        .get_endpoint_for_route(&route)
                        .and_then(|ep| ep.agent_handle.upgrade())
                    {
                        Ok(agent)
                    } else {
                        resolver
                            .try_open_route(route, spawner.deref())
                            .map(|(agent, _)| agent)
                    };
                    if request.send(result).is_err() {
                        event!(Level::WARN, DROPPED_REQUEST);
                    }
                }
                Either::Left(Some(PlaneRequest::Endpoint { id, request })) => {
                    if id.is_local() {
                        event!(Level::TRACE, GETTING_LOCAL_ENDPOINT, ?id);
                        let result = if let Some(tx) = resolver
                            .active_routes
                            .get_endpoint(&id)
                            .map(LocalEndpoint::route)
                        {
                            Ok(tx)
                        } else {
                            Err(Unresolvable(id))
                        };
                        if request.send(result).is_err() {
                            event!(Level::WARN, DROPPED_REQUEST);
                        }
                    } else {
                        event!(Level::TRACE, GETTING_REMOTE_ENDPOINT, ?id);
                        //TODO Attach external routing here.
                        if request.send_err(Unresolvable(id)).is_err() {
                            event!(Level::WARN, DROPPED_REQUEST);
                        }
                    }
                }
                Either::Left(Some(PlaneRequest::Resolve {
                    host: None,
                    name,
                    request,
                })) => {
                    event!(Level::TRACE, RESOLVING, ?name);

                    let route = get_route(name);

                    let result = if let Some(addr) = resolver.active_routes.addr_for_route(&route) {
                        Ok(addr)
                    } else {
                        match resolver.try_open_route(route, spawner.deref()) {
                            Ok((_, addr)) => Ok(addr),
                            Err(NoAgentAtRoute(uri)) => Err(RouterError::NoAgentAtRoute(uri)),
                        }
                    };
                    if request.send(result).is_err() {
                        event!(Level::WARN, DROPPED_REQUEST);
                    }
                }
                Either::Left(Some(PlaneRequest::Resolve {
                    host: Some(host_url),
                    name,
                    request,
                })) => {
                    event!(Level::TRACE, RESOLVING, ?host_url, ?name);
                    //TODO Attach external resolution here.
                    if request
                        .send_err(RouterError::ConnectionFailure(ConnectionError::Protocol(
                            ProtocolError::new(ProtocolErrorKind::WebSocket, None),
                        )))
                        .is_err()
                    {
                        event!(Level::WARN, DROPPED_REQUEST);
                    }
                }
                Either::Left(Some(PlaneRequest::Routes(request))) => {
                    event!(Level::TRACE, PROVIDING_ROUTES);
                    if request
                        .send(resolver.active_routes.routes().map(Clone::clone).collect())
                        .is_err()
                    {
                        event!(Level::WARN, DROPPED_REQUEST);
                    }
                }
                Either::Right(Some(AgentResult {
                    route,
                    dispatcher_errors,
                    failed,
                })) => {
                    if let Some(LocalEndpoint { drop_tx, .. }) =
                        resolver.active_routes.remove_endpoint(&route)
                    {
                        let _ = if failed {
                            drop_tx.provide(ConnectionDropped::AgentFailed)
                        } else {
                            drop_tx.provide(ConnectionDropped::Closed)
                        };
                    }
                    if failed {
                        event!(Level::ERROR, AGENT_TASK_FAILED, ?route, ?dispatcher_errors);
                    } else {
                        event!(
                            Level::DEBUG,
                            AGENT_TASK_COMPLETE,
                            ?route,
                            ?dispatcher_errors
                        );
                    }
                }
                Either::Left(None) => {
                    event!(Level::DEBUG, PLANE_STOPPING);
                    stopping = true;
                    if spawner.is_empty() {
                        break;
                    }
                }
                _ => {
                    if stopping {
                        break;
                    }
                }
            }
        }
    }
    .instrument(span!(Level::DEBUG, PLANE_EVENT_LOOP));

    let (lifecycle, _) = join(start_task, event_loop).await;
    if let Some(mut lc) = lifecycle {
        event!(Level::TRACE, ON_STOP_EVENT);
        lc.on_stop().await;
    }
    event!(Level::DEBUG, PLANE_STOPPED);
}

type PlaneAgentRoute<Clk, Delegate> = BoxAgentRoute<Clk, EnvChannel, PlaneRouter<Delegate>>;
type Params = HashMap<String, String>;

/// Find the appropriate specification for a route along with any parameters derived from the
/// route pattern.
fn route_for<'a, Clk, Delegate>(
    route: &RelativeUri,
    routes: &'a [RouteSpec<Clk, EnvChannel, PlaneRouter<Delegate>>],
) -> Result<(&'a PlaneAgentRoute<Clk, Delegate>, Params), NoAgentAtRoute> {
    //TODO This could be a lot more efficient though it would probably only matter for planes with a large number of routes.
    let matched = routes
        .iter()
        .filter_map(
            |RouteSpec {
                 pattern,
                 agent_route,
             }| {
                if let Ok(params) = pattern.unapply_relative_uri(route) {
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
        Err(NoAgentAtRoute(route.clone()))
    }
}
