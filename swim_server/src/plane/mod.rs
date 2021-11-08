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

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::AgentResult;
use crate::meta::get_route;
use crate::plane::context::PlaneContext;
use crate::plane::lifecycle::PlaneLifecycle;
use crate::plane::router::{PlaneRouter, PlaneRouterFactory};
use crate::plane::spec::RouteSpec;
use either::Either;
use futures::future::{join, BoxFuture};
use futures::{select_biased, FutureExt, StreamExt};
use futures_util::stream::TakeUntil;
use pin_utils::pin_mut;
use server_store::plane::PlaneStore;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::{Arc, Weak};
use swim_async_runtime::time::clock::Clock;
use swim_runtime::error::{ConnectionError, ProtocolError, ProtocolErrorKind};
use swim_utilities::future::request::Request;
use swim_client::interface::ClientContext;
use swim_utilities::future::task::Spawner;
use swim_utilities::routing::route_pattern::RoutePattern;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger::promise;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, span, Level};
use tracing_futures::Instrument;

pub mod context;
pub mod error;
pub mod lifecycle;
pub mod provider;
pub mod router;
pub mod spec;
#[cfg(test)]
mod tests;

/// Trait for agent routes. An agent route can construct and run any number of instances of a
/// [`SwimAgent`] type.
pub trait AgentRoute<Clk, Envelopes, Router, Store>: Debug + Send {
    /// Run an instance of the agent.
    ///
    /// # Arguments
    ///
    /// * `route` - The route of the agent instance and named parameters extracted from the agent
    /// URI with the route pattern.
    /// * `execution_config` - Configuration parameters controlling how the agent runs.
    /// * `agent_internals` - The internal components of the agent.
    fn run_agent(
        &self,
        route: RouteAndParameters,
        execution_config: AgentExecutionConfig,
        agent_internals: AgentInternals<Clk, Envelopes, Router, Store>,
    ) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>);

    fn boxed(self) -> BoxAgentRoute<Clk, Envelopes, Router, Store>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

type BoxAgentRoute<Clk, Envelopes, Router, Store> =
    Box<dyn AgentRoute<Clk, Envelopes, Router, Store> + Send>;

/// Internal components used for running an agent.
pub struct AgentInternals<Clk, Envelopes, Router, Store> {
    /// Clock for scheduling events.
    clock: Clk,
    /// Client context for opening downlinks.
    client_context: ClientContext<Path>,
    /// The stream of envelopes routed to the agent.
    incoming_envelopes: Envelopes,
    /// The router by which the agent can send messages.
    router: Router,
    /// A node store for persisting data, if the lane is not transient.
    store: Store,
}

impl<Clk, Envelopes, Router, Store> AgentInternals<Clk, Envelopes, Router, Store> {
    pub fn new(
        clock: Clk,
        client_context: ClientContext<Path>,
        incoming_envelopes: Envelopes,
        router: Router,
        store: Store,
    ) -> Self {
        AgentInternals {
            clock,
            client_context,
            incoming_envelopes,
            router,
            store,
        }
    }
}

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

pub type EnvChannel = TakeUntil<ReceiverStream<TaggedEnvelope>, CloseReceiver>;

/// A specification of a plane, consisting of the defined routes, store and an optional custom lifecycle
/// for the plane.
#[derive(Debug)]
pub struct PlaneSpec<Clk, Envelopes, Router, Store>
where
    Store: PlaneStore,
{
    routes: Vec<RouteSpec<Clk, Envelopes, Router, Store::NodeStore>>,
    lifecycle: Option<Box<dyn PlaneLifecycle>>,
    store: Store,
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

    pub fn take_lifecycle(&mut self) -> Option<Box<dyn PlaneLifecycle>> {
        self.lifecycle.take()
    }
}

/// Container for the running routes within a plane.
#[derive(Debug, Default)]
pub struct PlaneActiveRoutes {
    local_endpoints: HashMap<RoutingAddr, LocalEndpoint>,
    local_routes: HashMap<RelativeUri, RoutingAddr>,
}

impl PlaneActiveRoutes {
    fn get_endpoint(&self, addr: &RoutingAddr) -> Option<&LocalEndpoint> {
        self.local_endpoints.get(addr)
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

/// Plane context implementation.
pub struct ContextImpl {
    request_tx: mpsc::Sender<PlaneRoutingRequest>,
    routes: Vec<RoutePattern>,
}

impl ContextImpl {
    pub fn new(request_tx: mpsc::Sender<PlaneRoutingRequest>, routes: Vec<RoutePattern>) -> Self {
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
                .send(PlaneRoutingRequest::Agent {
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
                .send(PlaneRoutingRequest::Routes(Request::new(tx)))
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
pub struct RouteResolver<Clk, DelegateFac, Store>
where
    DelegateFac: RouterFactory,
    Store: PlaneStore,
{
    /// Clock for scheduling tasks.
    clock: Clk,
    /// Client for opening downlinks.
    client_context: ClientContext<Path>,
    /// The configuration for the agent routes that are opened.
    execution_config: AgentExecutionConfig,
    // The routes and store for for the plane
    plane_spec: PlaneSpec<Clk, EnvChannel, PlaneRouter<DelegateFac::Router>, Store>,
    /// Factory to create handles to the plane router when an agent is opened.
    router_fac: PlaneRouterFactory<DelegateFac>,
    /// External trigger that is fired when the plane should stop.
    stop_trigger: CloseReceiver,
    /// The map of currently active routes.
    active_routes: PlaneActiveRoutes,
    /// Monotonically increasing counter for assigning local routing addresses.
    counter: u32,
}

impl<Clk, DelegateFac: RouterFactory, Store: PlaneStore> RouteResolver<Clk, DelegateFac, Store> {
    pub fn new(
        clock: Clk,
        client_context: ClientContext<Path>,
        execution_config: AgentExecutionConfig,
        plane_spec: PlaneSpec<Clk, EnvChannel, PlaneRouter<DelegateFac::Router>, Store>,
        router_fac: PlaneRouterFactory<DelegateFac>,
        stop_trigger: CloseReceiver,
        active_routes: PlaneActiveRoutes,
    ) -> RouteResolver<Clk, DelegateFac, Store> {
        RouteResolver {
            clock,
            client_context,
            execution_config,
            plane_spec,
            router_fac,
            stop_trigger,
            active_routes,
            counter: 0,
        }
    }
}

impl<Clk, DelegateFac, Store> RouteResolver<Clk, DelegateFac, Store>
where
    Clk: Clock,
    DelegateFac: RouterFactory,
    Store: PlaneStore,
{
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
            client_context,
            execution_config,
            plane_spec,
            router_fac,
            stop_trigger,
            active_routes,
            counter,
        } = self;

        let PlaneSpec { routes, store, .. } = plane_spec;

        let (agent_route, params) = route_for(&route, routes.as_slice())?;
        let (tx, rx) = mpsc::channel::<TaggedEnvelope>(8);
        *counter = counter
            .checked_add(1)
            .expect("Local endpoint counter overflow.");
        let addr = RoutingAddr::plane(*counter);

        let agent_internals = AgentInternals::new(
            clock.clone(),
            client_context.clone(),
            ReceiverStream::new(rx).take_until(stop_trigger.clone()),
            router_fac.create_for(addr),
            store.node_store(route.path()),
        );

        let (agent, task) = agent_route.run_agent(
            RouteAndParameters::new(route.clone(), params),
            execution_config.clone(),
            agent_internals,
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
///
/// * `resolver` - The specifications of all routes that are within a plane.
/// * `lifecycle` - The lifecycle of the plane.
/// * `context` - The context of the plane.
/// * `stop_trigger` - Trigger to fire externally when the plane should stop.
/// * `spawner` - Tasks spawner.
/// * `context_rx` - Receiver for plane requests.
pub async fn run_plane<Clk, S, DelegateFac: RouterFactory, Store>(
    mut resolver: RouteResolver<Clk, DelegateFac, Store>,
    mut lifecycle: Option<Box<dyn PlaneLifecycle>>,
    mut context: ContextImpl,
    stop_trigger: CloseReceiver,
    spawner: S,
    context_rx: mpsc::Receiver<PlaneRoutingRequest>,
) where
    Clk: Clock,
    S: Spawner<BoxFuture<'static, AgentResult>>,
    DelegateFac: RouterFactory,
    Store: PlaneStore,
{
    event!(Level::DEBUG, STARTING);
    pin_mut!(spawner);

    let mut requests = ReceiverStream::new(context_rx)
        .take_until(stop_trigger.clone())
        .fuse();

    let start_task = async move {
        if let Some(lc) = &mut lifecycle {
            event!(Level::TRACE, ON_START_EVENT);
            lc.on_start(&mut context).await;
        }
        lifecycle
    }
    .instrument(span!(Level::DEBUG, PLANE_START_TASK));

    let event_loop = async move {
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
                Either::Left(Some(PlaneRoutingRequest::Agent { name, request })) => {
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
                Either::Left(Some(PlaneRoutingRequest::Endpoint { id, request })) => {
                    if id.is_plane() {
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
                Either::Left(Some(PlaneRoutingRequest::Resolve {
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
                Either::Left(Some(PlaneRoutingRequest::Resolve {
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
                Either::Left(Some(PlaneRoutingRequest::Routes(request))) => {
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
                    dispatcher_task,
                    store_task,
                })) => {
                    if let Some(LocalEndpoint { drop_tx, .. }) =
                        resolver.active_routes.remove_endpoint(&route)
                    {
                        let _ = if dispatcher_task.failed {
                            drop_tx.provide(ConnectionDropped::AgentFailed)
                        } else {
                            drop_tx.provide(ConnectionDropped::Closed)
                        };
                    }
                    if dispatcher_task.failed {
                        event!(Level::ERROR, AGENT_TASK_FAILED, ?route, ?dispatcher_task);
                    } else {
                        event!(Level::DEBUG, AGENT_TASK_COMPLETE, ?route, ?dispatcher_task);
                    }
                    if store_task.failed {
                        event!(Level::ERROR, AGENT_TASK_FAILED, ?route, ?store_task);
                    } else {
                        event!(Level::DEBUG, AGENT_TASK_COMPLETE, ?route, ?dispatcher_task);
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

type PlaneAgentRoute<Clk, Delegate, Store> =
    BoxAgentRoute<Clk, EnvChannel, PlaneRouter<Delegate>, Store>;
type Params = HashMap<String, String>;

pub struct RouteAndParameters {
    pub route: RelativeUri,
    pub parameters: HashMap<String, String>,
}

impl RouteAndParameters {
    pub fn new(route: RelativeUri, parameters: HashMap<String, String>) -> RouteAndParameters {
        RouteAndParameters { route, parameters }
    }
}

/// Find the appropriate specification for a route along with any parameters derived from the
/// route pattern.
fn route_for<'a, Clk, Delegate, Store>(
    route: &RelativeUri,
    routes: &'a [RouteSpec<Clk, EnvChannel, PlaneRouter<Delegate>, Store>],
) -> Result<(&'a PlaneAgentRoute<Clk, Delegate, Store>, Params), NoAgentAtRoute> {
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
