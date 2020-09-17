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
pub mod lifecycle;

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::{AgentParameters, AgentResult, SwimAgent};
use crate::plane::context::{NoAgentAtRoute, PlaneContext};
use crate::plane::lifecycle::PlaneLifecycle;
use crate::routing::{RoutingAddr, ServerRouter, TaggedEnvelope};
use either::Either;
use futures::future::BoxFuture;
use futures::{select_biased, FutureExt, Stream, StreamExt};
use futures_util::stream::TakeUntil;
use pin_utils::pin_mut;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{Arc, Weak};
use swim_common::request::Request;
use swim_common::routing::RoutingError;
use swim_common::sink::item::{ItemSink, MpscSend};
use swim_common::warp::envelope::Envelope;
use swim_runtime::time::clock::Clock;
use tokio::sync::{mpsc, oneshot};
use utilities::route_pattern::RoutePattern;
use utilities::sync::trigger;
use utilities::task::Spawner;

pub trait AgentRoute<Clk, Envelopes, Router>: Debug {
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
struct AgentProvider<Agent, Config, Lifecycle> {
    _agent_type: PhantomData<fn(Config) -> Agent>,
    configuration: Config,
    lifecycle: Lifecycle,
}

impl<Agent, Config, Lifecycle> AgentProvider<Agent, Config, Lifecycle>
where
    Config: Debug,
    Agent: SwimAgent<Config> + Debug,
    Lifecycle: AgentLifecycle<Agent> + Debug,
{
    fn new(configuration: Config, lifecycle: Lifecycle) -> Self {
        AgentProvider {
            _agent_type: PhantomData,
            configuration,
            lifecycle,
        }
    }
}

impl<Clk, Envelopes, Router, Agent, Config, Lifecycle> AgentRoute<Clk, Envelopes, Router>
    for AgentProvider<Agent, Config, Lifecycle>
where
    Clk: Clock,
    Envelopes: Stream<Item = TaggedEnvelope> + Send + 'static,
    Router: ServerRouter + Clone + 'static,
    Agent: SwimAgent<Config> + Send + Sync + Debug + 'static,
    Config: Send + Sync + Clone + Debug + 'static,
    Lifecycle: AgentLifecycle<Agent> + Send + Sync + Clone + Debug + 'static,
{
    fn run_agent(
        &self,
        uri: String,
        parameters: HashMap<String, String>,
        execution_config: AgentExecutionConfig,
        clock: Clk,
        incoming_envelopes: Envelopes,
        router: Router,
    ) -> (Arc<dyn Any + Send + Sync>, BoxFuture<'static, AgentResult>) {
        let AgentProvider {
            configuration,
            lifecycle,
            ..
        } = self;

        let parameters =
            AgentParameters::new(configuration.clone(), execution_config, uri, parameters);

        let (agent, task) = crate::agent::run_agent(
            lifecycle.clone(),
            clock,
            parameters,
            incoming_envelopes,
            router,
        );
        (agent, task.boxed())
    }
}

#[derive(Debug)]
struct RouteSpec<Clk, Envelopes, Router> {
    pattern: RoutePattern,
    agent_route: BoxAgentRoute<Clk, Envelopes, Router>,
}

impl<Clk, Envelopes, Router> RouteSpec<Clk, Envelopes, Router> {
    fn new(pattern: RoutePattern, agent_route: BoxAgentRoute<Clk, Envelopes, Router>) -> Self {
        RouteSpec {
            pattern,
            agent_route,
        }
    }
}

#[derive(Debug)]
pub struct PlaneSpec<Clk, Envelopes, Router> {
    routes: Vec<RouteSpec<Clk, Envelopes, Router>>,
    lifecycle: Option<Box<dyn PlaneLifecycle>>,
}

impl<Clk, Envelopes, Router> PlaneSpec<Clk, Envelopes, Router> {
    fn routes(&self) -> Vec<RoutePattern> {
        self.routes
            .iter()
            .map(|RouteSpec { pattern, .. }| pattern.clone())
            .collect()
    }
}

#[derive(Debug)]
pub struct PlaneBuilder<Clk, Envelopes, Router>(PlaneSpec<Clk, Envelopes, Router>);

impl<Clk, Envelopes, Router> Default for PlaneBuilder<Clk, Envelopes, Router> {
    fn default() -> Self {
        PlaneBuilder(PlaneSpec {
            routes: vec![],
            lifecycle: None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AmbiguousRoutes(RoutePattern, RoutePattern);

impl Display for AmbiguousRoutes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Routes '{}' and '{}' are ambiguous.", &self.0, &self.1)
    }
}

impl Error for AmbiguousRoutes {}

impl<Clk, Envelopes, Router> PlaneBuilder<Clk, Envelopes, Router>
where
    Clk: Clock,
    Envelopes: Stream<Item = TaggedEnvelope> + Send + 'static,
    Router: ServerRouter + Clone + 'static,
{
    pub fn add_route<Agent, Config, Lifecycle>(
        &mut self,
        route: RoutePattern,
        config: Config,
        lifecycle: Lifecycle,
    ) -> Result<(), AmbiguousRoutes>
    where
        Agent: SwimAgent<Config> + Send + Sync + Debug + 'static,
        Config: Send + Sync + Clone + Debug + 'static,
        Lifecycle: AgentLifecycle<Agent> + Send + Sync + Clone + Debug + 'static,
    {
        let PlaneBuilder(PlaneSpec { routes, .. }) = self;
        for RouteSpec {
            pattern: existing_route,
            ..
        } in routes.iter()
        {
            if RoutePattern::are_ambiguous(existing_route, &route) {
                return Err(AmbiguousRoutes(existing_route.clone(), route));
            }
        }
        routes.push(RouteSpec::new(
            route,
            AgentProvider::new(config, lifecycle).boxed(),
        ));
        Ok(())
    }

    pub fn build(self) -> PlaneSpec<Clk, Envelopes, Router> {
        self.0
    }

    pub fn build_with_lifecycle(
        mut self,
        custom_lc: Box<dyn PlaneLifecycle>,
    ) -> PlaneSpec<Clk, Envelopes, Router> {
        self.0.lifecycle = Some(custom_lc);
        self.0
    }
}

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

pub struct PlaneRouterSender {
    tag: RoutingAddr,
    inner: mpsc::Sender<TaggedEnvelope>,
}

impl PlaneRouterSender {
    fn new(tag: RoutingAddr, inner: mpsc::Sender<TaggedEnvelope>) -> Self {
        PlaneRouterSender { tag, inner }
    }
}

impl<'a> ItemSink<'a, Envelope> for PlaneRouterSender {
    type Error = RoutingError;
    type SendFuture = MpscSend<'a, TaggedEnvelope, RoutingError>;

    fn send_item(&'a mut self, envelope: Envelope) -> Self::SendFuture {
        let PlaneRouterSender { tag, inner } = self;
        MpscSend::new(inner, TaggedEnvelope(*tag, envelope))
    }
}

#[derive(Debug)]
pub struct PlaneRouterFactory {
    request_sender: mpsc::Sender<PlaneRequest>,
}

impl PlaneRouterFactory {
    fn new(request_sender: mpsc::Sender<PlaneRequest>) -> Self {
        PlaneRouterFactory { request_sender }
    }

    fn create(&self, tag: RoutingAddr) -> PlaneRouter {
        PlaneRouter::new(tag, self.request_sender.clone())
    }
}

#[derive(Debug, Clone)]
pub struct PlaneRouter {
    tag: RoutingAddr,
    request_sender: mpsc::Sender<PlaneRequest>,
}

impl PlaneRouter {
    fn new(tag: RoutingAddr, request_sender: mpsc::Sender<PlaneRequest>) -> Self {
        PlaneRouter {
            tag,
            request_sender,
        }
    }
}

impl ServerRouter for PlaneRouter {
    type Sender = PlaneRouterSender;

    fn get_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Self::Sender, RoutingError>> {
        async move {
            let PlaneRouter {
                tag,
                request_sender,
            } = self;
            let (tx, rx) = oneshot::channel();
            if request_sender
                .send(PlaneRequest::Endpoint {
                    id: addr,
                    request: Request::new(tx),
                })
                .await
                .is_err()
            {
                Err(RoutingError::RouterDropped)
            } else {
                match rx.await {
                    Ok(Ok(sender)) => Ok(PlaneRouterSender::new(*tag, sender)),
                    Ok(Err(_)) => Err(RoutingError::HostUnreachable),
                    Err(_) => Err(RoutingError::RouterDropped),
                }
            }
        }
        .boxed()
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

    /*fn addr_for_route(&self, route: &str) -> Option<RoutingAddr> {
        self.local_routes.get(route).map(|addr| *addr)
    }*/
}

struct Unresolvable;

type AgentRequest = Request<Result<Arc<dyn Any + Send + Sync>, NoAgentAtRoute>>;
type EndpointRequest = Request<Result<mpsc::Sender<TaggedEnvelope>, Unresolvable>>;
type RoutesRequest = Request<HashSet<String>>;

enum PlaneRequest {
    Agent {
        name: String,
        request: AgentRequest,
    },
    Endpoint {
        id: RoutingAddr,
        request: EndpointRequest,
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
    ) -> Result<Arc<dyn Any + Send + Sync>, NoAgentAtRoute>
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
        Ok(agent)
    }
}

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

    let PlaneSpec { routes, lifecycle } = spec;

    if let Some(lc) = lifecycle {
        lc.on_start(&context).await;
    }

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
                    resolver.try_open_route(name.clone(), spawner.deref())
                };
                if request.send(result).is_err() {
                    //TODO Log error.
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
                        Err(Unresolvable)
                    };
                    if request.send(result).is_err() {
                        //TODO Log error.
                    }
                } else {
                    //TODO Attach external routing here.
                    if request.send_err(Unresolvable).is_err() {
                        //TODO Log error.
                    }
                }
            }
            Some(Either::Left(PlaneRequest::Routes(request))) => {
                if request
                    .send(resolver.active_routes.routes().map(Clone::clone).collect())
                    .is_err()
                {
                    //TODO Log error.
                }
            }
            Some(Either::Right(AgentResult {
                dispatcher_errors: _,
                failed,
            })) => {
                if failed {
                    //TODO Log failed agents.
                }
            }
            _ => {
                break;
            }
        }
    }
}

type PlaneAgentRoute<Clk> = BoxAgentRoute<Clk, EnvChannel, PlaneRouter>;

fn route_for<'a, Clk>(
    route: &str,
    routes: &'a [RouteSpec<Clk, EnvChannel, PlaneRouter>],
) -> Result<(&'a PlaneAgentRoute<Clk>, HashMap<String, String>), NoAgentAtRoute> {
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
