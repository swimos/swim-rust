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

//! Interface for creating and running Swim server instances.
//!
//! The module provides methods and structures for creating and running Swim server instances.
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::plane::router::{PlaneRouter, PlaneRouterFactory};
use crate::plane::spec::PlaneSpec;
use crate::plane::ContextImpl;
use crate::plane::PlaneActiveRoutes;
use crate::plane::RouteResolver;
use crate::plane::{run_plane, EnvChannel};
use crate::routing::{TopLevelRouter, TopLevelRouterFactory};
use either::Either;
use futures::{io, join};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_client::configuration::downlink::ConfigHierarchy;
use swim_client::downlink::subscription::DownlinksHandle;
use swim_client::downlink::Downlinks;
use swim_client::interface::{SwimClient, SwimClientBuilder};
use swim_client::router::{ClientConnectionsManager, ClientRequest};
use swim_common::routing::error::RoutingError;
use swim_common::routing::remote::config::ConnectionConfig;
use swim_common::routing::remote::net::dns::Resolver;
use swim_common::routing::remote::net::plain::TokioPlainTextNetworking;
use swim_common::routing::remote::{RemoteConnectionChannels, RemoteConnectionsTask};
use swim_common::routing::ws::tungstenite::TungsteniteWsConnections;
use swim_common::routing::{CloseReceiver, CloseSender};
use swim_common::warp::path::Path;
use swim_runtime::task::TaskError;
use swim_runtime::time::clock::RuntimeClock;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::sync::promise;

/// Builder to create Swim server instance.
///
/// The builder can be created with default or custom configuration.
pub struct SwimServerBuilder {
    address: Option<SocketAddr>,
    config: SwimServerConfig,
    planes: Vec<PlaneSpec<RuntimeClock, EnvChannel, PlaneRouter<TopLevelRouter>>>,
}

impl Default for SwimServerBuilder {
    fn default() -> Self {
        SwimServerBuilder {
            address: None,
            config: SwimServerConfig::default(),
            planes: Vec::new(),
        }
    }
}

impl SwimServerBuilder {
    /// Create a new server builder with custom configuration.
    ///
    /// # Arguments
    /// * `config` - The custom configuration for the server.
    pub fn new(config: SwimServerConfig) -> Self {
        SwimServerBuilder {
            address: None,
            config,
            planes: Vec::new(),
        }
    }

    /// Set the address of the server.
    ///
    /// # Arguments
    /// * `addr` - The address of the serer.
    pub fn bind_to(self, addr: SocketAddr) -> Self {
        SwimServerBuilder {
            address: Some(addr),
            ..self
        }
    }

    /// Add a plane to the server.
    ///
    /// # Arguments
    /// * `plane` - The plane specification that will be added to the server.
    ///
    /// # Example
    /// ```
    /// use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// use swim_server::interface::{SwimServer, SwimServerBuilder};
    /// use swim_server::RoutePattern;
    /// use swim_server::agent_lifecycle;
    /// use swim_server::agent::SwimAgent;
    /// use swim_server::agent::AgentContext;
    /// use swim_server::plane::spec::PlaneBuilder;
    ///
    /// #[derive(Debug, SwimAgent)]
    /// #[agent(config = "RustAgentConfig")]
    /// pub struct RustAgent;
    ///
    /// #[derive(Debug, Clone)]
    /// pub struct RustAgentConfig;
    ///
    /// #[agent_lifecycle(agent = "RustAgent", on_start)]
    /// struct RustAgentLifecycle;
    ///
    /// impl RustAgentLifecycle {
    ///     async fn on_start<Context>(&self, _context: &Context)
    ///         where
    ///             Context: AgentContext<RustAgent> + Sized + Send + Sync,
    ///     {
    ///         println!("Rust agent has started!");
    ///     }
    /// }
    ///
    /// let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    ///
    /// let mut plane_builder = PlaneBuilder::new();
    /// plane_builder
    ///     .add_route(
    ///          RoutePattern::parse_str("/rust").unwrap(),
    ///          RustAgentConfig {},
    ///          RustAgentLifecycle {},
    ///     ).unwrap();
    ///
    /// let mut swim_server_builder = SwimServerBuilder::default();
    /// swim_server_builder.add_plane(plane_builder.build());
    /// ```
    pub fn add_plane(
        &mut self,
        plane: PlaneSpec<RuntimeClock, EnvChannel, PlaneRouter<TopLevelRouter>>,
    ) {
        if !self.planes.is_empty() {
            panic!("Multiple planes are not supported yet")
        }
        self.planes.push(plane)
    }

    /// Build the Swim Server.
    ///
    /// Returns an error if an address has not been provided for the server.
    ///
    /// # Example
    /// ```
    /// use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// use swim_server::interface::{SwimServer, SwimServerBuilder};
    /// use swim_server::RoutePattern;
    /// use swim_server::agent_lifecycle;
    /// use swim_server::agent::SwimAgent;
    /// use swim_server::agent::AgentContext;
    /// use swim_server::plane::spec::PlaneBuilder;
    ///
    /// #[derive(Debug, SwimAgent)]
    /// #[agent(config = "RustAgentConfig")]
    /// pub struct RustAgent;
    ///
    /// #[derive(Debug, Clone)]
    /// pub struct RustAgentConfig;
    ///
    /// #[agent_lifecycle(agent = "RustAgent", on_start)]
    /// struct RustAgentLifecycle;
    ///
    /// impl RustAgentLifecycle {
    ///     async fn on_start<Context>(&self, _context: &Context)
    ///         where
    ///             Context: AgentContext<RustAgent> + Sized + Send + Sync,
    ///     {
    ///         println!("Rust agent has started!");
    ///     }
    /// }
    ///
    /// let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    ///
    /// let mut plane_builder = PlaneBuilder::new();
    /// plane_builder
    ///     .add_route(
    ///          RoutePattern::parse_str("/rust").unwrap(),
    ///          RustAgentConfig {},
    ///          RustAgentLifecycle {},
    ///     ).unwrap();
    ///
    /// let mut swim_server_builder = SwimServerBuilder::default();
    /// swim_server_builder.add_plane(plane_builder.build());
    ///
    /// let (swim_server, server_handle) = swim_server_builder.bind_to(address).build().unwrap();
    /// ```
    pub fn build(self) -> Result<(SwimServer, ServerHandle), SwimServerBuilderError> {
        let SwimServerBuilder {
            address,
            planes,
            config,
        } = self;

        let (close_tx, close_rx) = promise::promise();
        let (address_tx, address_rx) = promise::promise();

        let (client_conn_request_tx, client_conn_request_rx) =
            mpsc::channel(config.conn_config.channel_buffer_size.get());

        let (downlinks, downlinks_handle) = Downlinks::new(
            client_conn_request_tx.clone(),
            Arc::new(ConfigHierarchy::default()),
            close_rx.clone(),
        );

        let client = SwimClientBuilder::build_from_downlinks(downlinks);

        Ok((
            SwimServer {
                config,
                planes,
                stop_trigger_rx: close_rx,
                address: address.ok_or(SwimServerBuilderError::MissingAddress)?,
                address_tx,
                client,
                downlinks_handle,
                client_conn_request_tx,
                client_conn_request_rx,
            },
            ServerHandle {
                either_address: Either::Left(address_rx),
                stop_trigger_tx: close_tx,
            },
        ))
    }
}

/// Swim server instance.
///
/// The Swim server runs a plane and its agents and a task for remote connections asynchronously.
pub struct SwimServer {
    address: SocketAddr,
    config: SwimServerConfig,
    planes: Vec<PlaneSpec<RuntimeClock, EnvChannel, PlaneRouter<TopLevelRouter>>>,
    stop_trigger_rx: CloseReceiver,
    address_tx: promise::Sender<SocketAddr>,
    client: SwimClient<Path>,
    downlinks_handle: DownlinksHandle<Path>,
    client_conn_request_tx: mpsc::Sender<ClientRequest<Path>>,
    client_conn_request_rx: mpsc::Receiver<ClientRequest<Path>>,
}

impl SwimServer {
    /// Runs the Swim server instance.
    ///
    /// Runs the planes and remote connections tasks of the server asynchronously
    /// and returns any errors from the connections task.
    ///
    /// # Panics
    /// The task will panic if the address provided to the server is already being used.
    pub async fn run(self) -> Result<(), io::Error> {
        let SwimServer {
            address,
            config,
            mut planes,
            stop_trigger_rx,
            address_tx,
            client,
            downlinks_handle,
            client_conn_request_tx: client_tx,
            client_conn_request_rx: client_rx,
        } = self;

        let SwimServerConfig {
            websocket_config,
            agent_config,
            conn_config,
        } = config;

        let DownlinksHandle {
            downlinks_task,
            request_receiver,
            task_manager,
            pool_task,
        } = downlinks_handle;

        // Todo add support for multiple planes in the future
        let spec = planes
            .pop()
            .expect("The server cannot be started without a plane");

        let (plane_tx, plane_rx) = mpsc::channel(agent_config.lane_attachment_buffer.get());
        let (remote_tx, remote_rx) = mpsc::channel(conn_config.router_buffer_size.get());

        let top_level_router_fac =
            TopLevelRouterFactory::new(plane_tx.clone(), client_tx.clone(), remote_tx.clone());

        let clock = swim_runtime::time::clock::runtime_clock();

        let conn_manager = ClientConnectionsManager::new(
            client_rx,
            remote_tx.clone(),
            Some(plane_tx.clone()),
            NonZeroUsize::new(conn_config.router_buffer_size.get()).unwrap(),
            stop_trigger_rx.clone(),
        );

        let context = ContextImpl::new(plane_tx.clone(), spec.routes());
        let PlaneSpec { routes, lifecycle } = spec;

        let resolver = RouteResolver::new(
            clock,
            client,
            agent_config,
            routes,
            PlaneRouterFactory::new(plane_tx, top_level_router_fac.clone()),
            stop_trigger_rx.clone(),
            PlaneActiveRoutes::default(),
        );

        let plane_future = run_plane(
            resolver,
            lifecycle,
            context,
            stop_trigger_rx.clone(),
            OpenEndedFutures::new(),
            plane_rx,
        );

        let connections_task = RemoteConnectionsTask::new_server_task(
            conn_config,
            TokioPlainTextNetworking::new(Arc::new(Resolver::new().await)),
            address,
            TungsteniteWsConnections {
                config: websocket_config,
            },
            top_level_router_fac,
            OpenEndedFutures::new(),
            RemoteConnectionChannels::new(remote_tx, remote_rx, stop_trigger_rx),
        )
        .await
        .unwrap_or_else(|err| panic!("Could not connect to \"{}\": {}", address, err));

        let _ = match connections_task.listener().unwrap().local_addr() {
            Ok(local_addr) => address_tx.provide(local_addr),
            Err(err) => panic!("Could not resolve server address: {}", err),
        };

        let connections_future = connections_task.run();
        join!(
            connections_future,
            plane_future,
            conn_manager.run(),
            downlinks_task.run(ReceiverStream::new(request_receiver)),
            task_manager.run(),
            pool_task.run()
        )
        .0
    }

    /// Return a swim client capable of opening downlinks to other servers and to local planes.
    pub fn client(&self) -> SwimClient<Path> {
        self.client.clone()
    }
}

/// Represents an error that can occur while using the server builder.
#[derive(Debug)]
pub enum SwimServerBuilderError {
    /// An error that occurs when trying to create a server without providing the address first.
    MissingAddress,
}

impl Display for SwimServerBuilderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SwimServerBuilderError::MissingAddress => {
                write!(f, "Cannot create a swim server without an address")
            }
        }
    }
}

impl Error for SwimServerBuilderError {}

/// Represents errors that can occur in the server.
#[derive(Debug)]
pub enum ServerError {
    /// An error that occurred when the server was running.
    RuntimeError(io::Error),
    /// An error that occurred in the client router.
    RoutingError(RoutingError),
    /// An error that occurred when closing the server.
    CloseError(TaskError),
}

impl Display for ServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::RuntimeError(err) => err.fmt(f),
            ServerError::CloseError(err) => err.fmt(f),
            ServerError::RoutingError(err) => err.fmt(f),
        }
    }
}

impl Error for ServerError {}

/// Swim server configuration.
///
/// * `conn_config` - Configuration parameters for remote connections.
/// * `agent_config` - Configuration parameters controlling how agents and lanes are executed.
/// * `websocket_config` - Configuration for WebSocket connections.
pub struct SwimServerConfig {
    conn_config: ConnectionConfig,
    agent_config: AgentExecutionConfig,
    websocket_config: WebSocketConfig,
}

impl Default for SwimServerConfig {
    fn default() -> Self {
        SwimServerConfig {
            conn_config: Default::default(),
            agent_config: Default::default(),
            websocket_config: Default::default(),
        }
    }
}

/// Handle for a server instance.
///
/// The handle is returned when a new server instance is created and is used for terminating
/// the server or obtaining its address.
pub struct ServerHandle {
    either_address: Either<promise::Receiver<SocketAddr>, Option<SocketAddr>>,
    stop_trigger_tx: CloseSender,
}

impl ServerHandle {
    /// Returns the local address that this server is bound to.
    ///
    /// This can be useful, for example, when binding to port 0 to figure out
    /// which port was actually bound.
    pub async fn address(&mut self) -> Option<&SocketAddr> {
        let ServerHandle { either_address, .. } = self;

        if either_address.is_left() {
            if let Either::Left(promise) = std::mem::replace(either_address, Either::Right(None)) {
                *either_address = Either::Right(promise.await.ok().map(|r| *r));
            }
        }

        either_address
            .as_ref()
            .right()
            .map(Option::as_ref)
            .flatten()
    }

    /// Terminates the associated server instance.
    ///
    /// Returns `Ok` if all sever tasks have been successfully notified to terminate
    /// and `ServerError` otherwise.
    ///
    /// # Example:
    /// ```
    /// use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// use swim_server::interface::{SwimServer, SwimServerBuilder};
    ///
    /// let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    /// let (mut swim_server, server_handle) = SwimServerBuilder::default().bind_to(address).build().unwrap();
    ///
    /// let future = server_handle.stop();
    /// ```
    pub async fn stop(self) -> Result<(), ServerError> {
        let ServerHandle {
            stop_trigger_tx, ..
        } = self;

        //Todo dm change buffer size
        let (tx, mut rx) = mpsc::channel(8);

        if stop_trigger_tx.provide(tx).is_err() {
            return Err(ServerError::CloseError(TaskError));
        }

        match rx.recv().await {
            Some(close_result) => {
                if let Err(routing_err) = close_result {
                    return Err(ServerError::RoutingError(routing_err));
                }
            }
            None => return Err(ServerError::CloseError(TaskError)),
        }

        Ok(())
    }
}
