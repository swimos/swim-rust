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

//! Interface for creating and running Swim server instances.
//!
//! The module provides methods and structures for creating and running Swim server instances.
//!
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::plane::PlaneActiveRoutes;
use crate::plane::RouteResolver;
use crate::plane::{run_plane, EnvChannel};
use crate::plane::{ContextImpl, PlaneSpec};
use either::Either;
use futures::{io, join};
use ratchet::{NoExtProvider, ProtocolRegistry, WebSocketConfig};
use server_store::plane::PlaneStore;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use swim_async_runtime::task::TaskError;
use swim_async_runtime::time::clock::RuntimeClock;
use swim_client::connections::{PoolTask, SwimConnPool};
use swim_client::downlink::subscription::DownlinksTask;
use swim_client::downlink::Downlinks;
use swim_client::interface::ClientContext;
use swim_model::path::Addressable;
use swim_runtime::configuration::{DownlinkConfig, DownlinkConnectionsConfig, DownlinksConfig};
use swim_runtime::ws::ext::RatchetNetworking;
use swim_utilities::future::open_ended::OpenEndedFutures;
use swim_utilities::trigger::promise;
use tokio::sync::mpsc;
use url::Url;

use crate::PlaneBuilder;
#[cfg(feature = "persistence")]
use server_store::rocks;
use server_store::{ServerStore, SwimStore};
use swim_client::downlink::error::SubscriptionError;
use swim_model::path::Path;
use swim_runtime::error::RoutingError;
use swim_runtime::remote::config::RemoteConnectionsConfig;
use swim_runtime::remote::net::dns::Resolver;
use swim_runtime::remote::net::plain::TokioPlainTextNetworking;
use swim_runtime::remote::{RemoteConnectionChannels, RemoteConnectionsTask};
use swim_runtime::router2::{PlaneRoutingRequest, RemoteRoutingRequest, Router};
use swim_runtime::routing::{CloseReceiver, CloseSender};
use swim_store::nostore::NoStoreOpts;
use swim_store::{Keyspaces, StoreError};

/// Builder to create Swim server instance.
///
/// The builder can be created with default or custom configuration.
pub struct SwimServerBuilder<S>
where
    S: SwimStore,
{
    address: Option<SocketAddr>,
    config: SwimServerConfig,
    planes: Vec<PlaneSpec<RuntimeClock, EnvChannel, S::PlaneStore>>,
    store: S,
}

type ServerPlaneBuilder<S> = PlaneBuilder<RuntimeClock, EnvChannel, S>;

impl<S> SwimServerBuilder<S>
where
    S: SwimStore,
{
    pub fn store(self, store: S) -> SwimServerBuilder<S> {
        SwimServerBuilder { store, ..self }
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
    /// use swim_server::interface::{SwimServer, SwimServerBuilder, SwimServerConfig};
    /// use swim_server::RoutePattern;
    /// use swim_server::agent_lifecycle;
    /// use swim_server::agent::SwimAgent;
    /// use swim_server::agent::AgentContext;
    /// use swim_server::PlaneBuilder;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
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
    /// let mut swim_server_builder = SwimServerBuilder::no_store(SwimServerConfig::default()).unwrap();
    /// let mut plane_builder = swim_server_builder.plane_builder("test").unwrap();
    ///
    /// plane_builder
    ///     .add_route(
    ///          RoutePattern::parse_str("/rust").unwrap(),
    ///          RustAgentConfig {},
    ///          RustAgentLifecycle {},
    ///     ).unwrap();
    ///
    /// swim_server_builder.add_plane(plane_builder.build());
    /// # }
    /// ```
    pub fn add_plane(&mut self, plane: PlaneDef<S::PlaneStore>) {
        if !self.planes.is_empty() {
            panic!("Multiple planes are not supported yet")
        }
        self.planes.push(plane)
    }

    pub fn plane_builder<I: Into<String>>(
        &mut self,
        name: I,
    ) -> Result<ServerPlaneBuilder<S::PlaneStore>, SwimServerBuilderError> {
        let store = self
            .store
            .plane_store(name.into())
            .map_err(SwimServerBuilderError::StoreError)?;
        Ok(PlaneBuilder::new(store))
    }

    /// Build the Swim Server.
    ///
    /// Returns an error if an address has not been provided for the server.
    ///
    /// # Example
    /// ```
    /// use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// use swim_server::interface::{SwimServer, SwimServerBuilder, SwimServerConfig};
    /// use swim_server::RoutePattern;
    /// use swim_server::agent_lifecycle;
    /// use swim_server::agent::SwimAgent;
    /// use swim_server::agent::AgentContext;
    /// use swim_server::PlaneBuilder;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
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
    /// let mut swim_server_builder = SwimServerBuilder::no_store(SwimServerConfig::default()).unwrap();
    /// let mut plane_builder = swim_server_builder.plane_builder("test").unwrap();
    ///
    /// plane_builder
    ///     .add_route(
    ///          RoutePattern::parse_str("/rust").unwrap(),
    ///          RustAgentConfig {},
    ///          RustAgentLifecycle {},
    ///     ).unwrap();
    ///
    /// swim_server_builder.add_plane(plane_builder.build());
    ///
    /// let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    /// let (swim_server, server_handle) = swim_server_builder.bind_to(address).build().unwrap();
    /// # }
    /// ```
    pub fn build(
        self,
    ) -> Result<(SwimServer<S::PlaneStore>, ServerHandle), SwimServerBuilderError> {
        let SwimServerBuilder {
            address,
            planes,
            config,
            store: _,
        } = self;

        let (close_tx, close_rx) = promise::promise();
        let (address_tx, address_rx) = promise::promise();

        let (client_tx, client_rx) = mpsc::channel(config.conn_config.channel_buffer_size.get());
        let (remote_tx, remote_rx) = mpsc::channel(config.conn_config.channel_buffer_size.get());
        let (plane_tx, plane_rx) = mpsc::channel(config.conn_config.channel_buffer_size.get());

        let router = Router::client(client_tx.clone(), remote_tx.clone());

        let (connection_pool, connection_pool_task) = SwimConnPool::new(
            config.downlink_connections_config,
            (client_tx, client_rx),
            router.clone(),
            close_rx.clone(),
        );

        let (downlinks, downlinks_task) = Downlinks::new(
            config.downlink_connections_config.dl_req_buffer_size,
            connection_pool,
            Arc::new(config.downlinks_config.clone()),
            close_rx.clone(),
        );

        let downlinks_context = ClientContext::new(downlinks);

        Ok((
            SwimServer {
                config,
                planes,
                stop_trigger_rx: close_rx,
                address: address.ok_or(SwimServerBuilderError::MissingAddress)?,
                address_tx,
                router,
                remote_channel: (remote_tx, remote_rx),
                plane_channel: (plane_tx, plane_rx),
                client_context: downlinks_context,
                connection_pool_task,
                downlinks_task,
            },
            ServerHandle {
                either_address: Either::Left(address_rx),
                stop_trigger_tx: close_tx,
            },
        ))
    }
}

#[cfg(feature = "persistence")]
impl SwimServerBuilder<ServerStore<rocks::RocksOpts>> {
    /// Create a new server builder with custom configuration backed by RocksDB.
    ///
    /// # Arguments
    /// * `config` - The custom configuration for the server.
    pub fn new(config: SwimServerConfig) -> io::Result<Self> {
        Ok(SwimServerBuilder {
            address: None,
            config,
            planes: Vec::new(),
            store: ServerStore::new(
                rocks::RocksOpts::default(),
                rocks::default_keyspaces(),
                "target".into(),
            )?,
        })
    }

    /// Constructs a new `SwimServerBuilder` with the default configuration and is backed by a
    /// temporary store.
    ///
    /// A temporary store uses a RocksDB engine that operates out of a temporary directory which is
    /// deleted when the application terminates.
    ///
    /// # Panics
    /// Panics if the directory cannot be created.
    pub fn temporary_store(config: SwimServerConfig, prefix: &str) -> io::Result<Self> {
        Ok(SwimServerBuilder {
            address: None,
            store: ServerStore::<rocks::RocksOpts>::transient(
                rocks::RocksOpts::default(),
                rocks::default_keyspaces(),
                prefix,
            )?,
            config,
            planes: vec![],
        })
    }
}

impl SwimServerBuilder<ServerStore<NoStoreOpts>> {
    /// Create a new server builder with custom configuration which will persist no data.
    ///
    /// # Arguments
    /// * `config` - The custom configuration for the server.
    pub fn no_store(config: SwimServerConfig) -> io::Result<Self> {
        Ok(SwimServerBuilder {
            address: None,
            config,
            planes: Vec::new(),
            store: ServerStore::new(NoStoreOpts, Keyspaces::new(vec![]), "target".into())?,
        })
    }
}

type PlaneDef<S> = PlaneSpec<RuntimeClock, EnvChannel, S>;

/// Swim server instance.
///
/// The Swim server runs a plane and its agents and a task for remote connections asynchronously.
pub struct SwimServer<S>
where
    S: PlaneStore,
{
    address: SocketAddr,
    config: SwimServerConfig,
    planes: Vec<PlaneDef<S>>,
    stop_trigger_rx: CloseReceiver,
    address_tx: promise::Sender<SocketAddr>,
    router: Router<Path>,
    remote_channel: (
        mpsc::Sender<RemoteRoutingRequest>,
        mpsc::Receiver<RemoteRoutingRequest>,
    ),
    plane_channel: (
        mpsc::Sender<PlaneRoutingRequest>,
        mpsc::Receiver<PlaneRoutingRequest>,
    ),
    client_context: ClientContext<Path>,
    connection_pool_task: PoolTask<Path>,
    downlinks_task: DownlinksTask<Path>,
}

impl<S> SwimServer<S>
where
    S: PlaneStore,
{
    /// Runs the Swim server instance.
    ///
    /// Runs the planes and remote connections tasks of the server asynchronously
    /// and returns any errors from the connections task.
    ///
    /// # Panics
    /// The task will panic if the address provided to the server is already being used.
    pub async fn run(self) -> Result<(), ServerError> {
        let SwimServer {
            address,
            config,
            mut planes,
            stop_trigger_rx,
            address_tx,
            router,
            remote_channel: (remote_tx, remote_rx),
            plane_channel: (plane_tx, plane_rx),
            client_context: downlinks_context,
            connection_pool_task,
            downlinks_task,
        } = self;

        let SwimServerConfig {
            websocket_config,
            agent_config,
            conn_config,
            ..
        } = config;

        // Todo add support for multiple planes in the future
        let mut spec = planes
            .pop()
            .expect("The server cannot be started without a plane");

        let clock = swim_async_runtime::time::clock::runtime_clock();

        let context = ContextImpl::new(plane_tx.clone(), spec.routes());

        let lifecycle = spec.take_lifecycle();

        let resolver = RouteResolver::new(
            clock,
            downlinks_context,
            agent_config,
            spec,
            router.clone(),
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
            RatchetNetworking {
                config: websocket_config,
                provider: NoExtProvider,
                subprotocols: ProtocolRegistry::new(vec!["warp0"]).unwrap(),
            },
            router,
            OpenEndedFutures::new(),
            RemoteConnectionChannels::new(remote_tx, remote_rx, stop_trigger_rx),
        )
        .await
        .unwrap_or_else(|err| panic!("Could not connect to \"{}\": {}", address, err));

        let _ = match connections_task.listener().unwrap().local_addr() {
            Ok(local_addr) => address_tx.provide(local_addr),
            Err(err) => panic!("Could not resolve server address: {}", err),
        };

        let result = join!(
            downlinks_task.run(),
            connections_task.run(),
            connection_pool_task.run(),
            plane_future,
        );

        match result {
            (Err(err), _, _, _) => Err(err.into()),
            (_, Err(err), _, _) => Err(err.into()),
            (_, _, Err(err), _) => Err(ServerError::RoutingError(RoutingError::PoolError(err))),
            _ => Ok(()),
        }
    }

    /// Get a client context capable of opening downlinks to other servers.
    pub fn client_context(&self) -> ClientContext<Path> {
        self.client_context.clone()
    }
}

/// Represents an error that can occur while using the server builder.
#[derive(Debug)]
pub enum SwimServerBuilderError {
    /// An error that occurs when trying to create a server without providing the address first.
    MissingAddress,
    /// An error occurred when attempting to build the delegate store.
    StoreError(StoreError),
}

impl Display for SwimServerBuilderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SwimServerBuilderError::MissingAddress => {
                write!(f, "Cannot create a swim server without an address")
            }
            SwimServerBuilderError::StoreError(e) => {
                write!(f, "{}", e)
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
    /// An error that occurred when subscribing to a downlink.
    Subscription(SubscriptionError<Path>),
    /// An error that occurred when closing the server.
    CloseError(TaskError),
}

impl Display for ServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::RuntimeError(err) => err.fmt(f),
            ServerError::CloseError(err) => err.fmt(f),
            ServerError::Subscription(err) => err.fmt(f),
            ServerError::RoutingError(err) => err.fmt(f),
        }
    }
}

impl Error for ServerError {}

impl From<io::Error> for ServerError {
    fn from(err: std::io::Error) -> Self {
        ServerError::RuntimeError(err)
    }
}

impl From<SubscriptionError<Path>> for ServerError {
    fn from(err: SubscriptionError<Path>) -> Self {
        ServerError::Subscription(err)
    }
}

/// Swim server configuration.
///
/// * `conn_config` - Configuration parameters for remote connections.
/// * `agent_config` - Configuration parameters controlling how agents and lanes are executed.
/// * `websocket_config` - Configuration for WebSocket connections.
/// * `downlink_connections_config` - Configuration parameters for the downlink connections.
/// * `downlinks_config` - CConfiguration for the behaviour of downlinks.
pub struct SwimServerConfig {
    pub conn_config: RemoteConnectionsConfig,
    pub agent_config: AgentExecutionConfig,
    pub websocket_config: WebSocketConfig,
    pub downlink_connections_config: DownlinkConnectionsConfig,
    pub downlinks_config: ServerDownlinksConfig,
}

impl Default for SwimServerConfig {
    fn default() -> Self {
        SwimServerConfig {
            conn_config: Default::default(),
            agent_config: Default::default(),
            websocket_config: Default::default(),
            downlink_connections_config: Default::default(),
            downlinks_config: Default::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ServerDownlinksConfig {
    default: DownlinkConfig,
    by_host: HashMap<Url, DownlinkConfig>,
    by_lane: HashMap<Path, DownlinkConfig>,
}

impl ServerDownlinksConfig {
    pub fn new(default: DownlinkConfig) -> ServerDownlinksConfig {
        ServerDownlinksConfig {
            default,
            by_host: HashMap::new(),
            by_lane: HashMap::new(),
        }
    }
}

impl DownlinksConfig for ServerDownlinksConfig {
    type PathType = Path;

    fn config_for(&self, path: &Self::PathType) -> DownlinkConfig {
        let ServerDownlinksConfig {
            default,
            by_host,
            by_lane,
            ..
        } = self;
        match by_lane.get(path) {
            Some(config) => *config,
            _ => {
                let maybe_host = path.host();

                match maybe_host {
                    Some(host) => match by_host.get(&host) {
                        Some(config) => *config,
                        _ => *default,
                    },
                    None => *default,
                }
            }
        }
    }

    fn for_host(&mut self, host: Url, params: DownlinkConfig) {
        self.by_host.insert(host, params);
    }

    fn for_lane(&mut self, lane: &Path, params: DownlinkConfig) {
        self.by_lane.insert(lane.clone(), params);
    }
}

impl Default for ServerDownlinksConfig {
    fn default() -> Self {
        ServerDownlinksConfig::new(Default::default())
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
    /// let (mut swim_server, server_handle) = SwimServerBuilder::no_store(Default::default()).unwrap().bind_to(address).build().unwrap();
    ///
    /// let future = server_handle.stop();
    /// ```
    pub async fn stop(self) -> Result<(), ServerError> {
        let ServerHandle {
            stop_trigger_tx, ..
        } = self;

        let (tx, mut rx) = mpsc::channel(8);

        if stop_trigger_tx.provide(tx).is_err() {
            return Err(ServerError::CloseError(TaskError));
        }

        if let Some(Err(routing_err)) = rx.recv().await {
            return Err(ServerError::RoutingError(routing_err));
        }

        Ok(())
    }
}
