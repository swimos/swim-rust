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
//!
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::plane::router::PlaneRouter;
use crate::plane::spec::{PlaneBuilder, PlaneSpec};
use crate::plane::{run_plane, EnvChannel};
use crate::routing::remote::config::ConnectionConfig;
use crate::routing::remote::net::dns::Resolver;
use crate::routing::remote::net::plain::TokioPlainTextNetworking;
use crate::routing::remote::{RemoteConnectionChannels, RemoteConnectionsTask};
use crate::routing::{TopLevelRouter, TopLevelRouterFactory};
use either::Either;
use futures::{io, join};
use ratchet::{NoExtProvider, ProtocolRegistry, WebSocketConfig};
use server_store::plane::PlaneStore;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use store::StoreError;
use swim_async_runtime::task::TaskError;
use swim_async_runtime::time::clock::RuntimeClock;
use swim_runtime::ws::ext::RatchetNetworking;
use swim_utilities::future::open_ended::OpenEndedFutures;
use swim_utilities::trigger;
use swim_utilities::trigger::promise;
use tokio::sync::mpsc;

#[cfg(feature = "persistence")]
use server_store::rocks;
use server_store::{ServerStore, SwimStore};
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
    planes: Vec<PlaneSpec<RuntimeClock, EnvChannel, PlaneRouter<TopLevelRouter>, S::PlaneStore>>,
    store: S,
}

type ServerPlaneBuilder<S> = PlaneBuilder<RuntimeClock, EnvChannel, PlaneRouter<TopLevelRouter>, S>;

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
    /// use swim_server::plane::spec::PlaneBuilder;
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
    /// use swim_server::plane::spec::PlaneBuilder;
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

        let (stop_trigger_tx, stop_trigger_rx) = trigger::trigger();
        let (address_tx, address_rx) = promise::promise();

        Ok((
            SwimServer {
                config,
                planes,
                address: address.ok_or(SwimServerBuilderError::MissingAddress)?,
                stop_trigger_rx,
                address_tx,
            },
            ServerHandle {
                either_address: Either::Left(address_rx),
                stop_trigger_tx,
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

type PlaneDef<S> = PlaneSpec<RuntimeClock, EnvChannel, PlaneRouter<TopLevelRouter>, S>;

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
    stop_trigger_rx: trigger::Receiver,
    address_tx: promise::Sender<SocketAddr>,
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
    pub async fn run(self) -> Result<(), io::Error> {
        let SwimServer {
            address,
            config,
            mut planes,
            stop_trigger_rx,
            address_tx,
        } = self;

        let SwimServerConfig {
            websocket_config,
            agent_config,
            conn_config,
        } = config;

        // Todo add support for multiple planes in the future
        let spec = planes
            .pop()
            .expect("The server cannot be started without a plane");

        let (plane_tx, plane_rx) = mpsc::channel(agent_config.lane_attachment_buffer.get());
        let (remote_tx, remote_rx) = mpsc::channel(conn_config.router_buffer_size.get());
        let top_level_router_fac = TopLevelRouterFactory::new(plane_tx.clone(), remote_tx.clone());

        let clock = swim_async_runtime::time::clock::runtime_clock();

        let plane_future = run_plane(
            agent_config.clone(),
            clock,
            spec,
            stop_trigger_rx.clone(),
            OpenEndedFutures::new(),
            (plane_tx, plane_rx),
            top_level_router_fac.clone(),
        );

        let connections_task = RemoteConnectionsTask::new(
            conn_config,
            TokioPlainTextNetworking::new(Arc::new(Resolver::new().await)),
            address,
            RatchetNetworking {
                config: websocket_config,
                provider: NoExtProvider,
                subprotocols: ProtocolRegistry::new(vec!["warp0"]).unwrap(),
            },
            top_level_router_fac,
            OpenEndedFutures::new(),
            RemoteConnectionChannels {
                request_tx: remote_tx,
                request_rx: remote_rx,
                stop_trigger: stop_trigger_rx,
            },
        )
        .await
        .unwrap_or_else(|err| panic!("Could not connect to \"{}\": {}", address, err));

        let _ = match connections_task.listener.local_addr() {
            Ok(local_addr) => address_tx.provide(local_addr),
            Err(err) => panic!("Could not resolve server address: {}", err),
        };

        let connections_future = connections_task.run();

        join!(connections_future, plane_future).0
    }
}

/// Represents an error that can occur while using the server builder.
#[derive(Debug)]
pub enum SwimServerBuilderError {
    /// An error that occurs when trying to create a server without providing the address first.
    MissingAddress,
    /// An error occured when attempting to build the delegate store.
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
    /// An error that occurred when closing the server.
    CloseError(TaskError),
}

impl Display for ServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::RuntimeError(err) => err.fmt(f),
            ServerError::CloseError(err) => err.fmt(f),
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
    stop_trigger_tx: trigger::Sender,
}

impl ServerHandle {
    /// Returns the local address that this server is bound to.
    ///
    /// This can be useful, for example, when binding to port 0 to figure out
    /// which port was actually bound.
    pub async fn address(&mut self) -> Option<&SocketAddr> {
        let ServerHandle {
            either_address,
            stop_trigger_tx: _,
        } = self;

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
    /// Returns `true` if all sever tasks have been successfully notified to terminate
    /// and `false` otherwise.
    ///
    /// # Example:
    /// ```
    /// use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// use swim_server::interface::{SwimServer, SwimServerBuilder};
    ///
    /// let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    /// let (mut swim_server, server_handle) = SwimServerBuilder::no_store(Default::default()).unwrap().bind_to(address).build().unwrap();
    ///
    /// let success = server_handle.stop();
    ///
    /// assert!(success);
    /// ```
    pub fn stop(self) -> bool {
        self.stop_trigger_tx.trigger()
    }
}
