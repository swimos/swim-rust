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
use crate::plane::router::PlaneRouter;
use crate::plane::spec::PlaneSpec;
use crate::plane::{run_plane, EnvChannel};
use crate::routing::remote::config::ConnectionConfig;
use crate::routing::remote::net::dns::Resolver;
use crate::routing::remote::net::plain::TokioPlainTextNetworking;
use crate::routing::remote::{RemoteConnectionChannels, RemoteConnectionsTask};
use crate::routing::{TopLevelRouter, TopLevelRouterFactory};
use futures::{io, join};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use swim_common::routing::ws::tungstenite::TungsteniteWsConnections;
use swim_runtime::task::{spawn, TaskError, TaskHandle};
use swim_runtime::time::clock::RuntimeClock;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::sync::trigger;

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
    /// let swim_server = tokio_test::block_on(swim_server_builder.bind_to(address).build());
    /// ```
    pub async fn build(self) -> Result<SwimServer, SwimServerBuilderError> {
        let SwimServerBuilder {
            address,
            mut planes,
            config,
        } = self;

        let address = address.ok_or(SwimServerBuilderError {
            msg: "Cannot create a swim server without an address".to_string(),
        })?;

        let (stop_trigger_tx, stop_trigger_rx) = trigger::trigger();

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

        let clock = swim_runtime::time::clock::runtime_clock();

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
            TungsteniteWsConnections {
                config: websocket_config,
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

        let local_addr = match connections_task.listener.local_addr() {
            Ok(local_addr) => local_addr,
            Err(err) => {
                return Err(SwimServerBuilderError {
                    msg: err.to_string(),
                })
            }
        };
        let connections_future = connections_task.run();

        let task_handle = spawn(async { join!(connections_future, plane_future).0 });

        Ok(SwimServer {
            local_addr,
            task_handle,
            stop_trigger_tx,
        })
    }
}

/// Represents an error that can occur while using the server builder.
#[derive(Debug)]
pub struct SwimServerBuilderError {
    msg: String,
}

impl Display for SwimServerBuilderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Error for SwimServerBuilderError {}

/// Swim server instance.
///
/// The Swim server runs a plane and its agents and a task for remote connections asynchronously.
pub struct SwimServer {
    local_addr: SocketAddr,
    task_handle: TaskHandle<Result<(), io::Error>>,
    stop_trigger_tx: trigger::Sender,
}

impl SwimServer {
    /// Returns the address of the server.
    pub fn addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    /// Returns the port of the server.
    pub fn port(&self) -> u16 {
        self.local_addr.port()
    }

    /// Terminates the server instance.
    ///
    /// Returns any runtime errors that the server has encountered.
    /// ```
    pub async fn stop(self) -> Result<(), ServerError> {
        self.stop_trigger_tx.trigger();

        match self.task_handle.await {
            Ok(result) => result.map_err(ServerError::RuntimeError),
            Err(err) => Err(ServerError::CloseError(err)),
        }
    }
}

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
