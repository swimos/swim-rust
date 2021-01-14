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
use std::net::SocketAddr;
use std::sync::Arc;
use swim_common::routing::ws::tungstenite::TungsteniteWsConnections;
use swim_runtime::time::clock::RuntimeClock;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::sync::trigger;

/// Swim server instance.
///
/// The Swim server runs a plane and its agents and a task for remote connections asynchronously.
pub struct SwimServer {
    address: SocketAddr,
    config: SwimServerConfig,
    planes: Vec<PlaneSpec<RuntimeClock, EnvChannel, PlaneRouter<TopLevelRouter>>>,
    stop_trigger_rx: trigger::Receiver,
}

impl SwimServer {
    /// Creates a new Swim server instance.
    ///
    /// # Arguments
    /// * `address` - The address on which the server will run.
    /// * `config` - Configuration parameters for the server.
    ///
    /// # Example
    /// ```
    /// use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// use swim_server::interface::{SwimServer, SwimServerConfig};
    /// use swim_server::routing::remote::config::ConnectionConfig;
    /// use swim_server::agent::lane::channels::AgentExecutionConfig;
    /// use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
    ///
    /// let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    /// let config = SwimServerConfig::default();
    ///
    /// let (mut swim_server, server_handle) = SwimServer::new(address, config);
    /// ```
    pub fn new(address: SocketAddr, config: SwimServerConfig) -> (SwimServer, ServerHandle) {
        let (stop_trigger_tx, stop_trigger_rx) = trigger::trigger();
        (
            SwimServer {
                address,
                config,
                planes: Vec::new(),
                stop_trigger_rx,
            },
            ServerHandle { stop_trigger_tx },
        )
    }

    /// Creates a new Swim server instance with default configuration.
    ///
    /// # Arguments
    /// * `address` - The address on which the server will run.
    ///
    /// # Example
    /// ```
    /// use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// use swim_server::interface::SwimServer;
    ///
    /// let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    /// let (mut swim_server, server_handle) = SwimServer::new_with_default(address);
    /// ```
    pub fn new_with_default(address: SocketAddr) -> (SwimServer, ServerHandle) {
        let config = SwimServerConfig::default();
        let (stop_trigger_tx, stop_trigger_rx) = trigger::trigger();

        (
            SwimServer {
                address,
                config,
                planes: Vec::new(),
                stop_trigger_rx,
            },
            ServerHandle { stop_trigger_tx },
        )
    }

    /// Add a plane to the server.
    ///
    /// # Arguments
    /// * `plane` - The plane specification that will be added to the server.
    ///
    /// # Example
    /// ```
    /// use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// use swim_server::interface::SwimServer;
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
    /// #[agent_lifecycle(agent = "RustAgent")]
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
    /// let (mut swim_server, server_handle) = SwimServer::new_with_default(address);
    ///
    /// let mut plane_builder = PlaneBuilder::new();
    /// plane_builder
    ///     .add_route(
    ///          RoutePattern::parse_str("/rust").unwrap(),
    ///          RustAgentConfig {},
    ///          RustAgentLifecycle {},
    ///     ).unwrap();
    ///
    /// swim_server.add_plane(plane_builder.build());
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

        let connections_future = RemoteConnectionsTask::new(
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
        .unwrap_or_else(|_| panic!("The address \"{}\" is already in use.", address))
        .run();

        join!(connections_future, plane_future).0
    }
}

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

/// Handle for stopping a server instance.
///
/// The handle is returned when a new server instance is created and is used for terminating
/// the server.
pub struct ServerHandle {
    stop_trigger_tx: trigger::Sender,
}

impl ServerHandle {
    /// Terminates the associated server instance.
    ///
    /// Returns `true` if all sever tasks have been successfully notified to terminate
    /// and `false` otherwise.
    ///
    /// # Example:
    /// ```
    /// use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// use swim_server::interface::SwimServer;
    ///
    /// let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    /// let (mut swim_server, server_handle) = SwimServer::new_with_default(address);
    ///
    /// let success = server_handle.stop();
    ///
    /// assert!(success);
    /// ```
    pub fn stop(self) -> bool {
        self.stop_trigger_tx.trigger()
    }
}
