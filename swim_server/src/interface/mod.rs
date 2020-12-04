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
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::SwimAgent;
use crate::plane::error::AmbiguousRoutes;
use crate::plane::lifecycle::PlaneLifecycle;
use crate::plane::router::PlaneRouter;
use crate::plane::spec::PlaneBuilder;
use crate::plane::{run_plane, EnvChannel};
use crate::routing::remote::config::ConnectionConfig;
use crate::routing::remote::net::plain::TokioPlainTextNetworking;
use crate::routing::remote::RemoteConnectionsTask;
use crate::routing::ws::tungstenite::TungsteniteWsConnections;
use crate::routing::{SuperRouter, SuperRouterFactory};
use futures::{io, join};
use std::fmt::Debug;
use std::net::SocketAddr;
use swim_runtime::time::clock::RuntimeClock;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::route_pattern::RoutePattern;
use utilities::sync::trigger;

/// Swim server instance.
///
/// The Swim server runs a plane and its agents and a task for remote connections asynchronously.
pub struct SwimServer {
    address: SocketAddr,
    conn_config: ConnectionConfig,
    agent_config: AgentExecutionConfig,
    websocket_config: WebSocketConfig,
    routes: PlaneBuilder<RuntimeClock, EnvChannel, PlaneRouter<SuperRouter>>,
    plane_lifecycle: Option<Box<dyn PlaneLifecycle>>,
    plane_stop_rx: trigger::Receiver,
    conn_stop_rx: trigger::Receiver,
}

impl SwimServer {
    /// Creates a new Swim server instance.
    ///
    /// # Arguments
    /// * `address` - The address on which the server will run.
    /// * `conn_config` - Configuration parameters for remote connections.
    /// * `agent_config` - Configuration parameters controlling how agents and lanes are executed.
    /// * `websocket_config` - Configuration for WebSocket connections.
    /// * `plane_lifecycle` - Custom lifecycle for the server plane.
    ///
    /// # Example
    /// ```
    /// use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// use swim_server::interface::SwimServer;
    /// use swim_server::routing::remote::config::ConnectionConfig;
    /// use swim_server::agent::lane::channels::AgentExecutionConfig;
    /// use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
    ///
    /// let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    /// let conn_config = ConnectionConfig::default();
    /// let agent_config = AgentExecutionConfig::default();
    /// let websocket_config = WebSocketConfig::default();
    /// let plane_lifecycle = None;      
    ///
    /// let (mut swim_server, server_handle) = SwimServer::new(address, conn_config, agent_config, websocket_config, plane_lifecycle);
    /// ```
    pub fn new(
        address: SocketAddr,
        conn_config: ConnectionConfig,
        agent_config: AgentExecutionConfig,
        websocket_config: WebSocketConfig,
        plane_lifecycle: Option<Box<dyn PlaneLifecycle>>,
    ) -> (SwimServer, ServerHandle) {
        let (plane_stop_tx, plane_stop_rx) = trigger::trigger();
        let (conn_stop_tx, conn_stop_rx) = trigger::trigger();

        (
            SwimServer {
                address,
                conn_config,
                agent_config,
                websocket_config,
                routes: PlaneBuilder::new(),
                plane_lifecycle,
                plane_stop_rx,
                conn_stop_rx,
            },
            ServerHandle {
                stop_plane_trigger: plane_stop_tx,
                stop_conn_trigger: conn_stop_tx,
            },
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
        let conn_config = ConnectionConfig::default();
        let agent_config = AgentExecutionConfig::default();
        let websocket_config = WebSocketConfig::default();
        let (plane_stop_tx, plane_stop_rx) = trigger::trigger();
        let (conn_stop_tx, conn_stop_rx) = trigger::trigger();

        (
            SwimServer {
                address,
                conn_config,
                agent_config,
                websocket_config,
                routes: PlaneBuilder::new(),
                plane_lifecycle: None,
                plane_stop_rx,
                conn_stop_rx,
            },
            ServerHandle {
                stop_plane_trigger: plane_stop_tx,
                stop_conn_trigger: conn_stop_tx,
            },
        )
    }

    /// Adds an agent to a given route on the Swim server plane.
    ///
    /// # Arguments
    /// * `route` - The route for the agent.
    /// * `config` - Configuration for the agent.
    /// * `lifecycle` - Lifecycle of the agent.
    ///
    /// # Example
    /// ```
    /// use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// use swim_server::interface::SwimServer;
    /// use swim_server::RoutePattern;
    /// use swim_server::agent_lifecycle;
    /// use swim_server::agent::SwimAgent;
    /// use swim_server::agent::AgentContext;
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
    /// let result = swim_server.add_route(
    ///                             RoutePattern::parse_str("/rust").unwrap(),
    ///                             RustAgentConfig {},
    ///                             RustAgentLifecycle {});
    /// assert!(result.is_ok());
    /// ```
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
        self.routes.add_route(route, config, lifecycle)
    }

    /// Runs the Swim server instance.
    ///
    /// Runs the plane and remote connections tasks of the server asynchronously
    /// and returns any errors from the connections task.
    ///
    /// # Panics
    /// The task will panic if the address provided to the server is already being used.
    pub async fn run(self) -> Result<(), io::Error> {
        let SwimServer {
            address,
            conn_config,
            agent_config,
            websocket_config,
            routes,
            plane_lifecycle,
            plane_stop_rx,
            conn_stop_rx,
        } = self;

        let spec = match plane_lifecycle {
            Some(pl) => routes.build_with_lifecycle(pl),
            None => routes.build(),
        };

        let (plane_tx, plane_rx) = mpsc::channel(agent_config.lane_attachment_buffer.get());
        let (remote_tx, remote_rx) = mpsc::channel(conn_config.router_buffer_size.get());
        let super_router_fac = SuperRouterFactory::new(plane_tx.clone(), remote_tx.clone());

        let clock = swim_runtime::time::clock::runtime_clock();

        let plane_future = run_plane(
            agent_config,
            clock,
            spec,
            plane_stop_rx,
            OpenEndedFutures::new(),
            (plane_tx, plane_rx),
            super_router_fac.clone(),
        );

        let connections_future = RemoteConnectionsTask::new(
            conn_config,
            TokioPlainTextNetworking {},
            address,
            TungsteniteWsConnections {
                config: websocket_config,
            },
            super_router_fac,
            conn_stop_rx,
            OpenEndedFutures::new(),
            (remote_tx, remote_rx),
        )
        .await
        .unwrap_or_else(|_| panic!("The address \"{}\" is already in use.", address))
        .run();

        join!(connections_future, plane_future).0
    }
}

/// Handle for stopping a server instance.
///
/// The handle is returned when a new server instance is created and is used for terminating
/// the server.
pub struct ServerHandle {
    stop_plane_trigger: trigger::Sender,
    stop_conn_trigger: trigger::Sender,
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
        self.stop_plane_trigger.trigger() && self.stop_conn_trigger.trigger()
    }
}
