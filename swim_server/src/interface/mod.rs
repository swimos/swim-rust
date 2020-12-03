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

#[cfg(test)]
mod tests;

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
        .expect("The server could not start.")
        .run();

        join!(connections_future, plane_future).0
    }
}

pub struct ServerHandle {
    stop_plane_trigger: trigger::Sender,
    stop_conn_trigger: trigger::Sender,
}

impl ServerHandle {
    pub fn stop(self) -> bool {
        self.stop_plane_trigger.trigger() && self.stop_conn_trigger.trigger()
    }
}
