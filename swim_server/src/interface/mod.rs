use crate::agent::command_lifecycle;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::lifecycle::{LaneLifecycle, StatefulLaneLifecycleBase};
use crate::agent::lane::model::action::CommandLane;
use crate::agent::lane::model::value::ValueLane;
use crate::agent::lane::strategy::Queue;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::value_lifecycle;
use crate::agent::AgentContext;
use crate::agent::SwimAgent;
use crate::agent_lifecycle;
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
use futures::join;
use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use swim_runtime::time::clock::RuntimeClock;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tracing::{event, Level};
use utilities::future::open_ended::OpenEndedFutures;
use utilities::route_pattern::RoutePattern;
use utilities::sync::trigger;
use utilities::trace::init_trace;

pub struct SwimServer {
    address: SocketAddr,
    conn_config: ConnectionConfig,
    agent_config: AgentExecutionConfig,
    websocket_config: WebSocketConfig,
    routes: PlaneBuilder<RuntimeClock, EnvChannel, PlaneRouter<SuperRouter>>,
    plane_lifecycle: Option<Box<dyn PlaneLifecycle>>,
}

impl SwimServer {
    pub fn new(
        address: SocketAddr,
        conn_config: ConnectionConfig,
        agent_config: AgentExecutionConfig,
        websocket_config: WebSocketConfig,
        plane_lifecycle: Option<Box<dyn PlaneLifecycle>>,
    ) -> SwimServer {
        SwimServer {
            address,
            conn_config,
            agent_config,
            websocket_config,
            routes: PlaneBuilder::new(),
            plane_lifecycle,
        }
    }

    pub fn new_with_default(address: SocketAddr) -> SwimServer {
        let conn_config = ConnectionConfig::default();
        let agent_config = AgentExecutionConfig::default();
        let websocket_config = WebSocketConfig::default();

        SwimServer {
            address,
            conn_config,
            agent_config,
            websocket_config,
            routes: PlaneBuilder::new(),
            plane_lifecycle: None,
        }
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

    pub async fn run(self) {
        let SwimServer {
            address,
            conn_config,
            agent_config,
            websocket_config,
            routes,
            plane_lifecycle,
        } = self;

        let spec = match plane_lifecycle {
            Some(pl) => routes.build_with_lifecycle(pl),
            None => routes.build(),
        };

        let (plane_tx, plane_rx) = mpsc::channel(agent_config.lane_attachment_buffer.get());
        let (remote_tx, remote_rx) = mpsc::channel(conn_config.router_buffer_size.get());
        let super_router_fac = SuperRouterFactory::new(plane_tx.clone(), remote_tx.clone());

        let (_plane_stop_tx, plane_stop_rx) = trigger::trigger();
        let (_conn_stop_tx, conn_stop_rx) = trigger::trigger();

        let clock = swim_runtime::time::clock::runtime_clock();

        let plane_future = run_plane(
            agent_config,
            clock,
            spec,
            plane_stop_rx,
            OpenEndedFutures::new(),
            plane_tx,
            plane_rx,
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
            remote_tx,
            remote_rx,
        )
        .await
        .expect("The server could not start.")
        .run();

        join!(connections_future, plane_future);
    }
}

pub struct ServerHandle {
    stop_trigger: trigger::Sender,
}

impl ServerHandle {
    pub fn stop(self) -> bool {
        self.stop_trigger.trigger()
    }
}

mod swim_server {
    pub(in crate) use crate::*;
}

#[derive(Debug, SwimAgent)]
#[agent(config = "RustAgentConfig")]
pub struct RustAgent {
    #[lifecycle(public, name = "EchoLifecycle")]
    echo: CommandLane<String>,
    #[lifecycle(public, name = "CounterLifecycle")]
    counter: ValueLane<i32>,
}

#[derive(Debug, Clone)]
pub struct RustAgentConfig;

#[agent_lifecycle(agent = "RustAgent")]
struct RustAgentLifecycle;

impl RustAgentLifecycle {
    async fn on_start<Context>(&self, _context: &Context)
    where
        Context: AgentContext<RustAgent> + Sized + Send + Sync,
    {
        event!(Level::DEBUG, "Rust agent has started!");
    }
}

#[command_lifecycle(agent = "RustAgent", command_type = "String")]
struct EchoLifecycle;

impl EchoLifecycle {
    async fn on_command<Context>(
        &self,
        command: String,
        _model: &CommandLane<String>,
        _context: &Context,
    ) where
        Context: AgentContext<RustAgent> + Sized + Send + Sync + 'static,
    {
        event!(Level::DEBUG, "Command received: {}", command);
    }
}

impl LaneLifecycle<RustAgentConfig> for EchoLifecycle {
    fn create(_config: &RustAgentConfig) -> Self {
        EchoLifecycle {}
    }
}

#[value_lifecycle(agent = "RustAgent", event_type = "i32")]
struct CounterLifecycle;

impl CounterLifecycle {
    async fn on_start<Context>(&self, _model: &ValueLane<i32>, _context: &Context)
    where
        Context: AgentContext<RustAgent> + Sized + Send + Sync,
    {
        event!(Level::DEBUG, "Counter lane has started!");
    }

    async fn on_event<Context>(&self, event: &Arc<i32>, _model: &ValueLane<i32>, _context: &Context)
    where
        Context: AgentContext<RustAgent> + Sized + Send + Sync + 'static,
    {
        event!(Level::DEBUG, "Event received: {}", event);
    }
}

impl LaneLifecycle<RustAgentConfig> for CounterLifecycle {
    fn create(_config: &RustAgentConfig) -> Self {
        CounterLifecycle {}
    }
}

impl StatefulLaneLifecycleBase for CounterLifecycle {
    type WatchStrategy = Queue;

    fn create_strategy(&self) -> Self::WatchStrategy {
        Queue::default()
    }
}

#[tokio::test]
// #[ignore]
async fn ws_plane() {
    init_trace(vec!["swim_server::interface"]);

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    let mut swim_server = SwimServer::new_with_default(address);

    swim_server
        .add_route(
            RoutePattern::parse_str("/rust").unwrap(),
            RustAgentConfig {},
            RustAgentLifecycle {},
        )
        .unwrap();

    swim_server.run().await;
}
