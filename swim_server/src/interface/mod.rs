use crate::agent::command_lifecycle;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::lifecycle::{LaneLifecycle, StatefulLaneLifecycleBase};
use crate::agent::lane::model::action::CommandLane;
use crate::agent::lane::model::value::ValueLane;
use crate::agent::lane::strategy::Queue;
use crate::agent::value_lifecycle;
use crate::agent::AgentContext;
use crate::agent::SwimAgent;
use crate::agent_lifecycle;
use crate::plane::run_plane;
use crate::plane::spec::PlaneBuilder;
use crate::routing::remote::config::ConnectionConfig;
use crate::routing::remote::net::plain::TokioPlainTextNetworking;
use crate::routing::remote::RemoteConnectionsTask;
use crate::routing::ws::tungstenite::TungsteniteWsConnections;
use crate::routing::SuperRouterFactory;
use futures::join;
use futures_util::core_reexport::time::Duration;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{event, Level};
use utilities::future::open_ended::OpenEndedFutures;
use utilities::route_pattern::RoutePattern;
use utilities::sync::trigger;
use utilities::trace::init_trace;

mod swim_server {
    pub use crate::*;
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
async fn ws_plane() {
    init_trace(vec!["swim_server::interface"]);

    let mut spec = PlaneBuilder::new();
    spec.add_route(
        RoutePattern::parse_str("/rust").unwrap(),
        RustAgentConfig {},
        RustAgentLifecycle {},
    )
    .unwrap();
    let spec = spec.build();

    let conn_config = ConnectionConfig {
        router_buffer_size: NonZeroUsize::new(10).unwrap(),
        channel_buffer_size: NonZeroUsize::new(10).unwrap(),
        activity_timeout: Duration::new(30, 00),
        connection_retries: Default::default(),
    };

    let agent_config = AgentExecutionConfig::default();

    let (plane_tx, plane_rx) = mpsc::channel(agent_config.lane_attachment_buffer.get());
    let (remote_tx, remote_rx) = mpsc::channel(conn_config.router_buffer_size.get());

    let super_router_fac = SuperRouterFactory::new(plane_tx.clone(), remote_tx.clone());

    let clock = swim_runtime::time::clock::runtime_clock();
    let (_stop_tx, stop_rx) = trigger::trigger();

    let plane = run_plane(
        agent_config,
        clock,
        spec,
        stop_rx,
        OpenEndedFutures::new(),
        plane_tx,
        plane_rx,
        super_router_fac.clone(),
    );

    let external = TokioPlainTextNetworking {};
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    let ws = TungsteniteWsConnections {
        config: Default::default(),
    };

    let (_stop_tx, stop_rx) = trigger::trigger();
    let spawner = OpenEndedFutures::new();

    let connections = RemoteConnectionsTask::new(
        conn_config,
        external,
        address,
        ws,
        super_router_fac,
        stop_rx,
        spawner,
        remote_tx,
        remote_rx,
    );

    let _ = join!(connections.await.unwrap().run(), plane);
}
