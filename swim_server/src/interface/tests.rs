use crate::agent::command_lifecycle;
use crate::agent::lane::lifecycle::{LaneLifecycle, StatefulLaneLifecycleBase};
use crate::agent::lane::model::action::CommandLane;
use crate::agent::lane::model::value::ValueLane;
use crate::agent::lane::strategy::Queue;
use crate::agent::value_lifecycle;
use crate::agent::AgentContext;
use crate::agent::SwimAgent;
use crate::agent_lifecycle;
use crate::interface::SwimServer;
use async_std::task;
use futures::join;
use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tracing::{event, Level};
use utilities::route_pattern::RoutePattern;
use utilities::trace::init_trace;

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
#[ignore]
async fn run_server() {
    init_trace(vec!["swim_server::interface"]);

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    let (mut swim_server, server_handle) = SwimServer::new_with_default(address);

    swim_server
        .add_route(
            RoutePattern::parse_str("/rust").unwrap(),
            RustAgentConfig {},
            RustAgentLifecycle {},
        )
        .unwrap();

    let stop = async {
        task::sleep(Duration::from_secs(60)).await;
        server_handle.stop();
    };

    join!(swim_server.run(), stop).0.unwrap();
}
