use async_std::task;
use futures::join;
use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use swim_server::agent::command_lifecycle;
use swim_server::agent::lane::lifecycle::{LaneLifecycle, StatefulLaneLifecycleBase};
use swim_server::agent::lane::model::action::CommandLane;
use swim_server::agent::lane::model::value::ValueLane;
use swim_server::agent::lane::strategy::Queue;
use swim_server::agent::value_lifecycle;
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;
use swim_server::agent_lifecycle;
use swim_server::interface::SwimServer;
use swim_server::RoutePattern;

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
        println!("Rust agent has started!");
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
        println!("Command received: {}", command);
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
        println!("Counter lane has started!");
    }

    async fn on_event<Context>(&self, event: &Arc<i32>, _model: &ValueLane<i32>, _context: &Context)
    where
        Context: AgentContext<RustAgent> + Sized + Send + Sync + 'static,
    {
        println!("Event received: {}", event);
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

#[tokio::main]
async fn main() {
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
