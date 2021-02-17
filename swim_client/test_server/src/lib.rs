use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use swim_server::agent::command_lifecycle;
use swim_server::agent::lane::model::command::CommandLane;
use swim_server::agent::lane::model::map::MapLane;
use swim_server::agent::lane::model::value::ValueLane;
use swim_server::agent::map_lifecycle;
use swim_server::agent::value_lifecycle;
use swim_server::agent::AgentContext;
use swim_server::agent::SwimAgent;
use swim_server::interface::{ServerHandle, SwimServer, SwimServerBuilder};
use swim_server::plane::spec::PlaneBuilder;
use swim_server::RoutePattern;

pub async fn build_server(port: u16) -> (SwimServer, ServerHandle) {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let mut plane_builder = PlaneBuilder::new();

    plane_builder
        .add_route::<UnitAgent, (), ()>(RoutePattern::parse_str("/unit/foo").unwrap(), (), ())
        .unwrap();

    let mut swim_server_builder = SwimServerBuilder::default();
    swim_server_builder.add_plane(plane_builder.build());
    swim_server_builder.bind_to(address).build().unwrap()
}

#[derive(Debug, SwimAgent)]
struct UnitAgent {
    #[lifecycle(name = "IdLifecycle")]
    pub id: ValueLane<i32>,

    #[lifecycle(name = "InfoLifecycle")]
    pub info: ValueLane<String>,

    #[lifecycle(name = "PublishInfoLifecycle")]
    pub publish_info: CommandLane<String>,

    #[lifecycle(name = "ShoppingCartLifecycle")]
    pub shopping_cart: MapLane<String, i32>,

    #[lifecycle(name = "IntegerMapLifecycle")]
    pub integer_map: MapLane<i32, i32>,
}

#[value_lifecycle(agent = "UnitAgent", event_type = "i32")]
struct IdLifecycle;

#[value_lifecycle(agent = "UnitAgent", event_type = "String")]
struct InfoLifecycle;

#[command_lifecycle(agent = "UnitAgent", command_type = "String", on_command)]
struct PublishInfoLifecycle;

impl PublishInfoLifecycle {
    async fn on_command<Context>(
        &self,
        command: &str,
        _model: &CommandLane<String>,
        context: &Context,
    ) where
        Context: AgentContext<UnitAgent> + Sized + Send + Sync + 'static,
    {
        context
            .agent()
            .info
            .store(format!("from publish_info: {} ", command))
            .await;
    }
}

#[map_lifecycle(agent = "UnitAgent", key_type = "String", value_type = "i32")]
struct ShoppingCartLifecycle;

#[map_lifecycle(agent = "UnitAgent", key_type = "i32", value_type = "i32")]
struct IntegerMapLifecycle;
