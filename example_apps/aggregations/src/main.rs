use std::error::Error;
use std::time::Duration;

use example_util::{example_logging, manage_handle};
use swimos::{
    agent::agent_model::AgentModel,
    route::RoutePattern,
    server::{Server, ServerBuilder},
};

use crate::car::{CarAgent, CarLifecycle};

mod area;
mod car;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    example_logging()?;

    let car_agent = AgentModel::new(CarAgent::default, CarLifecycle::default().into_lifecycle());

    let server = ServerBuilder::with_plane_name("Example Plane")
        .add_route(RoutePattern::parse_str("/car/:car_id")?, car_agent)
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(5 * 60);
        })
        .build()
        .await?;

    let (task, handle) = server.run();
    // handle
    //     .start_agent(RouteUri::from_str("/generator")?)
    //     .await
    //     .expect("Failed to start generator agent");

    let shutdown = manage_handle(handle);

    let (_, result) = tokio::join!(shutdown, task);

    result?;
    println!("Server stopped successfully.");
    Ok(())
}
