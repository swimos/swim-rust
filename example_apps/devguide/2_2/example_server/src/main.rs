use swimos::{
    agent::{
        agent_lifecycle::utility::HandlerContext, agent_model::AgentModel,
        event_handler::EventHandler, lanes::ValueLane, lifecycle, projections, AgentLaneModel,
    },
    route::RoutePattern,
    server::{Server, ServerBuilder, ServerHandle},
};

use std::{error::Error, time::Duration};
#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    state: ValueLane<i32>,
}

#[derive(Clone)]
pub struct ExampleLifecycle;

#[lifecycle(ExampleAgent)]
impl ExampleLifecycle {
    // Handler invoked when the agent starts.
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl EventHandler<ExampleAgent> {
        context.effect(|| println!("Starting agent."))
    }

    // Handler invoked when the agent is about to stop.
    #[on_stop]
    pub fn on_stop(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl EventHandler<ExampleAgent> {
        context.effect(|| println!("Stopping agent."))
    }

    // Handler invoked after the state of 'lane' has changed.
    #[on_event(state)]
    pub fn on_event(
        &self,
        context: HandlerContext<ExampleAgent>,
        value: &i32,
    ) -> impl EventHandler<ExampleAgent> {
        let n = *value;
        // EventHandler::effect accepts a FnOnce()
        // which runs a side effect.
        context.effect(move || {
            println!("Setting value to: {}", n);
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Create a dynamic route for our agents.
    let route = RoutePattern::parse_str("/example/:id")?;
    // Create an agent model which contains the factory for creating the agent as well
    // as the lifecycle which will be run.
    let agent = AgentModel::new(ExampleAgent::default, ExampleLifecycle.into_lifecycle());

    // Create a server builder.
    let server = ServerBuilder::with_plane_name("Plane")
        // Bind to port 8080
        .set_bind_addr("127.0.0.1:8080".parse().unwrap())
        // For this guide, ensure agents timeout fairly quickly.
        // An agent will timeout after they have received no new updates
        // for this configured period of time.
        .update_config(|config| {
            config.agent_runtime.inactive_timeout = Duration::from_secs(20);
        })
        // Register the agent against the route.
        .add_route(route, agent)
        .build()
        // Building the server may fail if many routes are registered and some
        // are ambiguous.
        .await?;

    // Run the server. A tuple of the server's runtime
    // future and a handle to the runtime is returned.
    let (task, handle) = server.run();
    // Watch for ctrl+c signals
    let shutdown = manage_handle(handle);

    // Join on the server and ctrl+c futures.
    let (_, result) = tokio::join!(shutdown, task);

    result?;
    println!("Server stopped successfully.");
    Ok(())
}

// Utility function for awaiting a stop signal in the terminal.
async fn manage_handle(mut handle: ServerHandle) {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to register interrupt handler.");

    println!("Stopping server.");
    handle.stop();
}
