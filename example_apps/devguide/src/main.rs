//! Reference code for the [Developer Guide](https://www.swimos.org/server/rust/developer-guide/).
//!
//! Run the server using the following:
//! ```text
//! $ cargo run --bin devguide
//! ```
//!
//! And run the client with the following:
//! ```text
//! $ cargo run --bin devguide_client
//! ```

use std::{error::Error, time::Duration};

use swimos::agent::event_handler::HandlerActionExt;
use swimos::agent::lanes::CommandLane;
use swimos::{
    agent::{
        agent_lifecycle::HandlerContext, agent_model::AgentModel, event_handler::EventHandler,
        lanes::ValueLane, lifecycle, projections, AgentLaneModel,
    },
    route::RoutePattern,
    server::{Server, ServerBuilder},
};
use swimos_form::Form;

// Note how as this is a custom type we need to derive `Form` for it.
// For most types, simply adding the derive attribute will suffice.
#[derive(Debug, Form, Copy, Clone)]
pub enum Operation {
    Add(i32),
    Sub(i32),
}

#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    state: ValueLane<i32>,
    exec: CommandLane<Operation>,
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

    #[on_command(exec)]
    pub fn on_command(
        &self,
        context: HandlerContext<ExampleAgent>,
        // Notice a reference to the deserialized command envelope is provided.
        operation: &Operation,
    ) -> impl EventHandler<ExampleAgent> {
        let operation = *operation;
        context
            // Get the current state of our `state` lane.
            .get_value(ExampleAgent::STATE)
            .and_then(move |state| {
                // Calculate the new state.
                let new_state = match operation {
                    Operation::Add(val) => state + val,
                    Operation::Sub(val) => state - val,
                };
                // Return a event handler which updates the state of the `state` lane.
                context.set_value(ExampleAgent::STATE, new_state)
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
    let (task, _handle) = server.run();
    task.await?;

    Ok(())
}
