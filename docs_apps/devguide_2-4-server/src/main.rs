use std::{error::Error, time::Duration};

use swim::{
    agent::{
        agent_lifecycle::utility::HandlerContext,
        agent_model::AgentModel,
        event_handler::{EventHandler, HandlerActionExt},
        lanes::{CommandLane, ValueLane},
        lifecycle, projections, AgentLaneModel,
    },
    form::Form,
    route::RoutePattern,
    server::{Server, ServerBuilder, ServerHandle},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Here we create a dynamic route named "/example/:name" to use for our `ExampleAgent`.
    let route = RoutePattern::parse_str("/example/:name}")?;
    let lifecycle = ExampleLifecycle;
    // We can think of this as creating our agent factory that will be used to create
    // instances of our agent.
    let agent = AgentModel::new(ExampleAgent::default, lifecycle.into_lifecycle());

    let server = ServerBuilder::with_plane_name("Plane")
        .update_config(|config| {
            // Ensure the agent stops fairly quickly.
            config.agent_runtime.inactive_timeout = Duration::from_secs(20);
        })
        // Register the agent againt the route. Now, we can send events to `"/example/:name"` and a new agent will be started for every unique path that is addressed.
        .add_route(route, agent)
        .build()
        // An error may be returned from this future if there is a connection issue or
        // a path collision.
        .await?;

    let (task, handle) = server.run();
    let shutdown = manage_handle(handle);

    let (_, result) = tokio::join!(shutdown, task);

    result?;
    println!("Server stopped successfully.");
    Ok(())
}

pub async fn manage_handle(mut handle: ServerHandle) {
    if let Some(addr) = handle.bound_addr().await {
        println!("Bound to: {}", addr);
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to register interrupt handler.");
    }

    println!("Stopping server.");
    handle.stop();
}

#[derive(Debug, Form, Copy, Clone)]
pub enum Operation {
    Add(i64),
    Sub(i64),
}

#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    state: ValueLane<i64>,
    exec: CommandLane<Operation>,
}

#[derive(Clone)]
pub struct ExampleLifecycle;

#[lifecycle(ExampleAgent)]
impl ExampleLifecycle {
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl EventHandler<ExampleAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Starting agent at: {}", uri);
            })
        })
    }

    #[on_stop]
    pub fn on_stop(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl EventHandler<ExampleAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Stopping agent at: {}", uri);
            })
        })
    }

    #[on_command(exec)]
    pub fn on_command(
        &self,
        context: HandlerContext<ExampleAgent>,
        operation: &Operation,
    ) -> impl EventHandler<ExampleAgent> {
        let operation = *operation;
        context
            .get_value(ExampleAgent::STATE)
            .and_then(move |state| {
                let new_state = match operation {
                    Operation::Add(val) => state + val,
                    Operation::Sub(val) => state - val,
                };
                context.set_value(ExampleAgent::STATE, new_state)
            })
    }
}
