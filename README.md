<a href="https://www.swimos.org"><img src="https://docs.swimos.org/readme/marlin-blue.svg" align="left"></a>
<br><br><br><br>

The Swim Rust SDK contains software framework for building stateful applications that can be interacted
with via multiplexed streaming APIs. It is built on top of the [Tokio asynchronous runtime](https://tokio.rs/)
and a Tokio runtime is required for any Swim application.

Each application consists of some number of stateful agents, each of which runs as a separate Tokio task
and can be individually addressed by a URI. An agent may have both public and private state which can either
be held solely in memory or, optionally, in persistent storage. The public state of the agent consists of a
number of lanes, analogous to a field in a record. There are multiple kinds of lanes that, for example, lanes
containing single values and those containing a map of key-value pairs.

The state of any lane can be observed by establishing a link to it (either from another agent instance or a
dedicated client). A established link will push all updates to the state of that lane to the subscriber and
will also allow the subscriber to request changes to the state (for lane kinds that support this). Links
operate over a web-socket connection and are multiplexed, meaning that links to multiple lanes on the same
host can share a single web-socket connection.

[![SwimOS Crates.io Version][swimos-badge]][swimos-crate]
[![SwimOS Client Crates.io Version][swimos-client-badge]][swimos-client-crate]
[![SwimOS Form Crates.io Version][swimos-form-badge]][swimos-form-crate]

[swimos-badge]: https://img.shields.io/crates/v/swimos?label=swimos
[swimos-crate]: https://crates.io/crates/swimos

[swimos-form-badge]: https://img.shields.io/crates/v/swimos?label=swimos_form
[swimos-form-crate]: https://crates.io/crates/swimos_form

[swimos-client-badge]: https://img.shields.io/crates/v/swimos?label=swimos_client
[swimos-client-crate]: https://crates.io/crates/swimos_client

[Website](https://swimos.org/) | [Developer Guide](https://www.swimos.org/server/rust/developer-guide/) | [Server API Docs](https://docs.rs/swimos/latest/swimos/) | [Client API Docs](https://docs.rs/swimos_client/latest/swimos_client/)

## Usage Guides

[Implementing Swim Agents in Rust](docs/agent.md)

[Building a Swim Server Application](docs/server.md)

[Reference Documentation](https://www.swimos.org/server/rust/)

## Examples

The following example application runs a SwimOS server that hosts a single agent route where each agent instance
has single lane, called `lane`. Each time a changes is made to the lane, it will be printed on the console by the
server.

```toml
[dependencies]
swimos = { version = "0.1.0", features = ["server", "agent"] }
```

```rust
use swimos::{
    agent::{
        agent_lifecycle::HandlerContext,
        agent_model::AgentModel,
        event_handler::{EventHandler, HandlerActionExt},
        lanes::ValueLane,
        lifecycle, AgentLaneModel,
    },
    route::RoutePattern,
    server::{until_termination, Server, ServerBuilder},
};

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // An agent route consists of the agent definition and a lifecycle.
    let model = AgentModel::new(ExampleAgent::default, ExampleLifecycle.into_lifecycle());

    let server = ServerBuilder::with_plane_name("Example Plane")
        .set_bind_addr("127.0.0.1:8080".parse()?) // Bind the server to this address.
        .add_route(RoutePattern::parse_str("/examples/{id}")?, model) // Register the agent we have defined.
        .build()
        .await?;

    // Run the server until we terminate it with Ctrl-C.
    let (task, handle) = server.run();
    let (ctrl_c_result, server_result) = tokio::join!(until_termination(handle, None), task);

    ctrl_c_result?;
    server_result?;
    Ok(())
}

// Deriving the `AgentLaneModel` trait makes this type into an agent.
#[derive(AgentLaneModel)]
struct ExampleAgent {
    lane: ValueLane<i32>,
}

// Any agent type can have any number of lifecycles defined for it. A lifecycle describes
// how the agent will react to events that occur as it executes.
#[derive(Default, Clone, Copy)]
struct ExampleLifecycle;

// The `lifecycle` macro creates an method called `into_lifecycle` for the type, using the
// annotated event handlers methods in the block.
#[lifecycle(ExampleAgent)]
impl ExampleLifecycle {
    #[on_event(lane)]
    fn lane_event(
        &self,
        context: HandlerContext<ExampleAgent>,
        value: &i32,
    ) -> impl EventHandler<ExampleAgent> {
        let n = *value;
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Received value: {} for 'lane' on agent at URI: {}.", n, uri);
            })
        })
    }
}
```

For example, if a Swim client sends an update, with the value `5`, to the agent at the URI `/examples/name` for the
lane `lane`, an instance of `ExampleAgent`, using `ExampleLifecycle`, will be started by the server. The value of the
lane will then be set to `5` and the following will be printed on the console:

```
Received value: 5 for 'lane' on agent at URI: /examples/name.
```

## Development

See the [development guide](DEVELOPMENT.md).

## License

This project is licensed under the [Apache 2.0 License](LICENSE).
