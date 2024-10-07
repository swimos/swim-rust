# SwimOS

<img align="right" width="200" src="https://docs.swimos.org/readme/marlin-blue.svg">

The Swim Rust SDK contains software framework for building stateful applications that can be interacted
with via multiplexed streaming APIs. It is built on top of the [Tokio asynchronous runtime](https://tokio.rs/)
and a Tokio runtime is required for any Swim application.

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

## What is SwimOS?

SwimOS is a framework for building stateful applications from data streams and provides the ability to execute logic
when the state changes and to observe only the data that you are interested in.

SwimOS applications observes one, or more, streams of events and uses them to build up a model of some other system in
the external world and it mains an internal state which in many cases will be bounded in size. As each event is observed
by the application, it updates its knowledge of the system and records that information in the state.

The state of the application is partitioned into a number of independent "agents" each of which consists of a collection
of "lanes" which store either single values or maps of key-value pairs. For very large applications, the agents can be
split across multiple processes over multiple hosts. Every lane is individually addressable, both from within the
applications and externally using a websocket connection. It is possible for a client to both alter the state of a lane
and to subscribe to a feed of changes from it. Subscribers receive push notifications each time a changes is made and do
no need to poll to receive them.

Agents are not passive repositories of state and can also have user-specified behaviour. Each agent, and lane, has a
number of "lifecycle events" to which event handlers can be attached. For example, it would be possible to assign a
handler to be executed each time the value of a lane changes. These event handlers can alter the values of other lanes
within the agent, establish communication links to other agents or cause side effects external to the application.

In addition to observing externally provided events, agents may also observe the states of the lanes of other agents. A
common use of this is the build up a hierarchical view of a system by setting up a tree of agents where each node
observes the state of its children. For example, consider an application that observes a stream of measurements from
sensors on a fleet of buses. Each sensor could be represented by an agent which then aggregates to an agent representing
a bus, then an agent for the bus route and one representing the city. At each level of the tree, aggregate statistics
could be computed on the fly.

### Demos

#### [Ripple](ripple)

![Ripple](/docs/assets/ripple.png "Ripple")

Ripple is a real-time synchronous shared multiplayer experience built on the Swim platform.

See a hosted version of this app [here](https://ripple.swim.inc).

#### [Traffic](traffic)

![Traffic](/docs/assets/traffic.png "Traffic")

Traffic processes 30,000 data points per second from connected traffic intersections.

See a hosted version of this app [here](https://traffic.swim.inc).

#### [Transit](transit)

![Transit](/docs/assets/transit.png "Transit")

Transit is a demo which monitors public transit vehicles and displays the vehicle's current position and information on
a map in real-time.

See a hosted version of this app [here](https://traffic.swim.inc).

## Getting Started

Getting started with a new framework isn't the same for everyone but our recommended journey is:

1. Get familiarised with SwimOS concepts [here](https://www.swimos.org/swimos-concepts/).
2. Read the developer guide [here](https://www.swimos.org/server/rust/developer-guide/).
3. Read the reference documentation [here](https://www.swimos.org/server/rust/).

The developer guide reference applications are
available [here](https://github.com/swimos/swim-rust/tree/main/example_apps) and the directory also includes a number of
extra example applications.

## Sinks and Sources

Swim servers may interact with external systems through the use of connectors. At present, only
a [Kafka](https://docs.rs/swimos_connector_kafka/latest/swimos_connector_kafka/) ingress and
egress connector is available but we are working on adding WebSockets, HTTP, and MQTT support.

As SwimOS Agents are capable of executing arbitrary logic after state changes, they are capable of sinking data to any
data source. Applications may elect to trigger alerts if state reaches a certain condition or to update metrics using
Prometheus.

## Community

Contributions are welcome that close existing issues in the repository:

- [Good first issues](https://github.com/swimos/swim-rust/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) -
  simple issues.
- [Help wanted issues](https://github.com/swimos/swim-rust/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22) -
  issues that are more or less clear to begin working on.

If you have a new idea for something to be implemented, please open an issue to discuss it first before making a PR.

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
swimos = { version = "0.1", features = ["server", "agent"] }
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

For example, if a Swim client sends an update with the value `5`, to the agent at the URI `/examples/name` for the
lane `lane`, an instance of `ExampleAgent`, using `ExampleLifecycle`, will be started by the server. The value of the
lane will then be set to `5` and the following will be printed on the console:

```
Received value: 5 for 'lane' on agent at URI: /examples/name.
```

A number of example applications are available in the [example_apps](example_apps) directory which demonstrate
individual features as well as more comprehensive applications.

## Development

See the [development guide](DEVELOPMENT.md).

## License

This project is licensed under the [Apache 2.0 License](LICENSE).