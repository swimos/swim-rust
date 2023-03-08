Writing a Server Application
============================

Getting started
---------------

At present, the Swim server application can only be run using `tokio` (it uses tokio timers and `tokio::spawn` internally.) You will need to add the following dependencies to your application:

```toml
[dependencies]
swim = { { git = "https://github.com/swimos/swim-rust" }, features = ["server", "agent"] }
tokio = { version = "1.22", features = ["rt", "macros", "signal"]}
```

The enabled features for `swim` are:

1. `server` - Includes to server application itself.
2. `agent` - Includes the API and macros for defining Swim agents in Rust.

The enabled `tokio` features are:

1. `rt` - This provides a single threaded runtime to run the server. It is also possible to specify `rt-multi-thread` for the multi-threaded runtime.
2. `macros` - The provides the `#[tokio::main]` attribute macro that we will use to define the `main` method of our application.
3. `signal` - This allows us to attach a signal handler to shut down the application with **Ctrl-C**.

Application skeleton
--------------------

We will start by creating a server application with no agent routes defined:

```rust
use std::error::Error;
use tokio::signal::ctrl_c;
use swim::server::{Server, ServerBuilder, ServerHandle};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let addr = "127.0.0.1:8080".parse()?;

    let server = ServerBuilder::with_plane_name("My Server")
        .set_bind_addr(addr)
        .build()
        .await?;

    let (task, handle) = server.run();

    let mut shutdown = Box::pin(async move {
        ctrl_c()
            .await
            .expect("Failed to register interrupt handler.")
            .await;
        println!("Stopping server.");
        handle.stop();
    });

    println!("Starting server.");
    let (_, result) = tokio::join!(shutdown, task);

    result?;
    println!("Server stopped successfully.");
    Ok(())
}
```

The `ServerBuilder` allows us to specify the routes that the server will be able to handle and to modify the default configuration parameters. In this case however, we call `build` immediately creating a default, empty server. By default, the server will bind to a random free port on all devices; here we request that it binds to port `8080` on the local host.

Calling `run` on the resulting server returns an async task that will run the server application and a handle that can be used to stop it. We associate the handler for **Ctrl-C** we we can request that the server stops from the terminal.

Adding an agent route
---------------------

Next we will need an agent and accompanying lifecycle to bind to a route in the server. To learn how to do this see the [guide on writing agents](agent.md). For the purposes of this section, we wil assume that there is such an agent type at `agent::ExampleAgent` and `agent::ExampleLifecycle`.

An agent routes is a URI path, potentially containing path variables of the form `{name}`.

1. The route `"/node/a"` will match the exact path "/node/a" and binds no parameters.
2. The route `"/node/{x}"` with match the paths:
     
     * `/node/a` with `x = 'a'`.
     * `/node/b` with `x = 'b'`.
     * `/node/name` with `x = 'name'`.
     * etc

Hence, a path with no variables defines a route to a singleton agent and a path with one or more variables defines a route to a parameterized family of agents.

To bind our example agent as a singleton agent we would do the following:

```rust
use swim::route::RoutePattern;
use swim::agent::agent_model::AgentModel;
use self::agent::{ExampleAgent, ExampleLifecycle};

let route = RoutePattern::parse_str("/example")?;
let lc = ExampleLifecycle::default();
let model = AgentModel::new(ExampleAgent::default, lc.into_lifecycle());

let server = ServerBuilder::with_plane_name("My Server")
        .set_bind_addr(addr)
        .add_route(route, model)
        .build()
        .await?;
```

Similarly, if we wanted our agent to be used as a parameterized family, we could replace the pattern with:

```rust
let route = RoutePattern::parse_str("/example/{name}")?;
```

Each running agent has access to its specific URI using a special `HandlerAction`, available to it through the `HandlerContext` for each event handler.

Enabling TLS
------------

TODO

Enabling websocket compression
------------------------------

The swim server can support deflate compression it the web socket frames that it sends (where the client also supports it). This can be enabled with:

```rust
ServerBuilder::with_plane_name("My Server")
    .add_deflate_support()
    ...
```

Alternatively, to specify custom configuration for the parameters:

```rust
use swim::server::DeflateConfig;

let config: DeflateConfig = ...;
ServerBuilder::with_plane_name("My Server")
    .configure_deflate_support(config)
    ...
```

Enabling agent persistence
--------------------------

By default, all agent instance are entirely transient. Stopping and restarting the application will reset the state of all agents. Similarly, an agent instance stopping and then restarting within a running process will also reset the state.

To persist agent state across restarts, enable the in-memory store:

```rust
ServerBuilder::with_plane_name("My Server")
    .with_in_memory_store()
    ...
```

With the in-memory store, state will be lost when the process stops. To persist agent state to disk there
is another state implementation backed by rocksdb. This must be enabled with the optional feature `rocks_store` on the `swim` dependency:

```toml
swim = { { git = "https://github.com/swimos/swim-rust" }, features = ["server", "agent", "rocks_store"] }
```

Persistence, implemented using a RocksDB database can then be enabled when constructing the server with:

```rust
ServerBuilder::with_plane_name("My Server")
    .enable_rocks_store()
    ...
```

This will use a database in a temporary folder in your system's default location for temporary files. State will be kept for all agents if they stop and restart within a single process. However, if the server is entirely stopped, the state will be lost.

To support persistence between different executions of the server, instead pass a path when enabling the RocksDB store:

```rust
ServerBuilder::with_plane_name("My Server")
    .set_rocks_store_path("/path/to/store")
    ...
```

Enabling agent introspection
----------------------------

The server has an introspection system that will run additional meta-agents which provide information about the lanes on running agents. This system is disabled by default and can be enabled by:

```rust
ServerBuilder::with_plane_name("My Server")
    .enable_introspection()
    ...
```