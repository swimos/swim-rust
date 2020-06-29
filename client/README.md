<div style="text-align: center;">
    <img src="https://docs.swimos.org/readme/marlin-blue.svg" alt="swim_logo" width="200"/>
</div>

-----

# SwimOS - Rust: Client
 [![Crates.io][crates-badge]][crates-url] | [API Docs](https://docs.rs/swimos/latest/swim_client)


[crates-badge]: https://img.shields.io/crates/v/swim_client.svg
[crates-url]: https://crates.io/crates/swim_client

A streaming API for linking to stateful lanes of Web Agents using the WARP protocol.

# Usage:
Add to your `Cargo.toml`:

```toml
[dependencies]
swim_client = "1.0"
```


# Demos
The [demos](demos) directory contains three demo applications built using the SwimOS Rust client:
- [swim-console](../demos/swim-console/). A simple client that communicates with a Value Downlink that produces random values. The client receives these values and calculates a rolling average.
- [swim-chart](../demos/swim-chart/). A WebAssembly example that communicates with a Value Downlink that produces random values, displaying them using the `@swim/chat` [NPM package](https://www.npmjs.com/package/@swim/chart). *This example requires Node to be installed*.
- [swim-wasm-chat](../demos/swim-chat). A simple chat room WebAssembly example with a React-JS UI. *This example requires Node to be installed*.

Each demo application contains a `start-server.sh` that builds and starts the demo application.


# Example
A minimal example using the [swim-console](../demos/swim-console/server) server built with the SwimOS Rust client:

```rust,no_run
    use futures::StreamExt;

    use swim_client::common::model::Value;
    use swim_client::common::warp::path::AbsolutePath;
    use swim_client::connections::factory::tungstenite::TungsteniteWsFactory;
    use swim_client::interface::SwimClient;
    
    #[tokio::main]
    async fn main() {
        let fac = TungsteniteWsFactory::new(5).await;
        let mut client = SwimClient::new_with_default(fac).await;
        let path = AbsolutePath::new(
            url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
            "/unit/foo",
            "random",
        );
    
        let (_downlink, mut receiver) = client.value_downlink(path, Value::Extant).await.unwrap();
    
        while let Some(event) = receiver.next().await {
            println!("{:?}", event);
        }
    }
```

Start the Swim Java server:
```
    swim-rust/demos/swim-console/server$ ./gradlew run
```

Run the Rust client:
```
    swim-rust/demos/swim-console$ cargo run
```

This will yield an output similar to:
```
    Remote(Int32Value(3583))
    Remote(Int32Value(439))
    Remote(Int32Value(9953))
    Remote(Int32Value(5014))
    Remote(Int32Value(8849))
```

See the [swim-wasm-chat](demos/swim-wasm-chat) demo application for more information.

# WebAssembly
The SwimOS client supports WebAssembly through the use of the [swim-wasm](https://crates.io/crates/swim-wasm) crate. To use SwimOS on WASM, the default features of the crate must be disabled and to create a `SwimClient` instance for WASM, a WASM WebSocket factory must be provided:

```rust,no_run
    use swim_client::interface::SwimClient;
    use swim_wasm::connection::WasmWsFactory;

    let fac = WasmWsFactory::new(5);
    let swim_client = SwimClient::new_with_default(fac).await;
```


# Other Client Implementations
Several other SwimOS client implementations are available:
- [Python](https://github.com/swimos/swim-system-python)
- [Java](https://github.com/swimos/swim-system-java/tree/master/swim-mesh-java/swim.client)
- [JavaScript](https://github.com/swimos/swim-system-js/tree/master/swim-mesh-js/%40swim/client)
