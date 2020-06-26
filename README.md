<!-- [![Build Status](https://travis-ci.com/swimos/swim-rust.svg?token=XRdC2qdFmdcvoFQjcbvN&branch=master)](https://travis-ci.com/swimos/swim-rust)
[![codecov](https://codecov.io/gh/swimos/swim-rust/branch/master/graph/badge.svg?token=IVWBLXCGW8)](https://codecov.io/gh/swimos/swim-rust)
<a href="https://www.swimos.org"><img src="https://docs.swimos.org/readme/marlin-blue.svg" align="left"></a>
<br><br><br>
## Development

### Dependencies
[Formatting](https://github.com/rust-lang/rustfmt): `rustup component add rustfmt`<br>
[Clippy](https://github.com/rust-lang/rust-clippy): `rustup component add clippy`<br>
[Tarpaulin](https://github.com/xd009642/tarpaulin) `cargo install cargo-tarpaulin`<br>

### Unit tests
##### Basic: `cargo test`
##### With coverage: `cargo tarpaulin --ignore-tests -o Html -t 300`

### Lint
##### Manual
1) `cargo fmt -- --check`
2) `cargo clippy --all-features -- -D warnings`

##### Automatic (before commit): 
- Install hook: `sh ./install-commit-hook.sh`
- Remove hook: `sh ./remove-commit-hook.sh`



---- -->
<center>
    <img src="https://docs.swimos.org/readme/marlin-blue.svg" alt="swim_logo" width="200"></img>
</center>

-----

# SwimOS - Rust




**ARM64** [![Build Status](https://travis-ci.com/swimos/swim-rust.svg?token=XRdC2qdFmdcvoFQjcbvN&branch=master)](https://travis-ci.com/swimos/swim-rust) |
**Linux/Mac OS/Windows/WASM** [![Build Status](https://dev.azure.com/swimai-build/swim-rust/_apis/build/status/swimos.swim-rust?branchName=master)](https://dev.azure.com/swimai-build/swim-rust/_build/latest?definitionId=1&branchName=master) 

[![codecov](https://codecov.io/gh/swimos/swim-rust/branch/master/graph/badge.svg?token=IVWBLXCGW8)](https://codecov.io/gh/swimos/swim-rust)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Crates.io][crates-badge]][crates-url]

[crates-badge]: https://img.shields.io/crates/v/swim.svg
[crates-url]: https://crates.io/crates/swim

----

[Website](https://swim.ai) |
[Swim Developer website](https://www.swimos.org/) |
[API Docs](https://docs.rs/swimos/latest/swim) |
[Chat](https://gitter.im/swimos)


# Overview

# Getting Started


# Demos
The [demos](demos) directory contains three demo applications built using the SwimOS Rust client:
- [swim-console](demos/swim-console/). A simple client that communicates with a Value Downlink that produces random values. The client receives these values and calculates a rolling average.
- [swim-chart](demos/swim-chart/). A WebAssembly example that communicates with a Value Downlink that produces random values, displaying them using the `@swim/chat` [NPM package](https://www.npmjs.com/package/@swim/chart). *This example requires Node to be installed*.
- [swim-wasm-chat](demos/swim-chat). A simple chat room WebAssembly example with a React-JS UI. *This example requires Node to be installed*.

Each demo application contains a `start-server.sh` that builds and starts the demo application.

Several demo applications are available in the [demos](demos) directory. 

# Example
A minimal example using the [swim-console](demos/swim-console/server) server built with the SwimOS Rust client:


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

        println!("Opened downlink");

        let mut values = Vec::new();
        let mut averages = Vec::new();
        let window_size = 200;

        while let Some(event) = receiver.next().await {
            if let Value::Int32Value(i) = event.get_inner() {
                values.push(i.clone());

                if values.len() > window_size {
                    values.remove(0);
                    averages.remove(0);
                }

                if !values.is_empty() {
                    let sum = values.iter().fold(0, |total, x| total + *x);
                    let sum = sum as f64 / values.len() as f64;

                    println!("Average: {:?}", sum);

                    averages.push(sum);
                }
            } else {
                panic!("Expected Int32 value");
            }
        }
    }
```

# WebAssembly
SwimOS - Rust supports WebAssembly through the use of the [swim-wasm](https://crates.io/crates/swim-wasm) crate. To use SwimOS - Rust on WASM, the default features of the crate must be disabled and to create a `SwimClient` instance for WASM, a WASM WebSocket factory must be provided:

```rust,no_run
    use swim_client::interface::SwimClient;
    use swim_wasm::connection::WasmWsFactory;

    let fac = WasmWsFactory::new(5);
    let swim_client = SwimClient::new_with_default(fac).await;
```

See the [swim-wasm-chat](demos/swim-wasm-chat) demo application for more information.

# Other Client Implementations
Several other SwimOS client implementations are available:
- [Python](https://github.com/swimos/swim-system-python)
- [Java](https://github.com/swimos/swim-system-java/tree/master/swim-mesh-java/swim.client)
- [JavaScript](https://github.com/swimos/swim-system-js/tree/master/swim-mesh-js/%40swim/client)

# Getting Help
First, check to see if a similar issue exists on [GitHub](https://github.com/swimos/swim-rust/issues). If there is no answer there, the 

# Licence
This project is licenced under the [Apache 2.0 licence](LICENCE). 

# Supported Rust Versions
SwimOS - Rust is built and tested against Rust 1.44.0, stable. 