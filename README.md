<div style="text-align: center;">
    <img src="https://docs.swimos.org/readme/marlin-blue.svg" alt="swim_logo" width="200"/>
</div>

-----

# SwimOS - Rust

**Linux/Mac OS/Windows/WASM** [![Build Status](https://dev.azure.com/swimai-build/swim-rust/_apis/build/status/swimos.swim-rust?branchName=master)](https://dev.azure.com/swimai-build/swim-rust/_build/latest?definitionId=1&branchName=master) |
**ARM64** [![Build Status](https://travis-ci.com/swimos/swim-rust.svg?token=XRdC2qdFmdcvoFQjcbvN&branch=master)](https://travis-ci.com/swimos/swim-rust)

[![codecov](https://codecov.io/gh/swimos/swim-rust/branch/master/graph/badge.svg?token=IVWBLXCGW8)](https://codecov.io/gh/swimos/swim-rust)
<a href="https://www.swimos.org"><img src="https://docs.swimos.org/readme/marlin-blue.svg" align="left"></a>
<br><br><br>

## Demo applications
A number of demo applications are available in the [swim-rust-demos](https://github.com/swimos/swim-rust-demos) repository.

## Client configuration CLI tool
A CLI tool is available for creating configuration files for client instances [here](https://github.com/swimos/rust-client-config-cli).

## Development
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
----

[Website](https://swim.ai) |
[Swim Developer website](https://www.swimos.org/) |
[Chat](https://gitter.im/swimos)

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

# Crates
SwimOS is composed of three crates:
- [client](/client). A streaming API for linking to stateful lanes of Web Agents using the WARP protocol.
- [swim_form](/swim_form). A crate for transforming between a Rust object and a structurally typed `Value` object. This allows for sending and receiving arbitary data types between the client and the server.
- [server](/server). A WIP Rust implementation of the [SwimOS Java](https://github.com/swimos/swim) software platform. 

# Getting Started
Before starting with the SwimOS client, it's worth reading through the [developer website](https://www.swimos.org/), the [concepts](https://www.swimos.org/concepts/) and [tutorials](https://www.swimos.org/tutorials/) for building a Java server. Following this, the [demos directory](demos) is a good starting point for learning how to build stateful streaming applications using SwimOS.

# Getting Help
First, check to see if a similar issue exists on [GitHub](https://github.com/swimos/swim-rust/issues). Following that, there is an active community on [Gitter](https://gitter.im/swimos) and issues may be opened here on GitHub.

# Licence
This project is licenced under the [Apache 2.0 licence](LICENCE). 

# Supported Rust Versions
SwimOS is built and tested against Rust 1.44.0, stable. 