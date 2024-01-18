#![allow(warnings)]

#[cfg(test)]
mod tests;

// use bytes::BytesMut;
// use std::collections::HashMap;
//
// use wasmtime::{Engine, Module, Store};
//
// /// Base host engine shared across the server's agents
// #[derive(Default)]
// struct WasmHostEngine {
//     engine: Engine,
// }
//
// struct HostModule {
//     module: Module,
//     store: Store<()>,
// }
//
// trait WasmModuleVTable {
//     extern "C" fn init();
// }

// - load agent definition that is attached to a node URI route (like java-ffi)
// - when a new agent is required, copy the definition and instantiate a new WASM environment for
//      each lane
