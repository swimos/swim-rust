//! Swim Web Agent Web Assembly (WASM) intermediate representation.

pub use spec::*;

pub mod agent;
mod spec;

pub mod connector;
/// Web Assembly Procedure Calls.
pub mod wpc;
