//! Swim Web Agent Web Assembly (WASM) intermediate representation.

pub use spec::*;

pub mod requests;
mod spec;

/// Web Assembly Procedure Calls.
pub mod wpc;
