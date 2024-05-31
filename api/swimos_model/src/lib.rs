// Copyright 2015-2023 Swim Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # SwimOS Model
//!
//! Model types used by the SwimOS runtime and Swim applications.

/// Functions to define an identifier for the SwimOS model.
pub mod identifier;
/// Functions to encode string literals.
pub mod literal;
/// Convenience macros to create [`Value`] instances.
#[doc(hidden)]
#[macro_use]
mod macros;

mod attr;
mod blob;
mod item;
mod num;
mod text;
mod time;
mod value;

#[cfg(test)]
mod tests;

pub use attr::Attr;
pub use blob::Blob;
pub use item::Item;
pub use num_bigint as bigint;
pub use text::Text;
pub use time::Timestamp;
pub use value::{ReconstructFromValue, ToValue, Value, ValueKind};
