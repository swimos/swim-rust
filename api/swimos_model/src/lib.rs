// Copyright 2015-2024 Swim Inc.
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
//! This model is a generic representation of the SwimOS serialization model. Any valid
//! serialized record can be deserialized as an instance of the [`Value`] type in this
//! crate.
//!
//! ## The Value type
//!
//! A SwimOS [`Value`] can be of any one of the following kinds.
//!
//! - An absent value (represented as [`Value::Extant`]).
//! - A primitive value from the following set:
//!  1. Booleans, represented as [`Value::BooleanValue`]
//!  2. Integers. When deserializing, an appropriate Rust integer type will be chosen, depending on this size of the integer.
//!  3. 64bit floating point numbers, represented as [`Value::Float64Value`].
//!  4. UTF-8 Strings, represented as [`Value::Text`].
//!  5. Arrays of bytes (represented as [`Value::Data`]).
//! - A record consisting of an list of attributes ([`Attr`]) and  and list of items ([`Item`]). This is
//! represented as [`Value::Record`].
//!
//! An attribute is a labelled (with a UTF-8 String) [`Value`].
//!
//! An item is one of:
//!
//! - A value item which is an instance of [`Value`]. This is represented as [`Item::ValueItem`].
//! - A slot item which is a pair of two [`Value`]s, the first of which is interpreted as a key. This
//! is represented as [`Item::Slot`].
//!
//! Note that the attributes and items of a record are always ordered, although the order may not always be
//! significant, for example if a record, consisting only of slots, is used to represent a map.
//!
//! The [`record`] and [`macro@value`] macros can be used to simplify the creation of [`Value`] instances.
//!
//! ## Using the model
//!
//! The use the model for serialization or deserialization, a format supporting the SwimOS model is required.
//! The default format used by SwimsOS is the Recon markup language, an example of which can be found in the
//! `swimos_recon` crate.
//!

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
pub use num_bigint::{BigInt, BigUint};
pub use text::Text;
pub use time::Timestamp;
pub use value::{ReconstructFromValue, ToValue, Value, ValueKind};
