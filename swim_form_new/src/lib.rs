// Copyright 2015-2020 SWIM.AI inc.
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

#![allow(clippy::match_wild_err_arm)]

#[macro_use]
#[allow(unused_imports)]
pub extern crate form_derive_new;

use common::model::schema::StandardSchema;
use common::model::Value;

#[allow(warnings)]
mod form_impls;
mod reader;
mod transmute_impls;
#[allow(warnings)]
mod writer;

#[cfg(test)]
mod tests;

pub use reader::{ValueReadError, ValueReader};
pub use writer::{as_value, ValueWriter, WriteValueError};

/// A [`Form`] transforms between a Rust object and a structurally typed [`Value`]. Decorating a
/// Rust object with [`#[form(Value)`] derives a method to serialise the object to a [`Value`] and to
/// deserialise it back into a Rust object. The argument to this macro is a name binding to the
/// [`Value`] enumeration; this differs between the Swim Client and Swim Server crates and so must be
/// specified.
///
/// The form macro performs compile-time checking to ensure that all fields implement the [`Form`]
/// trait. Forms are backed by Serde and so all of Serde's attributes work for serialisation and
/// deserialisation. As a result, Serde must be included as a dependency with the [`derive`] feature
/// enabled.
///
/// # Examples
///
/// ```
/// use swim_form_new::form_derive_new::*;
/// use swim_form_new::Form;
/// use common::model::{Value, Attr, Item};
///
/// #[form(Value)]
/// #[derive(PartialEq, Debug)]
/// struct Message {
///     id: i32,
///     msg: String
/// }
///
/// let record = Value::Record(
///     vec![Attr::of("Message")],
///     vec![
///         Item::from(("id", 1)),
///         Item::from(("msg", "Hello"))
///     ]
/// );
/// let msg = Message {
///     id: 1,
///     msg: String::from("Hello"),
/// };
///
/// let result = msg.as_value();
/// assert_eq!(record, result);
///
/// let result = Message::try_from_value(&record).unwrap();
/// assert_eq!(result, msg);
/// ```
///
/// The [`Form`] trait is implemented for: `i32`, `u32`, `i64`, `u64`, `String`, `f64`, `bool`,
/// `Option<V>`.
///
/// Swim forms do not support serializing and deserializing a single `BigInt` or `BigUint` structure
/// as there is no way to provide the serializer or deserializer with the correct implementation to use
/// as the `num-bigint` crate has its own implementation for serialize and deserialize that Swim forms
/// do not use. As `num-bigint` is a remote crate, the instance must be wrapped and provided with the
/// correct serializer and deserializer to use.
pub trait Form: Sized {
    fn as_value(&self) -> Value;

    fn into_value(self) -> Value {
        self.as_value()
    }

    fn try_from_value(value: &Value) -> Result<Self, ValueReadError>;

    fn try_convert(value: Value) -> Result<Self, ValueReadError> {
        Form::try_from_value(&value)
    }
}

/// A [`Form`] with an associated schema that can validate [`Value`] instances without attempting
/// to convert them.
pub trait ValidatedForm: Form {
    /// A schema for the form. If the schema returns true for a [`Value`] the form should be able
    /// to create an instance of the type from the [`Value`] without generating an error.
    fn schema() -> StandardSchema;
}

impl Form for Value {
    fn as_value(&self) -> Value {
        self.clone()
    }

    fn into_value(self) -> Value {
        self
    }

    fn try_from_value(value: &Value) -> Result<Self, ValueReadError> {
        Ok(value.clone())
    }

    fn try_convert(value: Value) -> Result<Self, ValueReadError> {
        Ok(value)
    }
}

impl ValidatedForm for Value {
    fn schema() -> StandardSchema {
        StandardSchema::Anything
    }
}

pub trait TransmuteValue: Form {
    fn transmute_to_value(&self, writer: &mut ValueWriter);

    fn transmute_from_value(&self, reader: &mut ValueReader) -> Result<Self, ValueReadError>;
}
