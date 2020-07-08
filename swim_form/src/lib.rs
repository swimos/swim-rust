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
pub extern crate form_derive;
pub extern crate deserialize as _deserialize;
pub extern crate serialize as _serialize;

#[allow(unused_imports)]
use common::model::blob::{self, deserialize_value_to_blob, serialize_blob_as_value};

use common::model::schema::StandardSchema;
use common::model::Value;

pub use deserialize::FormDeserializeErr;
pub use form_derive::*;
pub use serialize::FormSerializeErr;

#[cfg(test)]
mod tests;

pub mod collections;
pub mod impls;

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
/// use swim_form::form_derive::*;
/// use swim_form::Form;
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
pub trait Form: Sized {
    fn as_value(&self) -> Value;

    fn into_value(self) -> Value {
        self.as_value()
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr>;

    fn try_convert(value: Value) -> Result<Self, FormDeserializeErr> {
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

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        Ok(value.clone())
    }

    fn try_convert(value: Value) -> Result<Self, FormDeserializeErr> {
        Ok(value)
    }
}

impl ValidatedForm for Value {
    fn schema() -> StandardSchema {
        StandardSchema::Anything
    }
}
