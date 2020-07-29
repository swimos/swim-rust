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

use crate::model::schema::StandardSchema;
use crate::model::Value;
pub mod impls;
use core::fmt::Display;
use std::error::Error;
use std::fmt::Formatter;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq)]
pub enum FormErr {
    MismatchedTag,
    IncorrectType(String),
    Malformatted,
    Message(String),
}

impl Error for FormErr {}

impl Display for FormErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FormErr::IncorrectType(s) => write!(f, "Incorrect type: {}", s),
            FormErr::Malformatted => write!(f, "Malformatted"),
            FormErr::Message(msg) => write!(f, "{}", msg),
            FormErr::MismatchedTag => write!(f, "Incorrect tag"),
        }
    }
}

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
// # Examples
//
// ```no_run
// use form_derive::*;
// use common::form::Form;
// use common::model::{Value, Attr, Item};
//
// #[derive(PartialEq, Debug, Form)]
// struct Message {
//     id: i32,
//     msg: String
// }
//
// let record = Value::Record(
//     vec![Attr::of("Message")],
//     vec![
//         Item::from(("id", 1)),
//         Item::from(("msg", "Hello"))
//     ]
// );
// let msg = Message {
//     id: 1,
//     msg: String::from("Hello"),
// };
//
// let result = msg.as_value();
// assert_eq!(record, result);
//
// let result = Message::try_from_value(&record).unwrap();
// assert_eq!(result, msg);
// ```
pub trait Form: Sized {
    fn as_value(&self) -> Value;

    fn into_value(self) -> Value {
        self.as_value()
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr>;

    fn try_convert(value: Value) -> Result<Self, FormErr> {
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

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        Ok(value.clone())
    }

    fn try_convert(value: Value) -> Result<Self, FormErr> {
        Ok(value)
    }
}

impl ValidatedForm for Value {
    fn schema() -> StandardSchema {
        StandardSchema::Anything
    }
}
