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

use common::model::schema::StandardSchema;
use common::model::Value;

mod deserialize;
#[allow(warnings)]
mod form_impls;
#[allow(warnings)]
mod serialize;

#[cfg(test)]
mod tests;

#[macro_use]
#[allow(unused_imports)]
pub extern crate form_derive_new;

pub use deserialize::FormDeserializeErr;
#[allow(warnings)]
pub use serialize::{SerializeToValue, SerializerProps, ValueSerializer};

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
