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

use core::fmt::Display;
use std::error::Error;
use std::fmt::Formatter;

#[doc(hidden)]
#[allow(unused_imports)]
pub use form_derive::*;

use crate::model::schema::StandardSchema;
use crate::model::Value;

pub mod impls;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq)]
pub enum FormErr {
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
        }
    }
}

/// A [`Form`] transforms between a Rust object and a structurally typed [`Value`]. Swim forms
/// provide a derive macro to generate an implementation of [`Form`] for a structure providing all
/// members implement [`Form`]. Forms are supported by structures and enumerations in: New Type,
/// tuple, structure, and unit forms. Unions are not supported.
///
/// # Attributes
/// Forms provide a number of attributes that may be used to manipulate fields and properties of a
/// structure. All form attributes are available with the [`#[form(..)]`] path.
///
/// ## Container attributes
/// - [`#[form(tag = "name")]`] on [`struct`]ures will transmute the structure to a value with the
/// provided tag name.
///
/// ```
/// # mod swim_common {
/// #   pub use common::*;
/// # }
/// use swim_common::model::{Attr, Item, Value};
/// use swim_common::form::Form;
///
/// #[derive(Form)]
/// #[form(tag = "User")]
/// struct Person {
///     name: String,
/// }
///
/// let person = Person {
///     name: String::from("Dill"),
/// };
///
/// assert_eq!(person.as_value(), Value::Record(
///     vec![Attr::of("User")],
///     vec![Item::Slot(Value::text("name"), Value::text("Dill"))]
/// ));
/// ```
///
/// ## Variant attributes
/// Enumeration variant names are used as tags, to use a custom tag the attribute
/// [`#[form(tag = "name")]`] on an enumeration variant will transmute the enumeration to a value
/// with the provided tag name.
///
/// ```
/// # mod swim_common {
/// #   pub use common::*;
/// # }
/// use swim_common::model::{Attr, Item, Value};
/// use swim_common::form::Form;
///
/// #[derive(Form)]
/// enum Manufacturer {
///     BMW,
///     #[form(tag = "VW")]
///     Volkswagen
/// }
///
/// let vw = Manufacturer::Volkswagen;
///
/// assert_eq!(vw.as_value(), Value::Record(
///     vec![Attr::of("VW")],
///     Vec::new()
/// ));
/// ```
/// # Field attributes
/// ## Skip
/// Skip a field when transmuting the form. Fields annotated with this must implement
/// [`Default`].
///
/// ```
/// # mod swim_common {
/// #   pub use common::*;
/// # }
/// use swim_common::model::{Attr, Item, Value};
/// use swim_common::form::Form;
///
/// #[derive(Form, PartialEq, Debug)]
/// struct Food {
///     name: String,
///     #[form(skip)]
///     rating: i32,
/// }
///
/// let food = Food {
///     name: String::from("soup"),
///     rating: 0,
/// };
///
/// let rec = Value::Record(
///     vec![Attr::of("Food")],
///     vec![Item::Slot(Value::text("name"), Value::text("soup"))],
/// );
///
/// assert_eq!(food.as_value(), rec);
/// ```
///
/// ## Rename
/// Rename the field to the provided name. Structures and enumerations that contain unnamed fields
/// and are renamed will be written as [`Item::Slot`] in the output record.
///
/// ```
/// # mod swim_common {
/// #   pub use common::*;
/// # }
/// use swim_common::model::{Attr, Item, Value};
/// use swim_common::form::Form;
///
/// #[derive(Form, PartialEq, Debug)]
/// struct Food {
///     name: String,
///     #[form(rename = "quality")]
///     rating: i32,
/// }
/// let food = Food {
///     name: String::from("soup"),
///     rating: 80,
/// };
/// let rec = Value::Record(
///     vec![Attr::of("Food")],
///     vec![
///         Item::Slot(Value::text("name"), Value::text("soup")),
///         Item::Slot(Value::text("quality"), Value::Int32Value(80))
///     ],
/// );
/// assert_eq!(food.as_value(), rec);
///
/// ```
/// ## Attribute
/// The field should be written as an attribute in the output record. This is only supported by named
/// structures and enumerations or renamed fields.
///
/// ```
/// # mod swim_common {
/// #   pub use common::*;
/// # }
/// use swim_common::model::{Attr, Item, Value};
/// use swim_common::form::Form;
///
/// #[derive(Form)]
/// struct Structure {
///     item: String,
///     #[form(attr)]
///     attribute: String,
/// }
/// let structure = Structure {
///     item: String::from("an item"),
///     attribute: String::from("an attribute"),
/// };
/// let rec = Value::Record(
///     vec![
///         Attr::of("Structure"),
///         Attr::of(("attribute", Value::text("an attribute"))),
///     ],
///     vec![Item::Slot(Value::text("item"), Value::text("an item"))],
/// );
/// assert_eq!(structure.as_value(), rec);
///
/// ```
/// ## Slot
/// The field should be written as a slot in the main body or the header if another field is marked
/// as [`body`]. A field marked with no positional attribute will default to being written as a
/// slot in the output record.
///
/// ```
/// # mod swim_common {
/// #   pub use common::*;
/// # }
/// use swim_common::model::{Attr, Item, Value};
/// use swim_common::form::Form;
///
/// #[derive(Form)]
/// struct Structure {
///     slot_a: String,
///     slot_b: String,
/// }
/// let structure = Structure {
///     slot_a: String::from("slot_a"),
///     slot_b: String::from("slot_b"),
/// };
/// let rec = Value::Record(
///     vec![Attr::of("Structure")],
///     vec![
///         Item::Slot(Value::text("slot_a"), Value::text("slot_a")),
///         Item::Slot(Value::text("slot_b"), Value::text("slot_b")),
///     ],
/// );
/// assert_eq!(structure.as_value(), rec);
///
/// ```
///
/// ## Body
/// The field should be used to form the entire body of the record, all other fields that are marked
/// as slots will be promoted to headers. At most one field may be marked with this.
///
/// ```
/// # mod swim_common {
/// #   pub use common::*;
/// # }
/// use swim_common::model::{Attr, Item, Value};
/// use swim_common::form::Form;
///
/// #[derive(Form)]
/// struct Structure {
///     header: String,
///     #[form(body)]
///     body: String,
/// }
/// let structure = Structure {
///     header: String::from("header"),
///     body: String::from("body"),
/// };
/// let rec = Value::Record(
///     vec![Attr::of((
///         "Structure",
///         Value::Record(
///             Vec::new(),
///             vec![Item::Slot(Value::text("header"), Value::text("header"))],
///         ),
///     ))],
///     vec![Item::ValueItem(Value::text("body"))],
/// );
/// assert_eq!(structure.as_value(), rec);
///
/// ```
///
/// ## Header
/// The field should be written as a slot in the tag attribute.
///
/// ```
/// # mod swim_common {
/// #   pub use common::*;
/// # }
/// use swim_common::model::{Attr, Item, Value};
/// use swim_common::form::Form;
///
/// #[derive(Form)]
/// struct Structure {
///    item: String,
///    #[form(header)]
///    attribute: String,
/// }
///
/// let structure = Structure {
///     item: String::from("an item"),
///     attribute: String::from("an attribute"),
/// };
///
/// let rec = Value::Record(
///     vec![Attr::of((
///         "Structure",
///         Value::Record(
///             Vec::new(),
///             vec![Item::slot(
///                 Value::text("attribute"),
///                 Value::text("an attribute"),
///             )],
///         ),
///     ))],
///     vec![Item::Slot(Value::text("item"), Value::text("an item"))],
/// );
///
/// assert_eq!(structure.as_value(), rec);
/// ```
///
/// ## Header body
/// The field should be moved into the body of the tag attribute (unlabelled). If there are no
/// header fields it will form the entire body of the tag, otherwise it will be the first item of
/// the tag body. At most one field may be marked with this.
///
/// ```
/// # mod swim_common {
/// #   pub use common::*;
/// # }
/// use swim_common::model::{Attr, Item, Value};
/// use swim_common::form::Form;
///
/// #[derive(Form)]
/// struct Structure {
///     item: String,
///     #[form(header_body)]
///     attribute: String,
/// }
/// let structure = Structure {
///     item: String::from("an item"),
///     attribute: String::from("an attribute"),
/// };
/// let rec = Value::Record(
///     vec![Attr::of(("Structure", Value::text("an attribute")))],
///     vec![Item::Slot(Value::text("item"), Value::text("an item"))],
/// );
/// assert_eq!(structure.as_value(), rec);
/// ```
pub trait Form: Sized {
    /// Returns this object represented as a value.
    fn as_value(&self) -> Value;

    /// Consume this object and return it represented as a value.
    fn into_value(self) -> Value {
        self.as_value()
    }

    /// Attempt to create a new instance of this object from the provided [`Value`] instance.
    fn try_from_value(value: &Value) -> Result<Self, FormErr>;

    /// Consume the [`Value`] and attempt to create a new instance of this object from it.
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
