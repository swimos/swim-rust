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

//! # SwimOS serialization
//!
//! This crate contains the [`Form`] trait that describes how a type can be transformed to and from
//! the SwimOS serialization model (described in [`swimos_model`]). This trait is implemented for most
//! common primitive types and common standard library collections such as [`Vec`] and
//! [`std::collections::HashMap`].
//!
//! A derivation macro is provided that can automatically generate implementations for straightforward
//! struct and enum types. For instructions on how to use this, see the the [`Form`] trait or the SwimOS
//! documentation.

#![allow(clippy::match_wild_err_arm)]
#[doc(hidden)]
#[allow(unused_imports)]
pub use swimos_form_derive::Form;

use read::{ReadError, StructuralReadable};
use swimos_model::Value;
use write::StructuralWritable;

#[doc(hidden)]
pub use swimos_model as model;

mod structural;
pub use structural::{generic, read, write, Tag};

#[cfg(test)]
mod tests;

#[allow(unused_imports)]
#[macro_use]
extern crate swimos_form_derive;

/// A `Form` transforms between a Rust object and a structurally typed `Value`. Swim forms
/// provide a derive macro to generate an implementation of `Form` for a structure providing all
/// members implement `Form`. Forms are supported by structures and enumerations in: New Type,
/// tuple, structure, and unit forms. Unions are not supported.
///
/// # Attributes
/// Forms provide a number of attributes that may be used to manipulate fields and properties of a
/// structure. All form attributes are available with the `#[form(..)]` path.
///
/// ## Container attributes
/// - `#[form(tag = "name")]` on `struct`ures will transmute the structure to a value with the
/// provided tag name.
///
/// ```
/// use swimos_model::{Attr, Item, Value};
/// use swimos_form::Form;
///
/// #[derive(Form)]
/// #[form(tag = "User")]
/// #
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
/// `#[form(tag = "name")]` on an enumeration variant will transmute the enumeration to a value
/// with the provided tag name.
///
/// ```
/// use swimos_model::{Attr, Item, Value};
/// use swimos_form::Form;
///
/// #[derive(Form)]
/// #
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
///
/// # Field attributes
/// ## Skip
/// Skip a field when transmuting the form. Fields annotated with this must implement
/// `Default`.
///
/// ```
/// use swimos_model::{Attr, Item, Value};
/// use swimos_form::Form;
///
/// #[derive(Form, PartialEq, Debug)]
/// #
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
/// and are renamed will be written as `Item::Slot` in the output record.
///
/// ```
/// use swimos_model::{Attr, Item, Value};
/// use swimos_form::Form;
///
/// #[derive(Form, PartialEq, Debug)]
/// #
/// struct Food {
///     name: String,
///     #[form(name = "quality")]
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
/// use swimos_model::{Attr, Item, Value};
/// use swimos_form::Form;
///
/// #[derive(Form)]
/// #
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
/// as `body`. A field marked with no positional attribute will default to being written as a
/// slot in the output record.
///
/// ```
/// use swimos_model::{Attr, Item, Value};
/// use swimos_form::Form;
///
/// #[derive(Form)]
/// #
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
/// use swimos_model::{Attr, Item, Value};
/// use swimos_form::Form;
///
/// #[derive(Form)]
/// #
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
/// use swimos_model::{Attr, Item, Value};
/// use swimos_form::Form;
///
/// #[derive(Form)]
/// #
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
/// use swimos_model::{Attr, Item, Value};
/// use swimos_form::Form;
///
/// #[derive(Form)]
/// #
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
pub trait Form: StructuralReadable + StructuralWritable {
    /// Returns this object represented as a value.
    fn as_value(&self) -> Value {
        self.structure()
    }

    /// Consume this object and return it represented as a value.
    fn into_value(self) -> Value {
        self.into_structure()
    }

    /// Attempt to create a new instance of this object from the provided `Value` instance.
    fn try_from_value(value: &Value) -> Result<Self, ReadError> {
        Self::try_interpret_structure(value)
    }

    /// Consume the `Value` and attempt to create a new instance of this object from it.
    fn try_convert(value: Value) -> Result<Self, ReadError> {
        Self::try_from_structure(value)
    }
}

impl<T: StructuralReadable + StructuralWritable> Form for T {}

#[doc(hidden)]
#[allow(unused_imports)]
pub use swimos_form_derive::Tag;
