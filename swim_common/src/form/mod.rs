// Copyright 2015-2021 SWIM.AI inc.
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

#[doc(hidden)]
#[allow(unused_imports)]
pub use form_derive::{Form, ValidatedForm};

use crate::form::structural::read::{ReadError, StructuralReadable};
use crate::form::structural::write::StructuralWritable;
use crate::model::schema::StandardSchema;
use crate::model::Value;

pub mod impls;
pub mod macros;
pub mod structural;

#[cfg(test)]
mod tests;

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
/// `#[form(tag = "name")]` on an enumeration variant will transmute the enumeration to a value
/// with the provided tag name.
///
/// ```
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
///
/// # Field attributes
/// ## Skip
/// Skip a field when transmuting the form. Fields annotated with this must implement
/// `Default`.
///
/// ```
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
/// and are renamed will be written as `Item::Slot` in the output record.
///
/// ```
/// use swim_common::model::{Attr, Item, Value};
/// use swim_common::form::Form;
///
/// #[derive(Form, PartialEq, Debug)]
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
/// as `body`. A field marked with no positional attribute will default to being written as a
/// slot in the output record.
///
/// ```
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
        Self::try_read_from(value)
    }

    /// Consume the `Value` and attempt to create a new instance of this object from it.
    fn try_convert(value: Value) -> Result<Self, ReadError> {
        Self::try_transform(value)
    }
}

impl<T: StructuralReadable + StructuralWritable> Form for T {}

/// A `Form` with an associated schema that can validate `Value` instances without attempting
/// to convert them.
///
/// See `StandardSchema` for details of schema variants.
///
/// # How can I implement `ValidatedForm`?
/// # Structures
/// Given the following `struct` and its expected value:
///
/// ```
/// use swim_common::model::{Attr, Item, Value};
/// use swim_common::form::Form;
///
/// #[derive(Form)]
/// struct Cat {
///     name: String,
/// }
///
/// let cat = Cat {
///     name: String::from("Dill"),
/// };
///
/// assert_eq!(cat.as_value(), Value::Record(
///     vec![Attr::of("Cat")],
///     vec![Item::Slot(Value::text("name"), Value::text("Dill"))]
/// ));
/// ```
///
/// We can observe that the schema for this `Record` is a single attribute with an `Extant` body
/// and a single `Slot` with a key of "name" and a String value The expected schema for this
/// is:
/// ```
/// use swim_common::model::{Attr, Item, Value, ValueKind};
/// use swim_common::form::{Form, ValidatedForm};
/// use swim_common::model::schema::{StandardSchema, ItemSchema, Schema};
/// use swim_common::model::schema::attr::AttrSchema;
/// use swim_common::model::schema::slot::SlotSchema;
///
/// #[derive(Form)]
/// struct Cat {
///     name: String,
/// }
///
/// impl ValidatedForm for Cat {
///     fn schema() -> StandardSchema {
///        StandardSchema::HeadAttribute {
///            schema: Box::new(AttrSchema::named(
///                "Cat",
///                StandardSchema::OfKind(ValueKind::Extant),
///            )),
///            required: true,
///            remainder: Box::new(StandardSchema::Layout {
///                items: vec![(
///                    ItemSchema::Field(SlotSchema::new(
///                        StandardSchema::text("name"),
///                        String::schema(),
///                    )),
///                    true,
///                )],
///                exhaustive: true,
///            }),
///        }
///     }
/// }
///
/// let cat = Cat {
///     name: String::from("Dill"),
/// }
/// .as_value();
///
/// assert!(Cat::schema().matches(&cat));
/// ```
///
/// # Enumerations
/// Implementing `ValidatedForm` for enumerations works the same way as structures except that the
/// variants are logically OR'd together as a schema.
/// ```
/// use swim_common::model::{Attr, Item, Value, ValueKind};
/// use swim_common::form::{Form, ValidatedForm};
/// use swim_common::model::schema::{StandardSchema, ItemSchema, Schema};
/// use swim_common::model::schema::attr::AttrSchema;
/// use swim_common::model::schema::slot::SlotSchema;
///
/// #[derive(Form)]
/// enum Food {
///     Kebab,
///     Pizza,
///     Pasta,
/// }
///
/// impl ValidatedForm for Food {
///     fn schema() -> StandardSchema {
///         StandardSchema::Or(vec![
///             StandardSchema::HeadAttribute {
///                 schema: Box::new(AttrSchema::named(
///                     "Kebab",
///                     StandardSchema::OfKind(ValueKind::Extant),
///                 )),
///                 required: false,
///                 remainder: Box::new(StandardSchema::Layout {
///                     items: vec![],
///                     exhaustive: false,
///                 }),
///             },
///             StandardSchema::HeadAttribute {
///                 schema: Box::new(AttrSchema::named(
///                     "Pizza",
///                     StandardSchema::OfKind(ValueKind::Extant),
///                 )),
///                 required: false,
///                 remainder: Box::new(StandardSchema::Layout {
///                     items: vec![],
///                     exhaustive: false,
///                 }),
///             },
///             StandardSchema::HeadAttribute {
///                 schema: Box::new(AttrSchema::named(
///                     "Pasta",
///                     StandardSchema::OfKind(ValueKind::Extant),
///                 )),
///                 required: false,
///                 remainder: Box::new(StandardSchema::Layout {
///                     items: vec![],
///                     exhaustive: false,
///                 }),
///             },
///         ])
///     }
/// }
///
/// let food = Food::Kebab.as_value();
/// assert!(Food::schema().matches(&food));
/// ```
///
/// # Deriving `ValidatedForm`
/// The `ValidatedForm` trait can also be used with `#[derive]` if all of the fields implement
/// `ValidatedForm` and the implementor also implements `Form` (as required by the `ValidatedForm`
/// trait).
///
/// The derive macro supports all of the `StandardSchema` variants in a Snake Case format except
/// for: `HeadAttribute`, `HasAttributes`, `HasSlots` and `Layout`. Implementors which require these
/// variants will have to be implemented manually.
///
/// Derived schemas mandate that all fields are present and exhaustive validation is performed.
///
/// ## Usage
/// Any `struct` or `enum` annotated with `#[derive(ValidatedForm)]` will have a schema derived for
/// it based on its fields. Fields not attributed with `#[form(schema(..)]` will use the type's
/// implementation of `ValidatedForm`.
///
/// ```
/// use swim_common::model::{Attr, Item, Value, ValueKind};
/// use swim_common::form::{Form, ValidatedForm};
/// use swim_common::model::schema::{StandardSchema, ItemSchema, Schema};
/// use swim_common::model::schema::attr::AttrSchema;
/// use swim_common::model::schema::slot::SlotSchema;
///
/// #[derive(Form, ValidatedForm)]
/// #[form(tag = "Structure")]
/// struct S {
///     a: i32,
///     b: i64,
/// }
///
/// let expected_schema = StandardSchema::HeadAttribute {
///     schema: Box::new(AttrSchema::named(
///         "Structure",
///         StandardSchema::OfKind(ValueKind::Extant),
///     )),
///     required: true,
///     remainder: Box::new(StandardSchema::Layout {
///         items: vec![
///             (
///                 ItemSchema::Field(SlotSchema::new(StandardSchema::text("a"), i32::schema())),
///                 true,
///             ),
///             (
///                 ItemSchema::Field(SlotSchema::new(StandardSchema::text("b"), i64::schema())),
///                 true,
///             ),
///         ],
///         exhaustive: true,
///     }),
/// };
///
/// assert_eq!(S::schema(), expected_schema);
/// ```
///
/// ## Attributes
/// The `ValidatedForm` macro supports attributes in the path of `#[form(schema(..))]`.
///
/// The derive macro supports all the attributes that the `Form` derive macro supports. With the
/// exception of any field marked as `#[form(skip)]` may not also contain a schema.
///
/// Similar to the `Form` derive, `ValidatedForm` derivation supports attributes at `struct`,
/// variant, and field level placement. Detail on attributes that support this is listed below.
///
/// ### anything
/// ```
/// use swim_common::form::{Form, ValidatedForm};
///
/// #[derive(Form, ValidatedForm)]
/// #[form(schema(anything))]
/// struct S {
///     a: i32,
///     b: i64,
/// }
/// ```
/// The `anything` attribute is supported at a structure, variant and field level. When placed at
/// a structure or variant level, the derived schema is `StandardSchema::Anything`. At a field
/// level, the value of the field is written as `StandardSchema::Anything`.
///
/// ### nothing
/// ```
/// use swim_common::form::{Form, ValidatedForm};
///
/// #[derive(Form, ValidatedForm)]
/// #[form(schema(nothing))]
/// struct S {
///     a: i32,
///     b: i64,
/// }
/// ```
/// The `nothing` attribute is supported at a structure, variant and field level. When placed at
/// a structure or variant level, the derived schema is `StandardSchema::Nothing`. At a field
/// level, the value of the field is written as `StandardSchema::Nothing`.
///
/// ### num_attrs
/// ```
/// use swim_common::form::{Form, ValidatedForm};
/// use swim_common::model::Value;
///
/// #[derive(Form, ValidatedForm)]
/// struct S {
///     #[form(schema(num_attrs = 5))]
///     value: Value
/// }
/// ```
/// Asserts that a `Value` has the provided number of attributes.
///
/// ### num_items
/// ```
/// use swim_common::form::{Form, ValidatedForm};
/// use swim_common::model::Value;
///
/// #[derive(Form, ValidatedForm)]
/// struct S {
///     #[form(schema(num_items = 5))]
///     value: Value
/// }
/// ```
/// Asserts that a `Value` has the provided number of items.
///
/// ### of_kind
/// ```
/// use swim_common::form::{Form, ValidatedForm};
/// use swim_common::model::{Value, ValueKind};
///
/// #[derive(Form, ValidatedForm)]
/// struct S {
///     #[form(schema(of_kind(ValueKind::Int32)))]
///     value: Value
/// }
/// ```
/// Asserts that a `Value` is of the provided `ValueKind`
///
/// ### equal
/// ```
/// use swim_common::form::{Form, ValidatedForm};
/// use swim_common::model::Value;
///
/// fn equals_i32_max() -> Value {
///     Value::Int32Value(i32::max_value())
/// }
///
/// #[derive(Form, ValidatedForm)]
/// struct S {
///     #[form(schema(equal = "equals_i32_max"))]
///     value: i32
/// }
/// ```
/// Valid at structure, variant and field level, asserts that a value matches the provided value.
/// This attribute accepts a function identity in the format of `fn() -> Value`.
///
/// ### text
/// ```
/// use swim_common::form::{Form, ValidatedForm};
///
/// #[derive(Form, ValidatedForm)]
/// struct S {
///     #[form(schema(text = "Swim"))]
///     string: String
/// }
/// ```
/// Builds a `TextSchema` and asserts that a value matches it.
///
/// ### non_nan
/// ```
/// use swim_common::form::{Form, ValidatedForm};
///
/// #[derive(Form, ValidatedForm)]
/// struct S {
///     #[form(schema(non_nan))]
///     f: f64
/// }
/// ```
/// Asserts that a `Value` is a non-NaN floating point number.
///
/// ### finite
/// ```
/// use swim_common::form::{Form, ValidatedForm};
///
/// #[derive(Form, ValidatedForm)]
/// struct S {
///     #[form(schema(finite))]
///     f: f64
/// }
/// ```
/// Asserts that a `Value` is a finite floating point number.
///
/// ### Ranges
/// Numerical types may have their transmuted `Value`s asserted that they are within a provided
/// range for:
/// - `i32`/`i64` using `int_range`.
/// - `u32`/`u64` using `uint_range`.
/// - `f64` using `float_range`.
/// - `BigInt`/`BigUint` using `big_int_range`.
///
/// The `std::ops::Range` (`start..end`) and `std::ops::RangeInclusive` (start..=end) syntax is used
/// for deriving ranges.
///
/// ```
/// use swim_common::form::{Form, ValidatedForm};
///
/// #[derive(Form, ValidatedForm)]
/// struct S {
///     #[form(schema(int_range = "0..=10"))]
///     range: i32,
/// }
/// ```
///
/// ### all_items
/// Asserts that all of the items in the record match the given schema. At a container level, this
/// writes the argument as a layout
///
/// ```
/// use swim_common::form::{Form, ValidatedForm};
/// use swim_common::model::ValueKind;
///
/// #[derive(Form, ValidatedForm)]
/// #[form(schema(all_items(of_kind(ValueKind::Int32))))]
/// struct S {
///     a: i32,
///     b: i32,
///     c: i32
/// }
/// ```
/// Is equivalent to:
/// ```
/// use swim_common::form::{Form, ValidatedForm};
/// use swim_common::model::ValueKind;
///
/// #[derive(Form, ValidatedForm)]
/// struct S {
///     #[form(schema(of_kind(ValueKind::Int32)))]
///     a: i32,
///     #[form(schema(of_kind(ValueKind::Int32)))]
///     b: i32,
///     #[form(schema(of_kind(ValueKind::Int32)))]
///     c: i32
/// }
/// ```
///
/// ### and
/// ```
/// use swim_common::form::{Form, ValidatedForm};
/// use swim_common::model::ValueKind;
///
/// #[derive(Form, ValidatedForm)]
/// struct S {
///     #[form(schema(and(of_kind(ValueKind::Text), text = "swim")))]
///     company: String
/// }
/// ```
/// Performs a logical AND on two or more schemas.
///
/// ### or
/// ```
/// use swim_common::form::{Form, ValidatedForm};
/// use swim_common::model::{Value, ValueKind};
///
/// #[derive(Form, ValidatedForm)]
/// struct S {
///     #[form(schema(or(of_kind(ValueKind::Int32), of_kind(ValueKind::Int64))))]
///     value: Value
/// }
/// ```
/// Performs a logical OR on two or more schemas.
///
/// ### not
/// ```
/// use swim_common::form::{Form, ValidatedForm};
///
/// #[derive(Form, ValidatedForm)]
/// struct S {
///     #[form(schema(not(int_range = "0..=10")))]
///     range: i32,
/// }
/// ```
/// Negates the result of a schema.
///
pub trait ValidatedForm {
    /// A schema for the form. If the schema returns true for a `Value` the form should be able
    /// to create an instance of the type from the `Value` without generating an error.
    fn schema() -> StandardSchema;
}

impl ValidatedForm for Value {
    fn schema() -> StandardSchema {
        StandardSchema::Anything
    }
}
