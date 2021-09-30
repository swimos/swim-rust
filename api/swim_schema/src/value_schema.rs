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
pub use swim_form_derive::ValueSchema;

use crate::schema::StandardSchema;
use swim_model::Value;

/// A `Form` with an associated schema that can validate `Value` instances without attempting
/// to convert them.
///
/// See `StandardSchema` for details of schema variants.
///
/// # How can I implement `ValueSchema`?
/// # Structures
/// Given the following `struct` and its expected value:
///
/// ```
/// use swim_model::{Attr, Item, Value};
/// use swim_form::Form;
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
/// use swim_model::{Attr, Item, Value, ValueKind};
/// use swim_form::Form;
/// use swim_schema::ValueSchema;
/// use swim_schema::schema::{StandardSchema, ItemSchema, Schema};
/// use swim_schema::schema::attr::AttrSchema;
/// use swim_schema::schema::slot::SlotSchema;
///
/// #[derive(Form)]
/// struct Cat {
///     name: String,
/// }
///
/// impl ValueSchema for Cat {
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
/// Implementing `ValueSchema` for enumerations works the same way as structures except that the
/// variants are logically OR'd together as a schema.
/// ```
/// use swim_model::{Attr, Item, Value, ValueKind};
/// use swim_form::Form;
/// use swim_schema::ValueSchema;
/// use swim_schema::schema::{StandardSchema, ItemSchema, Schema};
/// use swim_schema::schema::attr::AttrSchema;
/// use swim_schema::schema::slot::SlotSchema;
///
/// #[derive(Form)]
/// enum Food {
///     Kebab,
///     Pizza,
///     Pasta,
/// }
///
/// impl ValueSchema for Food {
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
/// # Deriving `ValueSchema`
/// The `ValueSchema` trait can also be used with `#[derive]` if all of the fields implement
/// `ValueSchema` and the implementor also implements `Form` (as required by the `ValueSchema`
/// trait).
///
/// The derive macro supports all of the `StandardSchema` variants in a Snake Case format except
/// for: `HeadAttribute`, `HasAttributes`, `HasSlots` and `Layout`. Implementors which require these
/// variants will have to be implemented manually.
///
/// Derived schemas mandate that all fields are present and exhaustive validation is performed.
///
/// ## Usage
/// Any `struct` or `enum` annotated with `#[derive(ValueSchema)]` will have a schema derived for
/// it based on its fields. Fields not attributed with `#[form(schema(..)]` will use the type's
/// implementation of `ValueSchema`.
///
/// ```
/// use swim_model::{Attr, Item, Value, ValueKind};
/// use swim_form::Form;
/// use swim_schema::ValueSchema;
/// use swim_schema::schema::{StandardSchema, ItemSchema, Schema};
/// use swim_schema::schema::attr::AttrSchema;
/// use swim_schema::schema::slot::SlotSchema;
///
/// #[derive(Form, ValueSchema)]
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
/// The `ValueSchema` macro supports attributes in the path of `#[form(schema(..))]`.
///
/// The derive macro supports all the attributes that the `Form` derive macro supports. With the
/// exception of any field marked as `#[form(skip)]` may not also contain a schema.
///
/// Similar to the `Form` derive, `ValueSchema` derivation supports attributes at `struct`,
/// variant, and field level placement. Detail on attributes that support this is listed below.
///
/// ### anything
/// ```
/// use swim_form::Form;
/// use swim_schema::ValueSchema;
///
/// #[derive(Form, ValueSchema)]
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
/// use swim_form::Form;
/// use swim_schema::ValueSchema;
///
/// #[derive(Form, ValueSchema)]
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
/// use swim_form::Form;
/// use swim_schema::ValueSchema;
/// use swim_model::Value;
///
/// #[derive(Form, ValueSchema)]
/// struct S {
///     #[form(schema(num_attrs = 5))]
///     value: Value
/// }
/// ```
/// Asserts that a `Value` has the provided number of attributes.
///
/// ### num_items
/// ```
/// use swim_form::Form;
/// use swim_schema::ValueSchema;
/// use swim_model::Value;
///
/// #[derive(Form, ValueSchema)]
/// struct S {
///     #[form(schema(num_items = 5))]
///     value: Value
/// }
/// ```
/// Asserts that a `Value` has the provided number of items.
///
/// ### of_kind
/// ```
/// use swim_form::Form;
/// use swim_schema::ValueSchema;
/// use swim_model::{Value, ValueKind};
///
/// #[derive(Form, ValueSchema)]
/// struct S {
///     #[form(schema(of_kind(ValueKind::Int32)))]
///     value: Value
/// }
/// ```
/// Asserts that a `Value` is of the provided `ValueKind`
///
/// ### equal
/// ```
/// use swim_form::Form;
/// use swim_schema::ValueSchema;
/// use swim_model::Value;
///
/// fn equals_i32_max() -> Value {
///     Value::Int32Value(i32::max_value())
/// }
///
/// #[derive(Form, ValueSchema)]
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
/// use swim_form::Form;
/// use swim_schema::ValueSchema;
///
/// #[derive(Form, ValueSchema)]
/// struct S {
///     #[form(schema(text = "Swim"))]
///     string: String
/// }
/// ```
/// Builds a `TextSchema` and asserts that a value matches it.
///
/// ### non_nan
/// ```
/// use swim_form::Form;
/// use swim_schema::ValueSchema;
///
/// #[derive(Form, ValueSchema)]
/// struct S {
///     #[form(schema(non_nan))]
///     f: f64
/// }
/// ```
/// Asserts that a `Value` is a non-NaN floating point number.
///
/// ### finite
/// ```
/// use swim_form::Form;
/// use swim_schema::ValueSchema;
///
/// #[derive(Form, ValueSchema)]
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
/// use swim_form::{Form, ValueSchema};
///
/// #[derive(Form, ValueSchema)]
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
/// use swim_form::{Form, ValueSchema};
/// use swim_model::ValueKind;
///
/// #[derive(Form, ValueSchema)]
/// #[form(schema(all_items(of_kind(ValueKind::Int32))))]
/// struct S {
///     a: i32,
///     b: i32,
///     c: i32
/// }
/// ```
/// Is equivalent to:
/// ```
/// use swim_form::{Form, ValueSchema};
/// use swim_model::ValueKind;
///
/// #[derive(Form, ValueSchema)]
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
/// use swim_form::{Form, ValueSchema};
/// use swim_model::ValueKind;
///
/// #[derive(Form, ValueSchema)]
/// struct S {
///     #[form(schema(and(of_kind(ValueKind::Text), text = "swim")))]
///     company: String
/// }
/// ```
/// Performs a logical AND on two or more schemas.
///
/// ### or
/// ```
/// use swim_form::{Form, ValueSchema};
/// use swim_model::{Value, ValueKind};
///
/// #[derive(Form, ValueSchema)]
/// struct S {
///     #[form(schema(or(of_kind(ValueKind::Int32), of_kind(ValueKind::Int64))))]
///     value: Value
/// }
/// ```
/// Performs a logical OR on two or more schemas.
///
/// ### not
/// ```
/// use swim_form::{Form, ValueSchema};
///
/// #[derive(Form, ValueSchema)]
/// struct S {
///     #[form(schema(not(int_range = "0..=10")))]
///     range: i32,
/// }
/// ```
/// Negates the result of a schema.
///
pub trait ValueSchema {
    /// A schema for the form. If the schema returns true for a `Value` the form should be able
    /// to create an instance of the type from the `Value` without generating an error.
    fn schema() -> StandardSchema;
}

impl ValueSchema for Value {
    fn schema() -> StandardSchema {
        StandardSchema::Anything
    }
}
