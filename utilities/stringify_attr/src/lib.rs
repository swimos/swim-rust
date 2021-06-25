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

//! A macro to parse macro attributes in a path format into a string literal. This provides a
//! workaround for not being able to use the standard library's stringify macro in procedural and
//! derive macro attributes.
//!
//! The `stringify_attr` macro is supported at: container-level, all field types, and enumeration
//! variants.
//!
//! # Example:
//! ```
//! #![allow(non_snake_case, non_camel_case_types)]
//!
//! use attr_derive::stringify_attr;
//! use serde::Serialize;
//!
//! macro_rules! t {
//!     ($name:ident, $field:ident, $alias:ident) => {
//!         #[stringify_attr]
//!         #[derive(Serialize)]
//!         struct $name<$field> {
//!             #[stringify(serde(rename($field), alias($alias)))]
//!             key: $field,
//!         }
//!     };
//! }
//!
//! t!(Structure, i32, Count);
//!
//! let s = Structure { key: 1 };
//! let result = serde_json::to_string(&s);
//!
//! assert_eq!(result.unwrap(), "{\"i32\":1}".to_string());
//! ```
//!
//! Will expand to:
//! ```
//! use serde::Serialize;
//!
//! #[derive(Serialize)]
//! struct Structure<A> {
//!     #[serde(rename = "A", alias = "A")]
//!     key: A,
//! }
//! ```
//!
//!
//! The `stringify_attr` macro provides attribute manipulation at two levels:
//!
//! # Stringify field attributes:
//! Attributes may be stringified in one of two methods, using the `stringify` attribute which will
//! only accept items to stringify and nothing else, or using `stringify_raw` which will accept a
//! path, a list of items to stringify as well as a list of items to write to the attribute.
//!
//! Both the `stringify` and `stringify_raw` attribute may be decorated on to the same field
//! multiple times to parse tokens for different paths.
//!
//! ## Stringify:
//! Items are stringified in a format of `#[stringify(serde(rename(A), alias(B)))]`. Using lists of
//! simple paths to accept raw tokens which will be stringified and written to the output token
//! stream.
//!
//! ## Stringify raw:
//! Fields may be decorated with `stringify_raw` to provide extra flexibility in only parsing *some*
//! inputs to a string and writing raw tokens directly to the output stream. This is done using a
//! format of `#[stringify_raw(path = "serde", in(rename(A)), raw(alias = "B"))]`. Where `path` is
//! the name of the attribute that is being targeted.
//!
//! ### Example:
//! ```
//! use attr_derive::stringify_attr;
//! use serde::Serialize;
//!
//! macro_rules! t {
//!     ($name :ident, $field_name:ident, $alias:ident) => {
//!         #[stringify_attr]
//!         #[derive(Serialize)]
//!         struct $name {
//!             #[stringify_raw(
//!                 path = "serde",
//!                 in(rename($field_name), alias($alias)),
//!                 raw(alias = "D")
//!             )]
//!             $field_name: i32,
//!         }
//!     };
//! }
//!
//! t!(A, B, C);
//!
//! let result = serde_json::to_string(&A { B: 1 });
//! assert_eq!(result.unwrap(), "{\"B\":1}".to_string());
//! ```
//!
//! Will expand to:
//! ```
//! use serde::Serialize;
//!
//! #[derive(Serialize)]
//! struct Structure<A> {
//!     #[serde(rename = "A", alias = "A")]
//!     key: A,
//! }
//! ```
//!
//! ## Syntax
//! ```text
//! # [ stringify | stringify_raw ]
//!
//! stringify:
//!     SimplePath ( SimplePath ( SimplePath ) , * )
//!
//! stringify_raw:
//!     path = STRING_LITERAL,
//!     in ( SimplePath ( SimplePath) , * ),
//!     raw ( DelimTokenTree | = LiteralExpression )
//! ```
//!
//! # Container attributes:
//! Decorating a structure with `stringify_attr` and providing an optional list of paths to
//! stringify
//!
//! ```
//! use attr_derive::stringify_attr;
//! use serde::Serialize;
//!
//! macro_rules! t {
//!     ($name :ident, $field_name:ident, $style:ident) => {
//!         #[stringify_attr(serde(rename_all($style)))]
//!         #[derive(Serialize)]
//!         struct $name {
//!             $field_name: i32,
//!         }
//!     };
//! }
//!
//! t!(A, somefieldinlowercase, UPPERCASE);
//!
//! let result = serde_json::to_string(&A {
//!     somefieldinlowercase: 0,
//! });
//!
//! assert_eq!(result.unwrap(), "{\"SOMEFIELDINLOWERCASE\":0}".to_string());
//! ```
//!
//! This will expand to:
//!
//! ```
//! #![allow(non_snake_case, non_camel_case_types)]
//!
//! use serde::Serialize;
//!
//! #[serde(rename_all = "UPPERCASE")]
//! #[derive(Serialize)]
//! struct Structure {
//!     somefieldinlowercase: i32,
//! }
//! ```
//!
//! # See also:
//!
//! [Serde#450](https://github.com/serde-rs/serde/issues/450)
//! [Serde#1636](https://github.com/serde-rs/serde/issues/1636)
//! [Serde#1895](https://github.com/serde-rs/serde/issues/1895)

#![allow(legacy_derive_helpers)]

#[cfg(test)]
mod tests;

pub use attr_derive::{stringify_attr, stringify_attr_raw};
