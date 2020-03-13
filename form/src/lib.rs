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

pub extern crate common as _common;
pub extern crate deserialize as _deserialize;
#[macro_use]
#[allow(unused_imports)]
pub extern crate form_derive;
pub extern crate serialize as _serialize;

use common::model::Value;
use deserialize::FormDeserializeErr;
pub use form_derive::*;
use serialize::FormSerializeErr;

#[cfg(test)]
mod tests;

pub mod impls;

/// The preferred approach to deriving forms is to use the attribute [`[#form]`] as this will derive
/// [`Form`], [`Serialize`], and [`Deserialize`], as well as performing compile-time checking on the
/// types used. If both serialization anddeserialization is not required then
/// [`#[derive(Form, Serialize)]`] or [`#[derive(Form, Deserialize)]`] can be used. Or checking can
/// be avoided by [`#[derive(Form, Serialize, Deserialize)]`]
///
/// # Examples
///
/// ```
/// use form::form_derive::*;
/// use form::Form;
/// use common::model::{Value, Attr, Item};
///
/// #[form]
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
/// let result = msg.try_into_value().unwrap();
/// assert_eq!(record, result);
///
/// let result = Message::try_from_value(&record).unwrap();
/// assert_eq!(result, msg);
/// ```
pub trait Form: Sized {
    fn try_into_value(&self) -> std::result::Result<Value, FormSerializeErr>;
    fn try_from_value(value: &Value) -> std::result::Result<Self, FormDeserializeErr>;
}
