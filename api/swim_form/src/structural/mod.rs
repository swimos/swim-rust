// Copyright 2015-2021 Swim Inc.
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

#[cfg(test)]
mod tests;

use swim_model::Text;

mod bridge;
pub mod generic;
pub mod read;
#[macro_use]
pub mod write;
mod tags;

use std::str::FromStr;
#[doc(hidden)]
#[allow(unused_imports)]
pub use swim_form_derive::Tag;

/// A tag for a field in a form. When deriving the `Form` trait, a field that is annotated with
/// `#[form(tag)]` will be converted into a string and replace the original structure's name.
///
/// ```
/// use swim_form::Form;
/// use swim_form::structural::Tag;
/// use swim_model::{Attr, Item, Value};
/// use swim_model::time::Timestamp;
///
/// #[derive(Tag, Clone)]
/// enum Level {
///     #[form(tag = "info")]
///     Info,
///     #[form(tag = "warn")]
///     Warn
/// }
///
/// #[derive(Form)]
/// struct LogEntry {
///     #[form(tag)]
///     level: Level,
///     #[form(header)]
///     time: Timestamp,
///     message: String,
/// }
///
/// let now = Timestamp::now();
///
/// let entry = LogEntry {
///     level: Level::Info,
///     time: now,
///     message: String::from("message"),
/// };
///
/// assert_eq!(
///     entry.as_value(),
///     Value::Record(
///         vec![Attr::of((
///             "info",
///             Value::from_vec(vec![Item::Slot(Value::text("time"), now.as_value())])
///         ))],
///         vec![Item::Slot(Value::text("message"), Value::text("message"))]
///     )
/// )
///
/// ```
///
/// Tags can only be derived for enumerations and no variants may contain fields.
pub trait Tag: Sized + AsRef<str> + FromStr<Err = Text> {
    /// All possible string representations for this type.
    const VARIANTS: &'static [&'static str];
}
