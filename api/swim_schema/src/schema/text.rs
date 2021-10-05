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

use crate::schema::Schema;
use regex::{Error as RegexError, Regex};
use std::borrow::Borrow;
use std::cmp::Ordering;
use swim_model::{Attr, ToValue, Value};

/// Schema for UTF8 strings.
#[derive(Clone, Debug)]
pub enum TextSchema {
    /// Matches if an only if the string is non-empty.
    NonEmpty,
    /// Matches only a specific string.
    Exact(String),
    /// Matches a string against a regular expression.
    Matches(Box<Regex>),
    /// A logical OR of text schemas
    Or(Vec<TextSchema>),
}

impl TextSchema {
    /// A schema that matches a single string.
    pub fn exact(string: &str) -> TextSchema {
        TextSchema::Exact(string.to_string())
    }

    /// A schema that accepts strings matching a regular expression.
    pub fn regex(string: &str) -> Result<TextSchema, RegexError> {
        Regex::new(string).map(Box::new).map(TextSchema::Matches)
    }

    /// A schema that performs a logical OR over a number of text schemas.
    pub fn or(schemas: Vec<TextSchema>) -> TextSchema {
        TextSchema::Or(schemas)
    }
}

impl PartialOrd for TextSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else {
            match (self, other) {
                (TextSchema::NonEmpty, TextSchema::Exact(val)) if !val.is_empty() => {
                    Some(Ordering::Greater)
                }
                (TextSchema::NonEmpty, TextSchema::Matches(regex)) if !regex.is_match("") => {
                    Some(Ordering::Greater)
                }
                (TextSchema::Exact(val), TextSchema::NonEmpty) if !val.is_empty() => {
                    Some(Ordering::Less)
                }
                (TextSchema::Exact(val), TextSchema::Matches(regex)) if regex.is_match(val) => {
                    Some(Ordering::Less)
                }
                (TextSchema::Matches(regex), TextSchema::NonEmpty) if !regex.is_match("") => {
                    Some(Ordering::Less)
                }
                (TextSchema::Matches(regex), TextSchema::Exact(val)) if regex.is_match(val) => {
                    Some(Ordering::Greater)
                }
                _ => None,
            }
        }
    }
}

impl ToValue for TextSchema {
    fn to_value(&self) -> Value {
        match self {
            TextSchema::NonEmpty => Attr::of("non_empty"),
            TextSchema::Exact(v) => Attr::of(("equal", v.clone())),
            TextSchema::Matches(r) => Attr::of(("matches", r.to_string())),
            TextSchema::Or(vec) => {
                Attr::with_items("or", vec.iter().map(ToValue::to_value).collect())
            }
        }
        .into()
    }
}

impl PartialEq for TextSchema {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TextSchema::NonEmpty, TextSchema::NonEmpty) => true,
            (TextSchema::Exact(left), TextSchema::Exact(right)) => left == right,
            (TextSchema::Matches(left), TextSchema::Matches(right)) => {
                left.as_str() == right.as_str()
            }
            (TextSchema::Or(l), TextSchema::Or(r)) => l == r,
            _ => false,
        }
    }
}

impl Eq for TextSchema {}

impl TextSchema {
    pub fn matches_str(&self, text: &str) -> bool {
        match self {
            TextSchema::NonEmpty => !text.is_empty(),
            TextSchema::Exact(s) => text == s,
            TextSchema::Matches(r) => r.is_match(text),
            TextSchema::Or(vec) => vec.iter().any(|s| s.matches_str(text)),
        }
    }

    pub fn matches_value(&self, value: &Value) -> bool {
        match value {
            Value::Text(text) => self.matches_str(text.borrow()),
            _ => false,
        }
    }
}

impl Schema<String> for TextSchema {
    fn matches(&self, value: &String) -> bool {
        self.matches_str(value.borrow())
    }
}
