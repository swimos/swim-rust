use crate::model::schema::Schema;
use crate::model::{Attr, ToValue, Value};
use regex::{Error as RegexError, Regex};
use std::borrow::Borrow;
use std::cmp::Ordering;

/// Schema for UTF8 strings.
#[derive(Clone, Debug)]
pub enum TextSchema {
    /// Matches if an only if the string is non-empty.
    NonEmpty,
    /// Matches only a specific string.
    Exact(String),
    /// Matches a string against a regular expression.
    Matches(Regex),
}

impl TextSchema {
    /// A schema that matches a single string.
    pub fn exact(string: &str) -> TextSchema {
        TextSchema::Exact(string.to_string())
    }

    /// A schema that accepts strings matching a regular expression.
    pub fn regex(string: &str) -> Result<TextSchema, RegexError> {
        Regex::new(string).map(TextSchema::Matches)
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
                (TextSchema::Exact(val), TextSchema::Matches(regex)) if regex.is_match(&val) => {
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
