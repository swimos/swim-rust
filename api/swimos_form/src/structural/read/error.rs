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

#[cfg(test)]
mod tests;

use std::error::Error;
use std::fmt::{Display, Formatter};
use swimos_model::Text;
use swimos_model::ValueKind;
use swimos_utilities::format as print;

/// Enumeration of possible deserialization events for error reporting.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum ExpectedEvent {
    /// A value with the specified kind.
    ValueEvent(ValueKind),
    /// An attribute, possibly with a specific name.
    Attribute(Option<Text>),
    /// A record body.
    RecordBody,
    /// A slot.
    Slot,
    /// The end of a record.
    EndOfRecord,
    /// The end of an attribute.
    EndOfAttribute,
    /// Any one of a number of specified events.
    Or(Vec<ExpectedEvent>),
}

/// Errors that can occur deserializing from the SwimOS model.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum ReadError {
    /// The deserialization stream contained a valid event but of an unexpected kind.
    UnexpectedKind {
        /// The kind that was encountered.
        actual: ValueKind,
        /// The kind that was expected (or [`None`] if no event was expected).
        expected: Option<ExpectedEvent>,
    },
    /// The stream terminated before deserialization was complete.
    ReaderUnderflow,
    /// An invalid slot was encountered in the input.
    DoubleSlot,
    /// Deserialization complete but more input is still available.
    ReaderOverflow,
    /// A record did not terminate correctly.
    IncompleteRecord,
    /// One or more required slots were not present in the input.
    MissingFields(Vec<Text>),
    /// An unexpected attribute occurred in the input.
    UnexpectedAttribute(Text),
    /// The deserialization state became corrupted.
    InconsistentState,
    /// An unexpected item occurred in a record.
    UnexpectedItem,
    /// An unexpected slot occurred in a record.
    UnexpectedSlot,
    /// Two fields with the same name occurred in a record.
    DuplicateField(Text),
    /// An unexpected field occurred in a record.
    UnexpectedField(Text),
    /// A numeric value was outside of the expected range for the type being deserialized.
    NumberOutOfRange,
    /// A tag attribute was required for the type being deserialized but was absent.
    MissingTag,
    /// The content of a string component of the input was not valid for the type being deserialized.
    Malformatted {
        /// The invalid string.
        text: Text,
        /// A message describing the problem.
        message: Text,
    },
    /// A custom error message.
    Message(Text),
}

impl ReadError {
    pub fn unexpected_kind(actual: ValueKind, expected: Option<ExpectedEvent>) -> Self {
        ReadError::UnexpectedKind { actual, expected }
    }
}

impl Display for ExpectedEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpectedEvent::ValueEvent(kind) => {
                write!(f, "A value of kind {}", kind)
            }
            ExpectedEvent::Attribute(Some(name)) => {
                write!(f, "An attribute named '{}'", name)
            }
            ExpectedEvent::Attribute(_) => f.write_str("An attribute"),
            ExpectedEvent::RecordBody => f.write_str("A record body"),
            ExpectedEvent::Slot => f.write_str("A slot divider"),
            ExpectedEvent::EndOfRecord => f.write_str("The end of the record body"),
            ExpectedEvent::EndOfAttribute => f.write_str("The end of the attribute"),
            ExpectedEvent::Or(sub) => {
                if sub.len() < 2 {
                    if let Some(first) = sub.first() {
                        write!(f, "{}", first)
                    } else {
                        f.write_str("Nothing")
                    }
                } else {
                    f.write_str("One of: [")?;
                    let mut it = sub.iter();
                    if let Some(first) = it.next() {
                        write!(f, "{}", first)?;
                    }
                    for ev in it {
                        write!(f, ", {}", ev)?;
                    }
                    f.write_str("]")
                }
            }
        }
    }
}

impl Display for ReadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::UnexpectedKind { actual, expected } => {
                if let Some(expected) = expected {
                    write!(
                        f,
                        "Unexpected value kind: {}, expected: {}.",
                        actual, expected
                    )
                } else {
                    write!(f, "Unexpected value kind: {}", actual)
                }
            }
            ReadError::ReaderUnderflow => write!(f, "Stack underflow deserializing the value."),
            ReadError::DoubleSlot => {
                write!(f, "Slot divider encountered within the value of a slot.")
            }
            ReadError::ReaderOverflow => {
                write!(f, "Record more deeply nested than expected for type.")
            }
            ReadError::IncompleteRecord => write!(
                f,
                "The record ended before all parts of the value were deserialized."
            ),
            ReadError::MissingFields(names) => {
                write!(f, "Fields [{}] are required.", print::comma_sep(names))
            }
            ReadError::UnexpectedAttribute(name) => write!(f, "Unexpected attribute: '{}'", name),
            ReadError::InconsistentState => {
                write!(f, "The deserialization state became corrupted.")
            }
            ReadError::UnexpectedItem => write!(f, "Unexpected item in record."),
            ReadError::UnexpectedSlot => write!(f, "Unexpected slot in record."),
            ReadError::DuplicateField(name) => {
                write!(f, "Field '{}' occurred more than once.", name)
            }
            ReadError::UnexpectedField(name) => write!(f, "Unexpected field: '{}'", name),
            ReadError::NumberOutOfRange => write!(f, "Number out of range."),
            ReadError::MissingTag => write!(f, "Missing tag attribute for record type."),
            ReadError::Malformatted { text, message } => {
                write!(f, "Text value '{}' is invalid: {}", text, message)
            }
            ReadError::Message(text) => f.write_str(text.as_str()),
        }
    }
}

impl Error for ReadError {}
