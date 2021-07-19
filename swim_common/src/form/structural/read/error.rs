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

use crate::model::text::Text;
use crate::model::ValueKind;
use std::error::Error;
use std::fmt::{Display, Formatter};
use utilities::print;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum ReadError {
    UnexpectedKind(ValueKind),
    ReaderUnderflow,
    DoubleSlot,
    ReaderOverflow,
    IncompleteRecord,
    MissingFields(Vec<Text>),
    UnexpectedAttribute(Text),
    InconsistentState,
    UnexpectedItem,
    UnexpectedSlot,
    DuplicateField(Text),
    UnexpectedField(Text),
    NumberOutOfRange,
    MissingTag,
    Malformatted { text: Text, message: Text },
    Message(Text),
}

impl Display for ReadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::UnexpectedKind(kind) => write!(f, "Unexpected value kind: {}", kind),
            ReadError::ReaderUnderflow => write!(f, "Stack undeflow deserializing the value."),
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
                write!(f, "Field '{}' ocurred more than once.", name)
            }
            ReadError::UnexpectedField(name) => write!(f, "Unexpected field: '{}'", name),
            ReadError::NumberOutOfRange => write!(f, "Number out of range."),
            ReadError::MissingTag => write!(f, "Missing tag attribute for record type."),
            ReadError::Malformatted { text, message } => {
                write!(f, "Text value '{}' is invalud: {}", text, message)
            }
            ReadError::Message(text) => f.write_str(text.as_str()),
        }
    }
}

impl Error for ReadError {}
