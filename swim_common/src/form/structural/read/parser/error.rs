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

use super::Span;
use crate::form::structural::read::ReadError;
use nom::error::ErrorKind;
use std::error::Error;
use std::fmt::{Display, Formatter};

/// Error that can occur when parsing into a structurally readable type.
#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    /// Parsing the input text failed. At the moment this just indicates the location
    /// in the input and the `nom` rule that failed.
    Syntax {
        kind: ErrorKind,
        offset: usize,
        line: u32,
        column: usize,
    },
    /// The parsed strucuture was not valid for the target type.
    Structure(ReadError),
    /// The parser produced an invalid stream of events. This likely indicates
    /// a bug in the parser.
    InvalidEventStream,
}

impl From<ReadError> for ParseError {
    fn from(e: ReadError) -> Self {
        ParseError::Structure(e)
    }
}

impl<'a> From<nom::error::Error<Span<'a>>> for ParseError {
    fn from(e: nom::error::Error<Span<'a>>) -> Self {
        let nom::error::Error { input, code } = e;
        ParseError::Syntax {
            kind: code,
            offset: input.location_offset(),
            line: input.location_line(),
            column: input.get_utf8_column(),
        }
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Structure(err) => {
                write!(f, "Document did not match the expected structure: {}", err)
            }
            ParseError::Syntax {
                kind, line, column, ..
            } => write!(
                f,
                "Failed to parse the input. rule = '{:?}' at ({}:{}).",
                kind, line, column
            ),
            ParseError::InvalidEventStream => write!(
                f,
                "The stream of events did not constitute a valid record in the Swim model."
            ),
        }
    }
}

impl Error for ParseError {}

#[cfg(test)]
mod tests {

    use super::ParseError;
    use crate::form::structural::read::ReadError;
    use nom::error::ErrorKind;

    #[test]
    fn parse_error_display() {
        let err = ParseError::Syntax {
            kind: ErrorKind::Tag,
            offset: 567,
            line: 5,
            column: 12,
        };
        let string = format!("{}", err);
        assert_eq!(string, "Failed to parse the input. rule = 'Tag' at (5:12).");

        let err = ParseError::InvalidEventStream;
        let string = format!("{}", err);
        assert_eq!(string, "Parser internal error.");

        let err = ParseError::Structure(ReadError::DoubleSlot);
        let string = format!("{}", err);
        assert_eq!(string, "Document did not match the expected structure: Slot divider encountered within the value of a slot.");
    }
}
