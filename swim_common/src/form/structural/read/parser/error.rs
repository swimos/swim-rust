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

#[derive(Debug)]
pub enum ParseError {
    Syntax {
        kind: ErrorKind,
        offset: usize,
        line: u32,
        column: usize,
    },
    Structure(ReadError),
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
                "Failed to parse the input. {:?} at {}:{}.",
                kind, line, column
            ),
            ParseError::InvalidEventStream => write!(f, "Parser internal error."),
        }
    }
}

impl Error for ParseError {}
