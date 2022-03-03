// Copyright 2015-2022 Swim Inc.
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

use crate::parser::HashParser;
use crate::parser::{ParseError, Span};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::Hasher;

#[cfg(test)]
mod tests;

pub fn calculate_hash<H: Hasher>(value: &str, hasher: H) -> Result<u64, HashError> {
    let parse_iterator = HashParser::new(Span::new(value), hasher);
    parse_iterator.hash()
}

#[derive(Debug, PartialEq, Eq)]
pub struct HashError(ParseError);

impl From<ParseError> for HashError {
    fn from(err: ParseError) -> Self {
        HashError(err)
    }
}

impl<'a> From<nom::error::Error<Span<'a>>> for HashError {
    fn from(err: nom::error::Error<Span<'a>>) -> Self {
        HashError(err.into())
    }
}

impl Error for HashError {}

impl Display for HashError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
