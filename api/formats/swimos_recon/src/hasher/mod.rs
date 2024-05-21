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

use crate::recon_parser::record::HashParser;
use crate::recon_parser::{ParseError, Span};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

#[cfg(test)]
mod tests;

/// Add the Recon hash for a string (if it contains valid Recon). Recon strings that represent the same
/// Recon value will have the same hash.
///
/// # Arguments
/// * `value` - The string to hash.
/// * `hasher` - The hasher to contribute to.
pub fn recon_hash<H: Hasher>(value: &str, hasher: &mut H) {
    let parse_iterator = HashParser::new();

    if parse_iterator.hash(Span::new(value), hasher).is_some() {
        value.hash(hasher)
    }
}

/// A Recon hash could not be computed for a string as it did not contain valid Recon.
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
