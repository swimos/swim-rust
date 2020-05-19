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

#[cfg(test)]
mod tests;

use crate::model::parser::{parse_document_iteratee, IterateeDecoder, ParseFailure};
use crate::model::Item;
use futures::StreamExt;
use pin_utils::pin_mut;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::str::Utf8Error;
use tokio::prelude::AsyncRead;
use tokio_util::codec::FramedRead;
use utilities::iteratee::{coenumerate, Iteratee};

/// Error type for reading a configuration document.
#[derive(Debug)]
pub enum ConfigurationError {
    /// An IO error occurred reading the source data.
    Io(io::Error),
    /// The input was not valid UTF8 text.
    BadUtf8(Utf8Error),
    /// An error occurred attempting to parse the valid UTF8 input.
    Parser(ParseFailure),
}

impl Display for ConfigurationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigurationError::Io(err) => write!(f, "IO error loading configuration: {}", err),
            ConfigurationError::BadUtf8(err) => {
                write!(f, "Configuration data contained invalid UTF8: {}", err)
            }
            ConfigurationError::Parser(err) => {
                write!(f, "Error parsing configuration data: {}", err)
            }
        }
    }
}

impl Error for ConfigurationError {}

impl From<io::Error> for ConfigurationError {
    fn from(err: io::Error) -> Self {
        ConfigurationError::Io(err)
    }
}

impl From<Utf8Error> for ConfigurationError {
    fn from(err: Utf8Error) -> Self {
        ConfigurationError::BadUtf8(err)
    }
}

/// Read and parse a Recon configuration file from an [`AsyncRead`] instance.
pub async fn read_config_from<Src: AsyncRead>(input: Src) -> Result<Vec<Item>, ConfigurationError> {
    pin_mut!(input);
    let iteratee =
        coenumerate(parse_document_iteratee()).map(|r| r.map_err(ConfigurationError::Parser));
    let decoder = IterateeDecoder::new(iteratee);
    let mut framed = FramedRead::new(input, decoder);
    framed.next().await.unwrap_or_else(|| Ok(vec![]))
}
