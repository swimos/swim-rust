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

use flate2::{CompressError, DecompressError};
use http::header::InvalidHeaderValue;
use std::str::Utf8Error;
use thiserror::Error;

#[derive(Error, Debug)]
#[error("Err")]
pub enum DeflateExtensionError {
    /// An error produced when deflating a message.
    DeflateError(CompressError),
    /// An error produced when inflating a message.
    InflateError(DecompressError),
    /// An error produced during the WebSocket negotiation.
    NegotiationError(String),
    /// An invalid LZ77 window size was provided.
    InvalidMaxWindowBits,
}

impl From<CompressError> for DeflateExtensionError {
    fn from(e: CompressError) -> Self {
        DeflateExtensionError::DeflateError(e)
    }
}

impl From<DecompressError> for DeflateExtensionError {
    fn from(e: DecompressError) -> Self {
        DeflateExtensionError::InflateError(e)
    }
}

impl From<Utf8Error> for DeflateExtensionError {
    fn from(e: Utf8Error) -> Self {
        DeflateExtensionError::NegotiationError(format!(
            "Failed to parse extension parameter: {}",
            e
        ))
    }
}

impl From<InvalidHeaderValue> for DeflateExtensionError {
    fn from(e: InvalidHeaderValue) -> Self {
        DeflateExtensionError::NegotiationError(format!("Failed to write response header: {}", e))
    }
}
