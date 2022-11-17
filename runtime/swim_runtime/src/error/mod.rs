// Copyright 2015-2021 Swim Inc.
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

use bytes::Bytes;
use std::fmt::{Display, Formatter};
use std::str::Utf8Error;
use thiserror::Error;

pub use tls::*;

mod tls;

/// Error indicating that the key for a map message contained invalid UTF8.
#[derive(Debug, Error)]
pub struct InvalidKey {
    key_bytes: Bytes,
    source: Utf8Error,
}

impl Display for InvalidKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "The key {:?}, contains invalid UTF8: {}.",
            self.key_bytes, self.source
        )
    }
}

impl InvalidKey {
    pub fn new(key_bytes: Bytes, source: Utf8Error) -> Self {
        InvalidKey { key_bytes, source }
    }
}
