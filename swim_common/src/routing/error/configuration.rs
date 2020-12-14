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

use crate::routing::error::FmtResult;
use crate::routing::ConnectionError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use utilities::errors::Recoverable;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ConfigurationError {
    kind: ConfigurationErrorKind,
    cause: String,
}

impl ConfigurationError {
    pub fn new(kind: ConfigurationErrorKind, cause: String) -> ConfigurationError {
        ConfigurationError { kind, cause }
    }

    pub fn kind(&self) -> ConfigurationErrorKind {
        self.kind
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ConfigurationErrorKind {}

impl Error for ConfigurationError {}

impl Display for ConfigurationError {
    fn fmt(&self, _f: &mut Formatter<'_>) -> FmtResult {
        unimplemented!()
    }
}

impl Recoverable for ConfigurationError {
    fn is_fatal(&self) -> bool {
        true
    }
}

impl From<ConfigurationError> for ConnectionError {
    fn from(e: ConfigurationError) -> Self {
        ConnectionError::Configuration(e)
    }
}
