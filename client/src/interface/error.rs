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
use std::error::Error as StdError;
use std::fmt;
use std::fmt::{Display, Formatter};

type Cause = Box<dyn StdError + Send + Sync>;

#[derive(Debug)]
pub struct ClientError {
    kind: ErrorKind,
    cause: Option<Cause>,
}

impl StdError for ClientError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.cause
            .as_ref()
            .map(|cause| &**cause as &(dyn StdError + 'static))
    }
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let msg = match &self.kind {
            // todo: Add sensible messages
            ErrorKind::Shutdown => "A shutdown error occurred.",
            ErrorKind::RuntimeError => "A runtime error occurred.",
            ErrorKind::SubscriptionError => "Failed to subscribe to downlink",
        };

        match &self.cause {
            Some(c) => write!(f, "{} Caused by: {}", msg, c),
            None => write!(f, "{}", msg),
        }
    }
}

// todo: Add variant descriptions
#[derive(Debug, PartialOrd, PartialEq, Eq, Ord)]
pub enum ErrorKind {
    Shutdown,
    // todo: remove
    RuntimeError,
    SubscriptionError,
}

impl ClientError {
    pub(in crate::interface) fn with_cause<C>(kind: ErrorKind, cause: C) -> ClientError
    where
        C: Into<Cause>,
    {
        ClientError {
            kind,
            cause: Some(cause.into()),
        }
    }

    pub(in crate::interface) fn from(kind: ErrorKind, cause: Option<Cause>) -> ClientError {
        ClientError { kind, cause }
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    pub fn into_cause(self) -> Option<Box<dyn StdError + Send + Sync>> {
        self.cause
    }
}
