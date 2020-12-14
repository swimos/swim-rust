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

use crate::routing::{format_cause, ConnectionError};
use std::error::Error;
use std::fmt::{Display, Formatter};
use utilities::errors::Recoverable;

use tokio::sync::mpsc::error::SendError as MpscSendError;
use tokio::sync::oneshot::error::RecvError as MpscRecvError;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CloseError {
    kind: CloseErrorKind,
    cause: Option<String>,
}

impl CloseError {
    pub fn new(kind: CloseErrorKind, cause: Option<String>) -> CloseError {
        CloseError { kind, cause }
    }

    pub fn kind(&self) -> CloseErrorKind {
        self.kind
    }

    pub fn unexpected() -> CloseError {
        CloseError {
            kind: CloseErrorKind::Unexpected,
            cause: None,
        }
    }

    pub fn already_closed() -> CloseError {
        CloseError {
            kind: CloseErrorKind::AlreadyClosed,
            cause: None,
        }
    }

    pub fn closed() -> CloseError {
        CloseError {
            kind: CloseErrorKind::Closed,
            cause: None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CloseErrorKind {
    /// The channel is closed.
    Closed,
    /// An unexpected close event occurred.
    Unexpected,
    /// An invalid close code was supplied to the channel.
    InvalidCloseCode,
    /// The callee attempted to close an already closed channel.
    AlreadyClosed,
    /// The channel closed successfully. This is not an error.
    Normal,
    /// The channel was closed by a peer.
    ClosedRemotely,
}

impl Error for CloseError {}

impl Display for CloseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let CloseError { kind, cause } = self;
        let cause = format_cause(cause);

        match kind {
            CloseErrorKind::Closed => {
                write!(f, "Channel closed.{}", cause)
            }
            CloseErrorKind::Unexpected => {
                write!(f, "Unexpected close event.{}", cause)
            }
            CloseErrorKind::InvalidCloseCode => {
                write!(f, "Invalid close code.{}", cause)
            }
            CloseErrorKind::AlreadyClosed => {
                write!(f, "Channel already closed.{}", cause)
            }
            CloseErrorKind::Normal => {
                write!(f, "The connection has been closed.{}", cause)
            }
            CloseErrorKind::ClosedRemotely => {
                write!(f, "The connection was closed remotely.{}", cause)
            }
        }
    }
}

impl Recoverable for CloseError {
    fn is_fatal(&self) -> bool {
        true
    }
}

impl From<CloseError> for ConnectionError {
    fn from(e: CloseError) -> Self {
        ConnectionError::Closed(e)
    }
}

impl<T> From<MpscSendError<T>> for ConnectionError {
    fn from(_: MpscSendError<T>) -> Self {
        ConnectionError::Closed(CloseError::new(CloseErrorKind::Unexpected, None))
    }
}

impl From<MpscRecvError> for ConnectionError {
    fn from(_: MpscRecvError) -> Self {
        ConnectionError::Closed(CloseError::new(CloseErrorKind::Unexpected, None))
    }
}
