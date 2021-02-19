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

use crate::routing::error::FmtResult;

use crate::routing::{format_cause, ConnectionError};
use std::error::Error;
use std::fmt::{Display, Formatter};
use utilities::errors::Recoverable;

use tokio::sync::mpsc::error::RecvError as MpscRecvError;
use tokio::sync::mpsc::error::SendError as MpscSendError;
use tokio::sync::mpsc::error::TrySendError as MpscTrySendError;

use tokio::sync::oneshot::error::RecvError as OneshotRecvError;
use tokio::sync::oneshot::error::TryRecvError as OneshotTryRecvError;

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

impl<T> From<MpscSendError<T>> for CloseError {
    fn from(_: MpscSendError<T>) -> Self {
        CloseError::new(CloseErrorKind::Unexpected, None)
    }
}

impl From<MpscRecvError> for CloseError {
    fn from(_: MpscRecvError) -> Self {
        CloseError::new(CloseErrorKind::Unexpected, None)
    }
}

impl<T> From<MpscSendError<T>> for ConnectionError {
    fn from(e: MpscSendError<T>) -> Self {
        ConnectionError::Closed(e.into())
    }
}

impl From<MpscRecvError> for ConnectionError {
    fn from(e: MpscRecvError) -> Self {
        ConnectionError::Closed(e.into())
    }
}

impl<T> From<MpscTrySendError<T>> for CloseError {
    fn from(_e: MpscTrySendError<T>) -> Self {
        CloseError::unexpected()
    }
}

impl From<OneshotRecvError> for CloseError {
    fn from(_: OneshotRecvError) -> Self {
        CloseError::new(CloseErrorKind::Unexpected, None)
    }
}

impl From<OneshotTryRecvError> for CloseError {
    fn from(_: OneshotTryRecvError) -> Self {
        CloseError::new(CloseErrorKind::Unexpected, None)
    }
}

impl From<OneshotRecvError> for ConnectionError {
    fn from(e: OneshotRecvError) -> Self {
        ConnectionError::Closed(e.into())
    }
}

impl From<OneshotTryRecvError> for ConnectionError {
    fn from(e: OneshotTryRecvError) -> Self {
        ConnectionError::Closed(e.into())
    }
}

#[test]
fn test_recoverable() {
    assert!(CloseError::new(CloseErrorKind::Closed, None).is_fatal());
    assert!(CloseError::new(CloseErrorKind::Unexpected, None).is_fatal());
    assert!(CloseError::new(CloseErrorKind::InvalidCloseCode, None).is_fatal());
    assert!(CloseError::new(CloseErrorKind::AlreadyClosed, None).is_fatal());
    assert!(CloseError::new(CloseErrorKind::Normal, None).is_fatal());
    assert!(CloseError::new(CloseErrorKind::ClosedRemotely, None).is_fatal());
}

#[test]
fn test_display() {
    assert_eq!(
        CloseError::unexpected().to_string(),
        "Unexpected close event."
    );
    assert_eq!(
        CloseError::already_closed().to_string(),
        "Channel already closed."
    );
    assert_eq!(CloseError::closed().to_string(), "Channel closed.");

    assert_eq!(
        CloseError::new(CloseErrorKind::Closed, None).to_string(),
        "Channel closed."
    );
    assert_eq!(
        CloseError::new(CloseErrorKind::Unexpected, None).to_string(),
        "Unexpected close event."
    );
    assert_eq!(
        CloseError::new(CloseErrorKind::InvalidCloseCode, None).to_string(),
        "Invalid close code."
    );
    assert_eq!(
        CloseError::new(CloseErrorKind::AlreadyClosed, None).to_string(),
        "Channel already closed."
    );
    assert_eq!(
        CloseError::new(CloseErrorKind::Normal, None).to_string(),
        "The connection has been closed."
    );
    assert_eq!(
        CloseError::new(CloseErrorKind::ClosedRemotely, None).to_string(),
        "The connection was closed remotely."
    );

    assert_eq!(
        CloseError::new(CloseErrorKind::Closed, Some("Closed normally.".to_string())).to_string(),
        "Channel closed. Closed normally."
    );
    assert_eq!(
        CloseError::new(
            CloseErrorKind::Unexpected,
            Some("That wasn't supposed to happen.".to_string())
        )
        .to_string(),
        "Unexpected close event. That wasn't supposed to happen."
    );
    assert_eq!(
        CloseError::new(
            CloseErrorKind::InvalidCloseCode,
            Some(format!("Received closed code 0xf"))
        )
        .to_string(),
        "Invalid close code. Received closed code 0xf"
    );
    assert_eq!(
        CloseError::new(
            CloseErrorKind::AlreadyClosed,
            Some("Closed by another process".to_string())
        )
        .to_string(),
        "Channel already closed. Closed by another process"
    );
    assert_eq!(
        CloseError::new(CloseErrorKind::Normal, Some("No error".to_string())).to_string(),
        "The connection has been closed. No error"
    );
    assert_eq!(
        CloseError::new(
            CloseErrorKind::ClosedRemotely,
            Some("Closed by peer".to_string())
        )
        .to_string(),
        "The connection was closed remotely. Closed by peer"
    );
}

#[tokio::test]
async fn test_from() {
    use tokio::sync::mpsc;

    {
        let (tx, rx): (mpsc::Sender<i32>, mpsc::Receiver<i32>) = mpsc::channel(1);
        drop(rx);

        let next = tx.send(1).await;
        let err: CloseError = next.unwrap_err().into();

        assert_eq!(err, CloseError::unexpected());

        let next = tx.try_send(1);
        let err: CloseError = next.unwrap_err().into();

        assert_eq!(err, CloseError::unexpected());
    }
}
