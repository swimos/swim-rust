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

use std::any::Any;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use swim_api::error::DownlinkTaskError;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinError;

#[derive(Debug)]
pub struct TimeoutElapsed(Duration);

impl Display for TimeoutElapsed {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Timeout elapsed({})", self.0.as_secs())
    }
}

impl Error for TimeoutElapsed {}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum DownlinkErrorKind {
    Connection,
    Unresolvable,
    WebsocketNegotiationFailed,
    RemoteStopped,
    Timeout,
    Terminated,
    /// Error propagated from user-code, such as a Java exception cause through the FFI
    User,
}

#[derive(Debug)]
pub struct DownlinkRuntimeError {
    kind: DownlinkErrorKind,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl DownlinkRuntimeError {
    pub fn new(kind: DownlinkErrorKind) -> DownlinkRuntimeError {
        DownlinkRuntimeError { kind, source: None }
    }

    pub fn with_cause<E>(kind: DownlinkErrorKind, cause: E) -> DownlinkRuntimeError
    where
        E: Error + Send + Sync + 'static,
    {
        DownlinkRuntimeError {
            kind,
            source: Some(Box::new(cause)),
        }
    }

    pub fn timed_out(period: Duration) -> DownlinkRuntimeError {
        DownlinkRuntimeError {
            kind: DownlinkErrorKind::Timeout,
            source: Some(Box::new(TimeoutElapsed(period))),
        }
    }

    pub fn shared(self) -> Arc<DownlinkRuntimeError> {
        Arc::new(self)
    }

    pub fn kind(&self) -> DownlinkErrorKind {
        self.kind
    }

    pub fn is(&self, _kind: DownlinkErrorKind) -> bool {
        matches!(&self.kind, _kind)
    }

    pub fn downcast_ref<T: Any + Error>(&self) -> Option<&T> {
        self.source.as_deref().map(|e| e.downcast_ref()).flatten()
    }

    pub fn map_cause<T, F, R>(&self, f: F) -> Option<R>
    where
        T: Any + Error,
        F: Fn(&T) -> R,
    {
        self.downcast_ref::<T>().map(f)
    }
}

impl Display for DownlinkRuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for DownlinkRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|e| e as &dyn Error)
    }
}

impl<T> From<mpsc::error::TrySendError<T>> for DownlinkRuntimeError {
    fn from(_: mpsc::error::TrySendError<T>) -> Self {
        DownlinkRuntimeError::new(DownlinkErrorKind::Terminated)
    }
}

impl From<watch::error::RecvError> for DownlinkRuntimeError {
    fn from(_: watch::error::RecvError) -> Self {
        DownlinkRuntimeError::new(DownlinkErrorKind::Terminated)
    }
}

impl<T> From<watch::error::SendError<T>> for DownlinkRuntimeError {
    fn from(_: watch::error::SendError<T>) -> Self {
        DownlinkRuntimeError::new(DownlinkErrorKind::Terminated)
    }
}

impl<T> From<mpsc::error::SendError<T>> for DownlinkRuntimeError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        DownlinkRuntimeError::new(DownlinkErrorKind::Terminated)
    }
}

impl From<oneshot::error::RecvError> for DownlinkRuntimeError {
    fn from(_: oneshot::error::RecvError) -> Self {
        DownlinkRuntimeError::new(DownlinkErrorKind::Terminated)
    }
}

impl From<DownlinkTaskError> for DownlinkRuntimeError {
    fn from(_e: DownlinkTaskError) -> Self {
        unimplemented!()
    }
}

impl From<JoinError> for DownlinkRuntimeError {
    fn from(e: JoinError) -> Self {
        DownlinkRuntimeError::with_cause(DownlinkErrorKind::Terminated, e)
    }
}
