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

use crate::routing::RoutingAddr;
use std::error::Error as StdError;
use std::sync::Arc;
use swim_utilities::errors::Recoverable;
use swim_utilities::routing::uri::RelativeUri;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;

pub type BoxSendErr = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug, Clone, Error)]
pub enum ResolutionError {
    #[error("Router dropped")]
    Dropped,
    #[error("Malformatted request: `{0}`")]
    Malformatted(Arc<BoxSendErr>),
    #[error("Failed to resolve agent URI: `{0}`")]
    Agent(RelativeUri),
    #[error("Failed to resolve host: `{0}`")]
    Host(String),
    #[error("Failed to resolve routing addr: `{0}`")]
    Addr(RoutingAddr),
}

impl Recoverable for ResolutionError {
    fn is_fatal(&self) -> bool {
        true
    }
}

impl PartialEq<ResolutionError> for ResolutionError {
    fn eq(&self, other: &ResolutionError) -> bool {
        match (self, other) {
            (ResolutionError::Dropped, ResolutionError::Dropped) => true,
            (ResolutionError::Malformatted(_), ResolutionError::Malformatted(_)) => false,
            (ResolutionError::Agent(l), ResolutionError::Agent(r)) => l.eq(r),
            (ResolutionError::Host(l), ResolutionError::Host(r)) => l.eq(r),
            (ResolutionError::Addr(l), ResolutionError::Addr(r)) => l.eq(r),
            _ => false,
        }
    }
}

impl ResolutionError {
    pub fn malformatted<E>(e: E) -> ResolutionError
    where
        E: StdError + Send + Sync + 'static,
    {
        ResolutionError::Malformatted(Arc::new(Box::new(e)))
    }
}

impl From<RecvError> for ResolutionError {
    fn from(_: RecvError) -> Self {
        ResolutionError::Dropped
    }
}

impl<P> From<SendError<P>> for ResolutionError {
    fn from(_: SendError<P>) -> Self {
        ResolutionError::Dropped
    }
}
