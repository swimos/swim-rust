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

pub mod action;
pub mod command;
pub mod map;
pub mod value;

use crate::routing::RoutingAddr;
use futures::future::{ready, BoxFuture, Either, Ready};
use futures::stream::{iter, Iter};
use futures::Stream;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::time::Duration;
use stm::transaction::{RetryManager, TransactionError};
use swim_form::structural::read::ReadError;
use swim_runtime::time::delay::{delay_for, Delay};
use swim_utilities::future::retryable::RetryStrategy;
use swim_utilities::future::{
    SwimFutureExt, SwimStreamExt, Transform, TransformedFuture, TransformedStreamFut,
};

#[cfg(test)]
mod tests;

/// The error type for tasks that apply remote updates to lanes.
#[derive(Debug)]
pub enum UpdateError {
    FailedTransaction(TransactionError),
    BadEnvelopeBody(ReadError),
    FeedbackChannelDropped,
    OperationNotSupported,
}

impl Display for UpdateError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateError::FailedTransaction(err) => {
                write!(f, "Failed to apply a transaction to the lane: {}", err)
            }
            UpdateError::BadEnvelopeBody(err) => {
                write!(f, "The body of an incoming envelope was invalid: {}", err)
            }
            UpdateError::FeedbackChannelDropped => {
                write!(f, "Action lane feedback channel dropped.")
            }
            UpdateError::OperationNotSupported => {
                write!(f, "The requested operation is not supported by this lane")
            }
        }
    }
}

impl Error for UpdateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            UpdateError::FailedTransaction(err) => Some(err),
            UpdateError::BadEnvelopeBody(err) => Some(err),
            _ => None,
        }
    }
}

impl From<TransactionError> for UpdateError {
    fn from(err: TransactionError) -> Self {
        UpdateError::FailedTransaction(err)
    }
}

impl From<ReadError> for UpdateError {
    fn from(err: ReadError) -> Self {
        UpdateError::BadEnvelopeBody(err)
    }
}

pub trait LaneUpdate {
    type Msg: Send + Debug + 'static;

    fn run_update<Messages, Err>(
        self,
        messages: Messages,
    ) -> BoxFuture<'static, Result<(), UpdateError>>
    where
        Messages: Stream<Item = Result<(RoutingAddr, Self::Msg), Err>> + Send + 'static,
        Err: Send + Debug,
        UpdateError: From<Err>;
}

/// Wraps a [`RetryStrategy`] as an STM [`RetryManager`].
#[derive(Clone, Copy, Debug, Eq)]
pub struct StmRetryStrategy {
    template: RetryStrategy,
    retries: RetryStrategy,
}

impl PartialEq for StmRetryStrategy {
    fn eq(&self, other: &Self) -> bool {
        self.template.eq(other.template)
    }
}

impl StmRetryStrategy {
    pub fn new(strategy: RetryStrategy) -> Self {
        StmRetryStrategy {
            template: strategy,
            retries: strategy,
        }
    }
}

pub struct ToTrue;
impl Transform<()> for ToTrue {
    type Out = bool;

    fn transform(&self, _: ()) -> Self::Out {
        true
    }
}

pub struct WaitForDelay;

impl Transform<Option<Duration>> for WaitForDelay {
    type Out = Either<Ready<()>, Delay>;

    fn transform(&self, dur: Option<Duration>) -> Self::Out {
        if let Some(dur) = dur {
            Either::Right(delay_for(dur))
        } else {
            Either::Left(ready(()))
        }
    }
}

impl RetryManager for StmRetryStrategy {
    type ContentionManager =
        TransformedStreamFut<Iter<RetryStrategy>, WaitForDelay, Either<Ready<()>, Delay>>;
    type RetryFut = Either<Ready<bool>, TransformedFuture<Delay, ToTrue>>;

    fn contention_manager(&self) -> Self::ContentionManager {
        iter(self.template).transform_fut(WaitForDelay)
    }

    fn retry(&mut self) -> Self::RetryFut {
        match self.retries.next() {
            Some(Some(dur)) => {
                Either::Right(swim_runtime::time::delay::delay_for(dur).transform(ToTrue))
            }
            Some(_) => Either::Left(ready(true)),
            _ => Either::Left(ready(false)),
        }
    }
}
