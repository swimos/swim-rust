// Copyright 2015-2023 Swim Inc.
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

use std::{convert::Infallible, fmt::Display, num::NonZeroUsize};

use swim_utilities::format::comma_sep;
use thiserror::Error;
use tracing::warn;

#[derive(Debug, PartialEq, Eq)]
pub enum BadFrameResponse<E> {
    /// Instruction to ignore the bad envelope.
    Ignore,
    /// Instruction to abort the downlink runtime task with the specified error.
    Abort(E),
}

/// For some downlink types (particularly map downlinks) the runtime needs to inspect
/// the content of event messages. As this inspection can fail, the downlink must know
/// what to do when it encounters invalid event bodies. A strategy implementing this
/// trait describes whether the runtime task should continue (ignoring the bad envelope)
/// or abort.
pub trait BadFrameStrategy<E> {
    /// The type of error reports produced on an abort.
    type Report: std::error::Error;

    /// Determine whether to continue or abort.
    fn failed_with(&mut self, error: E) -> BadFrameResponse<Self::Report>;
}

/// Dummy strategy for downlink types that cannot fail in this way.
#[derive(Debug, Default)]
pub struct InfallibleStrategy;

impl BadFrameStrategy<Infallible> for InfallibleStrategy {
    type Report = Infallible;

    fn failed_with(&mut self, error: Infallible) -> BadFrameResponse<Infallible> {
        match error {}
    }
}

/// A strategy that always aborts with the error it is given.
#[derive(Debug, Default)]
pub struct AlwaysAbortStrategy;

impl<E: std::error::Error> BadFrameStrategy<E> for AlwaysAbortStrategy {
    type Report = E;

    fn failed_with(&mut self, error: E) -> BadFrameResponse<E> {
        BadFrameResponse::Abort(error)
    }
}

#[derive(Debug)]
pub struct ErrReport {
    message: String,
}

impl Display for ErrReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ErrReport {}

impl ErrReport {
    fn new<E: std::error::Error>(err: E) -> Self {
        ErrReport {
            message: err.to_string(),
        }
    }
}

pub struct ReportStrategy<S> {
    inner: S,
}

impl<S> ReportStrategy<S> {
    pub fn new(inner: S) -> Self {
        ReportStrategy { inner }
    }
}

impl<S> ReportStrategy<S> {
    pub fn boxed<E>(self) -> BoxedReportStrategy<'static, E>
    where
        S: BadFrameStrategy<E> + Send + 'static,
    {
        Box::new(self)
    }
}

impl<S, E> BadFrameStrategy<E> for ReportStrategy<S>
where
    S: BadFrameStrategy<E>,
{
    type Report = ErrReport;

    fn failed_with(&mut self, error: E) -> BadFrameResponse<Self::Report> {
        match self.inner.failed_with(error) {
            BadFrameResponse::Ignore => BadFrameResponse::Ignore,
            BadFrameResponse::Abort(err) => BadFrameResponse::Abort(ErrReport::new(err)),
        }
    }
}

pub type BoxedReportStrategy<'a, E> = Box<dyn BadFrameStrategy<E, Report = ErrReport> + Send + 'a>;

impl<'a, E> BadFrameStrategy<E> for BoxedReportStrategy<'a, E> {
    type Report = ErrReport;

    fn failed_with(&mut self, error: E) -> BadFrameResponse<Self::Report> {
        (**self).failed_with(error)
    }
}

/// A strategy that ignores all bad envelopes.
#[derive(Debug, Default)]
pub struct AlwaysIgnoreStrategy;

impl<E> BadFrameStrategy<E> for AlwaysIgnoreStrategy {
    type Report = Infallible;

    fn failed_with(&mut self, _: E) -> BadFrameResponse<Infallible> {
        BadFrameResponse::Ignore
    }
}

/// A strategy that will ignore several bad envelopes, collecting the errors, and then
/// abort.
pub struct CountStrategy<E> {
    max: usize,
    count: usize,
    errors: Vec<E>,
}

impl<E> CountStrategy<E> {
    pub fn new(max: NonZeroUsize) -> Self {
        CountStrategy {
            max: max.get(),
            count: 0,
            errors: vec![],
        }
    }
}

/// A collection of errors, accumulated by a [`CountStrategy`].
#[derive(Debug, Error, PartialEq, Eq)]
#[error("Too many bad frames: {errors}")]
pub struct ErrorLog<E> {
    errors: Errors<E>,
}

impl<E> AsRef<[E]> for ErrorLog<E> {
    fn as_ref(&self) -> &[E] {
        &self.errors.0
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Errors<E>(Vec<E>);

impl<E: Display> Display for Errors<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", comma_sep(&self.0))
    }
}

impl<E: std::error::Error> BadFrameStrategy<E> for CountStrategy<E> {
    type Report = ErrorLog<E>;

    fn failed_with(&mut self, error: E) -> BadFrameResponse<Self::Report> {
        let CountStrategy { max, count, errors } = self;
        *count += 1;
        if *count == *max {
            errors.push(error);
            *count = 0;
            BadFrameResponse::Abort(ErrorLog {
                errors: Errors(std::mem::take(errors)),
            })
        } else {
            warn!(
                "Received {n} of a maximum of {m} invalid map messages: {e}",
                n = *count,
                m = *max,
                e = error
            );
            errors.push(error);
            BadFrameResponse::Ignore
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use swim_utilities::non_zero_usize;
    use thiserror::Error;

    #[derive(Debug, Error, PartialEq, Eq)]
    #[error("{0}")]
    struct TestError(String);

    #[test]
    fn always_abort() {
        let mut handler = AlwaysAbortStrategy;
        let response = handler.failed_with(TestError("failed".to_string()));
        assert_eq!(
            response,
            BadFrameResponse::Abort(TestError("failed".to_string()))
        );
    }

    #[test]
    fn always_ignore() {
        let mut handler = AlwaysIgnoreStrategy;
        let response = handler.failed_with(TestError("failed".to_string()));
        assert_eq!(response, BadFrameResponse::Ignore);
    }

    const MAX: NonZeroUsize = non_zero_usize!(3);

    #[test]
    fn count_errors() {
        let mut handler = CountStrategy::new(MAX);

        let first = handler.failed_with(TestError("first".to_string()));
        assert_eq!(BadFrameResponse::Ignore, first);

        let second = handler.failed_with(TestError("second".to_string()));
        assert_eq!(BadFrameResponse::Ignore, second);

        let third = handler.failed_with(TestError("third".to_string()));
        match third {
            BadFrameResponse::Abort(report) => {
                assert_eq!(
                    report.as_ref(),
                    &[
                        TestError("first".to_string()),
                        TestError("second".to_string()),
                        TestError("third".to_string())
                    ]
                )
            }
            BadFrameResponse::Ignore => {
                panic!("Error ignored.");
            }
        }
    }
}
