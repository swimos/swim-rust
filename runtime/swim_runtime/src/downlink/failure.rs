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

use std::{convert::Infallible, fmt::Display, num::NonZeroUsize};

use swim_utilities::format::comma_sep;
use thiserror::Error;
use tracing::warn;

pub enum BadFrameResponse<E> {
    Ignore,
    Abort(E),
}

pub trait BadFrameStrategy<E> {
    type Report: std::error::Error;

    fn failed_with(&mut self, error: E) -> BadFrameResponse<Self::Report>;
}

#[derive(Debug, Default)]
pub struct InfallibleStrategy;

impl BadFrameStrategy<Infallible> for InfallibleStrategy {
    type Report = Infallible;

    fn failed_with(&mut self, error: Infallible) -> BadFrameResponse<Infallible> {
        match error {}
    }
}

#[derive(Debug, Default)]
pub struct AlwaysAbortStrategy;

impl<E: std::error::Error> BadFrameStrategy<E> for AlwaysAbortStrategy {
    type Report = E;

    fn failed_with(&mut self, error: E) -> BadFrameResponse<E> {
        BadFrameResponse::Abort(error)
    }
}

#[derive(Debug, Default)]
pub struct AlwaysIgnoreStrategy;

impl<E> BadFrameStrategy<E> for AlwaysIgnoreStrategy {
    type Report = Infallible;

    fn failed_with(&mut self, _: E) -> BadFrameResponse<Infallible> {
        BadFrameResponse::Ignore
    }
}

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

#[derive(Debug, Error)]
#[error("Too many bad frames: {errors}")]
pub struct ErrorLog<E> {
    errors: Errors<E>,
}

#[derive(Debug)]
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
