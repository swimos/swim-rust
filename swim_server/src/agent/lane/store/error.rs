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

use std::fmt::{Display, Formatter};
use swim_common::model::time::Timestamp;
use swim_store::StoreError;

#[derive(Debug)]
pub struct StoreTaskError {
    pub timestamp: Timestamp,
    pub error: StoreError,
}

/// A lane store error report.
#[derive(Debug)]
pub struct LaneStoreErrorReport {
    /// A vector of the store errors and the time at which they were generated.
    pub(crate) errors: Vec<StoreTaskError>,
}

impl LaneStoreErrorReport {
    pub fn new(errors: Vec<StoreTaskError>) -> Self {
        LaneStoreErrorReport { errors }
    }

    pub fn for_error(error: StoreTaskError) -> LaneStoreErrorReport {
        LaneStoreErrorReport {
            errors: vec![error],
        }
    }
}

impl Display for LaneStoreErrorReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let LaneStoreErrorReport { errors } = self;

        writeln!(f, "Lane store error report: ")?;

        writeln!(f, "\t- Errors: ")?;

        for e in errors.iter() {
            let StoreTaskError { timestamp, error } = e;
            // todo display for store error
            writeln!(f, "\t\t{}: `{:?}`", timestamp, error)?;
        }

        Ok(())
    }
}

/// A store error handler which aggregates and timestamps any store errors which are provided.
/// Upon reaching `max_errors`, the handler will return an error report.
pub struct StoreErrorHandler {
    /// The maximum number of errors to aggregate before returning a report.
    max_errors: usize,
    /// A vector of the store errors and the time at which they were generated.
    errors: Vec<StoreTaskError>,
}

fn is_operational(task_error: &StoreTaskError) -> bool {
    matches!(
        task_error.error,
        StoreError::InitialisationFailure(_) | StoreError::Io(_) | StoreError::Closing
    )
}

impl StoreErrorHandler {
    pub fn new(max_errors: usize) -> StoreErrorHandler {
        StoreErrorHandler {
            max_errors,
            errors: Vec::new(),
        }
    }

    pub fn on_error(&mut self, error: StoreTaskError) -> Result<(), LaneStoreErrorReport> {
        let StoreErrorHandler { max_errors, errors } = self;
        let is_operational_error = is_operational(&error);
        errors.push(error);

        let len = errors.len();

        if is_operational_error || len >= *max_errors {
            let errors: Vec<StoreTaskError> = errors.drain(0..len).collect();
            Err(LaneStoreErrorReport { errors })
        } else {
            Ok(())
        }
    }
}
