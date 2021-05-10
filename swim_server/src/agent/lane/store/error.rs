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
use store::{StoreError, StoreInfo};
use swim_common::model::time::Timestamp;

/// A lane store error report.
#[derive(Debug)]
pub struct LaneStoreErrorReport {
    /// Details about the store that generated this error report.
    pub(crate) store_info: StoreInfo,
    /// A vector of the store errors and the time at which they were generated.
    pub(crate) errors: Vec<(Timestamp, StoreError)>,
}

impl LaneStoreErrorReport {
    pub fn new(store_info: StoreInfo, errors: Vec<(Timestamp, StoreError)>) -> Self {
        LaneStoreErrorReport { store_info, errors }
    }

    pub fn for_error(store_info: StoreInfo, error: StoreError) -> Self {
        LaneStoreErrorReport {
            store_info,
            errors: vec![(Timestamp::now(), error)],
        }
    }
}

impl Display for LaneStoreErrorReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let LaneStoreErrorReport { store_info, errors } = self;

        writeln!(f, "Lane store error report: ")?;
        writeln!(f, "\t- Delegate store:")?;
        writeln!(f, "\t\tPath: `{}`", store_info.path)?;
        writeln!(f, "\t\tKind: `{}`", store_info.kind)?;
        writeln!(f, "\t- Errors: ")?;

        for (ts, error) in errors.iter() {
            // todo display for store error
            writeln!(f, "\t\t{}: `{:?}`", ts, error)?;
        }

        Ok(())
    }
}

/// A store error handler which aggregates and timestamps any store errors which are provided.
/// Upon reaching `max_errors`, the handler will return an error report.
pub struct StoreErrorHandler {
    /// The maximum number of errors to aggregate before returning a report.
    max_errors: usize,
    /// Details about the store generating the errors.
    store_info: StoreInfo,
    /// A vector of the store errors and the time at which they were generated.
    errors: Vec<(Timestamp, StoreError)>,
}

fn is_operational(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::InitialisationFailure(_) | StoreError::Io(_) | StoreError::Closing
    )
}

impl StoreErrorHandler {
    pub fn new(max_errors: usize, store_info: StoreInfo) -> StoreErrorHandler {
        StoreErrorHandler {
            max_errors,
            store_info,
            errors: Vec::new(),
        }
    }

    pub fn on_error(&mut self, error: StoreError) -> Result<(), LaneStoreErrorReport> {
        let StoreErrorHandler {
            max_errors,
            errors,
            store_info,
        } = self;

        if is_operational(&error) {
            errors.push((Timestamp::now(), error));

            let len = errors.len();
            let errors: Vec<(Timestamp, StoreError)> = errors.drain(0..len).collect();

            Err(LaneStoreErrorReport {
                errors,
                store_info: store_info.clone(),
            })
        } else {
            errors.push((Timestamp::now(), error));

            let len = errors.len();

            if len >= *max_errors {
                let errors: Vec<(Timestamp, StoreError)> = errors.drain(0..len).collect();
                Err(LaneStoreErrorReport {
                    errors,
                    store_info: store_info.to_owned(),
                })
            } else {
                Ok(())
            }
        }
    }
}
