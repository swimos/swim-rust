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

pub mod map;
pub mod value;

use std::error::Error;
use std::fmt::{Display, Formatter};
use stm::transaction::TransactionError;

#[cfg(test)]
mod tests;

/// The error type for tasks that apply remote updates to lanes.
#[derive(Debug)]
pub enum UpdateError {
    FailedTransaction(TransactionError),
}

impl Display for UpdateError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateError::FailedTransaction(err) => {
                write!(f, "Failed to apply a transaction to the lane: {}", err)
            }
        }
    }
}

impl Error for UpdateError {}

impl From<TransactionError> for UpdateError {
    fn from(err: TransactionError) -> Self {
        UpdateError::FailedTransaction(err)
    }
}
