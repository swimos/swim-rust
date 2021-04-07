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

use crate::agent::lane::channels::update::UpdateError;
use crate::engines::mem::transaction::TransactionError;
use swim_common::form::FormErr;

#[test]
fn update_error_display() {
    let err = UpdateError::FailedTransaction(TransactionError::InvalidRetry);

    let string = format!("{}", err);

    assert_eq!(string, "Failed to apply a transaction to the lane: Retry on transaction with no data dependencies.");

    let err2 = UpdateError::BadEnvelopeBody(FormErr::Malformatted);
    let string2 = format!("{}", err2);

    assert_eq!(
        string2,
        "The body of an incoming envelope was invalid: Malformatted"
    );

    let err3 = UpdateError::FeedbackChannelDropped;
    let string3 = format!("{}", err3);
    assert_eq!(string3, "Action lane feedback channel dropped.");
}

#[test]
fn update_error_from_transaction_error() {
    let err: UpdateError = TransactionError::InvalidRetry.into();
    assert!(matches!(
        err,
        UpdateError::FailedTransaction(TransactionError::InvalidRetry)
    ))
}

#[test]
fn update_error_from_form_error() {
    let err: UpdateError = FormErr::Malformatted.into();
    assert!(matches!(
        err,
        UpdateError::BadEnvelopeBody(FormErr::Malformatted)
    ))
}
