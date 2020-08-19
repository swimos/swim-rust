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

use crate::agent::lane::channels::update::UpdateError;
use stm::transaction::TransactionError;
use swim_form::FormDeserializeErr;

#[test]
fn update_error_display() {
    let err = UpdateError::FailedTransaction(TransactionError::InvalidRetry);

    let string = format!("{}", err);

    assert_eq!(string, "Failed to apply a transaction to the lane: Retry on transaction with no data dependencies.");

    let err2 = UpdateError::BadEnvelopeBody(FormDeserializeErr::Malformatted);
    let string2 = format!("{}", err2);

    assert_eq!(
        string2,
        "The body of an incoming envelope was invalid: Malformatted"
    );
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
    let err: UpdateError = FormDeserializeErr::Malformatted.into();
    assert!(matches!(
        err,
        UpdateError::BadEnvelopeBody(FormDeserializeErr::Malformatted)
    ))
}
