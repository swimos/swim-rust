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

use crate::structural::write::interpreters::msgpack::MsgPackWriteError;
use swim_model::bigint::{BigInt, BigUint};
use std::io::ErrorKind;

#[test]
fn msgpack_write_err_display() {
    let err = MsgPackWriteError::WrongNumberOfItems;
    let string = err.to_string();
    assert_eq!(
        string,
        "The number of items written did not match the number reported."
    );

    let err = MsgPackWriteError::IncorrectRecordKind;
    let string = err.to_string();
    assert_eq!(
        string,
        "The record items written did not match the kind reported."
    );

    let err = MsgPackWriteError::WrongNumberOfAttrs;
    let string = err.to_string();
    assert_eq!(
        string,
        "The number of attributes written did not match the number reported."
    );

    let err = MsgPackWriteError::TooManyItems(100000000000usize);
    let string = err.to_string();
    assert_eq!(
        string,
        "100000000000 items is too many to encode as MessagePack."
    );

    let err = MsgPackWriteError::TooManyAttrs(100000000000usize);
    let string = err.to_string();
    assert_eq!(
        string,
        "100000000000 attributes is too many to encode as MessagePack."
    );

    let err = MsgPackWriteError::BigIntTooLarge(BigInt::from(5));
    let string = err.to_string();
    assert_eq!(
        string,
        "Big integer too large to be written in MessagePack."
    );

    let err = MsgPackWriteError::BigUIntTooLarge(BigUint::from(5u32));
    let string = err.to_string();
    assert_eq!(
        string,
        "Big integer too large to be written in MessagePack."
    );

    let io_err = std::io::Error::from(ErrorKind::InvalidData);
    let io_string = io_err.to_string();
    let err = MsgPackWriteError::IoError(io_err);
    let string = err.to_string();
    assert_eq!(
        string,
        format!("An error ocurred writing the content: {}", io_string)
    );
}

#[test]
fn msgpack_write_err_partial_eq() {
    let refl_errs = vec![
        MsgPackWriteError::WrongNumberOfAttrs,
        MsgPackWriteError::WrongNumberOfItems,
        MsgPackWriteError::IncorrectRecordKind,
        MsgPackWriteError::TooManyAttrs(2),
        MsgPackWriteError::TooManyItems(2),
        MsgPackWriteError::BigIntTooLarge(BigInt::from(3)),
        MsgPackWriteError::BigUIntTooLarge(BigUint::from(3u32)),
    ];

    for (e1, i) in refl_errs.iter().zip(0..) {
        for (e2, j) in refl_errs.iter().zip(0..) {
            if i == j {
                assert_eq!(e1, e2);
            } else {
                assert_ne!(e1, e2);
            }
        }
    }

    assert_ne!(
        MsgPackWriteError::TooManyAttrs(1),
        MsgPackWriteError::TooManyAttrs(2)
    );
    assert_ne!(
        MsgPackWriteError::TooManyItems(1),
        MsgPackWriteError::TooManyItems(2)
    );
    assert_ne!(
        MsgPackWriteError::BigIntTooLarge(BigInt::from(3)),
        MsgPackWriteError::BigIntTooLarge(BigInt::from(4))
    );
    assert_ne!(
        MsgPackWriteError::BigUIntTooLarge(BigUint::from(3u32)),
        MsgPackWriteError::BigUIntTooLarge(BigUint::from(4u32))
    );

    let io_err = std::io::Error::from(ErrorKind::InvalidData);
    let err = MsgPackWriteError::IoError(io_err);
    assert_ne!(err, err);
}
