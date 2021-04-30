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

use crate::form::structural::read::msgpack::MsgPackReadError;
use crate::form::structural::read::ReadError;
use rmp::Marker;

const INVALID_UTF8: [u8; 2] = [0xc3, 0x28];

#[test]
fn msgpack_read_err_display() {
    let err = MsgPackReadError::Incomplete;
    let string = err.to_string();
    assert_eq!(string, "The input ended part way through a record.");

    let err = MsgPackReadError::EmptyBigInt;
    let string = err.to_string();
    assert_eq!(string, "A big integer consisted of 0 bytes.");

    let err = MsgPackReadError::UnknownExtType(3);
    let string = err.to_string();
    assert_eq!(string, "3 is not a regognized extension code.");

    let err = MsgPackReadError::InvalidMarker(Marker::Null);
    let string = err.to_string();
    assert_eq!(string, "Unexpected message pack marker: Null");

    let utf_err = std::str::from_utf8(&INVALID_UTF8).err().unwrap();

    let err = MsgPackReadError::StringDecode(utf_err);
    let string = err.to_string();
    assert_eq!(string, "A string value contained invalid UTF8.");

    let err = MsgPackReadError::Structure(ReadError::InconsistentState);
    let string = err.to_string();
    assert_eq!(
        string,
        "Invalid structure: The deserialization state became corrupted."
    );
}
