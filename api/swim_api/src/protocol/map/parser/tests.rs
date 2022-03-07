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

use bytes::BytesMut;
use std::fmt::Write;

use crate::protocol::map::{MapMessage, MapOperation};

#[test]
fn peel_clear_header() {
    let mut buffer = BytesMut::new();
    write!(&mut buffer, "@clear").unwrap();

    let bytes = buffer.freeze();

    let result = super::extract_header(&bytes);
    assert!(result.is_ok());

    let message = result.unwrap();

    assert!(matches!(
        message,
        MapMessage::Operation(MapOperation::Clear)
    ));
}

#[test]
fn peel_take_header() {
    let mut buffer = BytesMut::new();
    write!(&mut buffer, "@take(7)").unwrap();

    let bytes = buffer.freeze();

    let result = super::extract_header(&bytes);
    assert!(result.is_ok());

    let message = result.unwrap();

    assert!(matches!(message, MapMessage::Take(7)));
}

#[test]
fn peel_drop_header() {
    let mut buffer = BytesMut::new();
    write!(&mut buffer, "@drop(123)").unwrap();

    let bytes = buffer.freeze();

    let result = super::extract_header(&bytes);
    assert!(result.is_ok());

    let message = result.unwrap();

    assert!(matches!(message, MapMessage::Drop(123)));
}

#[test]
fn peel_remove_header() {
    let mut buffer = BytesMut::new();
    write!(&mut buffer, "@remove(key: 567)").unwrap();

    let bytes = buffer.freeze();

    let result = super::extract_header(&bytes);

    match result {
        Ok(MapMessage::Operation(MapOperation::Remove { key })) => {
            assert_eq!(key.as_ref(), b"567");
        }
        ow => {
            panic!("Unexpected result: {:?}", ow);
        }
    }
}

#[test]
fn peel_update_header() {
    let mut buffer = BytesMut::new();
    let body = "@update(key: \"my key\")@inner { a: 1, b: 2}";
    buffer.write_str(body).unwrap();

    let bytes = buffer.freeze();

    let result = super::extract_header(&bytes);

    match result {
        Ok(MapMessage::Operation(MapOperation::Update { key, value })) => {
            assert_eq!(key.as_ref(), b"\"my key\"");
            assert_eq!(value.as_ref(), b"@inner { a: 1, b: 2}");
        }
        ow => {
            panic!("Unexpected result: {:?}", ow);
        }
    }
}
