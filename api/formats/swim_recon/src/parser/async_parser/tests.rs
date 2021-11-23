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

use tokio::fs::File;

use crate::parser::async_parser::AsyncParseError;
use crate::parser::Span;
use crate::parser::{ParseError, RecognizerDecoder};
use bytes::{BufMut, BytesMut};
use std::fmt::Debug;
use std::path::PathBuf;
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::Form;
use swim_model::{Attr, Item, Value};
use tokio_util::codec::Decoder;

fn test_data_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("test-data");
    path
}

#[tokio::test]
async fn read_configuration_from_file() {
    let mut path = test_data_path();
    path.push("test.recon");

    let file = File::open(path).await;
    assert!(file.is_ok());
    let result = super::parse_recon_document(file.unwrap(), false).await;
    assert!(result.is_ok());
    let items = result.unwrap();
    let complex = Value::Record(
        vec![Attr::with_value("name", 7u32)],
        vec![Item::of(1u32), Item::of(2u32), Item::of(3u32)],
    );
    assert_eq!(
        items,
        vec![
            Item::slot("first", 3u32),
            Item::slot("second", "hello"),
            Item::ValueItem(complex),
            Item::of(true)
        ]
    );
}

#[tokio::test]
async fn read_invalid_file() {
    let mut path = test_data_path();
    path.push("invalid.recon");

    let file = File::open(path).await;
    assert!(file.is_ok());
    let result = super::parse_recon_document(file.unwrap(), false).await;
    assert!(matches!(result, Err(AsyncParseError::UnconsumedInput)));
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Form)]
struct Example {
    field: i32,
}

fn assert_complete<T: Eq + Debug>(result: Result<Option<T>, AsyncParseError>, expected: T) {
    match result {
        Ok(Some(value)) => assert_eq!(value, expected),
        Ok(None) => panic!("Incomplete."),
        Err(e) => panic!("Failed: {}", e),
    }
}

const COMPLETE: &str = "@Example { field: 2 }";

#[tokio::test]
async fn recognize_decode_complete() {
    let mut decoder = RecognizerDecoder::new(Example::make_recognizer());

    let mut buffer = BytesMut::new();
    buffer.put_slice(COMPLETE.as_bytes());

    let result = decoder.decode(&mut buffer);

    assert!(buffer.is_empty());

    let expected = crate::parser::parse_recognize::<Example>(Span::new(COMPLETE)).unwrap();

    assert_complete(result, expected);
}

const FIRST_PART: &str = "@Example { ";
const SECOND_PART: &str = "field: 2 }";

#[tokio::test]
async fn recognize_decode_two_parts() {
    let mut decoder = RecognizerDecoder::new(Example::make_recognizer());

    let mut buffer = BytesMut::new();

    buffer.put_slice(FIRST_PART.as_bytes());
    let result = decoder.decode(&mut buffer);

    assert_eq!(buffer.as_ref(), b" ");

    assert!(matches!(result, Ok(None)));

    buffer.put_slice(SECOND_PART.as_bytes());
    let result = decoder.decode(&mut buffer);

    let expected = crate::parser::parse_recognize::<Example>(Span::new(COMPLETE)).unwrap();

    assert_complete(result, expected);
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Form)]
struct UnitExample;

const UNIT_RECON: &str = "@UnitExample";

#[tokio::test]
async fn recognize_decode_eof() {
    let mut decoder = RecognizerDecoder::new(UnitExample::make_recognizer());

    let mut buffer = BytesMut::new();

    buffer.put_slice(UNIT_RECON.as_bytes());
    let result = decoder.decode(&mut buffer);

    assert!(matches!(result, Ok(None)));

    let result = decoder.decode_eof(&mut buffer);

    assert_complete(result, UnitExample);
}

#[tokio::test]
async fn recognize_decode_complete_eof() {
    let mut decoder = RecognizerDecoder::new(Example::make_recognizer());

    let mut buffer = BytesMut::new();
    buffer.put_slice(COMPLETE.as_bytes());

    let result = decoder.decode_eof(&mut buffer);

    assert!(buffer.is_empty());

    let expected = crate::parser::parse_recognize::<Example>(Span::new(COMPLETE)).unwrap();

    assert_complete(result, expected);
}

const BAD1: &str = "$Example { field: 2 }";
const BAD2: &str = "@Example $ field: 2 }";
const BAD3A: &str = "@Example ";
const BAD3B: &str = " $ field: 2 }";
const BAD4: &str = "@Record {\n first $ 4 }";
const BAD5A: &str = "@Record {\n";
const BAD5B: &str = " first: 2\n second";
const BAD5C: &str = ": $ }";

#[tokio::test]
async fn simple_error_loc() {
    let mut decoder = RecognizerDecoder::new(Example::make_recognizer());

    let mut buffer = BytesMut::new();
    buffer.put_slice(BAD1.as_bytes());

    let result = decoder.decode(&mut buffer);

    match result {
        Err(AsyncParseError::Parser(ParseError::Syntax {
            offset,
            line,
            column,
            ..
        })) => {
            assert_eq!(offset, 0);
            assert_eq!(line, 1);
            assert_eq!(column, 1);
        }
        Err(e) => panic!("Unexpected error: {}", e),
        _ => panic!("Unexpected success."),
    }
}

#[tokio::test]
async fn offset_error_loc() {
    let mut decoder = RecognizerDecoder::new(Value::make_recognizer());

    let mut buffer = BytesMut::new();
    buffer.put_slice(BAD2.as_bytes());

    let result = decoder.decode(&mut buffer);

    match result {
        Err(AsyncParseError::Parser(ParseError::Syntax {
            offset,
            line,
            column,
            ..
        })) => {
            assert_eq!(offset, 9);
            assert_eq!(line, 1);
            assert_eq!(column, 10);
        }
        Err(e) => panic!("Unexpected error: {}", e),
        _ => panic!("Unexpected success."),
    }
}

#[tokio::test]
async fn split_error_loc() {
    let mut decoder = RecognizerDecoder::new(Value::make_recognizer());

    let mut buffer = BytesMut::new();
    buffer.put_slice(BAD3A.as_bytes());

    let result = decoder.decode(&mut buffer);
    assert!(matches!(result, Ok(None)));

    buffer.put_slice(BAD3B.as_bytes());
    let result = decoder.decode(&mut buffer);

    match result {
        Err(AsyncParseError::Parser(ParseError::Syntax {
            offset,
            line,
            column,
            ..
        })) => {
            assert_eq!(offset, 10);
            assert_eq!(line, 1);
            assert_eq!(column, 11);
        }
        Err(e) => panic!("Unexpected error: {}", e),
        _ => panic!("Unexpected success."),
    }
}

#[tokio::test]
async fn multi_line_error_loc() {
    let mut decoder = RecognizerDecoder::new(Value::make_recognizer());

    let mut buffer = BytesMut::new();
    buffer.put_slice(BAD4.as_bytes());

    let result = decoder.decode(&mut buffer);

    match result {
        Err(AsyncParseError::Parser(ParseError::Syntax {
            offset,
            line,
            column,
            ..
        })) => {
            assert_eq!(offset, 17);
            assert_eq!(line, 2);
            assert_eq!(column, 8);
        }
        Err(e) => panic!("Unexpected error: {}", e),
        _ => panic!("Unexpected success."),
    }
}

#[tokio::test]
async fn split_multi_line_error_loc() {
    let mut decoder = RecognizerDecoder::new(Value::make_recognizer());

    let mut buffer = BytesMut::new();
    buffer.put_slice(BAD5A.as_bytes());

    let result = decoder.decode(&mut buffer);
    assert!(matches!(result, Ok(None)));

    buffer.put_slice(BAD5B.as_bytes());
    assert!(matches!(result, Ok(None)));

    buffer.put_slice(BAD5C.as_bytes());
    let result = decoder.decode(&mut buffer);

    match result {
        Err(AsyncParseError::Parser(ParseError::Syntax {
            offset,
            line,
            column,
            ..
        })) => {
            assert_eq!(offset, 29);
            assert_eq!(line, 3);
            assert_eq!(column, 10);
        }
        Err(e) => panic!("Unexpected error: {}", e),
        _ => panic!("Unexpected success."),
    }
}
