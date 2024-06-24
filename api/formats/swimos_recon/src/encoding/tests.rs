// Copyright 2015-2023 Swim Inc.
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

use bytes::{BufMut, BytesMut};
use swimos_form::read::RecognizerReadable;
use swimos_model::{Attr, Item, Value};
use tokio_util::codec::{Decoder, Encoder};

use crate::{WithLenRecognizerDecoder, WithLenReconEncoder};

#[test]
fn with_len_recon_encoding() {
    let mut encoder = WithLenReconEncoder;
    let mut decoder = WithLenRecognizerDecoder::new(String::make_recognizer());

    let mut buffer = BytesMut::new();

    assert!(encoder.encode("hello".to_string(), &mut buffer).is_ok());

    let restored = decoder
        .decode(&mut buffer)
        .expect("Decoding failed.")
        .expect("Incomplete.");
    assert_eq!(restored, "hello");
}

fn put_str(s: &str) -> BytesMut {
    let mut buffer = BytesMut::new();
    buffer.put_u64(s.len() as u64);
    buffer.put(s.as_bytes());
    buffer
}

fn put_strs(s: &str, t: &str) -> BytesMut {
    let mut buffer = BytesMut::new();
    buffer.put_u64(s.len() as u64);
    buffer.put(s.as_bytes());
    buffer.put_u64(t.len() as u64);
    buffer.put(t.as_bytes());
    buffer
}

#[test]
fn decode_with_padding() {
    let mut buffer = put_str("  @attr { a: 1}   ");
    let mut decoder = WithLenRecognizerDecoder::new(Value::make_recognizer());
    let restored = decoder
        .decode(&mut buffer)
        .expect("Decoding failed.")
        .expect("Incomplete.");
    let expected = Value::Record(vec![Attr::of("attr")], vec![Item::slot("a", 1)]);
    assert_eq!(restored, expected);
    assert!(buffer.is_empty());
}

#[test]
fn decode_twice() {
    let mut buffer = put_strs("  @attr { a: 1}   ", "{b:2, c:3}");
    let mut decoder = WithLenRecognizerDecoder::new(Value::make_recognizer());

    let restored1 = decoder
        .decode(&mut buffer)
        .expect("Decoding failed.")
        .expect("Incomplete.");
    let expected1 = Value::Record(vec![Attr::of("attr")], vec![Item::slot("a", 1)]);
    assert_eq!(restored1, expected1);

    let restored2 = decoder
        .decode(&mut buffer)
        .expect("Decoding failed.")
        .expect("Incomplete.");
    let expected2 = Value::Record(vec![], vec![Item::slot("b", 2), Item::slot("c", 3)]);
    assert_eq!(restored2, expected2);

    assert!(buffer.is_empty());
}
