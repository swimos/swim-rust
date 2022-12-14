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

use bytes::{Buf, BufMut, BytesMut};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::Text;
use tokio_util::codec::{Decoder, Encoder};

use super::{WithLenRecognizerDecoder, WithLenReconEncoder};

#[test]
fn recognizer_decode_with_len() {
    let string = "test";
    let mut data = BytesMut::new();
    data.put_u64(string.len() as u64);
    data.put_slice(string.as_bytes());

    let mut decoder = WithLenRecognizerDecoder::new(Text::make_recognizer());

    let result = decoder.decode(&mut data);

    match result {
        Ok(Some(text)) => {
            assert_eq!(text, string);
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}

#[test]
fn recognizer_decode_with_len_fails_on_overrun() {
    let string = "\"two words\"";
    let mut data = BytesMut::new();
    data.put_u64(5);
    data.put_slice(string.as_bytes());

    let mut decoder = WithLenRecognizerDecoder::new(Text::make_recognizer());

    let result = decoder.decode(&mut data);

    assert!(result.is_err());
}

#[test]
fn encode_recon_with_length() {
    let mut encoder = WithLenReconEncoder::default();
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(Text::new("hello"), &mut buffer).is_ok());

    assert_eq!(buffer.len(), 13);

    let len = buffer.get_u64();
    assert_eq!(len, 5);
    assert_eq!(buffer.as_ref(), b"hello");
}
