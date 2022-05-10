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

use bytes::{BufMut, BytesMut};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::Text;
use swim_recon::parser::{AsyncParseError, ParseError};
use tokio_util::codec::Decoder;

use super::WithLenRecognizerDecoder;

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
