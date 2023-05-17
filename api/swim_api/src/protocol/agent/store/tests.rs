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
use std::{fmt::Write, mem::size_of};
use swim_form::{structural::read::recognizer::RecognizerReadable, Form};
use swim_recon::printer::print_recon_compact;
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::{
    agent::{
        StoreInitMessage, StoreInitMessageDecoder, StoreInitMessageEncoder, StoreInitialized,
        StoreInitializedCodec, StoreResponse, StoreResponseEncoder, TAG_LEN,
    },
    WithLenRecognizerDecoder, WithLengthBytesCodec,
};

use super::StoreResponseDecoder;

const LEN_SIZE: usize = size_of::<u64>();

#[test]
fn encode_command_store_request() {
    let mut encoder = StoreInitMessageEncoder::value();
    let mut buffer = BytesMut::new();
    let content = b"body";
    let message = StoreInitMessage::Command(content);
    assert!(encoder.encode(message, &mut buffer).is_ok());

    assert_eq!(buffer.remaining(), TAG_LEN + LEN_SIZE + content.len());
    assert_eq!(buffer.get_u8(), super::COMMAND);
    assert_eq!(buffer.get_u64(), content.len() as u64);
    assert_eq!(buffer.as_ref(), content);
}

#[test]
fn encode_init_store_request() {
    let mut encoder = StoreInitMessageEncoder::value();
    let mut buffer = BytesMut::new();
    let message: StoreInitMessage<&[u8]> = StoreInitMessage::InitComplete;
    assert!(encoder.encode(message, &mut buffer).is_ok());

    assert_eq!(buffer.remaining(), TAG_LEN);
    assert_eq!(buffer.get_u8(), super::INIT_DONE);
}

#[derive(Debug, Form, Clone, Copy, PartialEq, Eq)]
#[form_root(::swim_form)]
struct Example {
    a: i32,
    b: i32,
}

fn round_trip_message(request: StoreInitMessage<Example>) {
    let with_bytes = match &request {
        StoreInitMessage::Command(value) => {
            let mut buffer = BytesMut::new();
            assert!(write!(buffer, "{}", print_recon_compact(value)).is_ok());
            StoreInitMessage::Command(buffer.freeze())
        }
        StoreInitMessage::InitComplete => StoreInitMessage::InitComplete,
    };

    let mut encoder = StoreInitMessageEncoder::value();
    let mut buffer = BytesMut::new();
    assert!(encoder.encode(with_bytes, &mut buffer).is_ok());

    let mut decoder =
        StoreInitMessageDecoder::new(WithLenRecognizerDecoder::new(Example::make_recognizer()));
    match decoder.decode(&mut buffer) {
        Ok(Some(restored)) => {
            assert_eq!(restored, request);
        }
        Ok(_) => {
            panic!("Decoding incomplete.");
        }
        Err(e) => {
            panic!("Decoding failed: {}", e);
        }
    }
    assert!(buffer.is_empty());
}

#[test]
fn decode_command_store_message() {
    round_trip_message(StoreInitMessage::Command(Example { a: 6, b: -56 }));
}

#[test]
fn decode_init_complete_store_message() {
    round_trip_message(StoreInitMessage::InitComplete);
}

#[test]
fn encode_store_initialized() {
    let mut encoder = StoreInitializedCodec;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(StoreInitialized, &mut buffer).is_ok());

    assert_eq!(buffer.remaining(), TAG_LEN);
    assert_eq!(buffer.get_u8(), super::INITIALIZED);
}

#[test]
fn decode_store_initialized() {
    let mut decoder = StoreInitializedCodec;
    let mut buffer = BytesMut::new();
    buffer.reserve(TAG_LEN);
    buffer.put_u8(super::INITIALIZED);
    decoder
        .decode(&mut buffer)
        .expect("Decode failed.")
        .expect("Decode did not complete.");
    assert!(buffer.is_empty());
}

#[test]
fn encode_store_response() {
    let mut encoder = StoreResponseEncoder::new(WithLengthBytesCodec::default());
    let mut buffer = BytesMut::new();
    let content = b"body";
    let response = StoreResponse::new(content);

    assert!(encoder.encode(response, &mut buffer).is_ok());

    assert_eq!(buffer.remaining(), TAG_LEN + LEN_SIZE + content.len());
    assert_eq!(buffer.get_u8(), super::EVENT);
    assert_eq!(buffer.get_u64(), content.len() as u64);
    assert_eq!(buffer.as_ref(), content);
}

#[test]
fn decode_store_response() {
    let mut decoder = StoreResponseDecoder::new(WithLengthBytesCodec::default());
    let mut buffer = BytesMut::new();
    let content = b"body";

    buffer.reserve(TAG_LEN + LEN_SIZE + content.len());
    buffer.put_u8(super::EVENT);
    buffer.put_u64(content.len() as u64);
    buffer.put(content.as_slice());

    let StoreResponse { message } = decoder
        .decode(&mut buffer)
        .expect("Decode failed.")
        .expect("Decode incomplete.");
    assert_eq!(message.as_ref(), content);
    assert!(buffer.is_empty());
}
