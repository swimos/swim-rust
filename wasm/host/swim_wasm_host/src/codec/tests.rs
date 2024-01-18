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

use std::io::ErrorKind;

use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::Decoder;

use super::{LaneResponseDecoder, LaneResponseDecoderState, LaneResponseElement};

#[test]
fn empty() {
    let mut buf = BytesMut::new();
    let mut decoder = LaneResponseDecoder::default();
    assert_eq!(decoder.decode(&mut buf).unwrap(), None);
}

#[test]
fn invalid_header() {
    let mut buf = BytesMut::from_iter(&[2]);
    let mut decoder = LaneResponseDecoder::default();
    assert_eq!(
        decoder.decode(&mut buf).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn incomplete() {
    let mut buf = BytesMut::new();
    buf.put_i8(1);
    buf.put_i32(13);
    buf.put_i32(8);
    buf.extend_from_slice(&[0, 1]);

    let mut decoder = LaneResponseDecoder::default();
    assert_eq!(
        decoder.decode(&mut buf).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn round_trip_cont() {
    let mut buf = BytesMut::new();
    buf.put_i8(1);
    buf.put_i32(13);
    buf.put_i32(8);
    buf.extend_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7]);

    let mut decoder = LaneResponseDecoder::default();
    assert_eq!(
        decoder.decode(&mut buf).unwrap(),
        Some(LaneResponseElement::Response {
            lane_id: 13,
            data: Bytes::from_static(&[0, 1, 2, 3, 4, 5, 6, 7]),
        })
    );

    assert_eq!(
        decoder.decode(&mut buf).unwrap(),
        Some(LaneResponseElement::Feed)
    );

    assert!(matches!(&decoder.state, &LaneResponseDecoderState::Header));
}

#[test]
fn round_trip_fin() {
    let mut buf = BytesMut::new();
    buf.put_i8(0);
    buf.put_i32(13);
    buf.put_i32(8);
    buf.extend_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7]);

    let mut decoder = LaneResponseDecoder::default();
    assert_eq!(
        decoder.decode(&mut buf).unwrap(),
        Some(LaneResponseElement::Response {
            lane_id: 13,
            data: Bytes::from_static(&[0, 1, 2, 3, 4, 5, 6, 7]),
        })
    );

    assert!(matches!(
        &decoder.state,
        &LaneResponseDecoderState::ResponseHeader
    ));
    assert_eq!(decoder.decode(&mut buf).unwrap(), None);
    assert!(matches!(&decoder.state, &LaneResponseDecoderState::Header));
}
