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

use bytes::{Buf, Bytes, BytesMut};
use std::fmt::Write;
use swim_form::{
    structural::{read::recognizer::RecognizerReadable, write::StructuralWritable},
    Form,
};
use swim_recon::printer::print_recon_compact;
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

use crate::protocol::{
    agent::{
        LaneRequestDecoder, LaneRequestEncoder, LaneResponse, MapLaneResponse,
        MapLaneResponseDecoder, MapLaneResponseEncoder, ValueLaneResponseDecoder,
        ValueLaneResponseEncoder,
    },
    map::{MapOperation, MapOperationEncoder},
    WithLenRecognizerDecoder,
};

use super::LaneRequest;

#[test]
fn encode_sync_lane_request() {
    let mut encoder = LaneRequestEncoder::value();
    let mut buffer = BytesMut::new();
    let request: LaneRequest<&[u8]> = LaneRequest::Sync(Uuid::from_u128(67));
    assert!(encoder.encode(request, &mut buffer).is_ok());

    assert_eq!(buffer.remaining(), 17);
    assert_eq!(buffer.get_u8(), super::SYNC);
    assert_eq!(buffer.get_u128(), 67);
}

#[test]
fn encode_command_lane_request() {
    let mut encoder = LaneRequestEncoder::value();
    let mut buffer = BytesMut::new();
    let content = b"body";
    let request = LaneRequest::Command(content);
    assert!(encoder.encode(request, &mut buffer).is_ok());

    assert_eq!(buffer.remaining(), 9 + content.len());
    assert_eq!(buffer.get_u8(), super::COMMAND);
    assert_eq!(buffer.get_u64(), content.len() as u64);
    assert_eq!(buffer.as_ref(), content);
}

#[derive(Debug, Form, Clone, Copy, PartialEq, Eq)]
#[form_root(::swim_form)]
struct Example {
    a: i32,
    b: i32,
}

fn round_trip_request(request: LaneRequest<Example>) {
    let with_bytes = match &request {
        LaneRequest::Sync(n) => LaneRequest::Sync(*n),
        LaneRequest::Command(value) => {
            let mut buffer = BytesMut::new();
            assert!(write!(buffer, "{}", print_recon_compact(value)).is_ok());
            LaneRequest::Command(buffer.freeze())
        }
        LaneRequest::InitComplete => LaneRequest::InitComplete,
    };

    let mut encoder = LaneRequestEncoder::value();
    let mut buffer = BytesMut::new();
    assert!(encoder.encode(with_bytes, &mut buffer).is_ok());

    let mut decoder =
        LaneRequestDecoder::new(WithLenRecognizerDecoder::new(Example::make_recognizer()));
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
}

#[test]
fn decode_sync_lane_request() {
    round_trip_request(LaneRequest::Sync(Uuid::from_u128(892)));
}

#[test]
fn decode_command_lane_request() {
    round_trip_request(LaneRequest::Command(Example { a: 6, b: -56 }));
}

#[test]
fn encode_sync_value_lane_response() {
    let mut encoder = ValueLaneResponseEncoder::default();
    let mut buffer = BytesMut::new();
    let request = LaneResponse::sync_event(Uuid::from_u128(563883), Example { a: 6, b: 234 });
    assert!(encoder.encode(request, &mut buffer).is_ok());

    assert!(buffer.remaining() > 25);
    assert_eq!(buffer.get_u8(), super::SYNC);
    assert_eq!(buffer.get_u128(), 563883);
    let len = buffer.get_u64() as usize;
    assert_eq!(buffer.remaining(), len);
    assert_eq!(buffer.as_ref(), b"@Example{a:6,b:234}");
}

#[test]
fn encode_synced_lane_response() {
    let mut encoder = ValueLaneResponseEncoder::default();
    let mut buffer = BytesMut::new();
    let request = LaneResponse::<Example>::synced(Uuid::from_u128(563883));
    assert!(encoder.encode(request, &mut buffer).is_ok());

    assert_eq!(buffer.get_u8(), super::SYNC_COMPLETE);
    assert_eq!(buffer.get_u128(), 563883);
    assert_eq!(buffer.remaining(), 0);
}

#[test]
fn encode_initialized_lane_response() {
    let mut encoder = ValueLaneResponseEncoder::default();
    let mut buffer = BytesMut::new();
    let request = LaneResponse::<Example>::Initialized;
    assert!(encoder.encode(request, &mut buffer).is_ok());

    assert_eq!(buffer.get_u8(), super::INITIALIZED);
    assert_eq!(buffer.remaining(), 0);
}

#[test]
fn encode_event_value_lane_response() {
    let mut encoder = ValueLaneResponseEncoder::default();
    let mut buffer = BytesMut::new();
    let request = LaneResponse::event(Example { a: 6, b: 234 });
    assert!(encoder.encode(request, &mut buffer).is_ok());

    assert!(buffer.remaining() > 9);
    assert_eq!(buffer.get_u8(), super::EVENT);
    let len = buffer.get_u64() as usize;
    assert_eq!(buffer.remaining(), len);
    assert_eq!(buffer.as_ref(), b"@Example{a:6,b:234}");
}

fn to_bytes<T: StructuralWritable>(response: &LaneResponse<T>) -> LaneResponse<BytesMut> {
    match response {
        LaneResponse::StandardEvent(body) => {
            let mut buffer = BytesMut::new();
            assert!(write!(buffer, "{}", print_recon_compact(body)).is_ok());
            LaneResponse::StandardEvent(buffer)
        }
        LaneResponse::Initialized => LaneResponse::Initialized,
        LaneResponse::SyncEvent(id, body) => {
            let mut buffer = BytesMut::new();
            assert!(write!(buffer, "{}", print_recon_compact(body)).is_ok());
            LaneResponse::SyncEvent(*id, buffer)
        }
        LaneResponse::Synced(id) => LaneResponse::Synced(*id),
    }
}

fn round_trip_value_response(response: LaneResponse<Example>) {
    let with_bytes = to_bytes(&response);
    let mut encoder = ValueLaneResponseEncoder::default();
    let mut buffer = BytesMut::new();
    assert!(encoder.encode(response, &mut buffer).is_ok());

    let mut decoder = ValueLaneResponseDecoder::default();
    match decoder.decode(&mut buffer) {
        Ok(Some(restored)) => {
            assert_eq!(restored, with_bytes);
        }
        Ok(_) => {
            panic!("Decoding incomplete.");
        }
        Err(e) => {
            panic!("Decoding failed: {}", e);
        }
    }
}

#[test]
fn decode_sync_value_lane_response() {
    round_trip_value_response(LaneResponse::sync_event(
        Uuid::from_u128(12),
        Example { a: -8, b: 0 },
    ));
}

#[test]
fn decode_event_value_lane_response() {
    round_trip_value_response(LaneResponse::event(Example {
        a: 74737,
        b: 928938,
    }));
}

#[test]
fn encode_sync_complete_map_lane_response() {
    let mut encoder = MapLaneResponseEncoder::default();
    let mut buffer = BytesMut::new();
    let request: MapLaneResponse<i32, Example> = MapLaneResponse::Synced(Uuid::from_u128(7574));
    assert!(encoder.encode(request, &mut buffer).is_ok());

    assert_eq!(buffer.remaining(), 17);
    assert_eq!(buffer.get_u8(), super::SYNC_COMPLETE);
    let id = buffer.get_u128();
    assert_eq!(id, 7574);
}

fn expected_operation(op: MapOperation<i32, Example>) -> Bytes {
    let mut encoder = MapOperationEncoder::default();
    let mut buffer = BytesMut::new();
    assert!(encoder.encode(op, &mut buffer).is_ok());
    buffer.freeze()
}

#[test]
fn encode_sync_event_map_lane_response() {
    let mut encoder = MapLaneResponseEncoder::default();
    let mut buffer = BytesMut::new();
    let operation = MapOperation::Update {
        key: 5,
        value: Example { a: 7, b: -7 },
    };
    let request: MapLaneResponse<i32, Example> =
        MapLaneResponse::sync_event(Uuid::from_u128(85874), operation);

    let exp_op = expected_operation(operation);

    assert!(encoder.encode(request, &mut buffer).is_ok());

    assert!(buffer.remaining() > 17);
    assert_eq!(buffer.get_u8(), super::SYNC);
    let id = buffer.get_u128();
    assert_eq!(id, 85874);

    assert_eq!(buffer.freeze(), exp_op);
}

#[test]
fn encode_event_map_lane_response() {
    let mut encoder = MapLaneResponseEncoder::default();
    let mut buffer = BytesMut::new();
    let operation = MapOperation::Update {
        key: 5,
        value: Example { a: 7, b: -7 },
    };
    let request: MapLaneResponse<i32, Example> = MapLaneResponse::event(operation);

    let exp_op = expected_operation(operation);

    assert!(encoder.encode(request, &mut buffer).is_ok());

    assert!(buffer.remaining() > 1);
    assert_eq!(buffer.get_u8(), super::EVENT);

    assert_eq!(buffer.freeze(), exp_op);
}

fn map_op_to_bytes(op: &MapOperation<i32, Example>) -> MapOperation<BytesMut, BytesMut> {
    match op {
        MapOperation::Update { key, value } => {
            let key_str = format!("{}", print_recon_compact(key));
            let value_str = format!("{}", print_recon_compact(value));
            let mut key = BytesMut::new();
            let mut value = BytesMut::new();
            key.extend_from_slice(key_str.as_bytes());
            value.extend_from_slice(value_str.as_bytes());
            MapOperation::Update { key, value }
        }
        MapOperation::Remove { key } => {
            let key_str = format!("{}", print_recon_compact(key));
            let mut key = BytesMut::new();
            key.extend_from_slice(key_str.as_bytes());
            MapOperation::Remove { key }
        }
        MapOperation::Clear => MapOperation::Clear,
    }
}

fn round_trip_map_response(response: MapLaneResponse<i32, Example>) {
    let expected = match response {
        LaneResponse::StandardEvent(body) => LaneResponse::StandardEvent(map_op_to_bytes(&body)),
        LaneResponse::Initialized => LaneResponse::Initialized,
        LaneResponse::SyncEvent(id, body) => LaneResponse::SyncEvent(id, map_op_to_bytes(&body)),
        LaneResponse::Synced(id) => LaneResponse::Synced(id),
    };

    let mut encoder = MapLaneResponseEncoder::default();
    let mut buffer = BytesMut::new();
    assert!(encoder.encode(response, &mut buffer).is_ok());

    let mut decoder = MapLaneResponseDecoder::default();
    match decoder.decode(&mut buffer) {
        Ok(Some(restored)) => {
            assert_eq!(restored, expected);
        }
        Ok(_) => {
            panic!("Decoding incomplete.");
        }
        Err(e) => {
            panic!("Decoding failed: {}", e);
        }
    }
}

#[test]
fn decode_sync_complete_map_lane_response() {
    round_trip_map_response(MapLaneResponse::Synced(Uuid::from_u128(7482)));
}

#[test]
fn decode_event_map_lane_response() {
    round_trip_map_response(MapLaneResponse::event(MapOperation::Update {
        key: 5,
        value: Example { a: 7, b: 77 },
    }));
}

#[test]
fn decode_syncevent_map_lane_response() {
    round_trip_map_response(MapLaneResponse::sync_event(
        Uuid::from_u128(47389),
        MapOperation::Update {
            key: 5,
            value: Example { a: 7, b: 77 },
        },
    ));
}
