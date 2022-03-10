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
use swim_form::structural::{read::recognizer::RecognizerReadable, write::StructuralWritable};
use swim_form::Form;
use swim_recon::printer::print_recon_compact;
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::map::{DROP, TAKE};
use crate::protocol::{LEN_SIZE, TAG_SIZE};

use super::{
    MapMessage, MapMessageDecoder, MapMessageEncoder, MapOperation, MapOperationDecoder,
    MapOperationEncoder, RawMapOperation, RawMapOperationDecoder, RawMapOperationEncoder, CLEAR,
    REMOVE, UPDATE,
};

fn encode_raw_operation(op: MapOperation<&[u8], &[u8]>) -> Bytes {
    let mut buffer = BytesMut::new();
    assert!(RawMapOperationEncoder.encode(op, &mut buffer).is_ok());
    buffer.freeze()
}

fn encode_operation<K: StructuralWritable, V: StructuralWritable>(op: MapOperation<K, V>) -> Bytes {
    let mut buffer = BytesMut::new();
    assert!(MapOperationEncoder.encode(op, &mut buffer).is_ok());
    buffer.freeze()
}

fn round_trip<K: StructuralWritable, V: StructuralWritable>(
    op: MapOperation<K, V>,
) -> RawMapOperation {
    let mut buffer = BytesMut::new();
    assert!(MapOperationEncoder.encode(op, &mut buffer).is_ok());
    let result = RawMapOperationDecoder.decode(&mut buffer);
    assert!(buffer.is_empty());
    match result {
        Ok(Some(value)) => value,
        Ok(None) => {
            panic!("Incomplete.");
        }
        Err(e) => {
            panic!("Bad frame: {}", e);
        }
    }
}

fn round_trip_raw<K: RecognizerReadable, V: RecognizerReadable>(
    op: RawMapOperation,
) -> MapOperation<K, V> {
    let mut buffer = BytesMut::new();
    assert!(RawMapOperationEncoder.encode(op, &mut buffer).is_ok());
    let mut decoder = MapOperationDecoder::default();
    let result = decoder.decode(&mut buffer);
    assert!(buffer.is_empty());
    match result {
        Ok(Some(value)) => value,
        Ok(None) => {
            panic!("Incomplete.");
        }
        Err(e) => {
            panic!("Bad frame: {}", e);
        }
    }
}

#[derive(Debug, Form, Clone, Copy, PartialEq, Eq)]
struct Example {
    a: i32,
    b: i32,
}

#[test]
fn encode_clear_operation_raw() {
    let mut bytes = encode_raw_operation(MapOperation::Clear);
    assert_eq!(bytes.len(), 1);
    assert_eq!(bytes.get_u8(), CLEAR);
}

#[test]
fn encode_clear_operation() {
    let mut bytes = encode_operation::<String, Example>(MapOperation::Clear);
    assert_eq!(bytes.len(), 1);
    assert_eq!(bytes.get_u8(), CLEAR);
}

const KEY: &str = "key";
const VALUE: &str = "value";

#[test]
fn encode_update_operation_raw() {
    let mut bytes = encode_raw_operation(MapOperation::Update {
        key: KEY.as_bytes(),
        value: VALUE.as_bytes(),
    });
    assert!(bytes.len() > TAG_SIZE + 2 * LEN_SIZE);
    assert_eq!(bytes.get_u8(), UPDATE);
    assert_eq!(bytes.get_u64() as usize, KEY.len());
    assert_eq!(bytes.get_u64() as usize, VALUE.len());
    assert_eq!(bytes.len(), KEY.len() + VALUE.len());

    let key_str = std::str::from_utf8(&bytes.as_ref()[0..KEY.len()]).unwrap();
    assert_eq!(key_str, KEY);
    bytes.advance(KEY.len());

    let value_str = std::str::from_utf8(&bytes.as_ref()[0..VALUE.len()]).unwrap();
    assert_eq!(value_str, VALUE);
    bytes.advance(VALUE.len());
}

#[test]
fn encode_update_operation() {
    let key = KEY.to_string();
    let value = Example { a: 1, b: 2 };

    let expected_value = format!("{}", print_recon_compact(&value));

    let mut bytes = encode_operation(MapOperation::Update { key, value });
    assert!(bytes.len() > TAG_SIZE + 2 * LEN_SIZE);
    assert_eq!(bytes.get_u8(), UPDATE);
    assert_eq!(bytes.get_u64() as usize, KEY.len());
    assert_eq!(bytes.get_u64() as usize, expected_value.len());
    assert_eq!(bytes.len(), KEY.len() + expected_value.len());

    let key_str = std::str::from_utf8(&bytes.as_ref()[0..KEY.len()]).unwrap();
    assert_eq!(key_str, KEY);
    bytes.advance(KEY.len());

    let value_str = std::str::from_utf8(&bytes.as_ref()[0..expected_value.len()]).unwrap();
    assert_eq!(value_str, expected_value);
    bytes.advance(expected_value.len());
}

#[test]
fn encode_remove_operation_raw() {
    let mut bytes = encode_raw_operation(MapOperation::Remove {
        key: KEY.as_bytes(),
    });
    assert!(bytes.len() > TAG_SIZE + LEN_SIZE);
    assert_eq!(bytes.get_u8(), REMOVE);
    assert_eq!(bytes.get_u64() as usize, KEY.len());
    assert_eq!(bytes.len(), KEY.len());

    let key_str = std::str::from_utf8(&bytes.as_ref()[0..KEY.len()]).unwrap();
    assert_eq!(key_str, KEY);
    bytes.advance(KEY.len());
}

#[test]
fn encode_remove_operation() {
    let key = Example { a: 1, b: 2 };

    let expected_key = format!("{}", print_recon_compact(&key));

    let mut bytes = encode_operation::<Example, ()>(MapOperation::Remove { key });
    assert!(bytes.len() > TAG_SIZE + LEN_SIZE);
    assert_eq!(bytes.get_u8(), REMOVE);
    assert_eq!(bytes.get_u64() as usize, expected_key.len());
    assert_eq!(bytes.len(), expected_key.len());

    let key_str = std::str::from_utf8(&bytes.as_ref()[0..expected_key.len()]).unwrap();
    assert_eq!(key_str, expected_key);
    bytes.advance(expected_key.len());
}

#[test]
fn decode_clear_operation() {
    let restored = round_trip_raw::<String, String>(MapOperation::Clear);
    assert_eq!(restored, MapOperation::Clear);

    let restored = round_trip::<String, String>(MapOperation::Clear);
    assert_eq!(restored, MapOperation::Clear);
}

#[test]
fn decode_update_operation() {
    let raw = RawMapOperation::Update {
        key: Bytes::from_static(KEY.as_bytes()),
        value: Bytes::from_static(VALUE.as_bytes()),
    };
    let restored = round_trip_raw::<String, String>(raw);
    assert_eq!(
        restored,
        MapOperation::Update {
            key: KEY.to_string(),
            value: VALUE.to_string(),
        }
    );

    let value = Example { a: 1, b: 2 };

    let expected_key = Bytes::from_static(KEY.as_bytes());
    let expected_value = Bytes::from(format!("{}", print_recon_compact(&value)).into_bytes());

    let op = MapOperation::Update {
        key: KEY.to_string(),
        value,
    };

    let restored = round_trip::<String, Example>(op);
    assert_eq!(
        restored,
        MapOperation::Update {
            key: expected_key,
            value: expected_value,
        }
    );
}

#[test]
fn decode_remove_operation() {
    let raw = RawMapOperation::Remove {
        key: Bytes::from_static(KEY.as_bytes()),
    };
    let restored = round_trip_raw::<String, String>(raw);
    assert_eq!(
        restored,
        MapOperation::Remove {
            key: KEY.to_string(),
        }
    );

    let expected_key = Bytes::from_static(KEY.as_bytes());

    let op = MapOperation::Remove {
        key: KEY.to_string(),
    };

    let restored = round_trip::<String, Example>(op);
    assert_eq!(restored, MapOperation::Remove { key: expected_key });
}

fn encode_message<K: StructuralWritable, V: StructuralWritable>(op: MapMessage<K, V>) -> Bytes {
    let mut buffer = BytesMut::new();
    let mut encoder = MapMessageEncoder::new(MapOperationEncoder);
    assert!(encoder.encode(op, &mut buffer).is_ok());
    buffer.freeze()
}

fn round_trip_message<K: Form, V: Form>(op: MapMessage<K, V>) -> MapMessage<K, V> {
    let mut buffer = BytesMut::new();
    let mut encoder = MapMessageEncoder::new(MapOperationEncoder);
    let mut decoder = MapMessageDecoder::new(MapOperationDecoder::<K, V>::default());
    assert!(encoder.encode(op, &mut buffer).is_ok());
    let result = decoder.decode(&mut buffer);
    assert!(buffer.is_empty());
    match result {
        Ok(Some(value)) => value,
        Ok(None) => {
            panic!("Incomplete.");
        }
        Err(e) => {
            panic!("Bad frame: {}", e);
        }
    }
}

#[test]
fn encode_take_message() {
    let mut bytes = encode_message::<String, String>(MapMessage::Take(7));
    assert_eq!(bytes.len(), TAG_SIZE + LEN_SIZE);
    assert_eq!(bytes.get_u8(), TAKE);
    assert_eq!(bytes.get_u64(), 7);
}

#[test]
fn encode_drop_message() {
    let mut bytes = encode_message::<String, String>(MapMessage::Drop(9));
    assert_eq!(bytes.len(), TAG_SIZE + LEN_SIZE);
    assert_eq!(bytes.get_u8(), DROP);
    assert_eq!(bytes.get_u64(), 9);
}

#[test]
fn encode_op_message() {
    let mut bytes = encode_message::<String, String>(MapOperation::Clear.into());
    assert_eq!(bytes.len(), TAG_SIZE);
    assert_eq!(bytes.get_u8(), CLEAR);
}

#[test]
fn decode_take_message() {
    let restored = round_trip_message::<String, String>(MapMessage::Take(678));
    assert_eq!(restored, MapMessage::Take(678));
}

#[test]
fn decode_drop_message() {
    let restored = round_trip_message::<String, String>(MapMessage::Drop(75783932));
    assert_eq!(restored, MapMessage::Drop(75783932));
}

#[test]
fn decode_op_message() {
    let op = MapOperation::Update {
        key: KEY.to_string(),
        value: VALUE.to_string(),
    };
    let restored = round_trip_message::<String, String>(op.clone().into());
    assert_eq!(restored, op.into());
}

#[test]
fn test_map_operation_form() {
    let op = MapOperation::Update { key: 0, value: 1};
    let recon = format!("{}", print_recon_compact(&op));
    assert_eq!(recon, "@update(key:0) 1");

    let op: MapOperation<i32, i32> = MapOperation::Remove { key: 0 };
    let recon = format!("{}", print_recon_compact(&op));
    assert_eq!(recon, "@remove(key:0)");

    let op: MapOperation<i32, i32> = MapOperation::Clear;
    let recon = format!("{}", print_recon_compact(&op));
    assert_eq!(recon, "@clear");
}

#[test]
fn test_map_message_form() {
    let op = MapMessage::Update { key: 0, value: 1};
    let recon = format!("{}", print_recon_compact(&op));
    assert_eq!(recon, "@update(key:0) 1");

    let op: MapMessage<i32, i32> = MapMessage::Remove { key: 0 };
    let recon = format!("{}", print_recon_compact(&op));
    assert_eq!(recon, "@remove(key:0)");

    let op: MapMessage<i32, i32> = MapMessage::Clear;
    let recon = format!("{}", print_recon_compact(&op));
    assert_eq!(recon, "@clear");

    let op: MapMessage<i32, i32> = MapMessage::Take(1);
    let recon = format!("{}", print_recon_compact(&op));
    assert_eq!(recon, "@take(1)");

    let op: MapMessage<i32, i32> = MapMessage::Drop(1);
    let recon = format!("{}", print_recon_compact(&op));
    assert_eq!(recon, "@drop(1)");
}