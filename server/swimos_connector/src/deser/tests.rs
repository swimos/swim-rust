// Copyright 2015-2024 Swim Inc.
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

use swimos_model::{Attr, Item, Value};
use uuid::Uuid;

use crate::deser::{
    F32Deserializer, F64Deserializer, I32Deserializer, I64Deserializer, MessagePart,
    ReconDeserializer, U32Deserializer, U64Deserializer, UuidDeserializer,
};

use super::{Endianness, MessageDeserializer, MessageView, StringDeserializer};

pub fn view_of(bytes: &[u8], part: MessagePart) -> MessageView<'_> {
    match part {
        MessagePart::Key => MessageView {
            topic: "",
            key: bytes,
            payload: &[],
        },
        MessagePart::Payload => MessageView {
            topic: "",
            key: &[],
            payload: bytes,
        },
    }
}

#[test]
fn string_deserializer() {
    let deserializer = StringDeserializer;

    let bytes = "hello".as_bytes();

    let view = view_of(bytes, MessagePart::Key);
    assert_eq!(
        deserializer.deserialize(&view.key),
        Ok(Value::text("hello"))
    );

    let view = view_of(bytes, MessagePart::Payload);
    assert_eq!(
        deserializer.deserialize(&view.payload),
        Ok(Value::text("hello"))
    );
}

#[test]
fn i32_deserializer() {
    let le_deserializer = I32Deserializer::new(Endianness::LittleEndian);
    let be_deserializer = I32Deserializer::new(Endianness::BigEndian);

    let le_bytes: &[u8] = &567i32.to_le_bytes();
    let be_bytes: &[u8] = &((-874636i32).to_be_bytes());

    let view = view_of(le_bytes, MessagePart::Key);
    assert_eq!(
        le_deserializer.deserialize(&view.key).expect("Failed."),
        Value::from(567i32)
    );

    let view = view_of(le_bytes, MessagePart::Payload);
    assert_eq!(
        le_deserializer
            .deserialize(&&view.payload)
            .expect("Failed."),
        Value::from(567i32)
    );

    let view = view_of(be_bytes, MessagePart::Key);
    assert_eq!(
        be_deserializer.deserialize(&view.key).expect("Failed."),
        Value::from(-874636i32)
    );

    let view = view_of(be_bytes, MessagePart::Payload);
    assert_eq!(
        be_deserializer
            .deserialize(&&view.payload)
            .expect("Failed."),
        Value::from(-874636i32)
    );
}

#[test]
fn i64_deserializer() {
    let le_deserializer = I64Deserializer::new(Endianness::LittleEndian);
    let be_deserializer = I64Deserializer::new(Endianness::BigEndian);

    let le_bytes: &[u8] = &7476383847i64.to_le_bytes();
    let be_bytes: &[u8] = &((-84728282872734i64).to_be_bytes());

    let view = view_of(le_bytes, MessagePart::Key);
    assert_eq!(
        le_deserializer.deserialize(&view.key).expect("Failed."),
        Value::from(7476383847i64)
    );

    let view = view_of(le_bytes, MessagePart::Payload);
    assert_eq!(
        le_deserializer
            .deserialize(&&view.payload)
            .expect("Failed."),
        Value::from(7476383847i64)
    );

    let view = view_of(be_bytes, MessagePart::Key);
    assert_eq!(
        be_deserializer.deserialize(&view.key).expect("Failed."),
        Value::from(-84728282872734i64)
    );

    let view = view_of(be_bytes, MessagePart::Payload);
    assert_eq!(
        be_deserializer
            .deserialize(&&view.payload)
            .expect("Failed."),
        Value::from(-84728282872734i64)
    );
}

#[test]
fn u32_deserializer() {
    let le_deserializer = U32Deserializer::new(Endianness::LittleEndian);
    let be_deserializer = U32Deserializer::new(Endianness::BigEndian);

    let le_bytes: &[u8] = &567u32.to_le_bytes();
    let be_bytes: &[u8] = &874636u32.to_be_bytes();

    let view = view_of(le_bytes, MessagePart::Key);
    assert_eq!(
        le_deserializer.deserialize(&view.key).expect("Failed."),
        Value::from(567u32)
    );

    let view = view_of(le_bytes, MessagePart::Payload);
    assert_eq!(
        le_deserializer
            .deserialize(&&view.payload)
            .expect("Failed."),
        Value::from(567u32)
    );

    let view = view_of(be_bytes, MessagePart::Key);
    assert_eq!(
        be_deserializer.deserialize(&view.key).expect("Failed."),
        Value::from(874636u32)
    );

    let view = view_of(be_bytes, MessagePart::Payload);
    assert_eq!(
        be_deserializer
            .deserialize(&&view.payload)
            .expect("Failed."),
        Value::from(874636u32)
    );
}

#[test]
fn u64_deserializer() {
    let le_deserializer = U64Deserializer::new(Endianness::LittleEndian);
    let be_deserializer = U64Deserializer::new(Endianness::BigEndian);

    let le_bytes: &[u8] = &7476383847u64.to_le_bytes();
    let be_bytes: &[u8] = &84728282872734u64.to_be_bytes();

    let view = view_of(le_bytes, MessagePart::Key);
    assert_eq!(
        le_deserializer.deserialize(&view.key).expect("Failed."),
        Value::from(7476383847u64)
    );

    let view = view_of(le_bytes, MessagePart::Payload);
    assert_eq!(
        le_deserializer
            .deserialize(&&view.payload)
            .expect("Failed."),
        Value::from(7476383847u64)
    );

    let view = view_of(be_bytes, MessagePart::Key);
    assert_eq!(
        be_deserializer.deserialize(&view.key).expect("Failed."),
        Value::from(84728282872734u64)
    );

    let view = view_of(be_bytes, MessagePart::Payload);
    assert_eq!(
        be_deserializer
            .deserialize(&&view.payload)
            .expect("Failed."),
        Value::from(84728282872734u64)
    );
}

#[test]
fn f64_deserializer() {
    let le_deserializer = F64Deserializer::new(Endianness::LittleEndian);
    let be_deserializer = F64Deserializer::new(Endianness::BigEndian);

    let le_bytes: &[u8] = &4.657366e7f64.to_le_bytes();
    let be_bytes: &[u8] = &((-84.657366e-87f64).to_be_bytes());

    let view = view_of(le_bytes, MessagePart::Key);
    assert_eq!(
        le_deserializer.deserialize(&view.key).expect("Failed."),
        Value::from(4.657366e7)
    );

    let view = view_of(le_bytes, MessagePart::Payload);
    assert_eq!(
        le_deserializer
            .deserialize(&&view.payload)
            .expect("Failed."),
        Value::from(4.657366e7)
    );

    let view = view_of(be_bytes, MessagePart::Key);
    assert_eq!(
        be_deserializer.deserialize(&view.key).expect("Failed."),
        Value::from(-84.657366e-87)
    );

    let view = view_of(be_bytes, MessagePart::Payload);
    assert_eq!(
        be_deserializer
            .deserialize(&&view.payload)
            .expect("Failed."),
        Value::from(-84.657366e-87)
    );
}

#[test]
fn f32_deserializer() {
    let le_deserializer = F32Deserializer::new(Endianness::LittleEndian);
    let be_deserializer = F32Deserializer::new(Endianness::BigEndian);

    let le_bytes: &[u8] = &4.657366e7f32.to_le_bytes();
    let be_bytes: &[u8] = &((-84.6573e-87f32).to_be_bytes());

    let view = view_of(le_bytes, MessagePart::Key);
    assert_eq!(
        le_deserializer.deserialize(&view.key).expect("Failed."),
        Value::from(4.657366e7f32 as f64)
    );

    let view = view_of(le_bytes, MessagePart::Payload);
    assert_eq!(
        le_deserializer
            .deserialize(&&view.payload)
            .expect("Failed."),
        Value::from(4.657366e7f32 as f64)
    );

    let view = view_of(be_bytes, MessagePart::Key);
    assert_eq!(
        be_deserializer.deserialize(&view.key).expect("Failed."),
        Value::from(-84.6573e-87f32 as f64)
    );

    let view = view_of(be_bytes, MessagePart::Payload);
    assert_eq!(
        be_deserializer
            .deserialize(&&view.payload)
            .expect("Failed."),
        Value::from(-84.6573e-87f32 as f64)
    );
}

#[test]
fn uuid_deserializer() {
    let deserializer = UuidDeserializer;

    let id = Uuid::new_v4();
    let expected = Value::BigInt(id.as_u128().into());

    let bytes: &[u8] = id.as_bytes();

    let view = view_of(bytes, MessagePart::Key);
    assert_eq!(
        deserializer.deserialize(&view.key).expect("Failed."),
        expected
    );

    let view = view_of(bytes, MessagePart::Payload);
    assert_eq!(
        deserializer.deserialize(&&view.payload).expect("Failed."),
        expected
    );
}

#[test]
fn recon_deserializer() {
    let deserializer = ReconDeserializer;

    let bytes: &[u8] = "@record { a: 1, b: 2}".as_bytes();
    let expected = Value::Record(
        vec![Attr::from("record")],
        vec![Item::slot("a", 1), Item::slot("b", 2)],
    );

    let view = view_of(bytes, MessagePart::Key);
    assert_eq!(
        deserializer.deserialize(&view.key).expect("Failed."),
        expected
    );

    let view = view_of(bytes, MessagePart::Payload);
    assert_eq!(
        deserializer.deserialize(&&view.payload).expect("Failed."),
        expected
    );
}
