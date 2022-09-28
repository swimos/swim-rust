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
use futures::{SinkExt, StreamExt};
use std::fmt::Write;
use swim_api::protocol::{
    agent::{
        LaneRequest, LaneRequestEncoder, LaneResponse, MapLaneResponse, MapLaneResponseDecoder,
        ValueLaneResponseDecoder,
    },
    map::{MapMessage, MapMessageEncoder, MapOperation, MapOperationEncoder},
    WithLengthBytesCodec,
};
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

pub struct ValueLaneSender {
    buffer: BytesMut,
    inner: FramedWrite<ByteWriter, LaneRequestEncoder<WithLengthBytesCodec>>,
}

pub struct ValueLaneReceiver {
    inner: FramedRead<ByteReader, ValueLaneResponseDecoder>,
}

impl ValueLaneSender {
    pub fn new(writer: ByteWriter) -> ValueLaneSender {
        ValueLaneSender {
            buffer: BytesMut::new(),
            inner: FramedWrite::new(writer, Default::default()),
        }
    }

    pub async fn command(&mut self, n: i32) {
        let ValueLaneSender { buffer, inner } = self;
        write!(buffer, "{}", n).expect("Writing to buffer failed.");
        let bytes = buffer.split();
        inner
            .send(LaneRequest::Command(bytes))
            .await
            .expect("Sending to value lane failed.");
    }

    pub async fn sync(&mut self, id: Uuid) {
        let ValueLaneSender { inner, .. } = self;
        let req: LaneRequest<BytesMut> = LaneRequest::Sync(id);
        inner
            .send(req)
            .await
            .expect("Sending to value lane failed.");
    }
}

impl ValueLaneReceiver {
    pub fn new(reader: ByteReader) -> Self {
        ValueLaneReceiver {
            inner: FramedRead::new(reader, Default::default()),
        }
    }

    pub async fn get_response(&mut self) -> LaneResponse<BytesMut> {
        let ValueLaneReceiver { inner } = self;
        inner
            .next()
            .await
            .expect("Stream closed unexpectedly.")
            .expect("Corrupted frame.")
    }

    pub async fn expect_event(&mut self, expected: i32) {
        let response = self.get_response().await;
        if let LaneResponse::StandardEvent(value) = response {
            let n: i32 = read_int(value);
            assert_eq!(n, expected);
        } else {
            panic!("Unexpected response.");
        }
    }

    pub async fn expect_sync_event(&mut self, id: Uuid, expected: i32) {
        let first = self.get_response().await;
        let second = self.get_response().await;

        if let LaneResponse::SyncEvent(sync_id, value) = first {
            assert_eq!(sync_id, id);
            let n: i32 = read_int(value);
            assert_eq!(n, expected);
        } else {
            panic!("Unexpected response.");
        }
        if let LaneResponse::Synced(sync_id) = second {
            assert_eq!(sync_id, id);
        } else {
            panic!("Unexpected response.");
        }
    }
}

fn read_int(bytes: impl AsRef<[u8]>) -> i32 {
    std::str::from_utf8(bytes.as_ref())
        .expect("Invalid UTF8.")
        .parse()
        .expect("Invalid integer.")
}

type MapEncoder = LaneRequestEncoder<MapMessageEncoder<MapOperationEncoder>>;

pub struct MapLaneSender {
    inner: FramedWrite<ByteWriter, MapEncoder>,
}

pub struct MapLaneReceiver {
    inner: FramedRead<ByteReader, MapLaneResponseDecoder>,
}

impl MapLaneSender {
    pub fn new(writer: ByteWriter) -> MapLaneSender {
        MapLaneSender {
            inner: FramedWrite::new(writer, Default::default()),
        }
    }

    pub async fn command(&mut self, key: i32, value: i32) {
        let MapLaneSender { inner } = self;
        inner
            .send(LaneRequest::Command(MapMessage::Update { key, value }))
            .await
            .expect("Sending to map lane failed.");
    }
}

impl MapLaneReceiver {
    pub fn new(reader: ByteReader) -> Self {
        MapLaneReceiver {
            inner: FramedRead::new(reader, Default::default()),
        }
    }

    pub async fn get_response(&mut self) -> MapLaneResponse<BytesMut, BytesMut> {
        let MapLaneReceiver { inner } = self;
        inner
            .next()
            .await
            .expect("Stream closed unexpectedly.")
            .expect("Corrupted frame.")
    }

    pub async fn expect_event(&mut self, expected: MapOperation<i32, i32>) {
        let response = self.get_response().await;

        match response {
            MapLaneResponse::StandardEvent(operation) => {
                assert_eq!(read_op(operation), expected)
            }
            ow => panic!("Unexpected response: {:?}", ow),
        }
    }
}

fn read_op(operation: MapOperation<BytesMut, BytesMut>) -> MapOperation<i32, i32> {
    match operation {
        MapOperation::Update { key, value } => MapOperation::Update {
            key: read_int(key),
            value: read_int(value),
        },
        MapOperation::Remove { key } => MapOperation::Remove { key: read_int(key) },
        MapOperation::Clear => MapOperation::Clear,
    }
}
