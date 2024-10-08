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

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::BytesMut;
use futures::{ready, SinkExt, Stream, StreamExt};
use swimos_agent_protocol::{
    encoding::lane::{RawMapLaneRequestDecoder, RawValueLaneRequestDecoder},
    LaneRequest, MapMessage,
};
use swimos_api::{agent::HttpLaneRequest, error::FrameIoError};
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};
use tokio::sync::mpsc;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

type ValueLaneReader = FramedRead<ByteReader, RawValueLaneRequestDecoder>;
type MapLaneReader = FramedRead<ByteReader, RawMapLaneRequestDecoder>;

/// Used internally by the agent model for writing to items.
pub struct ItemWriter {
    id: u64,
    writer: FramedWrite<ByteWriter, BytesCodec>,
    pub buffer: BytesMut,
}

enum LaneReaderInner {
    Value(ValueLaneReader),
    Map(MapLaneReader),
    Http(mpsc::Receiver<HttpLaneRequest>),
}

/// Used internally by the agent model for reading from lanes.
pub struct LaneReader {
    id: u64,
    inner: LaneReaderInner,
}

impl LaneReader {
    pub fn value(id: u64, reader: ByteReader) -> Self {
        LaneReader {
            id,
            inner: LaneReaderInner::Value(FramedRead::new(reader, Default::default())),
        }
    }

    pub fn map(id: u64, reader: ByteReader) -> Self {
        LaneReader {
            id,
            inner: LaneReaderInner::Map(FramedRead::new(reader, Default::default())),
        }
    }

    pub fn http(id: u64, rx: mpsc::Receiver<HttpLaneRequest>) -> Self {
        LaneReader {
            id,
            inner: LaneReaderInner::Http(rx),
        }
    }
}

impl ItemWriter {
    pub fn new(id: u64, tx: ByteWriter) -> Self {
        ItemWriter {
            id,
            writer: FramedWrite::new(tx, BytesCodec::default()),
            buffer: Default::default(),
        }
    }

    pub async fn write(mut self) -> (Self, Result<(), std::io::Error>) {
        let ItemWriter { writer, buffer, .. } = &mut self;
        let data = buffer.split().freeze();
        let result = writer.send(data).await;
        (self, result)
    }

    pub fn lane_id(&self) -> u64 {
        self.id
    }
}

pub enum LaneReadEvent {
    Value(LaneRequest<BytesMut>),
    Map(LaneRequest<MapMessage<BytesMut, BytesMut>>),
    Http(HttpLaneRequest),
}

impl Stream for LaneReader {
    type Item = (u64, Result<LaneReadEvent, FrameIoError>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let LaneReader { id, inner } = self.get_mut();
        match inner {
            LaneReaderInner::Value(reader) => {
                let result = ready!(reader.poll_next_unpin(cx));
                Poll::Ready(result.map(|r| (*id, r.map(LaneReadEvent::Value))))
            }
            LaneReaderInner::Map(reader) => {
                let result = ready!(reader.poll_next_unpin(cx));
                Poll::Ready(result.map(|r| (*id, r.map(LaneReadEvent::Map))))
            }
            LaneReaderInner::Http(rx) => {
                let result = ready!(rx.poll_recv(cx));
                Poll::Ready(result.map(|r| (*id, Ok(LaneReadEvent::Http(r)))))
            }
        }
    }
}
