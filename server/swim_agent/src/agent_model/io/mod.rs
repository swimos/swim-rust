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

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::{future::Either, ready, SinkExt, Stream, StreamExt};
use swim_api::{
    error::FrameIoError,
    protocol::{
        agent::{LaneRequest, LaneRequestDecoder},
        map::{MapMessage, MapMessageDecoder, RawMapOperationDecoder},
        WithLengthBytesCodec,
    },
};
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

type ValueLaneReader = FramedRead<ByteReader, LaneRequestDecoder<WithLengthBytesCodec>>;
type MapLaneReader =
    FramedRead<ByteReader, LaneRequestDecoder<MapMessageDecoder<RawMapOperationDecoder>>>;

/// Used internally by the agent model for writing to lanes.
pub struct LaneWriter {
    id: u64,
    writer: FramedWrite<ByteWriter, BytesCodec>,
    pub buffer: BytesMut,
}

/// Used internally by the agent model for readig from lanes.
pub struct LaneReader {
    id: u64,
    inner: Either<ValueLaneReader, MapLaneReader>,
}

impl LaneReader {
    pub fn value(id: u64, reader: ByteReader) -> Self {
        LaneReader {
            id,
            inner: Either::Left(FramedRead::new(reader, Default::default())),
        }
    }

    pub fn map(id: u64, reader: ByteReader) -> Self {
        LaneReader {
            id,
            inner: Either::Right(FramedRead::new(reader, Default::default())),
        }
    }
}

impl LaneWriter {
    pub fn new(id: u64, tx: ByteWriter) -> Self {
        LaneWriter {
            id,
            writer: FramedWrite::new(tx, BytesCodec::default()),
            buffer: Default::default(),
        }
    }

    pub async fn write(mut self) -> (Self, Result<(), std::io::Error>) {
        let LaneWriter { writer, buffer, .. } = &mut self;
        let data = buffer.split().freeze();
        let result = writer.send(data).await;
        (self, result)
    }

    pub fn lane_id(&self) -> u64 {
        self.id
    }
}

impl Stream for LaneReader {
    type Item = (
        u64,
        Result<Either<LaneRequest<Bytes>, LaneRequest<MapMessage<Bytes, Bytes>>>, FrameIoError>,
    );

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let LaneReader { id, inner } = self.get_mut();
        match inner {
            Either::Left(reader) => {
                let result = ready!(reader.poll_next_unpin(cx));
                Poll::Ready(result.map(|r| (*id, r.map(Either::Left))))
            }
            Either::Right(reader) => {
                let result = ready!(reader.poll_next_unpin(cx));
                Poll::Ready(result.map(|r| (*id, r.map(Either::Right))))
            }
        }
    }
}
