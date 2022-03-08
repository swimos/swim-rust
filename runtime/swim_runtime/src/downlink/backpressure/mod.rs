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

use std::{convert::Infallible, fmt::Display, str::Utf8Error};

use bytes::{BufMut, Bytes, BytesMut};
use swim_api::protocol::{
    downlink::{DownlinkOperation, DownlinkOperationDecoder},
    map::{RawMapOperation, RawMapOperationDecoder, RawMapOperationEncoder},
};
use tokio_util::codec::{Decoder, Encoder};

use super::map_queue::MapOperationQueue;

/// Backpressure strategy for the output task of a downlink. This is used to encode the
/// difference in behaviour between different kinds of downlink (particuarly value and
/// map downlinks.)
pub trait DownlinkBackpressure {
    /// The type of operations expected from the downlink implementation.
    type Operation;
    /// Decoder for operations received from the downlink implementation.
    type Dec: Decoder<Item = Self::Operation>;
    /// Errors that could ocurr when a frame is pushed into the backpressure relief mechanism.
    type Err: Display;

    fn make_decoder() -> Self::Dec;

    /// Called when a record has been sent on the downlink but writing is blocked.
    fn push_operation(&mut self, op: Self::Operation) -> Result<(), Self::Err>;

    /// When writing is not blocked, this is called to write the outgoing record into
    /// the output buffer.
    fn write_direct(&mut self, op: Self::Operation, buffer: &mut BytesMut);

    /// Determine whether the strategy has data to be written to the output buffer.
    fn has_data(&self) -> bool;

    /// Drain one record from the strategy to the output buffer.
    fn prepare_write(&mut self, buffer: &mut BytesMut);
}

/// Backpressure implementation for value-like downlinks. This contains a buffer which
/// is repeatedly overwritten each time a new record is pushed.
#[derive(Debug, Default)]
pub struct ValueBackpressure {
    current: BytesMut,
}

/// Backpressure implementation for map-like downlinks. Map updates are pushed into a
/// [`MapOperationQueue`] that relieves backpressure on a per-key basis.
#[derive(Debug, Default)]
pub struct MapBackpressure {
    queue: MapOperationQueue,
    encoder: RawMapOperationEncoder,
}

impl DownlinkBackpressure for ValueBackpressure {
    type Operation = DownlinkOperation<Bytes>;

    type Dec = DownlinkOperationDecoder;
    type Err = Infallible;

    fn make_decoder() -> Self::Dec {
        Default::default()
    }

    fn push_operation(&mut self, op: Self::Operation) -> Result<(), Infallible> {
        let ValueBackpressure { current } = self;
        let DownlinkOperation { body } = op;

        current.clear();
        current.reserve(body.len());
        current.put(body);
        Ok(())
    }

    fn has_data(&self) -> bool {
        !self.current.is_empty()
    }

    fn write_direct(&mut self, op: Self::Operation, buffer: &mut BytesMut) {
        let DownlinkOperation { body } = op;
        buffer.clear();
        buffer.reserve(body.len());
        buffer.put(body);
    }

    fn prepare_write(&mut self, buffer: &mut BytesMut) {
        std::mem::swap(&mut self.current, buffer);
        self.current.clear()
    }
}

impl DownlinkBackpressure for MapBackpressure {
    type Operation = RawMapOperation;

    type Dec = RawMapOperationDecoder;
    type Err = Utf8Error;

    fn make_decoder() -> Self::Dec {
        Default::default()
    }

    fn push_operation(&mut self, op: Self::Operation) -> Result<(), Utf8Error> {
        self.queue.push(op)
    }

    fn has_data(&self) -> bool {
        !self.queue.is_empty()
    }

    fn write_direct(&mut self, op: Self::Operation, buffer: &mut BytesMut) {
        let MapBackpressure { encoder, .. } = self;
        // Encoding the operation cannot fail.
        encoder
            .encode(op, buffer)
            .expect("Encoding should be unfallible.");
    }

    fn prepare_write(&mut self, buffer: &mut BytesMut) {
        let MapBackpressure { queue, encoder } = self;
        if let Some(head) = queue.pop() {
            // Encoding the operation cannot fail.
            encoder
                .encode(head, buffer)
                .expect("Encoding should be unfallible.");
        }
    }
}
