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

use std::{convert::Infallible, fmt::Display};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use swim_api::protocol::{
    downlink::DownlinkOperation,
    map::{RawMapOperation, RawMapOperationMut},
};
use tokio_util::codec::Encoder;

use map_queue::MapOperationQueue;
mod key;
mod map_queue;
pub mod recon;

use recon::MapOperationReconEncoder;

use crate::error::InvalidKey;

#[cfg(test)]
mod tests;

/// Backpressure strategy for the output task of a downlink. This is used to encode the
/// difference in behaviour between different kinds of downlink (particuarly value and
/// map downlinks.)
pub trait BackpressureStrategy {
    /// The type of operations expected from the downlink implementation.
    type Operation;
    /// Errors that could ocurr when a frame is pushed into the backpressure relief mechanism.
    type Err: Display;

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

/// Backpressure implementation for value-like uplinks/downlinks. This contains a buffer which
/// is repeatedly overwritten each time a new record is pushed.
#[derive(Debug, Default)]
pub struct ValueBackpressure {
    current: BytesMut,
}

impl ValueBackpressure {
    pub fn push_bytes(&mut self, body: Bytes) {
        let ValueBackpressure { current } = self;

        current.clear();
        current.reserve(body.len());
        current.put(body);
    }

    pub fn has_data(&self) -> bool {
        !self.current.is_empty()
    }
}

/// Backpressure implementation for supply uplinks. This, in fact, provides no backpressure
/// relief at all and is simply an unbounded buffer.
#[derive(Debug, Default)]
pub struct SupplyBackpressure {
    buffer: BytesMut,
}

const LEN_SIZE: usize = std::mem::size_of::<u64>();

impl SupplyBackpressure {
    pub fn push_bytes(&mut self, body: Bytes) {
        let SupplyBackpressure { buffer } = self;
        buffer.reserve(body.len() + LEN_SIZE);
        let len = u64::try_from(body.len()).expect("Length does not fit into a u64.");
        buffer.put_u64(len);
        buffer.put(body);
    }

    pub fn has_data(&self) -> bool {
        !self.buffer.is_empty()
    }
}

/// Backpressure implementation for map-like uplinks/downlinks. Map updates are pushed into a
/// [`MapOperationQueue`] that relieves backpressure on a per-key basis.
#[derive(Debug, Default)]
pub struct MapBackpressure {
    queue: MapOperationQueue,
    encoder: MapOperationReconEncoder,
}

impl MapBackpressure {
    pub fn push(&mut self, operation: RawMapOperationMut) -> Result<(), InvalidKey> {
        self.queue.push(operation)
    }

    pub fn pop(&mut self) -> Option<RawMapOperation> {
        self.queue.pop()
    }
}

impl BackpressureStrategy for ValueBackpressure {
    type Operation = DownlinkOperation<Bytes>;

    type Err = Infallible;

    fn push_operation(&mut self, op: Self::Operation) -> Result<(), Infallible> {
        let DownlinkOperation { body } = op;
        self.push_bytes(body);
        Ok(())
    }

    fn has_data(&self) -> bool {
        ValueBackpressure::has_data(self)
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

impl BackpressureStrategy for SupplyBackpressure {
    type Operation = DownlinkOperation<Bytes>;

    type Err = Infallible;

    fn push_operation(&mut self, op: Self::Operation) -> Result<(), Self::Err> {
        let DownlinkOperation { body } = op;
        self.push_bytes(body);
        Ok(())
    }

    fn write_direct(&mut self, op: Self::Operation, buffer: &mut BytesMut) {
        let DownlinkOperation { body } = op;
        buffer.clear();
        buffer.reserve(body.len());
        buffer.put(body);
    }

    fn has_data(&self) -> bool {
        SupplyBackpressure::has_data(self)
    }

    fn prepare_write(&mut self, target: &mut BytesMut) {
        target.clear();
        if self.has_data() {
            let SupplyBackpressure { buffer } = self;
            let len = usize::try_from(buffer.get_u64()).expect("u64 does not fit into a usize.");
            target.reserve(len);
            target.put((buffer).take(len));
        }
    }
}

impl BackpressureStrategy for MapBackpressure {
    type Operation = RawMapOperationMut;

    type Err = InvalidKey;

    fn push_operation(&mut self, op: Self::Operation) -> Result<(), InvalidKey> {
        self.queue.push(op)
    }

    fn has_data(&self) -> bool {
        !self.queue.is_empty()
    }

    fn write_direct(&mut self, op: Self::Operation, buffer: &mut BytesMut) {
        let MapBackpressure { encoder, .. } = self;
        buffer.clear();
        // Encoding the operation cannot fail.
        encoder
            .encode(op, buffer)
            .expect("Encoding should be unfallible.");
    }

    fn prepare_write(&mut self, buffer: &mut BytesMut) {
        buffer.clear();
        if let Some(head) = self.pop() {
            // Encoding the operation cannot fail.
            self.encoder
                .encode(head, buffer)
                .expect("Encoding should be unfallible.");
        }
    }
}
