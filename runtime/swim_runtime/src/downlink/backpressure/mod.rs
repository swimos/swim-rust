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

use std::{convert::Infallible, str::Utf8Error, fmt::Display};

use bytes::{Bytes, BytesMut, BufMut};
use swim_api::protocol::{downlink::{DownlinkOperation, DownlinkOperationDecoder}, map::{RawMapOperation, RawMapOperationEncoder, RawMapOperationDecoder}};
use tokio_util::codec::{Decoder, Encoder};

use super::map_queue::MapOperationQueue;

pub trait DownlinkBackpressure {

    type Operation;
    type Dec: Decoder<Item = Self::Operation>;
    type Err: Display;

    fn make_decoder() -> Self::Dec;

    fn push_operation(&mut self, op: Self::Operation) -> Result<(), Self::Err>;

    fn write_direct(&mut self, op: Self::Operation, buffer: &mut BytesMut);

    fn has_data(&self) -> bool;

    fn prepare_write(&mut self, buffer: &mut BytesMut);

}

#[derive(Debug)]
pub enum DebugEvent {
    HasData(bool),
    Push(Vec<u8>),
    WriteDirect(Vec<u8>),
    PrepareWrite(Vec<u8>, Vec<u8>)
}

#[derive(Debug, Default)]
pub struct ValueBackpressure {
    tx: Option<tokio::sync::mpsc::UnboundedSender<DebugEvent>>,
    current: BytesMut,
}

impl ValueBackpressure {

    pub fn new(tx: tokio::sync::mpsc::UnboundedSender<DebugEvent>) -> Self {
        ValueBackpressure {
            tx: Some(tx),
            current: Default::default(),
        }
    }

}

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
        let ValueBackpressure { tx, current } = self;
        let DownlinkOperation { body } = op;
        
        if let Some(tx) = tx {
            let _ = tx.send(DebugEvent::Push(body.to_vec()));
        }
        
        current.reserve(body.len());
        current.put(body);
        Ok(())
    }

    fn has_data(&self) -> bool {
        if let Some(tx) = &self.tx {
            let _ = tx.send(DebugEvent::HasData(!self.current.is_empty()));
        }
        !self.current.is_empty()
    }

    fn write_direct(&mut self, op: Self::Operation, buffer: &mut BytesMut) {
        let DownlinkOperation { body } = op;
        if let Some(tx) = &self.tx {
            let _ = tx.send(DebugEvent::WriteDirect(body.to_vec()));
        }
        buffer.reserve(body.len());
        buffer.put(body);
    }

    fn prepare_write(&mut self, buffer: &mut BytesMut) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(DebugEvent::PrepareWrite(self.current.to_vec(), buffer.to_vec()));
        }
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
        let MapBackpressure {
            encoder, ..
        } = self;
         // Encoding the operation cannot fail.
        encoder.encode(op, buffer).expect("Encoding should be unfallible.");
    }

    fn prepare_write(&mut self, buffer: &mut BytesMut) {
        let MapBackpressure {
            queue, encoder,
        } = self;
        if let Some(head) = queue.pop() {
            // Encoding the operation cannot fail.
            encoder.encode(head, buffer).expect("Encoding should be unfallible.");
        }
    }
}