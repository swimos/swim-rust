// Copyright 2015-2021 SWIM.AI inc.
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

mod dispatch;
pub mod router;
pub use dispatch::{DispatchError, Dispatcher};
use futures_util::SinkExt;
pub use router::Address;
use swim_utilities::io::byte_channel::ByteWriter;
use tokio_util::codec::{Encoder, FramedWrite};

pub struct Route<E> {
    pub framed: FramedWrite<ByteWriter, E>,
    // pub on_drop: promise::Receiver<ConnectionDropped>,
}

impl<E> Route<E> {
    /// Encode and send `item` into this route.
    pub async fn send<I>(&mut self, item: I) -> Result<(), E::Error>
    where
        E: Encoder<I>,
    {
        self.framed.send(item).await
    }

    pub fn is_closed(&self) -> bool {
        self.framed.get_ref().is_closed()
    }
}

#[derive(Debug)]
pub struct RawRoute {
    pub writer: ByteWriter,
    // pub on_drop: promise::Receiver<ConnectionDropped>,
}

impl RawRoute {
    /// Attach `encoder` to this writer for sending messages.
    pub fn into_framed<E>(self, encoder: E) -> Route<E> {
        let RawRoute { writer } = self;
        Route {
            framed: FramedWrite::new(writer, encoder),
        }
    }
}

impl RawRoute {
    pub fn is_closed(&self) -> bool {
        self.writer.is_closed()
    }
}
