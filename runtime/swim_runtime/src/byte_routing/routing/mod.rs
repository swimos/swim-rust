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
use crate::byte_routing::Taggable;
use crate::routing::RoutingAddr;
pub use dispatch::{DispatchError, Dispatcher};
use futures_util::SinkExt;
pub use router::Address;
use swim_utilities::io::byte_channel::ByteWriter;
use tokio_util::codec::{Encoder, FramedWrite};

/// A unique route from which messages originate from `addr`.
pub struct Route<E> {
    pub addr: RoutingAddr,
    pub framed: FramedWrite<ByteWriter, E>,
}

impl<E> Route<E> {
    pub fn new(addr: RoutingAddr, writer: ByteWriter, encoder: E) -> Route<E> {
        Route {
            addr,
            framed: FramedWrite::new(writer, encoder),
        }
    }

    /// Tag, encode and send `item` into this route.
    pub async fn send<I>(&mut self, item: I) -> Result<(), E::Error>
    where
        E: Encoder<I::Out>,
        I: Taggable,
    {
        let Route { addr, framed } = self;
        framed.send(item.tag(*addr)).await
    }

    pub fn is_closed(&self) -> bool {
        self.framed.get_ref().is_closed()
    }
}

/// A route that has no encoder associated with it but has an origin.
#[derive(Debug)]
pub struct TaggedRawRoute {
    pub addr: RoutingAddr,
    pub writer: ByteWriter,
}

impl TaggedRawRoute {
    pub fn new(addr: RoutingAddr, writer: ByteWriter) -> TaggedRawRoute {
        TaggedRawRoute { addr, writer }
    }
}

/// A route that has no tag or encoder associated with it.
///
/// Created by the router.
#[derive(Debug)]
pub struct RawRoute {
    pub writer: ByteWriter,
}

impl RawRoute {
    pub fn is_closed(&self) -> bool {
        self.writer.is_closed()
    }
}
