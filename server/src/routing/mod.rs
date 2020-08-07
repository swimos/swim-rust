// Copyright 2015-2020 SWIM.AI inc.
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

use common::routing::RoutingError;
use common::sink::item::ItemSender;
use common::warp::envelope::Envelope;
use pin_utils::core_reexport::fmt::Formatter;
use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Location {
    RemoteEndpoint(u32),
    Local(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoutingAddr(Location);

impl RoutingAddr {
    pub fn remote(id: u32) -> Self {
        RoutingAddr(Location::RemoteEndpoint(id))
    }

    pub fn local(id: u32) -> Self {
        RoutingAddr(Location::Local(id))
    }
}

impl Display for RoutingAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingAddr(Location::RemoteEndpoint(id)) => write!(f, "Remote Endpoint ({:X}).", id),
            RoutingAddr(Location::Local(id)) => write!(f, "Local consumer ({:X}).", id),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaggedEnvelope(pub RoutingAddr, pub Envelope);

pub trait ServerRouter {
    type Sender: ItemSender<Envelope, RoutingError> + Send + 'static;

    fn get_sender(&mut self, addr: RoutingAddr) -> Result<Self::Sender, RoutingError>;
}
