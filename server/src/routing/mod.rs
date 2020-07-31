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

use common::warp::envelope::Envelope;
use common::sink::item::ItemSender;
use common::routing::RoutingError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Location {
    RemoteEndpoint(u32),
    Local(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutingAddr(Location);

#[derive(Debug, Clone, PartialEq)]
pub struct TaggedEnvelope(pub RoutingAddr, pub Envelope);

pub trait ServerRouter {

    type Sender: ItemSender<Envelope, RoutingError>;

    fn get_sender(&mut self, addr: RoutingAddr) -> Result<Self::Sender, RoutingError>;

}