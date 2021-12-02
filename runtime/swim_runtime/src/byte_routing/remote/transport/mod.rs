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

mod read;
mod write;

use crate::byte_routing::routing::{RawRoute, Router};
use crate::routing::RoutingAddr;
use futures_util::future::join;
use ratchet::{SplittableExtension, WebSocket, WebSocketStream};
use swim_model::path::RelativePath;
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub struct TransportIo<S, E, R> {
    socket: WebSocket<S, E>,
    router: R,
}

impl<S, E, R> TransportIo<S, E, R>
where
    S: WebSocketStream,
    E: SplittableExtension,
    R: Router,
{
    pub fn new(socket: WebSocket<S, E>, router: R) -> TransportIo<S, E, R> {
        TransportIo { socket, router }
    }

    // todo replace with tokio::Notify
    pub async fn run(
        self,
        id: RoutingAddr,
        stop_on: trigger::Receiver,
        dl_rx: mpsc::Receiver<(RelativePath, RawRoute)>,
    ) -> Result<(), ()> {
        let TransportIo { socket, router } = self;

        let (io_tx, io_rx) = socket.split().unwrap();

        let write_task = write::write_task(io_tx, stop_on.clone());
        let read_task = read::read_task(id, router, io_rx, ReceiverStream::new(dl_rx), stop_on);

        join(write_task, read_task).await;

        Ok(())
    }
}
