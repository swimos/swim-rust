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

#[cfg(test)]
mod tests;

mod read;
mod write;

use crate::byte_routing::remote::transport::read::ReadError;
use crate::byte_routing::remote::transport::write::WriteError;
use crate::byte_routing::routing::{RawRoute, Router};
use crate::compat::{AgentMessageDecoder, RawResponseMessageDecoder};
use crate::routing::RoutingAddr;
use futures_util::future::try_join;
use futures_util::TryFutureExt;
use ratchet::{SplittableExtension, WebSocket, WebSocketStream};
use swim_form::structural::read::from_model::ValueMaterializer;
use swim_model::path::RelativePath;
use swim_model::Value;
use swim_utilities::io::byte_channel::ByteReader;
use swim_utilities::trigger;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::FramedRead;

type AttachmentChannel<D> = mpsc::Receiver<FramedRead<ByteReader, D>>;
type DownlinkChannel = AttachmentChannel<RawResponseMessageDecoder>;
type AgentChannel = AttachmentChannel<AgentMessageDecoder<Value, ValueMaterializer>>;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("Transport read error: `{0}`")]
    Read(#[from] ReadError),
    #[error("Transport write error: `{0}`")]
    Write(#[from] WriteError),
}

pub struct TransportIo<S, E> {
    socket: WebSocket<S, E>,
    router: Router,
    downlinks: DownlinkChannel,
    agents: AgentChannel,
}

impl<S, E> TransportIo<S, E>
where
    S: WebSocketStream,
    E: SplittableExtension,
{
    pub fn new(
        socket: WebSocket<S, E>,
        router: Router,
        downlinks: DownlinkChannel,
        agents: AgentChannel,
    ) -> TransportIo<S, E> {
        TransportIo {
            socket,
            router,
            downlinks,
            agents,
        }
    }

    // todo replace with tokio::Notify
    pub async fn run(
        self,
        id: RoutingAddr,
        stop_on: trigger::Receiver,
        dl_rx: mpsc::Receiver<(RelativePath, RawRoute)>,
    ) -> Result<(), TransportError> {
        let TransportIo {
            socket,
            router,
            downlinks,
            agents,
        } = self;

        let (io_tx, io_rx) = socket
            .split()
            .map_err(|e| TransportError::Read(ReadError::WebSocket(e)))?;
        let write_task = write::write_task(io_tx, downlinks, agents, stop_on.clone());
        let read_task = read::read_task(id, router, io_rx, ReceiverStream::new(dl_rx), stop_on);

        try_join(
            write_task.map_err(Into::into),
            read_task.map_err(Into::into),
        )
        .await
        .map(|_| ())
    }
}
