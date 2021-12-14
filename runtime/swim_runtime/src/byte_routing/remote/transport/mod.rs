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
use crate::byte_routing::remote::TransportConfiguration;
use crate::byte_routing::routing::router::RawServerRouter;
use crate::byte_routing::routing::RawRoute;
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

type AttachmentChannel = mpsc::Receiver<Attachment>;
type DownlinkChannel = FramedRead<ByteReader, RawResponseMessageDecoder>;
type AgentChannel = FramedRead<ByteReader, AgentMessageDecoder<Value, ValueMaterializer>>;

pub enum Attachment {
    Agent(AgentChannel),
    Downlink(DownlinkChannel),
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("Transport read error: `{0}`")]
    Read(#[from] ReadError),
    #[error("Transport write error: `{0}`")]
    Write(#[from] WriteError),
}

pub struct TransportIo<S, E> {
    configuration: TransportConfiguration,
    socket: WebSocket<S, E>,
    router: RawServerRouter,
    attachments: AttachmentChannel,
    downlink_read: mpsc::Receiver<(RelativePath, RawRoute)>,
}

impl<S, E> TransportIo<S, E>
where
    S: WebSocketStream,
    E: SplittableExtension,
{
    pub fn new(
        configuration: TransportConfiguration,
        socket: WebSocket<S, E>,
        router: RawServerRouter,
        attachments: AttachmentChannel,
        downlink_read: mpsc::Receiver<(RelativePath, RawRoute)>,
    ) -> TransportIo<S, E> {
        TransportIo {
            configuration,
            socket,
            router,
            attachments,
            downlink_read,
        }
    }

    pub async fn run(
        self,
        id: RoutingAddr,
        stop_on: trigger::Receiver,
    ) -> Result<(), TransportError> {
        let TransportIo {
            configuration,
            socket,
            router,
            attachments,
            downlink_read,
        } = self;
        let TransportConfiguration {
            chunk_after,
            timeout,
            reap_after,
        } = configuration;

        let (io_tx, io_rx) = socket
            .split()
            .map_err(|e| TransportError::Read(ReadError::WebSocket(e)))?;
        let write_task = write::task(io_tx, chunk_after, attachments, stop_on.clone());
        let read_task = read::task(
            id,
            router,
            io_rx,
            ReceiverStream::new(downlink_read),
            stop_on,
        );

        try_join(
            write_task.map_err(Into::into),
            read_task.map_err(Into::into),
        )
        .await
        .map(|_| ())
    }
}
