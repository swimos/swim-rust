// Copyright 2015-2023 Swim Inc.
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

use bytes::Bytes;
use futures::SinkExt;
use swimos_agent_protocol::{
    encoding::lane::{RawMapLaneRequestEncoder, RawValueLaneRequestEncoder},
    peeling::extract_header,
    LaneRequest, MapMessage,
};
use swimos_api::agent::UplinkKind;
use swimos_recon::parser::MessageExtractError;
use swimos_utilities::byte_channel::ByteWriter;
use thiserror::Error;
use tokio_util::codec::{Encoder, FramedWrite};
use uuid::Uuid;

use crate::agent::reporting::UplinkReporter;

type ValueLaneEncoder = RawValueLaneRequestEncoder;
type MapLaneEncoder = RawMapLaneRequestEncoder;

/// Type of errors that can occur attempting to forward an incoming message to a lane.
#[derive(Debug, Error)]
pub enum LaneSendError {
    /// The lane failed to receive the data.
    #[error("Sending lane message failed: {0}")]
    Io(#[from] std::io::Error),
    /// The incoming message was not valid according to the sub-protocol used by the lane.
    #[error("Interpreting lane message failed: {0}")]
    Extraction(#[from] MessageExtractError),
}

/// Sender to communicate with a lane.
#[derive(Debug)]
enum LaneSenderWriter {
    Value {
        sender: FramedWrite<ByteWriter, ValueLaneEncoder>,
    },
    Map {
        sender: FramedWrite<ByteWriter, MapLaneEncoder>,
    },
}

pub struct LaneSender {
    writer: LaneSenderWriter,
    reporter: Option<UplinkReporter>,
}

impl LaneSender {
    pub fn new(tx: ByteWriter, kind: UplinkKind, reporter: Option<UplinkReporter>) -> Self {
        let writer = match kind {
            UplinkKind::Value | UplinkKind::Supply => LaneSenderWriter::Value {
                sender: FramedWrite::new(tx, RawValueLaneRequestEncoder::default()),
            },
            UplinkKind::Map => LaneSenderWriter::Map {
                sender: FramedWrite::new(tx, RawMapLaneRequestEncoder::default()),
            },
        };
        LaneSender { writer, reporter }
    }

    pub async fn start_sync(&mut self, id: Uuid) -> Result<(), std::io::Error> {
        match &mut self.writer {
            LaneSenderWriter::Value { sender } => {
                let req: LaneRequest<Bytes> = LaneRequest::Sync(id);
                sender.send(req).await
            }
            LaneSenderWriter::Map { sender } => {
                let req: LaneRequest<MapMessage<Bytes, Bytes>> = LaneRequest::Sync(id);
                sender.send(req).await
            }
        }
    }

    pub async fn feed_frame(&mut self, data: Bytes) -> Result<(), LaneSendError> {
        let LaneSender { writer, reporter } = self;
        if let Some(reporter) = reporter {
            reporter.count_commands(1);
        }
        match writer {
            LaneSenderWriter::Value { sender } => {
                sender.feed(LaneRequest::Command(data)).await?;
            }
            LaneSenderWriter::Map { sender } => {
                let message = extract_header(&data)?;
                sender.send(LaneRequest::Command(message)).await?;
            }
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), std::io::Error> {
        match &mut self.writer {
            LaneSenderWriter::Value { sender } => flush_sender_val(sender).await,
            LaneSenderWriter::Map { sender } => flush_sender_map(sender).await,
        }
    }
}

async fn flush_sender_val<T>(sender: &mut FramedWrite<ByteWriter, T>) -> Result<(), T::Error>
where
    T: Encoder<LaneRequest<Bytes>>,
{
    sender.flush().await
}

async fn flush_sender_map<T>(sender: &mut FramedWrite<ByteWriter, T>) -> Result<(), T::Error>
where
    T: Encoder<LaneRequest<MapMessage<Bytes, Bytes>>>,
{
    sender.flush().await
}
