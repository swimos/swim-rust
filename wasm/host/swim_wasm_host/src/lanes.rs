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

use bytes::BytesMut;
use futures::Stream;
use futures_util::future::BoxFuture;
use futures_util::stream::unfold;
use futures_util::{SinkExt, StreamExt};
use tokio_util::codec::{Decoder, FramedRead, FramedWrite};
use tracing::debug;

use swim_api::agent::AgentContext;
use swim_api::error::{AgentRuntimeError, FrameIoError};
use swim_api::lane::WarpLaneKind;
use swim_api::protocol::agent::{
    StoreInitMessage, StoreInitMessageDecoder, StoreInitialized, StoreInitializedCodec,
};
use swim_model::Text;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use wasm_ir::{LaneKindRepr, LaneSpec};

use crate::codec::LaneReader;
use crate::GuestEnvironment;

pub enum OpenLaneError {
    AgentRuntime(AgentRuntimeError),
    FrameIo(FrameIoError),
}

impl From<AgentRuntimeError> for OpenLaneError {
    fn from(value: AgentRuntimeError) -> Self {
        OpenLaneError::AgentRuntime(value)
    }
}

impl From<FrameIoError> for OpenLaneError {
    fn from(value: FrameIoError) -> Self {
        OpenLaneError::FrameIo(value)
    }
}

trait GuestItemInitializer {
    fn initialize<'s, S>(&'s mut self, stream: S) -> BoxFuture<Result<(), FrameIoError>>
    where
        S: Stream<Item = Result<BytesMut, FrameIoError>> + Send + 's;
}

struct ValueItemInitializer;

impl GuestItemInitializer for ValueItemInitializer {
    fn initialize<'s, S>(&'s mut self, _stream: S) -> BoxFuture<Result<(), FrameIoError>>
    where
        S: Stream<Item = Result<BytesMut, FrameIoError>> + Send + 's,
    {
        todo!()
    }
}

struct MapItemInitializer;

impl GuestItemInitializer for MapItemInitializer {
    fn initialize<'s, S>(&'s mut self, _stream: S) -> BoxFuture<Result<(), FrameIoError>>
    where
        S: Stream<Item = Result<BytesMut, FrameIoError>> + Send + 's,
    {
        todo!()
    }
}

fn map_lane_kind(value: LaneKindRepr) -> WarpLaneKind {
    match value {
        LaneKindRepr::Command => WarpLaneKind::Command,
        LaneKindRepr::Demand => WarpLaneKind::Demand,
        LaneKindRepr::DemandMap => WarpLaneKind::DemandMap,
        LaneKindRepr::Map => WarpLaneKind::Map,
        LaneKindRepr::JoinMap => WarpLaneKind::JoinMap,
        LaneKindRepr::JoinValue => WarpLaneKind::JoinValue,
        LaneKindRepr::Supply => WarpLaneKind::Supply,
        LaneKindRepr::Spatial => WarpLaneKind::Spatial,
        LaneKindRepr::Value => WarpLaneKind::Value,
    }
}

pub async fn open_lane(
    spec: LaneSpec,
    uri: &str,
    context: &Box<dyn AgentContext + Send>,
    agent_environment: &mut GuestEnvironment,
) -> Result<(), OpenLaneError> {
    let GuestEnvironment {
        config,
        lane_identifiers,
        lane_readers,
        lane_writers,
        ..
    } = agent_environment;
    let LaneSpec {
        is_transient,
        lane_idx,
        lane_kind_repr,
    } = spec;

    let kind = map_lane_kind(lane_kind_repr);
    debug!(uri, lane_kind = ?lane_kind_repr, is_transient, "Adding lane");

    let text_uri: Text = uri.into();
    let mut lane_conf = config.default_lane_config.unwrap_or_default();
    lane_conf.transient = is_transient;

    let (tx, rx) = context.add_lane(text_uri.as_str(), kind, lane_conf).await?;

    if is_transient {
        let reader = if lane_kind_repr.map_like() {
            LaneReader::map(lane_idx, rx)
        } else {
            LaneReader::value(lane_idx, rx)
        };
        lane_readers.push(reader);
        lane_writers.insert(lane_idx, tx);
    } else {
        // let (reader, id, tx) = if lane_kind_repr.map_like() {
        //     let InitializedLane { io: (tx, rx), id } = run_lane_initializer(
        //         MapLikeLaneGuestInitializer::new(guest_agent, spec.lane_idx),
        //         (tx, rx),
        //         WithLengthBytesCodec::default(),
        //         lane_idx,
        //     )
        //     .await?;
        //
        //     (LaneReader::map(id, rx), id, tx)
        // } else {
        //     let InitializedLane { io: (tx, rx), id } = run_lane_initializer(
        //         ValueLikeLaneGuestInitializer::new(guest_agent, spec.lane_idx),
        //         (tx, rx),
        //         WithLengthBytesCodec::default(),
        //         lane_idx,
        //     )
        //     .await?;
        //     (LaneReader::value(id, rx), id, tx)
        // };
        //
        // lane_readers.push(reader);
        // lane_writers.insert(id, tx);
        unimplemented!()
    }

    lane_identifiers.insert(spec.lane_idx, text_uri);

    Ok(())
}

struct InitializedLane {
    io: (ByteWriter, ByteReader),
    id: u64,
}

fn init_stream<'a, D>(
    reader: &'a mut ByteReader,
    decoder: D,
) -> impl Stream<Item = Result<D::Item, FrameIoError>> + 'a
where
    D: Decoder + 'a,
    FrameIoError: From<D::Error>,
{
    let framed = FramedRead::new(reader, StoreInitMessageDecoder::new(decoder));
    unfold(Some(framed), |maybe_framed| async move {
        if let Some(mut framed) = maybe_framed {
            match framed.next().await {
                Some(Ok(StoreInitMessage::Command(body))) => Some((Ok(body), Some(framed))),
                Some(Ok(StoreInitMessage::InitComplete)) => None,
                Some(Err(e)) => Some((Err(e), None)),
                None => Some((Err(FrameIoError::InvalidTermination), None)),
            }
        } else {
            None
        }
    })
}

async fn run_lane_initializer<'a, I, D>(
    mut initializer: I,
    io: (ByteWriter, ByteReader),
    decoder: D,
    id: u64,
) -> Result<InitializedLane, FrameIoError>
where
    D: Decoder<Item = BytesMut> + Send,
    I: GuestItemInitializer,
    FrameIoError: From<D::Error>,
{
    let (mut tx, mut rx) = io;
    let stream = init_stream(&mut rx, decoder);
    match initializer.initialize(stream).await {
        Ok(()) => {
            let mut writer = FramedWrite::new(&mut tx, StoreInitializedCodec);
            writer
                .send(StoreInitialized)
                .await
                .map_err(|e| FrameIoError::Io(e))
                .map(move |_| InitializedLane { io: (tx, rx), id })
        }
        Err(e) => Err(e),
    }
}
