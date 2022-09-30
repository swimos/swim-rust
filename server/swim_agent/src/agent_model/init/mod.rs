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

use std::future::ready;

use bytes::BytesMut;
use futures::{
    future::BoxFuture,
    stream::{unfold, BoxStream},
    FutureExt, SinkExt, Stream, StreamExt,
};
use swim_api::{
    agent::UplinkKind,
    error::{FrameIoError, InvalidFrame},
    protocol::{
        agent::{LaneRequest, LaneRequestDecoder, LaneResponse, LaneResponseEncoder},
        WithLengthBytesCodec,
    },
};
use swim_model::Text;
use swim_recon::parser::AsyncParseError;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use tokio_util::codec::{Decoder, FramedRead, FramedWrite};

const UNEXPECTED_SYNC: &str = "Sync messages are not permitted during initialization.";

fn init_stream<'a, D>(
    reader: &'a mut ByteReader,
    decoder: D,
) -> impl Stream<Item = Result<D::Item, FrameIoError>> + 'a
where
    D: Decoder + 'a,
    FrameIoError: From<D::Error>,
{
    let framed = FramedRead::new(reader, LaneRequestDecoder::new(decoder));
    unfold(Some(framed), |maybe_framed| async move {
        if let Some(mut framed) = maybe_framed {
            match framed.next().await {
                Some(Ok(LaneRequest::Command(body))) => Some((Ok(body), Some(framed))),
                Some(Ok(LaneRequest::InitComplete)) => None,
                Some(Ok(LaneRequest::Sync(_))) => Some((
                    Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                        problem: Text::new(UNEXPECTED_SYNC),
                    })),
                    None,
                )),
                Some(Err(e)) => Some((Err(e), None)),
                None => Some((Err(FrameIoError::InvalidTermination), None)),
            }
        } else {
            None
        }
    })
}

pub type InitFn<Agent> = Box<dyn FnOnce(&Agent) + Send + 'static>;

pub trait LaneInitializer<Agent, Msg> {
    fn initialize<'a>(
        self: Box<Self>,
        stream: BoxStream<'a, Result<Msg, FrameIoError>>,
    ) -> BoxFuture<'a, Result<InitFn<Agent>, AsyncParseError>>;
}

pub struct NoInit;

impl<Agent: 'static, Msg> LaneInitializer<Agent, Msg> for NoInit {
    fn initialize<'a>(
        self: Box<Self>,
        _stream: BoxStream<'a, Result<Msg, FrameIoError>>,
    ) -> BoxFuture<'a, Result<InitFn<Agent>, AsyncParseError>> {
        //TODO Temporary placeholder until macro is updated.
        let f: InitFn<Agent> = Box::new(|_| {});
        ready(Ok(f)).boxed()
    }
}

pub struct InitializedLane<'a, Agent> {
    pub name: &'a str,
    pub kind: UplinkKind,
    pub init_fn: InitFn<Agent>,
    pub io: (ByteWriter, ByteReader),
}

impl<'a, Agent> InitializedLane<'a, Agent> {
    pub fn new(
        name: &'a str,
        kind: UplinkKind,
        init_fn: InitFn<Agent>,
        io: (ByteWriter, ByteReader),
    ) -> Self {
        InitializedLane {
            name,
            kind,
            init_fn,
            io,
        }
    }
}

pub async fn run_lane_initializer<'a, Agent, D>(
    name: &'a str,
    kind: UplinkKind,
    io: (ByteWriter, ByteReader),
    decoder: D,
    init: Box<dyn LaneInitializer<Agent, D::Item> + Send + 'static>,
) -> Result<InitializedLane<'a, Agent>, FrameIoError>
where
    D: Decoder + Send,
    FrameIoError: From<D::Error>,
{
    let (mut tx, mut rx) = io;
    let stream = init_stream(&mut rx, decoder);
    match init.initialize(stream.boxed()).await {
        Err(e) => Err(e.into()),
        Ok(init_fn) => {
            let mut writer = FramedWrite::new(
                &mut tx,
                LaneResponseEncoder::new(WithLengthBytesCodec::default()),
            );
            writer
                .send(LaneResponse::<BytesMut>::Initialized)
                .await
                .map_err(FrameIoError::Io)
                .map(move |_| InitializedLane::new(name, kind, init_fn, (tx, rx)))
        }
    }
}
