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

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

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
        map::MapMessage,
        WithLengthBytesCodec,
    },
};
use swim_form::structural::read::{recognizer::RecognizerReadable, ReadError};
use swim_model::Text;
use swim_recon::parser::{AsyncParseError, ParseError, RecognizerDecoder};
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use tokio_util::codec::{Decoder, FramedRead, FramedWrite};

use crate::lanes::{MapLane, ValueLane};

#[cfg(test)]
mod tests;

const UNEXPECTED_SYNC: &str = "Sync messages are not permitted during initialization.";

/// A stream that will consume command messages from the channel for a lane until an `InitComplete`
/// message is received. It is invalid to receive a `Sync` message while the lane is initializing
/// so this will result in the stream failing with an error.
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

/// A function that will initialize the state of one of the lanes of the agent.
pub type InitFn<Agent> = Box<dyn FnOnce(&Agent) + Send + 'static>;

/// A lane initializer consumers a stream of commands from the runtime and creates a function
/// that will initialize a lane on the instance of the agent.
pub trait LaneInitializer<Agent, Msg> {
    fn initialize(
        self: Box<Self>,
        stream: BoxStream<'_, Result<Msg, FrameIoError>>,
    ) -> BoxFuture<'_, Result<InitFn<Agent>, FrameIoError>>;
}

/// The result of running the initialization process for a new lane.
pub struct InitializedLane<'a, Agent> {
    // The name of the lane.
    pub name: &'a str,
    // The uplink kind for communication with the runtime.
    pub kind: UplinkKind,
    // Function to initialize the state of the lane in the agent.
    pub init_fn: InitFn<Agent>,
    // Channels for communication with the runtime.
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

/// Run the initialization process for a lane.
///
/// #Arguments
/// * `name` - The name of the lane.
/// * `kind` - The uplink kind (determines the protocol for communication with the runtime).
/// * `io` - Channels for commmunication with the runtime.
/// * `decoder` - Decoder to interpret the command messages from the runtime, during the
/// initialization process.
/// * `init` - Initializer to consume the incoming command and assemble the initial state of the lane.
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
        Err(e) => Err(e),
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

/// [`LaneInitializer`] to construct the state of a value lane.
pub struct ValueLaneInitializer<Agent, T> {
    projection: fn(&Agent) -> &ValueLane<T>,
}

impl<Agent, T> ValueLaneInitializer<Agent, T> {
    pub fn new(projection: fn(&Agent) -> &ValueLane<T>) -> Self {
        ValueLaneInitializer { projection }
    }
}

/// [`LaneInitializer`] to construct the state of a map lane.
pub struct MapLaneInitializer<Agent, K, V> {
    projection: fn(&Agent) -> &MapLane<K, V>,
}

impl<Agent, K, V> MapLaneInitializer<Agent, K, V> {
    pub fn new(projection: fn(&Agent) -> &MapLane<K, V>) -> Self {
        MapLaneInitializer { projection }
    }
}

impl<Agent, T> LaneInitializer<Agent, BytesMut> for ValueLaneInitializer<Agent, T>
where
    Agent: 'static,
    T: RecognizerReadable + Send + 'static,
{
    fn initialize(
        self: Box<Self>,
        mut stream: BoxStream<'_, Result<BytesMut, FrameIoError>>,
    ) -> BoxFuture<'_, Result<InitFn<Agent>, FrameIoError>> {
        let ValueLaneInitializer { projection } = *self;
        async move {
            let mut body = None;
            while let Some(result) = stream.next().await {
                body = Some(result?);
            }
            if let Some(mut body) = body {
                let mut decoder = RecognizerDecoder::new(T::make_recognizer());
                if let Some(value) = decoder.decode_eof(&mut body)? {
                    let f = move |agent: &Agent| projection(agent).init(value);
                    let f_init: InitFn<Agent> = Box::new(f);
                    Ok(f_init)
                } else {
                    Err(
                        AsyncParseError::Parser(ParseError::Structure(ReadError::IncompleteRecord))
                            .into(),
                    )
                }
            } else {
                let f = move |_: &Agent| {};
                let f_init: InitFn<Agent> = Box::new(f);
                Ok(f_init)
            }
        }
        .boxed()
    }
}

impl<Agent, K, V> LaneInitializer<Agent, MapMessage<BytesMut, BytesMut>>
    for MapLaneInitializer<Agent, K, V>
where
    Agent: 'static,
    K: RecognizerReadable + Hash + Eq + Ord + Clone + Send + 'static,
    K::Rec: Send,
    V: RecognizerReadable + Send + 'static,
    V::Rec: Send,
{
    fn initialize(
        self: Box<Self>,
        mut stream: BoxStream<'_, Result<MapMessage<BytesMut, BytesMut>, FrameIoError>>,
    ) -> BoxFuture<'_, Result<InitFn<Agent>, FrameIoError>> {
        let MapLaneInitializer { projection } = *self;
        async move {
            let mut key_decoder = RecognizerDecoder::new(K::make_recognizer());
            let mut value_decoder = RecognizerDecoder::new(V::make_recognizer());
            let mut map = BTreeMap::new();
            while let Some(message) = stream.next().await {
                match message? {
                    MapMessage::Update { mut key, mut value } => {
                        let key = init_decode(&mut key_decoder, &mut key)?;
                        let value = init_decode(&mut value_decoder, &mut value)?;
                        map.insert(key, value);
                    }
                    MapMessage::Remove { mut key } => {
                        let key = init_decode(&mut key_decoder, &mut key)?;
                        map.remove(&key);
                    }
                    MapMessage::Clear => {
                        map.clear();
                    }
                    MapMessage::Take(n) => {
                        let to_take = usize::try_from(n).expect("Number to take too large.");
                        let to_remove = map.len().saturating_sub(to_take);
                        if to_remove > 0 {
                            for k in map
                                .keys()
                                .rev()
                                .take(to_remove)
                                .cloned()
                                .collect::<Vec<_>>()
                            {
                                map.remove(&k);
                            }
                        }
                    }
                    MapMessage::Drop(n) => {
                        let to_remove = usize::try_from(n).expect("Number to drop too large.");
                        if to_remove > 0 {
                            for k in map.keys().take(to_remove).cloned().collect::<Vec<_>>() {
                                map.remove(&k);
                            }
                        }
                    }
                }
            }
            let map_init = map.into_iter().collect::<HashMap<_, _>>();
            let f = move |agent: &Agent| projection(agent).init(map_init);
            let f_init: InitFn<Agent> = Box::new(f);
            Ok(f_init)
        }
        .boxed()
    }
}

fn init_decode<D>(decoder: &mut D, bytes: &mut BytesMut) -> Result<D::Item, AsyncParseError>
where
    D: Decoder<Error = AsyncParseError>,
{
    if let Some(value) = decoder.decode_eof(bytes)? {
        Ok(value)
    } else {
        Err(AsyncParseError::Parser(ParseError::Structure(
            ReadError::IncompleteRecord,
        )))
    }
}
