// Copyright 2015-2024 Swim Inc.
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
use swimos_agent_protocol::{
    encoding::store::StoreInitializedCodec, MapMessage, StoreInitMessage, StoreInitialized,
};
use swimos_api::error::FrameIoError;
use swimos_form::read::{ReadError, RecognizerReadable};
use swimos_recon::parser::{AsyncParseError, ParseError, RecognizerDecoder};
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};
use swimos_utilities::future::try_last;
use tokio_util::codec::{Decoder, FramedRead, FramedWrite};

use crate::item::{MapItem, ValueItem};
use crate::lanes::{MapLane, ValueLane};
use crate::map_storage::MapOps;
use crate::stores::value::ValueStore;
use crate::stores::MapStore;

use super::ItemKind;

#[cfg(test)]
mod tests;

/// A stream that will consume command messages from the channel for a lane until an `InitComplete`
/// message is received. It is invalid to receive a `Sync` message while the lane is initializing
/// so this will result in the stream failing with an error.
fn init_stream<'a, D, M>(
    reader: &'a mut ByteReader,
    decoder: D,
) -> impl Stream<Item = Result<M, FrameIoError>> + 'a
where
    D: Decoder<Item = StoreInitMessage<M>> + 'a,
    FrameIoError: From<D::Error>,
{
    let framed = FramedRead::new(reader, decoder);
    unfold(Some(framed), |maybe_framed| async move {
        if let Some(mut framed) = maybe_framed {
            match framed.next().await {
                Some(Ok(StoreInitMessage::Command(body))) => Some((Ok(body), Some(framed))),
                Some(Ok(StoreInitMessage::InitComplete)) => None,
                Some(Err(e)) => Some((Err(e.into()), None)),
                None => Some((Err(FrameIoError::InvalidTermination), None)),
            }
        } else {
            None
        }
    })
}

/// A function that will initialize the state of one of the lanes of the agent.
pub type InitFn<Agent> = Box<dyn FnOnce(&Agent) + Send + 'static>;

/// An item initializer consumers a stream of commands from the runtime and creates a function
/// that will initialize a item on the instance of the agent.
pub trait ItemInitializer<Agent, Msg> {
    fn initialize(
        self: Box<Self>,
        stream: BoxStream<'_, Result<Msg, FrameIoError>>,
    ) -> BoxFuture<'_, Result<InitFn<Agent>, FrameIoError>>;
}

/// The result of running the initialization process for a new item.
pub struct InitializedItem<'a, Agent> {
    pub item_kind: ItemKind,
    // The name of the lane.
    pub name: &'a str,
    // Function to initialize the state of the lane in the agent.
    pub init_fn: InitFn<Agent>,
    // Channels for communication with the runtime.
    pub io: (ByteWriter, ByteReader),
}

impl<'a, Agent> InitializedItem<'a, Agent> {
    pub fn new(
        item_kind: ItemKind,
        name: &'a str,
        init_fn: InitFn<Agent>,
        io: (ByteWriter, ByteReader),
    ) -> Self {
        InitializedItem {
            item_kind,
            name,
            init_fn,
            io,
        }
    }
}

/// Run the initialization process for a lane.
///
/// # Arguments
/// * `name` - The name of the lane.
/// * `io` - Channels for communication with the runtime.
/// * `decoder` - Decoder to interpret the command messages from the runtime, during the
///    initialization process.
/// * `init` - Initializer to consume the incoming command and assemble the initial state of the lane.
pub async fn run_item_initializer<'a, Agent, D, M>(
    item_kind: ItemKind,
    name: &'a str,
    io: (ByteWriter, ByteReader),
    decoder: D,
    init: Box<dyn ItemInitializer<Agent, M> + Send + 'static>,
) -> Result<InitializedItem<'a, Agent>, FrameIoError>
where
    D: Decoder<Item = StoreInitMessage<M>> + Send,
    FrameIoError: From<D::Error>,
{
    let (mut tx, mut rx) = io;
    let stream = init_stream(&mut rx, decoder);
    match init.initialize(stream.boxed()).await {
        Err(e) => Err(e),
        Ok(init_fn) => {
            let mut writer = FramedWrite::new(&mut tx, StoreInitializedCodec);
            writer
                .send(StoreInitialized)
                .await
                .map_err(FrameIoError::Io)
                .map(move |_| InitializedItem::new(item_kind, name, init_fn, (tx, rx)))
        }
    }
}

/// [`ItemInitializer`] to construct the state of a value lane.
pub struct ValueLaneInitializer<Agent, T> {
    projection: fn(&Agent) -> &ValueLane<T>,
}

impl<Agent, T> ValueLaneInitializer<Agent, T> {
    pub fn new(projection: fn(&Agent) -> &ValueLane<T>) -> Self {
        ValueLaneInitializer { projection }
    }
}

/// [`ItemInitializer`] to construct the state of a value store.
pub struct ValueStoreInitializer<Agent, T> {
    projection: fn(&Agent) -> &ValueStore<T>,
}

impl<Agent, T> ValueStoreInitializer<Agent, T> {
    pub fn new(projection: fn(&Agent) -> &ValueStore<T>) -> Self {
        ValueStoreInitializer { projection }
    }
}

/// [`ItemInitializer`] to construct the state of a map lane.
pub struct MapLaneInitializer<Agent, K, V, M = HashMap<K, V>> {
    projection: fn(&Agent) -> &MapLane<K, V, M>,
}

impl<Agent, K, V, M> MapLaneInitializer<Agent, K, V, M> {
    pub fn new(projection: fn(&Agent) -> &MapLane<K, V, M>) -> Self {
        MapLaneInitializer { projection }
    }
}

/// [`ItemInitializer`] to construct the state of a map store.
pub struct MapStoreInitializer<Agent, K, V, M = HashMap<K, V>> {
    projection: fn(&Agent) -> &MapStore<K, V, M>,
}

impl<Agent, K, V, M> MapStoreInitializer<Agent, K, V, M> {
    pub fn new(projection: fn(&Agent) -> &MapStore<K, V, M>) -> Self {
        MapStoreInitializer { projection }
    }
}

async fn value_like_init<Agent, F, T>(
    stream: BoxStream<'_, Result<BytesMut, FrameIoError>>,
    init: F,
) -> Result<InitFn<Agent>, FrameIoError>
where
    Agent: 'static,
    T: RecognizerReadable + Send + 'static,
    F: FnOnce(&Agent, T) + Send + 'static,
{
    let body = try_last(stream).await?;
    if let Some(mut body) = body {
        let mut decoder = RecognizerDecoder::new(T::make_recognizer());
        if let Some(value) = decoder.decode_eof(&mut body)? {
            let f = move |agent: &Agent| init(agent, value);
            let f_init: InitFn<Agent> = Box::new(f);
            Ok(f_init)
        } else {
            Err(AsyncParseError::Parser(ParseError::Structure(ReadError::IncompleteRecord)).into())
        }
    } else {
        let f = move |_: &Agent| {};
        let f_init: InitFn<Agent> = Box::new(f);
        Ok(f_init)
    }
}

impl<Agent, T> ItemInitializer<Agent, BytesMut> for ValueLaneInitializer<Agent, T>
where
    Agent: 'static,
    T: RecognizerReadable + Send + 'static,
{
    fn initialize(
        self: Box<Self>,
        stream: BoxStream<'_, Result<BytesMut, FrameIoError>>,
    ) -> BoxFuture<'_, Result<InitFn<Agent>, FrameIoError>> {
        let ValueLaneInitializer { projection } = *self;
        value_like_init(stream, move |agent, value| projection(agent).init(value)).boxed()
    }
}

impl<Agent, T> ItemInitializer<Agent, BytesMut> for ValueStoreInitializer<Agent, T>
where
    Agent: 'static,
    T: RecognizerReadable + Send + 'static,
{
    fn initialize(
        self: Box<Self>,
        stream: BoxStream<'_, Result<BytesMut, FrameIoError>>,
    ) -> BoxFuture<'_, Result<InitFn<Agent>, FrameIoError>> {
        let ValueStoreInitializer { projection } = *self;
        value_like_init(stream, move |agent, value| projection(agent).init(value)).boxed()
    }
}

async fn map_like_init<Agent, Item, K, V, M, F>(
    mut stream: BoxStream<'_, Result<MapMessage<BytesMut, BytesMut>, FrameIoError>>,
    projection: F,
) -> Result<InitFn<Agent>, FrameIoError>
where
    Agent: 'static,
    K: Eq + Hash + Ord + Clone + RecognizerReadable + Send + 'static,
    V: RecognizerReadable + Send + 'static,
    Item: MapItem<K, V, M> + 'static,
    F: Fn(&Agent) -> &Item + Send + 'static,
    M: MapOps<K, V>,
{
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
                if to_remove >= map.len() {
                    map.clear()
                } else if to_remove > 0 {
                    for k in map.keys().take(to_remove).cloned().collect::<Vec<_>>() {
                        map.remove(&k);
                    }
                }
            }
        }
    }
    let f = move |agent: &Agent| projection(agent).init(M::from_entries(map));
    let f_init: InitFn<Agent> = Box::new(f);
    Ok(f_init)
}

impl<Agent, K, V, M> ItemInitializer<Agent, MapMessage<BytesMut, BytesMut>>
    for MapLaneInitializer<Agent, K, V, M>
where
    Agent: 'static,
    K: RecognizerReadable + Hash + Eq + Ord + Clone + Send + 'static,
    K::Rec: Send,
    V: RecognizerReadable + Send + 'static,
    V::Rec: Send,
    M: MapOps<K, V> + 'static,
{
    fn initialize(
        self: Box<Self>,
        stream: BoxStream<'_, Result<MapMessage<BytesMut, BytesMut>, FrameIoError>>,
    ) -> BoxFuture<'_, Result<InitFn<Agent>, FrameIoError>> {
        let MapLaneInitializer { projection } = *self;
        map_like_init(stream, projection).boxed()
    }
}

impl<Agent, K, V, M> ItemInitializer<Agent, MapMessage<BytesMut, BytesMut>>
    for MapStoreInitializer<Agent, K, V, M>
where
    Agent: 'static,
    K: RecognizerReadable + Hash + Eq + Ord + Clone + Send + 'static,
    K::Rec: Send,
    V: RecognizerReadable + Send + 'static,
    V::Rec: Send,
    M: MapOps<K, V> + 'static,
{
    fn initialize(
        self: Box<Self>,
        stream: BoxStream<'_, Result<MapMessage<BytesMut, BytesMut>, FrameIoError>>,
    ) -> BoxFuture<'_, Result<InitFn<Agent>, FrameIoError>> {
        let MapStoreInitializer { projection } = *self;
        map_like_init(stream, projection).boxed()
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
