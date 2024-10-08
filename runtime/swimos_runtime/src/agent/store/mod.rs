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

use bytes::BytesMut;
use futures::{future::BoxFuture, FutureExt, SinkExt};
use std::fmt::Debug;
use swimos_agent_protocol::{
    encoding::store::{RawMapStoreInitEncoder, RawValueStoreInitEncoder},
    MapMessage, MapOperation, StoreInitMessage,
};
use swimos_api::{
    error::StoreError,
    persistence::{NodePersistence, RangeConsumer, StoreDisabled},
};
use swimos_model::Text;
use swimos_utilities::byte_channel::ByteWriter;
use thiserror::Error;
use tokio_util::codec::FramedWrite;

use super::AgentExecError;

#[cfg(test)]
mod tests;

#[derive(Debug, Error)]
#[error("Failed to initialize item named {name}.")]
pub struct AgentItemInitError {
    pub name: Text,
    #[source]
    pub source: StoreInitError,
}

impl From<AgentItemInitError> for AgentExecError {
    fn from(error: AgentItemInitError) -> Self {
        let AgentItemInitError { name, source } = error;
        AgentExecError::FailedRestoration {
            item_name: name,
            error: source,
        }
    }
}

impl AgentItemInitError {
    pub fn new(name: Text, source: StoreInitError) -> Self {
        AgentItemInitError { name, source }
    }
}

/// Possible error conditions when the runtime attempts to initialize the state of a lane in an agent.
#[derive(Debug, Error)]
pub enum StoreInitError {
    #[error("An error occurred reading the state from the store.")]
    Store(#[from] StoreError),
    #[error("An error occurred sending the state to the agent implementation.")]
    Channel(#[from] std::io::Error),
    #[error("The item did not acknowledge initialization.")]
    NoAckFromItem,
    #[error("Attempting to initialize an item timed out.")]
    ItemInitializationTimeout,
}

pub type InitFut<'a> = BoxFuture<'a, Result<(), StoreInitError>>;

/// An initializer will attempt to transmit the state of the lane to the lane implementation
/// over a channel. Typically, the value will be read from a store implementation.
pub trait Initializer<'a> {
    /// Attempt to initialize the state of the lane.
    fn initialize<'b>(self: Box<Self>, writer: &'b mut ByteWriter) -> InitFut<'b>
    where
        'a: 'b;
}

/// [`Initializer`] that attempts to initialize a value lane from a [`NodePersistence`] store.
struct ValueInit<'a, S, Id> {
    store: &'a S,
    store_id: Id,
}

impl<'a, S> Initializer<'a> for ValueInit<'a, S, S::LaneId>
where
    S: NodePersistence + Send + Sync + 'static,
{
    fn initialize<'b>(self: Box<Self>, channel: &'b mut ByteWriter) -> InitFut<'b>
    where
        'a: 'b,
    {
        async move {
            let ValueInit { store, store_id } = *self;
            let mut writer = FramedWrite::new(channel, RawValueStoreInitEncoder::default());
            let mut buffer = BytesMut::new();
            if store.get_value(store_id, &mut buffer)?.is_some() {
                writer.send(StoreInitMessage::Command(buffer)).await?;
            }
            writer.send(StoreInitMessage::<&[u8]>::InitComplete).await?;
            Ok(())
        }
        .boxed()
    }
}

/// [`Initializer`] that attempts to initialize a map lane from a [`NodePersistence`] store.
struct MapInit<'a, S, Id> {
    store: &'a S,
    store_id: Id,
}

impl<'a, S> Initializer<'a> for MapInit<'a, S, S::LaneId>
where
    S: NodePersistence + Send + Sync + 'static,
{
    fn initialize<'b>(self: Box<Self>, channel: &'b mut ByteWriter) -> InitFut<'b>
    where
        'a: 'b,
    {
        async move {
            let MapInit { store, store_id } = *self;
            let mut writer = FramedWrite::new(channel, RawMapStoreInitEncoder::default());
            let mut it = store.read_map(store_id)?;
            while let Some((key, value)) = it.consume_next()? {
                writer
                    .send(StoreInitMessage::Command(MapMessage::Update { key, value }))
                    .await?;
            }
            writer
                .send(StoreInitMessage::<MapMessage<&[u8], &[u8]>>::InitComplete)
                .await?;
            Ok(())
        }
        .boxed()
    }
}

pub type BoxInitializer<'a> = Box<dyn Initializer<'a> + Send + 'a>;

/// Operations required by an agent to interact with a store of the state of its lanes.
pub trait AgentPersistence {
    /// Type of IDs assigned to the names of the items of the agent.
    type StoreId: Debug + Copy + Unpin + Send + Sync + Eq + 'static;

    /// Attempt to get the ID associated with a store name.
    fn store_id(&self, name: &str) -> Result<Self::StoreId, StoreError>;

    /// If a state for a value store exists in the store, create an initializer that
    /// will communicate that state to the store.
    fn init_value_store(&self, _store_id: Self::StoreId) -> Option<BoxInitializer<'_>>;

    /// If a state for a map store exists in the store, create an initializer that
    /// will communicate that state to the store.
    fn init_map_store(&self, _store_id: Self::StoreId) -> Option<BoxInitializer<'_>>;

    /// Put a value from a value store into the store.
    fn put_value(&mut self, store_id: Self::StoreId, bytes: &[u8]) -> Result<(), StoreError>;

    /// Apply an operation from a map store into the store.
    fn apply_map<B: AsRef<[u8]>>(
        &mut self,
        store_id: Self::StoreId,
        op: &MapOperation<B, B>,
    ) -> Result<(), StoreError>;
}

impl AgentPersistence for StoreDisabled {
    type StoreId = ();

    fn store_id(&self, _name: &str) -> Result<Self::StoreId, StoreError> {
        Err(StoreError::NoStoreAvailable)
    }

    fn init_value_store(&self, _store_id: Self::StoreId) -> Option<BoxInitializer<'_>> {
        None
    }

    fn init_map_store(&self, _store_id: Self::StoreId) -> Option<BoxInitializer<'_>> {
        None
    }

    fn put_value(&mut self, _store_id: Self::StoreId, _bytes: &[u8]) -> Result<(), StoreError> {
        Err(StoreError::NoStoreAvailable)
    }

    fn apply_map<B: AsRef<[u8]>>(
        &mut self,
        _store_id: Self::StoreId,
        _op: &MapOperation<B, B>,
    ) -> Result<(), StoreError> {
        Err(StoreError::NoStoreAvailable)
    }
}

/// Binding to use an implementation of [`NodePersistence`] as an implementation of
/// [`AgentPersistence`].
#[derive(Debug, Clone)]
pub struct StorePersistence<S>(pub S);

impl<S> AgentPersistence for StorePersistence<S>
where
    S: NodePersistence + Send + Sync + 'static,
{
    type StoreId = <S as NodePersistence>::LaneId;

    fn store_id(&self, name: &str) -> Result<Self::StoreId, StoreError> {
        let StorePersistence(store) = self;
        store.id_for(name)
    }

    fn put_value(&mut self, lane_id: Self::StoreId, bytes: &[u8]) -> Result<(), StoreError> {
        let StorePersistence(store) = self;
        store.put_value(lane_id, bytes)
    }

    fn apply_map<B: AsRef<[u8]>>(
        &mut self,
        lane_id: Self::StoreId,
        op: &MapOperation<B, B>,
    ) -> Result<(), StoreError> {
        let StorePersistence(store) = self;
        match op {
            MapOperation::Update { key, value } => {
                store.update_map(lane_id, key.as_ref(), value.as_ref())
            }
            MapOperation::Remove { key } => store.remove_map(lane_id, key.as_ref()),
            MapOperation::Clear => store.clear_map(lane_id),
        }
    }

    fn init_value_store(&self, store_id: Self::StoreId) -> Option<BoxInitializer<'_>> {
        let StorePersistence(store) = self;
        let init = ValueInit { store, store_id };
        Some(Box::new(init))
    }

    fn init_map_store(&self, store_id: Self::StoreId) -> Option<BoxInitializer<'_>> {
        let StorePersistence(store) = self;
        let init = MapInit { store, store_id };
        Some(Box::new(init))
    }
}

struct NoValueInit;
struct NoMapInit;

/// Dummy initializer for when there is no store for a value.
pub fn no_value_init<'a>() -> BoxInitializer<'a> {
    Box::new(NoValueInit)
}

/// Dummy initializer for when there is no store for a map.
pub fn no_map_init<'a>() -> BoxInitializer<'a> {
    Box::new(NoMapInit)
}

impl<'a> Initializer<'a> for NoValueInit {
    fn initialize<'b>(self: Box<Self>, channel: &'b mut ByteWriter) -> InitFut<'b>
    where
        'a: 'b,
    {
        async move {
            let mut writer = FramedWrite::new(channel, RawValueStoreInitEncoder::default());
            writer.send(StoreInitMessage::<&[u8]>::InitComplete).await?;
            Ok(())
        }
        .boxed()
    }
}

impl<'a> Initializer<'a> for NoMapInit {
    fn initialize<'b>(self: Box<Self>, channel: &'b mut ByteWriter) -> InitFut<'b>
    where
        'a: 'b,
    {
        async move {
            let mut writer = FramedWrite::new(channel, RawMapStoreInitEncoder::default());
            writer
                .send(StoreInitMessage::<MapMessage<&[u8], &[u8]>>::InitComplete)
                .await?;
            Ok(())
        }
        .boxed()
    }
}
