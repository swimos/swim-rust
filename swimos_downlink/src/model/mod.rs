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

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;

use tokio::sync::{mpsc, oneshot};

use crate::model::lifecycle::{BasicMapDownlinkLifecycle, MapDownlinkLifecycle};
use lifecycle::{
    BasicEventDownlinkLifecycle, BasicValueDownlinkLifecycle, EventDownlinkLifecycle,
    ValueDownlinkLifecycle,
};
use swimos_api::protocol::map::MapMessage;

pub mod lifecycle;

#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
#[error("Downlink not yet synced")]
pub struct NotYetSyncedError;

#[derive(Debug)]
pub struct ValueDownlinkSet<T> {
    pub to: T,
}

pub struct ValueDownlinkModel<T, LC> {
    pub handle: mpsc::Receiver<ValueDownlinkSet<T>>,
    pub lifecycle: LC,
}

pub struct EventDownlinkModel<T, LC> {
    _type: PhantomData<T>,
    pub lifecycle: LC,
}

impl<T, LC> ValueDownlinkModel<T, LC> {
    pub fn new(handle: mpsc::Receiver<ValueDownlinkSet<T>>, lifecycle: LC) -> Self {
        ValueDownlinkModel { handle, lifecycle }
    }
}

impl<T, LC> EventDownlinkModel<T, LC> {
    pub fn new(lifecycle: LC) -> Self {
        EventDownlinkModel {
            _type: PhantomData,
            lifecycle,
        }
    }
}

pub struct MapDownlinkModel<K, V, LC> {
    pub actions: mpsc::Receiver<MapMessage<K, V>>,
    pub lifecycle: LC,
    pub remote: bool,
}

impl<K, V, LC> MapDownlinkModel<K, V, LC> {
    pub fn new(
        actions: mpsc::Receiver<MapMessage<K, V>>,
        lifecycle: LC,
        remote: bool,
    ) -> MapDownlinkModel<K, V, LC> {
        MapDownlinkModel {
            actions,
            lifecycle,
            remote,
        }
    }
}

pub type DefaultValueDownlinkModel<T> = ValueDownlinkModel<T, BasicValueDownlinkLifecycle<T>>;

pub type DefaultEventDownlinkModel<T> = EventDownlinkModel<T, BasicEventDownlinkLifecycle<T>>;

pub type DefaultMapDownlinkModel<K, V> = MapDownlinkModel<K, V, BasicMapDownlinkLifecycle<K, V>>;

pub fn value_downlink<T>(
    handle: mpsc::Receiver<ValueDownlinkSet<T>>,
) -> DefaultValueDownlinkModel<T> {
    ValueDownlinkModel {
        handle,
        lifecycle: Default::default(),
    }
}

pub fn event_downlink<T>() -> DefaultEventDownlinkModel<T> {
    EventDownlinkModel {
        _type: PhantomData,
        lifecycle: Default::default(),
    }
}

pub fn map_downlink<K, V>(
    actions: mpsc::Receiver<MapMessage<K, V>>,
    remote: bool,
) -> DefaultMapDownlinkModel<K, V> {
    MapDownlinkModel::new(actions, Default::default(), remote)
}

#[derive(Debug)]
pub struct ChannelError;

impl Error for ChannelError {}

impl Display for ChannelError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Channel closed")
    }
}

impl<T> From<mpsc::error::SendError<T>> for ChannelError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        ChannelError
    }
}

impl From<oneshot::error::RecvError> for ChannelError {
    fn from(_: oneshot::error::RecvError) -> Self {
        ChannelError
    }
}

/// A map downlink handle.
#[derive(Debug, Clone)]
pub struct MapDownlinkHandle<K, V> {
    inner: mpsc::Sender<MapMessage<K, V>>,
}

impl<K, V> MapDownlinkHandle<K, V> {
    pub fn new(inner: mpsc::Sender<MapMessage<K, V>>) -> MapDownlinkHandle<K, V> {
        MapDownlinkHandle { inner }
    }

    /// Updates or inserts the key-value pair into the map.
    pub async fn update(&self, key: K, value: V) -> Result<(), ChannelError> {
        Ok(self.inner.send(MapMessage::Update { key, value }).await?)
    }

    /// Removes the value corresponding to the key.
    pub async fn remove(&self, key: K) -> Result<(), ChannelError> {
        Ok(self.inner.send(MapMessage::Remove { key }).await?)
    }

    /// Clears the map, removing all of the elements.
    pub async fn clear(&self) -> Result<(), ChannelError> {
        Ok(self.inner.send(MapMessage::Clear).await?)
    }

    /// Retains the last `n` elements in the map.
    pub async fn take(&self, n: u64) -> Result<(), ChannelError> {
        Ok(self.inner.send(MapMessage::Take(n)).await?)
    }

    /// Retains the first `n` elements in the map.
    pub async fn drop(&self, n: u64) -> Result<(), ChannelError> {
        Ok(self.inner.send(MapMessage::Drop(n)).await?)
    }

    /// Completes when the downlink closes; a downlink closes when the connection closes or an
    /// error occurs.
    pub async fn closed(&self) {
        self.inner.closed().await;
    }
}

impl<T, LC> ValueDownlinkModel<T, LC>
where
    LC: ValueDownlinkLifecycle<T>,
{
    pub fn with_lifecycle<F, LC2>(self, f: F) -> ValueDownlinkModel<T, LC2>
    where
        F: Fn(LC) -> LC2,
        LC2: ValueDownlinkLifecycle<T>,
    {
        let ValueDownlinkModel { handle, lifecycle } = self;
        ValueDownlinkModel {
            handle,
            lifecycle: f(lifecycle),
        }
    }
}

impl<T, LC> EventDownlinkModel<T, LC>
where
    LC: EventDownlinkLifecycle<T>,
{
    pub fn with_lifecycle<F, LC2>(self, f: F) -> EventDownlinkModel<T, LC2>
    where
        F: Fn(LC) -> LC2,
        LC2: EventDownlinkLifecycle<T>,
    {
        let EventDownlinkModel { lifecycle, .. } = self;

        EventDownlinkModel {
            _type: PhantomData,
            lifecycle: f(lifecycle),
        }
    }
}

impl<K, V, LC> MapDownlinkModel<K, V, LC>
where
    LC: MapDownlinkLifecycle<K, V>,
{
    pub fn with_lifecycle<F, LC2>(self, f: F) -> MapDownlinkModel<K, V, LC2>
    where
        F: Fn(LC) -> LC2,
        LC2: MapDownlinkLifecycle<K, V>,
    {
        let MapDownlinkModel {
            actions,
            lifecycle,
            remote,
        } = self;

        MapDownlinkModel {
            actions,
            lifecycle: f(lifecycle),
            remote,
        }
    }
}
