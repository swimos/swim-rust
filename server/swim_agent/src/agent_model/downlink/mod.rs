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

pub mod handlers;
pub mod hosted;

use std::{cell::RefCell, marker::PhantomData, num::NonZeroUsize};

use futures::{Future, StreamExt};
use std::hash::Hash;
use swim_api::{downlink::DownlinkKind, error::AgentRuntimeError, protocol::map::MapOperation};
use swim_form::Form;
use swim_model::{address::Address, Text};
use swim_utilities::sync::circular_buffer;
use tokio::sync::mpsc;
use tracing::error;

use crate::{
    agent_model::downlink::handlers::DownlinkChannelExt,
    downlink_lifecycle::{map::MapDownlinkLifecycle, value::ValueDownlinkLifecycle},
    event_handler::{
        ActionContext, DownlinkSpawner, HandlerAction, StepResult, UnitHandler, WriteStream,
    },
    meta::AgentMetadata,
};

use self::{
    handlers::BoxDownlinkChannel,
    hosted::{
        map_dl_write_stream, value_dl_write_stream, HostedMapDownlinkChannel,
        HostedValueDownlinkChannel, MapDlState, ValueDownlinkHandle,
    },
};

pub enum DownlinkMessage<T> {
    Linked,
    Synced,
    Event(T),
    Unlinked,
}

struct Inner<LC> {
    host: Option<Text>,
    node: Text,
    lane: Text,
    lifecycle: LC,
}

#[derive(Debug, Clone, Copy)]
pub struct ValueDownlinkConfig {
    pub events_when_not_synced: bool,
    pub terminate_on_unlinked: bool,
}

impl Default for ValueDownlinkConfig {
    fn default() -> Self {
        Self {
            events_when_not_synced: false,
            terminate_on_unlinked: true,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DlState {
    Unlinked,
    Linked,
    Synced,
}

pub struct OpenValueDownlink<T, LC> {
    _type: PhantomData<fn(T) -> T>,
    inner: Option<Inner<LC>>,
    config: ValueDownlinkConfig,
}

type KvInvariant<K, V> = fn(K, V) -> (K, V);

pub struct OpenMapDownlink<K, V, LC> {
    _type: PhantomData<KvInvariant<K, V>>,
    inner: Option<Inner<LC>>,
    channel_size: NonZeroUsize,
}

impl<T, LC> OpenValueDownlink<T, LC> {
    pub fn new(
        host: Option<Text>,
        node: Text,
        lane: Text,
        lifecycle: LC,
        config: ValueDownlinkConfig,
    ) -> Self {
        OpenValueDownlink {
            _type: PhantomData,
            inner: Some(Inner {
                host,
                node,
                lane,
                lifecycle,
            }),
            config,
        }
    }
}

impl<K, V, LC> OpenMapDownlink<K, V, LC> {
    pub fn new(
        host: Option<Text>,
        node: Text,
        lane: Text,
        lifecycle: LC,
        channel_size: NonZeroUsize,
    ) -> Self {
        OpenMapDownlink {
            _type: PhantomData,
            inner: Some(Inner {
                host,
                node,
                lane,
                lifecycle,
            }),
            channel_size,
        }
    }
}

pub struct MapDownlinkHandle<K, V> {
    sender: mpsc::Sender<MapOperation<K, V>>,
}

impl<K, V> MapDownlinkHandle<K, V> {
    fn new(sender: mpsc::Sender<MapOperation<K, V>>) -> Self {
        MapDownlinkHandle { sender }
    }
}

impl<K, V> MapDownlinkHandle<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    pub fn update(
        &self,
        key: K,
        value: V,
    ) -> impl Future<Output = Result<(), AgentRuntimeError>> + 'static {
        let tx = self.sender.clone();
        async move {
            tx.send(MapOperation::Update { key, value }).await?;
            Ok(())
        }
    }

    pub fn remove(&self, key: K) -> impl Future<Output = Result<(), AgentRuntimeError>> + 'static {
        let tx = self.sender.clone();
        async move {
            tx.send(MapOperation::Remove { key }).await?;
            Ok(())
        }
    }

    pub fn clear(&self) -> impl Future<Output = Result<(), AgentRuntimeError>> + 'static {
        let tx = self.sender.clone();
        async move {
            tx.send(MapOperation::Clear).await?;
            Ok(())
        }
    }
}

impl<T, LC, Context> HandlerAction<Context> for OpenValueDownlink<T, LC>
where
    Context: 'static,
    T: Form + Send + Sync + 'static,
    LC: ValueDownlinkLifecycle<T, Context> + Send + 'static,
    T::Rec: Send,
{
    type Completion = ValueDownlinkHandle<T>;

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let OpenValueDownlink { inner, config, .. } = self;
        if let Some(Inner {
            host,
            node,
            lane,
            lifecycle,
        }) = inner.take()
        {
            let path = Address::new(host, node, lane);
            let state: RefCell<Option<T>> = Default::default();
            let (tx, rx) = circular_buffer::watch_channel();

            let config = *config;
            action_context.start_downlink(
                path,
                DownlinkKind::Value,
                move |reader| {
                    HostedValueDownlinkChannel::new(reader, lifecycle, state, config).boxed()
                },
                move |writer| value_dl_write_stream(writer, rx).boxed(),
                |result| {
                    if let Err(err) = result {
                        error!(error = %err, "Registering value downlink failed.");
                    }
                    UnitHandler::default()
                },
            );
            let handle = ValueDownlinkHandle::new(tx);
            StepResult::done(handle)
        } else {
            StepResult::after_done()
        }
    }
}

struct RegInner<Context> {
    channel: BoxDownlinkChannel<Context>,
    write_stream: WriteStream,
}

pub struct RegisterHostedDownlink<Context> {
    inner: Option<RegInner<Context>>,
}

impl<Context> RegisterHostedDownlink<Context> {
    pub fn new(channel: BoxDownlinkChannel<Context>, write_stream: WriteStream) -> Self {
        RegisterHostedDownlink {
            inner: Some(RegInner {
                channel,
                write_stream,
            }),
        }
    }
}

impl<Context> HandlerAction<Context> for RegisterHostedDownlink<Context> {
    type Completion = Result<(), AgentRuntimeError>;

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let RegisterHostedDownlink { inner } = self;
        if let Some(RegInner {
            channel,
            write_stream,
        }) = inner.take()
        {
            StepResult::done(action_context.spawn_downlink(channel, write_stream))
        } else {
            StepResult::after_done()
        }
    }
}

impl<K, V, LC, Context> HandlerAction<Context> for OpenMapDownlink<K, V, LC>
where
    Context: 'static,
    K: Form + Hash + Eq + Ord + Clone + Send + Sync + 'static,
    V: Form + Send + Sync + 'static,
    LC: MapDownlinkLifecycle<K, V, Context> + Send + 'static,
    K::Rec: Send,
    V::Rec: Send,
{
    type Completion = MapDownlinkHandle<K, V>;

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let OpenMapDownlink {
            inner,
            channel_size,
            ..
        } = self;
        if let Some(Inner {
            host,
            node,
            lane,
            lifecycle,
        }) = inner.take()
        {
            let path = Address::new(host, node, lane);
            let state: RefCell<MapDlState<K, V>> = Default::default();
            let (tx, rx) = mpsc::channel::<MapOperation<K, V>>(channel_size.get());
            action_context.start_downlink(
                path,
                DownlinkKind::Map,
                move |reader| HostedMapDownlinkChannel::new(reader, lifecycle, state).boxed(),
                move |writer| map_dl_write_stream(writer, rx).boxed(),
                |result| {
                    if let Err(err) = result {
                        error!(error = %err, "Registering map downlink failed.");
                    }
                    UnitHandler::default()
                },
            );
            let handle = MapDownlinkHandle::new(tx);
            StepResult::done(handle)
        } else {
            StepResult::after_done()
        }
    }
}
