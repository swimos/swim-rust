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

use futures::StreamExt;
use std::hash::Hash;
use swim_api::{downlink::DownlinkKind, protocol::map::MapOperation};
use swim_form::Form;
use swim_model::{address::Address, Text};
use swim_utilities::{algebra::non_zero_usize, sync::circular_buffer};
use tokio::sync::mpsc;
use tracing::error;

use crate::{
    agent_model::downlink::handlers::DownlinkChannelExt,
    downlink_lifecycle::{map::MapDownlinkLifecycle, value::ValueDownlinkLifecycle},
    event_handler::{
        ActionContext, HandlerAction, StepResult, UnitHandler,
    },
    meta::AgentMetadata,
};

use self::{
    hosted::{
        map_dl_write_stream, value_dl_write_stream, HostedMapDownlinkChannel,
        HostedValueDownlinkChannel, MapDlState, MapDownlinkHandle, ValueDownlinkHandle,
    },
};

struct Inner<LC> {
    host: Option<Text>,
    node: Text,
    lane: Text,
    lifecycle: LC,
}

/// Configuration parameters for hosted value downlinks.
#[derive(Debug, Clone, Copy)]
pub struct ValueDownlinkConfig {
    /// If this is set, lifecycle events will becalled for events before the downlink is synchronized with the remote lane.
    /// (default: false).
    pub events_when_not_synced: bool,
    /// If this is set, the downlink will stop if it enters the unlinked state (default: true).
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

/// Configuration parameters for hosted value downlinks.
#[derive(Debug, Clone, Copy)]
pub struct MapDownlinkConfig {
    /// If this is set, lifecycle events will becalled for events before the downlink is synchronized with the remote lane.
    /// (default: false).
    pub events_when_not_synced: bool,
    /// If this is set, the downlink will stop if it enters the unlinked state (default: true).
    pub terminate_on_unlinked: bool,
    /// Size of the channel used for sending operations to the remote lane.
    pub channel_size: NonZeroUsize,
}

const DEFAULT_CHAN_SIZE: NonZeroUsize = non_zero_usize!(8);

impl Default for MapDownlinkConfig {
    fn default() -> Self {
        Self {
            events_when_not_synced: false,
            terminate_on_unlinked: true,
            channel_size: DEFAULT_CHAN_SIZE,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DlState {
    Unlinked,
    Linked,
    Synced,
}

/// [`HandlerAction`] that attempts to open a value downlink to a remote lane and results in
/// a handle to the downlink.
pub struct OpenValueDownlink<T, LC> {
    _type: PhantomData<fn(T) -> T>,
    inner: Option<Inner<LC>>,
    config: ValueDownlinkConfig,
}

type KvInvariant<K, V> = fn(K, V) -> (K, V);

/// [`HandlerAction`] that attempts to open a map downlink to a remote lane and results in
/// a handle to the downlink.
pub struct OpenMapDownlink<K, V, LC> {
    _type: PhantomData<KvInvariant<K, V>>,
    inner: Option<Inner<LC>>,
    config: MapDownlinkConfig,
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
        config: MapDownlinkConfig,
    ) -> Self {
        OpenMapDownlink {
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
            let path_cpy = path.clone();
            action_context.start_downlink(
                path.clone(),
                DownlinkKind::Value,
                move |reader| {
                    HostedValueDownlinkChannel::new(path_cpy, reader, lifecycle, state, config)
                        .boxed()
                },
                move |writer| value_dl_write_stream(writer, rx).boxed(),
                |result| {
                    if let Err(err) = result {
                        error!(error = %err, "Registering value downlink failed.");
                    }
                    UnitHandler::default()
                },
            );
            let handle = ValueDownlinkHandle::new(path, tx);
            StepResult::done(handle)
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
        let OpenMapDownlink { inner, config, .. } = self;
        if let Some(Inner {
            host,
            node,
            lane,
            lifecycle,
        }) = inner.take()
        {
            let path = Address::new(host, node, lane);
            let state: RefCell<MapDlState<K, V>> = Default::default();
            let (tx, rx) = mpsc::channel::<MapOperation<K, V>>(config.channel_size.get());
            let config = *config;
            let path_cpy = path.clone();
            action_context.start_downlink(
                path,
                DownlinkKind::Map,
                move |reader| {
                    HostedMapDownlinkChannel::new(path_cpy, reader, lifecycle, state, config)
                        .boxed()
                },
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
