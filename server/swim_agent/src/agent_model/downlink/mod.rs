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
#[cfg(test)]
mod tests;

use std::{cell::RefCell, marker::PhantomData};

use futures::StreamExt;
use std::hash::Hash;
use swim_api::{downlink::DownlinkKind, protocol::map::MapOperation};
use swim_form::Form;
use swim_model::{address::Address, Text};
use swim_utilities::sync::circular_buffer;
use tokio::sync::mpsc;
use tracing::error;

use crate::{
    agent_model::downlink::handlers::DownlinkChannelExt,
    config::{MapDownlinkConfig, ValueDownlinkConfig},
    downlink_lifecycle::{map::MapDownlinkLifecycle, value::ValueDownlinkLifecycle},
    event_handler::{ActionContext, HandlerAction, StepResult, UnitHandler},
    meta::AgentMetadata,
};

use self::hosted::{
    map_dl_write_stream, value_dl_write_stream, HostedMapDownlinkChannel,
    HostedValueDownlinkChannel, MapDlState, MapDownlinkHandle, ValueDownlinkHandle,
};

struct Inner<LC> {
    address: Address<Text>,
    lifecycle: LC,
}

/// [`HandlerAction`] that attempts to open a value downlink to a remote lane and results in
/// a handle to the downlink.
pub struct OpenValueDownlinkAction<T, LC> {
    _type: PhantomData<fn(T) -> T>,
    inner: Option<Inner<LC>>,
    config: ValueDownlinkConfig,
}

type KvInvariant<K, V> = fn(K, V) -> (K, V);

/// [`HandlerAction`] that attempts to open a map downlink to a remote lane and results in
/// a handle to the downlink.
pub struct OpenMapDownlinkAction<K, V, LC> {
    _type: PhantomData<KvInvariant<K, V>>,
    inner: Option<Inner<LC>>,
    config: MapDownlinkConfig,
}

impl<T, LC> OpenValueDownlinkAction<T, LC> {
    pub fn new(address: Address<Text>, lifecycle: LC, config: ValueDownlinkConfig) -> Self {
        OpenValueDownlinkAction {
            _type: PhantomData,
            inner: Some(Inner { address, lifecycle }),
            config,
        }
    }
}

impl<K, V, LC> OpenMapDownlinkAction<K, V, LC> {
    pub fn new(address: Address<Text>, lifecycle: LC, config: MapDownlinkConfig) -> Self {
        OpenMapDownlinkAction {
            _type: PhantomData,
            inner: Some(Inner { address, lifecycle }),
            config,
        }
    }
}

impl<T, LC, Context> HandlerAction<Context> for OpenValueDownlinkAction<T, LC>
where
    Context: 'static,
    T: Form + Send + 'static,
    LC: ValueDownlinkLifecycle<T, Context> + Send + 'static,
    T::Rec: Send,
{
    type Completion = ValueDownlinkHandle<T>;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let OpenValueDownlinkAction { inner, config, .. } = self;
        if let Some(Inner {
            address: path,
            lifecycle,
        }) = inner.take()
        {
            let (tx, rx) = circular_buffer::watch_channel();

            let config = *config;
            let path_cpy = path.clone();
            action_context.start_downlink(
                path.clone(),
                DownlinkKind::Value,
                move |reader| {
                    HostedValueDownlinkChannel::new(path_cpy, reader, lifecycle, config).boxed()
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

impl<K, V, LC, Context> HandlerAction<Context> for OpenMapDownlinkAction<K, V, LC>
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
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let OpenMapDownlinkAction { inner, config, .. } = self;
        if let Some(Inner {
            address: path,
            lifecycle,
        }) = inner.take()
        {
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
