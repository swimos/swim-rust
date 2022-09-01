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

mod bridge;
pub mod handlers;
pub mod hosted;

use std::{marker::PhantomData, num::NonZeroUsize};

use futures::FutureExt;
use swim_api::{downlink::DownlinkConfig, error::AgentRuntimeError};
use swim_form::Form;
use swim_model::{address::Address, Text};
use tokio::sync::mpsc;
use tracing::error;

use crate::{
    agent_model::downlink::handlers::{DownlinkChannel, DownlinkChannelExt},
    downlink_lifecycle::value::ValueDownlinkLifecycle,
    event_handler::{
        ActionContext, DownlinkSpawner, EventHandlerExt, Fail, HandlerAction, Spawner, StepResult,
        UnitHandler,
    },
    meta::AgentMetadata,
};

use self::{bridge::make_downlink, handlers::ValueDownlinkEndpoint};

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

pub struct OpenValueDownlink<T, LC> {
    _type: PhantomData<fn(T) -> T>,
    inner: Option<Inner<LC>>,
    config: DownlinkConfig,
    channel_size: NonZeroUsize,
}

impl<T, LC> OpenValueDownlink<T, LC> {
    pub fn new(
        host: Option<Text>,
        node: Text,
        lane: Text,
        lifecycle: LC,
        config: DownlinkConfig,
        channel_size: NonZeroUsize,
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
            channel_size,
        }
    }
}

pub struct ValueDownlinkHandle<T> {
    sender: mpsc::Sender<T>,
}

impl<T> ValueDownlinkHandle<T> {
    pub async fn set_direct(&self, value: T) -> Result<(), AgentRuntimeError> {
        self.sender.send(value).await?;
        Ok(())
    }

    pub fn set(&self, value: T) -> DownlinkSend<'_, T> {
        DownlinkSend {
            handle: self,
            value: Some(value),
        }
    }
}

pub struct DownlinkSend<'a, T> {
    handle: &'a ValueDownlinkHandle<T>,
    value: Option<T>,
}

impl<'a, T, Context> HandlerAction<Context> for DownlinkSend<'a, T>
where
    T: Send + 'static,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let DownlinkSend { handle, value } = self;
        if let Some(value) = value.take() {
            let tx = handle.sender.clone();
            let fut = async move {
                let result = tx
                    .send(value)
                    .await
                    .map_err(|_| AgentRuntimeError::Stopping);
                Fail::new(result).boxed()
            };
            action_context.spawn_suspend(fut.boxed());
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}

impl<T> ValueDownlinkHandle<T> {
    pub fn new(sender: mpsc::Sender<T>) -> Self {
        ValueDownlinkHandle { sender }
    }
}

impl<T, LC, Context> HandlerAction<Context> for OpenValueDownlink<T, LC>
where
    T: Form + Clone + Send + Sync + 'static,
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
        let OpenValueDownlink {
            inner,
            config,
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
            let (bridge_tx, bridge_rx) = mpsc::channel(channel_size.get());
            let (set_tx, set_rx) = mpsc::channel(channel_size.get());
            let downlink = make_downlink(bridge_tx, set_rx);
            let endpoint = ValueDownlinkEndpoint::new(bridge_rx, lifecycle);

            let handle = ValueDownlinkHandle::new(set_tx);
            let path = Address::new(host, node, lane);
            action_context.start_downlink(path, *config, downlink, endpoint, move |result| {
                if let Err(err) = result {
                    error!(error = %err, "Registering downlink failed.");
                }
                UnitHandler::default()
            });
            StepResult::done(handle)
        } else {
            StepResult::after_done()
        }
    }
}

pub struct RegisterChannel<C> {
    channel: Option<C>,
}

impl<C> RegisterChannel<C> {
    pub fn new(channel: C) -> Self {
        RegisterChannel {
            channel: Some(channel),
        }
    }
}

impl<C, Context> HandlerAction<Context> for RegisterChannel<C>
where
    C: DownlinkChannel<Context> + Send + 'static,
{
    type Completion = Result<(), AgentRuntimeError>;

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let RegisterChannel { channel } = self;
        if let Some(channel) = channel.take() {
            StepResult::done(action_context.spawn_downlink(channel.boxed()))
        } else {
            StepResult::after_done()
        }
    }
}
