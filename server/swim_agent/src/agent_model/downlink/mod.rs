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

use std::{cell::RefCell, marker::PhantomData};

use futures::StreamExt;
use swim_api::{downlink::DownlinkKind, error::AgentRuntimeError};
use swim_form::Form;
use swim_model::{address::Address, Text};
use tokio::sync::watch;
use tracing::error;

use crate::{
    agent_model::downlink::handlers::DownlinkChannelExt,
    downlink_lifecycle::value::ValueDownlinkLifecycle,
    event_handler::{
        ActionContext, DownlinkSpawner, HandlerAction, StepResult, UnitHandler, WriteStream,
    },
    meta::AgentMetadata,
};

use self::{
    handlers::BoxDownlinkChannel,
    hosted::{value_dl_write_stream, HostedValueDownlinkChannel},
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

pub struct OpenValueDownlink<T, LC> {
    _type: PhantomData<fn(T) -> T>,
    inner: Option<Inner<LC>>,
}

impl<T, LC> OpenValueDownlink<T, LC> {
    pub fn new(host: Option<Text>, node: Text, lane: Text, lifecycle: LC) -> Self {
        OpenValueDownlink {
            _type: PhantomData,
            inner: Some(Inner {
                host,
                node,
                lane,
                lifecycle,
            }),
        }
    }
}

pub struct ValueDownlinkHandle<T> {
    sender: watch::Sender<Option<T>>,
}

impl<T> ValueDownlinkHandle<T> {
    fn new(sender: watch::Sender<Option<T>>) -> Self {
        ValueDownlinkHandle { sender }
    }
}

impl<T> ValueDownlinkHandle<T> {
    pub fn set(&self, value: T) -> Result<(), AgentRuntimeError> {
        self.sender.send(Some(value))?;
        Ok(())
    }
}

impl<T, LC, Context> HandlerAction<Context> for OpenValueDownlink<T, LC>
where
    Context: 'static,
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
        let OpenValueDownlink { inner, .. } = self;
        if let Some(Inner {
            host,
            node,
            lane,
            lifecycle,
        }) = inner.take()
        {
            let path = Address::new(host, node, lane);
            let state: RefCell<Option<T>> = Default::default();
            let (tx, rx) = watch::channel::<Option<T>>(None);
            action_context.start_downlink(
                path,
                DownlinkKind::Value,
                move |reader| HostedValueDownlinkChannel::new(reader, lifecycle, state).boxed(),
                move |writer| {
                    value_dl_write_stream(writer, rx).boxed()
                },
                |result| {
                    if let Err(err) = result {
                        error!(error = %err, "Registering downlink failed.");
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
