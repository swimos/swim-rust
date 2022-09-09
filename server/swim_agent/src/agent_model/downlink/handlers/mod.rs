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

use futures::future::BoxFuture;
use swim_api::error::FrameIoError;

use crate::event_handler::BoxEventHandler;

/// Allows the incoming half or a downlink to be driven by an agent task, without any knowledge
/// of the types used internally by the downlink. A [`DownlinkChannel`] is essentially a stream
/// of event handlers. However, the operation to get the next handler is split across two methods
/// (on to wait for an incoming message and the other to create an event handler, where
/// appropraite). This split is necessary to avoid holding a reference to the downlink lifecyle across
/// an await point. Otherwise, we would need to add a bound that the lifecycles must be [`Sync`] which
/// is undesriable as it would prevent the use of [`std::cell::RefCell`] in lifecycle fields.
///
/// #Type Arguments
///
/// * `Context` - The context within which the event handlers must be run (typically an agent type).
pub trait DownlinkChannel<Context> {
    /// Await the next downlink event. If this returns [`None`], the downlink has terminated. If an
    /// error is returned, the downlink has failed and should not longer be watied on.
    fn await_ready(&mut self) -> BoxFuture<'_, Option<Result<(), FrameIoError>>>;

    /// After a successful call to `await_ready` this may reutnr an event handler to be executed by
    /// the agent task. At any other time, it will return [`None`].
    fn next_event(&mut self, context: &Context) -> Option<BoxEventHandler<'_, Context>>;
}

/// A downlink channel that can be used by dynamic dispatch. As the agent task cannnot know about the
/// specific type of any opened downlinks, it views them through this interface.
pub type BoxDownlinkChannel<Context> = Box<dyn DownlinkChannel<Context> + Send>;

pub trait DownlinkChannelExt<Context>: DownlinkChannel<Context> {
    /// Box a downlink channel for use through dynamic dispatch.
    fn boxed(self) -> BoxDownlinkChannel<Context>
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }
}

impl<Context, C> DownlinkChannelExt<Context> for C where C: DownlinkChannel<Context> {}
