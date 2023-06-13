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

use futures::future::BoxFuture;
use swim_api::downlink::DownlinkKind;
use swim_model::{address::Address, Text};
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use thiserror::Error;

use crate::event_handler::BoxEventHandler;

pub enum DownlinkChannelEvent {
    HandlerReady,
    WriteCompleted,
    WriteStreamTerminated,
}

#[derive(Debug, Error)]
pub enum DownlinkChannelError {
    #[error("Reading from the downlink input failed.")]
    ReadFailed,
    #[error("Writing to the downlink output failed: {0}")]
    WriteFailed(#[from] std::io::Error),
}

/// Allows a downlink to be driven by an agent task, without any knowledge of the types used
/// internally by the downlink. A [`DownlinkChannel`] is essentially a stream of event handlers.
/// However, the operation to get the next handler is split across two methods(on to wait for
/// an incoming message and the other to create an event handler, where appropriate). This split
/// is necessary to avoid holding a reference to the downlink lifecycle across an await point.
/// Otherwise, we would need to add a bound that the lifecycles must be [`Sync`] which
/// is undesirable as it would prevent the use of [`std::cell::RefCell`] in lifecycle fields.
///
/// #Type Arguments
///
/// * `Context` - The context within which the event handlers must be run (typically an agent type).
pub trait DownlinkChannel<Context> {
    fn connect(&mut self, context: &Context, output: ByteWriter, input: ByteReader);

    fn can_restart(&self) -> bool;

    fn address(&self) -> &Address<Text>;

    /// Get the kind of the downlink.
    fn kind(&self) -> DownlinkKind;

    /// Await the next channel event. If this returns [`None`], the downlink has terminated. If an
    /// error is returned, the downlink has failed and should not longer be waited on.
    fn await_ready(
        &mut self,
    ) -> BoxFuture<'_, Option<Result<DownlinkChannelEvent, DownlinkChannelError>>>;

    /// After a call to `await_ready` that does not return [`None`] this may return an event handler to
    /// be executed by the agent task. At any other time, it will return [`None`].
    fn next_event(&mut self, context: &Context) -> Option<BoxEventHandler<'_, Context>>;
}

#[derive(Clone, Copy, Default, Debug)]
pub struct DownlinkFailed;

/// A downlink channel that can be used by dynamic dispatch. As the agent task cannot know about the
/// specific type of any opened downlinks, it views them through this interface.
pub type BoxDownlinkChannel<Context> = Box<dyn DownlinkChannel<Context> + Send>;
