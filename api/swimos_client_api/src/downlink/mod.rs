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

use std::num::NonZeroUsize;

use futures::future::BoxFuture;
use swimos_api::address::Address;
use swimos_api::{agent::DownlinkKind, error::DownlinkTaskError};
use swimos_model::Text;
use swimos_utilities::{
    byte_channel::{ByteReader, ByteWriter},
    non_zero_usize,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// General downlink configuration parameters.
pub struct DownlinkConfig {
    /// If this is set, lifecycle events will be called for events before the downlink is synchronized with the remote lane.
    /// (default: false).
    pub events_when_not_synced: bool,
    /// If this is set, the downlink will stop if it enters the unlinked state (default: true).
    pub terminate_on_unlinked: bool,
    /// The size of the channel to send commands to the downlinks.
    pub buffer_size: NonZeroUsize,
}

const DEFAULT_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(1024);

impl Default for DownlinkConfig {
    fn default() -> Self {
        Self {
            events_when_not_synced: false,
            terminate_on_unlinked: true,
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }
}

/// Trait to define a consumer of a downlink. Instances of this will be passed to the runtime
/// to be executed. User code should not generally need to implement this directly. It is
/// necessary for this trait to be object safe and any changes to it should take that into
/// account.
pub trait Downlink {
    /// The kind of the downlink, used by the runtime to configure how the downlink is managed.
    fn kind(&self) -> DownlinkKind;

    /// Create a task that will manage the state of the downlink and service anything that is
    /// observing it/ pushing data to it.
    ///
    /// # Arguments
    /// * `path` - The path to the lane to which the downlink should be attached.
    /// * `config` - Configuration parameters for the downlink task.
    /// * `input` - Byte channel on which updates will be received from the runtime.
    /// * `output` - Byte channel on which command will be sent to the runtime.
    fn run(
        self,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>>;

    fn run_boxed(
        self: Box<Self>,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>>;
}

static_assertions::assert_obj_safe!(Downlink);

impl<T: Downlink> Downlink for Box<T> {
    fn kind(&self) -> DownlinkKind {
        (**self).kind()
    }

    fn run(
        self,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        (*self).run(path, config, input, output)
    }

    fn run_boxed(
        self: Box<Self>,
        path: Address<Text>,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        (**self).run(path, config, input, output)
    }
}
