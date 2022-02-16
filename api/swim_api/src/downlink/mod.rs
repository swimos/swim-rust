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
use swim_model::path::Path;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};

use crate::error::DownlinkTaskError;

/// Indicates the kind of the downlink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownlinkKind {
    /// Accepts single values and maintains an internal state.
    Value,
    // Accepts single values but has no state.
    Event,
    /// Accepts key-value pairs and maintains a state as a map.
    Map,
}

#[derive(Debug, Clone, Copy)]
/// General downlink configuration parameters.
pub struct DownlinkConfig {
    pub events_when_not_synced: bool,
    pub terminate_on_unlinked: bool,
}

impl Default for DownlinkConfig {
    fn default() -> Self {
        Self {
            events_when_not_synced: false,
            terminate_on_unlinked: true,
        }
    }
}

/// Trait to define a consumer to a downlink. Instaces of this will be passed to the runtime
/// to be executed. User code should not generally need to implement this directly. It is
/// necessary for this trait to be object safe and any changes to it should take that into
/// account.
pub trait Downlink {
    /// The kind of the downlink, used by the runtime to configure how the downlink is managed.
    fn kind(&self) -> DownlinkKind;

    /// Create a task that will manage the state of the downlink and service anything that is
    /// observing it/ pushing data to it.
    ///
    /// #Arguments
    /// * `path` - The path to the lane to which the downlink should be attached.
    /// * `config` - Configuration parameters for the downlink task.
    /// * `input` - Byte channel on which updates will be received from the runtime.
    /// * `output` - Byte channel on which command will be sent to the runtime.
    fn run(
        self,
        path: Path,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>>;
}

impl<T: Downlink> Downlink for Box<T> {
    fn kind(&self) -> DownlinkKind {
        (**self).kind()
    }

    fn run(
        self,
        path: Path,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        (*self).run(path, config, input, output)
    }
}
