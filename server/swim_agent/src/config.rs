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

use std::num::NonZeroUsize;

use swim_utilities::algebra::non_zero_usize;

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
