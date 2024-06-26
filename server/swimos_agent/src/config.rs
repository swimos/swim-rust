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

/// Configuration parameters for hosted value and event downlinks.
#[derive(Debug, Clone, Copy)]
pub struct SimpleDownlinkConfig {
    /// If this is set, lifecycle events will be called for events before the downlink is synchronized with the remote lane.
    /// (default: false).
    pub events_when_not_synced: bool,
    /// If this is set, the downlink will stop if it enters the unlinked state (default: true).
    pub terminate_on_unlinked: bool,
}

impl Default for SimpleDownlinkConfig {
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
    /// If this is set, lifecycle events will be called for events before the downlink is synchronized with the remote lane.
    /// (default: false).
    pub events_when_not_synced: bool,
    /// If this is set, the downlink will stop if it enters the unlinked state (default: true).
    pub terminate_on_unlinked: bool,
}

impl Default for MapDownlinkConfig {
    fn default() -> Self {
        Self {
            events_when_not_synced: false,
            terminate_on_unlinked: true,
        }
    }
}
