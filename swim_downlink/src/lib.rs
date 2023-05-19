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

pub use model::{
    ChannelError,  DefaultEventDownlinkModel, DefaultMapDownlinkModel,
    DefaultValueDownlinkModel,  event_downlink, EventDownlinkModel,
     map_downlink,
    MapDownlinkHandle, MapDownlinkModel, NotYetSyncedError, value_downlink,
      ValueDownlinkModel, ValueDownlinkOperation,
};
pub use task::{DownlinkTask, MapKey, MapValue};

mod model;
mod task;

pub mod lifecycle {
    pub use crate::model::lifecycle::{
        BasicEventDownlinkLifecycle, BasicMapDownlinkLifecycle, BasicValueDownlinkLifecycle,
        EventDownlinkLifecycle, MapDownlinkLifecycle, StatefulEventDownlinkLifecycle,
        StatefulMapDownlinkLifecycle, StatefulValueDownlinkLifecycle, ValueDownlinkLifecycle,
    };
}
