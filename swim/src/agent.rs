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

pub use swim_agent_derive::{AgentLaneModel, projections, lifecycle};

pub trait AgentLaneModel: agent_model::AgentSpec {}

impl<A> AgentLaneModel for A where A: agent_model::AgentSpec {}

pub use swim_agent::reexport;
pub use swim_agent::agent_lifecycle;
pub use swim_agent::downlink_lifecycle;
pub use swim_agent::event_handler;

pub mod model {
    pub use swim_agent::model::{MapMessage, Text};
}

pub mod agent_model {
    pub use swim_agent::agent_model::{
        AgentSpec, LaneInitializer, LaneSpec, LaneFlags, MapLaneInitializer, ValueLaneInitializer, WriteResult,
    };
}

pub mod lanes {
    pub use swim_agent::lanes::{CommandLane, Lane, MapLane, ValueLane};

    pub mod command {
        pub use swim_agent::lanes::command::{decode_and_command, DecodeAndCommand};
        pub mod lifecycle {
            pub use swim_agent::lanes::command::lifecycle::StatefulCommandLaneLifecycle;
        }
    }

    pub mod value {
        pub use swim_agent::lanes::value::{decode_and_set, DecodeAndSet, ValueLaneSync};
        pub mod lifecycle {
            pub use swim_agent::lanes::value::lifecycle::StatefulValueLaneLifecycle;
        }
    }

    pub mod map {
        pub use swim_agent::lanes::map::{decode_and_apply, DecodeAndApply, MapLaneSync};
        pub mod lifecycle {
            pub use swim_agent::lanes::map::lifecycle::StatefulMapLaneLifecycle;
        }
    }
}