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

pub mod agent_lifecycle;
pub mod agent_model;
pub mod config;
pub mod downlink_lifecycle;
pub mod event_handler;
mod event_queue;
pub mod item;
pub mod lanes;
mod lifecycle_fn;
mod map_storage;
pub mod meta;
pub mod state;
pub mod stores;
#[cfg(test)]
mod test_context;

pub use agent_model::AgentSpec;

pub mod model {
    pub use swimos_agent_protocol::{MapMessage, MapOperation};
    pub use swimos_api::agent::HttpLaneRequest;
    pub use swimos_model::Text;
}

pub mod reexport {
    pub mod coproduct {
        pub use frunk::coproduct::CNil;
        pub use frunk::Coproduct;
    }

    pub mod bytes {
        pub use bytes::{Buf, BufMut, Bytes, BytesMut};
    }

    pub mod uuid {
        pub use uuid::Uuid;
    }
}
