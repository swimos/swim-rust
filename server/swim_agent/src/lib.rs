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

#[doc(hidden)]
#[allow(unused_imports)]
pub use swim_agent_derive::{projections, lifecycle, AgentLaneModel};

pub mod agent_model;
pub mod event_handler;
pub mod lanes;
pub mod lifecycle;
pub mod meta;

pub use agent_model::AgentLaneModel;

pub mod model {
    pub use swim_api::protocol::map::{MapMessage, MapOperation};
    pub use swim_model::Text;
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
