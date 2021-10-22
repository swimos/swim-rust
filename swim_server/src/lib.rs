// Copyright 2015-2021 SWIM.AI inc.
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

#![allow(clippy::match_wild_err_arm)]

pub mod agent;
pub mod interface;
#[macro_use]
pub mod macros;
pub mod meta;
pub mod routing;
pub mod store;
pub mod sync {
    pub mod circular_buffer {
        pub use swim_utilities::sync::circular_buffer::{
            channel,
            error::{RecvError, SendError},
            watch_channel, Receiver, Sender,
        };
    }
}
mod plane;

#[allow(unused_imports)]
pub use agent_derive::*;
pub use plane::spec::PlaneBuilder;
pub use stringify_attr::{stringify_attr, stringify_attr_raw};
pub use swim_utilities::future::retryable::RetryStrategy;
pub use swim_utilities::future::SwimStreamExt;
pub use swim_utilities::routing::route_pattern::RoutePattern;
pub use swim_utilities::routing::uri;

#[doc(hidden)]
pub use agent::model::value::ValueLaneStoreIo;
