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

//! # Agent Model and Lifecycle Definition API
//!
//! This crate provides a framework for building agent implementations that are executed by the
//! SwimOS runtime. Typically, the proc macros defined in the `swimos_agent_derive` crate will be used
//! to generate [agent specifications](`agent_model::AgentSpec`) and [agent lifecycles](`agent_lifecycle::AgentLifecycle`).
//!
//! An agent specification defines the structure of the agent in terms of [lanes](`lanes`) and [stores](`stores`).
//! A specification is sufficient, on its own, to define an agent route for the runtime. However, it will have
//! only state with no associated behaviour.
//!
//! An agent lifecycle is tied to a specific type of agent (any implementation of [`agent_model::AgentSpec`]).
//! It adds behaviour to the agent by attaching [event handlers](`event_handler::EventHandler`) to various lifecycle
//! events defined for the agent and its lanes and stores. As the agent is executed by the runtime, each time one of these
//! events is encountered, the appropriate event handler will be selected by the lifecycle and then executed.
//!
//! Multiple lifecycle implementations maybe defined for the same agent specification, allowing the same structure
//! to be used with different behaviour on separate agent routes.

/// Defines the [agent lifecycle](`agent_lifecycle::AgentLifecycle`) trait, use to specify lifecycles that can be associated with an
/// agent specification.
pub mod agent_lifecycle;

/// Defines the [agent specification](`agent_model::AgentSpec`) trait used to specify the structure of an agent it terms of lanes
/// and stores.
pub mod agent_model;

/// Configuration types for downlinks that are started from agent lifecycles.
pub mod config;

/// Traits and builders for constructing downlink lifecycles for downlinks started from agent lifecycles.
pub mod downlink_lifecycle;

/// Defines the [`event_handler::HandlerAction`] and [`event_handler::EventHandler`] traits used to specify event handlers in agent lifecycle.
pub mod event_handler;

mod event_queue;
mod item;

/// Defines the lane types that can be included in agent specifications. Lanes are exposed externally by the runtime,
/// using the WARP protocol (or HTTP in the case of [HTTP lanes](`lanes::HttpLane`)). The states of lanes may be stored
/// persistently by the runtime.
pub mod lanes;

mod lifecycle_fn;
mod map_storage;
mod meta;

/// Utility types for building stateful agent lifecycles.
pub mod state;

/// Defines the store types that can be included in agent specifications. These are not exposed externally but their states
/// may be stored persistently by the runtime.
pub mod stores;
#[cfg(test)]
mod test_context;
#[cfg(test)]
mod test_util;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub use agent_model::AgentSpec;

pub use item::AgentItem;

#[doc(hidden)]
pub use meta::AgentMetadata;

#[doc(hidden)]
pub mod model {
    pub use swimos_agent_protocol::{MapMessage, MapOperation};
    pub use swimos_api::agent::HttpLaneRequest;
    pub use swimos_model::Text;
}

#[doc(hidden)]
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
