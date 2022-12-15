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

#[cfg(feature = "form")]
pub use swim_form as form;
pub use swim_model as model;

pub mod api {
    pub use swim_api::agent::{Agent, UplinkKind};
    pub use swim_api::downlink::{Downlink, DownlinkKind};

    pub mod agent {
        pub use swim_api::agent::{
            AgentConfig, AgentContext, AgentInitResult, AgentTask, BoxAgent, LaneConfig,
        };

        #[cfg(feature = "server")]
        pub use swim_server_app::AgentExt;

        pub mod error {
            pub use swim_api::error::{AgentInitError, AgentRuntimeError, AgentTaskError};
        }
    }

    pub mod downlink {
        pub use swim_api::downlink::DownlinkConfig;

        pub mod error {
            pub use swim_api::error::DownlinkTaskError;
        }
    }

    pub mod handlers {
        pub use swim_api::handlers::NoHandler;
    }
}

pub mod route {
    pub use swim_utilities::routing::route_pattern::{
        ApplyError, ParseError, RoutePattern, UnapplyError,
    };
}

pub mod io {
    pub mod channels {
        pub use swim_utilities::io::byte_channel::{
            are_connected, byte_channel, ByteReader, ByteWriter,
        };
    }
    pub mod errors {
        pub use swim_api::error::{FrameIoError, InvalidFrame};
        pub use swim_recon::parser::AsyncParseError;
    }
}

#[cfg(feature = "server")]
pub mod server {
    pub use swim_server_app::{
        BoxServer, DeflateConfig, RemoteConnectionsConfig, Server, ServerBuilder, ServerHandle,
    };
}

/// This module contains the API for implementing the [`crate::api::Agent`] trait in Rust. The key
/// traits are:
///
/// 1. [`crate::agent::AgentLaneModel`] - This defines the structure of the agent as a fixed collection
/// of typed lanes. This trait has a derive macro which can be applied to any struct with named fields
/// where each field is a lane type. See the documentation for the trait for instructions on how to use
/// the derive macro.
/// 2. [`crate::agent::agent_lifecycle::AgentLifecycle`] - The trait defines custom behaviour that can be
/// attached to an agent. An agent, and its lanes have associated lifecycle events (for example, the
/// `on_start` event that triggers when an instance of the agent starts.)
///
/// # The [`crate::agent::lifecycle`] Macro
///
/// [`crate::agent::agent_lifecycle::AgentLifecycle`]s can be created for an agent using the attribute
/// macro [`crate::agent::lifecycle`]. To create a trivial lifecycle (that does nothing), create a new
/// type and attach the attribute to an impl block:
///
/// ```no_run
/// use swim::agent::{AgentLaneModel, lifecycle};
/// use swim::agent::lanes::ValueLane;
///
/// #[derive(AgentLaneModel)]
/// struct ExampleAgent {
///     lane: ValueLane<i32>
/// }
///
/// struct ExampleLifecycle;
///
/// #[lifecycle(ExampleAgent)]
/// impl ExampleLifecycle {}
/// ```
///
/// This macro will add a function `into_lifecycle` that will convert an instance of `ExampleLifecycle` into
/// an [`crate::agent::agent_lifecycle::AgentLifecycle`].
///
/// Lifecycle events can then be handled by adding handler functions to the impl block. For example,
/// to attach an event to the `on_start` event, a function with the following signature could be
/// added:
///
///  ```no_run
/// use swim::agent::{AgentLaneModel, lifecycle};
/// use swim::agent::agent_lifecycle::utility::HandlerContext;
/// use swim::agent::event_handler::EventHandler;
/// use swim::agent::lanes::ValueLane;
///
/// #[derive(AgentLaneModel)]
/// struct ExampleAgent {
///     lane: ValueLane<i32>
/// }
///
/// struct ExampleLifecycle;
///
/// #[lifecycle(ExampleAgent)]
/// impl ExampleLifecycle {
///     
///     #[on_start]
///     fn my_start_handler(&self, context: HandlerContext<ExampleAgent>) -> impl EventHandler<ExampleAgent> {
///         context.effect(|| println!("Starting agent."))
///     }
/// }
/// ```
///
/// For full instructions on implementing agents and lifecycles, please see the crate level documentation.
///
/// # The [`crate::agent::projections`] Macro
///
/// Many [`crate::agent::event_handler::EventHandler`]s that operate on agents' lanes required projections
/// onto the fields of an agent type (a function taking a reference to the agent type and returning a
/// reference to the lane of interest). These can be tedious to write and make coder harder to read.
/// Therefore, the [`crate::agent::projections`] is provided to generate projections for all fields
/// of a struct as constant members of the type. The names of the projections will be the names of
/// the corresponding fields in upper case. For example:
///
/// ```no_run
/// use swim::agent::projections;
/// use swim::agent::lanes::{ValueLane, MapLane};
///
/// #[projections]
/// struct ExampleAgent {
///     value_lane: ValueLane<i32>,
///     map_lane: MapLane<String, i32>,
/// }
/// ```
///
/// This will generate constants called `VALUE_LANE` and `MAP_LANE` with types
/// `fn(&ExampleAgent) -> &ValueLane<i32>` and `fn(&ExampleAgent) -> &MapLane<String, i32>`,
/// respectively.
///
#[cfg(feature = "agent")]
pub mod agent;
