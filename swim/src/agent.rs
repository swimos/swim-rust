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

//! This module contains the API for implementing the [`crate::api::Agent`] trait in Rust. The key
//! traits are:
//!
//! 1. [`crate::agent::AgentLaneModel`] - This defines the structure of the agent as a fixed collection
//! of typed lanes. This trait has a derive macro which can be applied to any struct with named fields
//! where each field is a lane type. See the documentation for the trait for instructions on how to use
//! the derive macro.
//! 2. [`crate::agent::agent_lifecycle::AgentLifecycle`] - The trait defines custom behaviour that can be
//! attached to an agent. An agent, and its lanes have associated lifecycle events (for example, the
//! `on_start` event that triggers when an instance of the agent starts.)
//!
//! # The [`crate::agent::lifecycle`] Macro
//!
//! [`crate::agent::agent_lifecycle::AgentLifecycle`]s can be created for an agent using the attribute
//! macro [`crate::agent::lifecycle`]. To create a trivial lifecycle (that does nothing), create a new
//! type and attach the attribute to an impl block:
//!
//! ```no_run
//! use swim::agent::{AgentLaneModel, lifecycle};
//! use swim::agent::lanes::ValueLane;
//!
//! #[derive(AgentLaneModel)]
//! struct ExampleAgent {
//!     lane: ValueLane<i32>
//! }
//!
//! #[derive(Clone, Copy)]
//! struct ExampleLifecycle;
//!
//! #[lifecycle(ExampleAgent)]
//! impl ExampleLifecycle {}
//! ```
//!
//! This macro will add a function `into_lifecycle` that will convert an instance of `ExampleLifecycle` into
//! an [`crate::agent::agent_lifecycle::AgentLifecycle`].
//!
//! Lifecycle events can then be handled by adding handler functions to the impl block. For example,
//! to attach an event to the `on_start` event, a function with the following signature could be
//! added:
//!
//!  ```no_run
//! use swim::agent::{AgentLaneModel, lifecycle};
//! use swim::agent::agent_lifecycle::utility::HandlerContext;
//! use swim::agent::event_handler::EventHandler;
//! use swim::agent::lanes::ValueLane;
//!
//! #[derive(AgentLaneModel)]
//! struct ExampleAgent {
//!     lane: ValueLane<i32>
//! }
//!
//! #[derive(Clone, Copy)]
//! struct ExampleLifecycle;
//!
//! #[lifecycle(ExampleAgent)]
//! impl ExampleLifecycle {
//!     
//!     #[on_start]
//!     fn my_start_handler(&self, context: HandlerContext<ExampleAgent>) -> impl EventHandler<ExampleAgent> {
//!         context.effect(|| println!("Starting agent."))
//!     }
//! }
//! ```
//!
//! For full instructions on implementing agents and lifecycles, please see the crate level documentation.
//!
//! # The [`crate::agent::projections`] Macro
//!
//! Many [`crate::agent::event_handler::EventHandler`]s that operate on agents' lanes require projections
//! onto the fields of an agent type (a function taking a reference to the agent type and returning a
//! reference to the lane of interest). These can be tedious to write and make code harder to read.
//! Therefore, the [`crate::agent::projections`] is provided to generate projections for all fields
//! of a struct as constant members of the type. The names of the projections will be the names of
//! the corresponding fields in upper case. For example:
//!
//! ```no_run
//! use swim::agent::projections;
//! use swim::agent::lanes::{ValueLane, MapLane};
//!
//! #[projections]
//! struct ExampleAgent {
//!     value_lane: ValueLane<i32>,
//!     map_lane: MapLane<String, i32>,
//! }
//! ```
//!
//! This will generate constants called `VALUE_LANE` and `MAP_LANE` with types
//! `fn(&ExampleAgent) -> &ValueLane<i32>` and `fn(&ExampleAgent) -> &MapLane<String, i32>`,
//! respectively.
//!
//! As an example, consider the following `on_start` handler for the above `ExampleAgent`:
//! ```ignore
//! #[on_start]
//! fn my_start_handler(&self, context: HandlerContext<ExampleAgent>) -> impl EventHandler<ExampleAgent> {
//!     context.set_value(VALUE_LANE, 8)
//! }
//! ```
//!
//! This will set the value of `value_lane` to 8 when the agent starts using the `VALUE_LANE` projection
//! to select the lane.
//!

pub use swim_agent_derive::{lifecycle, projections, AgentLaneModel};

/// This trait allows for the definition of an [`crate::api::Agent`] with fixed items (lanes and stores)
/// that are all registered at startup. Particularly, it defines the names, types and any flags (such as
/// whether the state is transient) for those items. It merely describes the structure and does not
/// attach any behaviour to the items (such as event handlers). To create an [`crate::api::Agent`]
/// from a type implementing this trait, it must be combined with an implementation of
/// [`crate::agent::agent_lifecycle::AgentLifecycle`] using [`crate::agent::agent_model::AgentModel`].
///
/// Implementing this trait should generally be provided by the associated derive macro. The macro can be
/// applied to any struct with labelled fields where the field types are any type implementing the
/// [`crate::agent::item::AgentItem`] trait, defined in this crate. Note that, although this trait does not
/// require [`std::default::Default`], the derive macro will generate an implementation of it so you should
/// not try to add your own implementation.
///
/// The supported lane types are:
///
/// 1. [`crate::agent::lanes::ValueLane`]
/// 2. [`crate::agent::lanes::CommandLane`]
/// 3. [`crate::agent::lanes::MapLane`]
/// 4. [`crate::agent::lanes::JoinValueLane`]
/// 5. [`crate::agent::lanes::HttpLane`] (or [`crate::agent::lanes::SimpleHttpLane`])
///
/// For [`crate::agent::lanes::ValueLane`] and [`crate::agent::lanes::CommandLane`], the type parameter
/// must implement that [`crate::form::Form`] trait (used for serialization and deserialization). For
/// [`crate::agent::lanes::MapLane`] and [`crate::agent::lanes::JoinValueLane`], both parameters must
/// implement [`crate::form::Form`] and additionally, the key type `K` must satisfy
/// `K: Hash + Eq + Ord + Clone + Form`.
///
/// For [`crate::agent::lanes::HttpLane`], the constraints on the type parameters are determined by the
/// codec that is selected for the lane (using the appropriate type parameter). By default, this is the
/// [`crate::agent::lanes::http::DefaultCodec`]. This codec always requires that type parameters implement
/// [`crate::form::Form`] and, if the 'json' feature is active, that the they are Serde serializable.
///
/// The supported store types are:
///
/// 1. [`crate::agent::stores::ValueStore`]
/// 2. [`crate::agent::stores::MapStore`]
///
/// These have exactly the same restrictions on their type parameters as the corresponding lane types.
///
/// As an example, the following is a valid agent type defining items of each supported kind:
///
/// ```no_run
/// use swim::agent::AgentLaneModel;
/// use swim::agent::lanes::{ValueLane, CommandLane, MapLane, JoinValueLane, SimpleHttpLane};
/// use swim::agent::stores::{ValueStore, MapStore};
///
/// #[derive(AgentLaneModel)]
/// struct ExampleAgent {
///     value_lane: ValueLane<i32>,
///     command_lane: CommandLane<String>,
///     map_lane: MapLane<String, i64>,
///     value_store: ValueStore<i32>,
///     map_store: MapStore<String, i64>,
///     join_value: JoinValueLane<String, i64>,
///     http_lane: SimpleHttpLane<String>,
/// }
/// ```
///
/// The macro will use the name of the field as the name of the item (the value lane from this example will
/// have the name `"value_lane"`).
///
/// By default [`crate::agent::lanes::ValueLane`]s, [`crate::agent::lanes::MapLane`]s and
/// [`crate::agent::lanes::JoinValueLane`]s (and the corresponding stores types) will persist their state
/// (where the server has a persistence store). To disable this, the lane field may be marked as transient
/// with an attribute:
///
/// / ```no_run
/// use swim::agent::AgentLaneModel;
/// use swim::agent::lanes::ValueLane;
///
/// #[derive(AgentLaneModel)]
/// struct TransientAgent {
///     #[transient]
///     value_lane: ValueLane<i32>,
/// }
/// ```
pub trait AgentLaneModel: agent_model::AgentSpec {}

impl<A> AgentLaneModel for A where A: agent_model::AgentSpec {}

pub use swim_agent::agent_lifecycle;
pub use swim_agent::downlink_lifecycle;
pub use swim_agent::event_handler;
pub use swim_agent::reexport;

pub mod config {
    pub use swim_agent::config::{MapDownlinkConfig, SimpleDownlinkConfig};
}
pub mod model {
    pub use swim_agent::model::{HttpLaneRequest, MapMessage, Text};
}

pub mod agent_model {
    pub use swim_agent::agent_model::{
        AgentModel, AgentSpec, ItemFlags, ItemInitializer, ItemKind, ItemSpec,
        JoinValueInitializer, MapLaneInitializer, MapStoreInitializer, ValueLaneInitializer,
        ValueStoreInitializer, WriteResult,
    };

    pub mod downlink {
        pub mod hosted {
            pub use swim_agent::agent_model::downlink::hosted::{
                EventDownlinkHandle, MapDownlinkHandle, ValueDownlinkHandle,
            };
        }
    }
}

pub mod item {
    pub use swim_agent::item::AgentItem;
}

pub mod lanes {
    pub use swim_agent::lanes::{
        CommandLane, DemandLane, DemandMapLane, HttpLane, JoinValueLane, LaneItem, MapLane,
        SimpleHttpLane, ValueLane,
    };

    pub mod command {
        pub use swim_agent::lanes::command::{decode_and_command, DecodeAndCommand};
        pub mod lifecycle {
            pub use swim_agent::lanes::command::lifecycle::StatefulCommandLaneLifecycle;
        }
    }

    pub mod demand {
        pub use swim_agent::lanes::demand::DemandLaneSync;

        pub mod lifecycle {
            pub use swim_agent::lanes::demand::lifecycle::StatefulDemandLaneLifecycle;
        }
    }

    pub mod demand_map {
        pub use swim_agent::lanes::demand_map::DemandMapLaneSync;

        pub mod lifecycle {
            pub use swim_agent::lanes::demand_map::lifecycle::StatefulDemandMapLaneLifecycle;
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

    pub mod join_value {
        pub use swim_agent::lanes::join_value::{JoinValueLaneSync, LinkClosedResponse};
        pub mod lifecycle {
            pub use swim_agent::lanes::join_value::lifecycle::JoinValueLaneLifecycle;
        }
    }

    pub mod http {
        pub use swim_agent::lanes::http::{
            CodecError, DefaultCodec, HttpLaneAccept, HttpLaneCodec, HttpLaneCodecSupport,
            HttpRequestContext, Recon, Response, UnitResponse,
        };

        #[cfg(feature = "json")]
        pub use swim_agent::lanes::http::Json;

        pub mod lifecycle {
            pub use swim_agent::lanes::http::lifecycle::StatefulHttpLaneLifecycle;
        }
    }
}

pub mod state {
    pub use swim_agent::state::{History, State};
}

pub mod stores {
    pub use swim_agent::stores::{MapStore, StoreItem, ValueStore};
}
