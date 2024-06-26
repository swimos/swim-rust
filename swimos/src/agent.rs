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
//! 1. [`AgentLaneModel`] - This defines the structure of the agent as a fixed collection
//! of typed lanes. This trait has a derive macro which can be applied to any struct with named fields
//! where each field is a lane type. See the documentation for the trait for instructions on how to use
//! the derive macro.
//! 2. [`agent_lifecycle::AgentLifecycle`] - The trait defines custom behaviour that can be
//! attached to an agent. An agent, and its lanes have associated lifecycle events (for example, the
//! `on_start` event that triggers when an instance of the agent starts.)
//!
//! It also provides attribute macros to generate agent lifecycles:
//!
//! 1. The macro [`lifecycle`] which can be applied to impl blocks to derive agent lifecycles.
//! 2. The macro [`projections`] which can be applied to agent types to make it easier to refer to its lanes and stores in event handlers.

/// # The Lifecycle Macro
///
/// [Agent lifecycles](`agent_lifecycle::AgentLifecycle`) can be created for an agent using the lifecycle
/// attribute macro. To create a trivial lifecycle (that does nothing), create a new
/// type and attach the attribute to an impl block:
///
/// ```no_run
/// use swimos::agent::{AgentLaneModel, lifecycle};
/// use swimos::agent::lanes::ValueLane;
///
/// #[derive(AgentLaneModel)]
/// struct ExampleAgent {
///     lane: ValueLane<i32>
/// }
///
/// #[derive(Clone, Copy)]
/// struct ExampleLifecycle;
///
/// #[lifecycle(ExampleAgent)]
/// impl ExampleLifecycle {}
/// ```
///
/// This macro will add a function `into_lifecycle` that will convert an instance of `ExampleLifecycle` into
/// an [agent lifecycle](`agent_lifecycle::AgentLifecycle`).
///
/// Lifecycle events can then be handled by adding handler functions to the impl block. For example,
/// to attach an event to the `on_start` event, a function with the following signature could be
/// added:
///
///  ```no_run
/// use swimos::agent::{AgentLaneModel, lifecycle};
/// use swimos::agent::agent_lifecycle::HandlerContext;
/// use swimos::agent::event_handler::EventHandler;
/// use swimos::agent::lanes::ValueLane;
///
/// #[derive(AgentLaneModel)]
/// struct ExampleAgent {
///     lane: ValueLane<i32>
/// }
///
/// #[derive(Clone, Copy)]
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
pub use swimos_agent_derive::lifecycle;

/// # The Projections Macro
///
/// Many [event handlers](`event_handler::EventHandler`) that operate on agents' lanes require projections
/// onto the fields of an agent type (a function taking a reference to the agent type and returning a
/// reference to the lane of interest). These can be tedious to write and make code harder to read.
/// Therefore, the projections macro is provided to generate projections for all fields
/// of a struct, as constant members of the type. The names of the projections will be the names of
/// the corresponding fields in upper case. For example:
///
/// ```no_run
/// use swimos::agent::projections;
/// use swimos::agent::lanes::{ValueLane, MapLane};
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
/// As an example, consider the following `on_start` handler for the above `ExampleAgent`:
/// ```ignore
/// #[on_start]
/// fn my_start_handler(&self, context: HandlerContext<ExampleAgent>) -> impl EventHandler<ExampleAgent> {
///     context.set_value(VALUE_LANE, 8)
/// }
/// ```
///
/// This will set the value of `value_lane` to 8 when the agent starts using the `VALUE_LANE` projection
/// to select the lane.
pub use swimos_agent_derive::projections;

pub use swimos_agent_derive::AgentLaneModel;

/// This trait allows for the definition of an [agent](`crate::api::Agent`) with fixed items (lanes and stores)
/// that are all registered at startup. Particularly, it defines the names, types and any flags (such as
/// whether the state is transient) for those items. It merely describes the structure and does not
/// attach any behaviour to the items (such as event handlers). To create an [agent](`crate::api::Agent`)
/// from a type implementing this trait, it must be combined with an implementation of an
/// [agent lifecycle](`agent_lifecycle::AgentLifecycle`) using [the agent model trait](`agent_model::AgentModel`).
///
/// Implementing this trait should generally be provided by the associated derive macro. The macro can be
/// applied to any struct with labelled fields where the field types are any type implementing the
/// [agent item](`crate::agent::AgentItem`), defined in this crate. Note that, although this trait does not
/// require [`std::default::Default`], the derive macro will generate an implementation of it so you should
/// not try to add your own implementation.
///
/// The supported lane types are:
///
/// 1. [Value Lanes](`lanes::ValueLane`)
/// 2. [Command Lanes](`lanes::CommandLane`)
/// 3. [Map Lanes](`lanes::MapLane`)
/// 4. [Join-Value Lanes](`lanes::JoinValueLane`)
/// 5. [Join-Map Lanes](`lanes::JoinMapLane`)
/// 6. [HTTP Lanes](`lanes::HttpLane`) (or [Simple HTTP Lanes](`lanes::SimpleHttpLane`))
///
/// For [Value Lanes](`lanes::ValueLane`) and [Map Lanes](`lanes::CommandLane`), the type parameter
/// must implement the [`swimos_form::Form`] trait (used for serialization and deserialization). For
/// [Map Lanes](`lanes::MapLane`), [Join-Value Lanes](`lanes::JoinValueLane`) and [Join-Map Lanes](`lanes::JoinMapLane`),
/// both parameters must implement [`swimos_form::Form`] and additionally, the key type `K` must additionally
/// satisfy `K: Hash + Eq + Ord + Clone`.
///
/// Additionally, for [Join-Map Lanes](`lanes::JoinMapLane`), the link key type `L` must satisfy`L: Hash + Eq + Clone`.
///
/// The supported store types are:
///
/// 1. [Value Stores](`stores::ValueStore`)
/// 2. [Map Stores](`stores::MapStore`)
///
/// These have the same restrictions on their type parameters as the corresponding lane types.
///
/// For [HTTP Lanes](`lanes::HttpLane`), the constraints on the type parameters are determined by the
/// codec that is selected for the lane (using the appropriate type parameter). By default, this is the
/// [Default Codec](`lanes::http::DefaultCodec`). This codec always requires that type parameters implement
/// [`swimos_form::Form`] and, if the `json` feature is active, that they are Serde serializable.
/// [`crate::agent::lanes::http::DefaultCodec`]. 
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
/// use swimos::agent::AgentLaneModel;
/// use swimos::agent::lanes::{ValueLane, CommandLane, MapLane, JoinValueLane, JoinMapLane, SimpleHttpLane};
/// use swimos::agent::stores::{ValueStore, MapStore};
///
/// #[derive(AgentLaneModel)]
/// struct ExampleAgent {
///     value_lane: ValueLane<i32>,
///     command_lane: CommandLane<String>,
///     map_lane: MapLane<String, i64>,
///     value_store: ValueStore<i32>,
///     map_store: MapStore<String, i64>,
///     join_value: JoinValueLane<String, i64>,
///     join_map: JoinMapLane<String, String, i64>,
///     http_lane: SimpleHttpLane<String>,
/// }
/// ```
///
/// The macro will use the name of the field as the name of the item (the value lane from this example will
/// have the name `"value_lane"`).
///
/// By default [Value Lanes](`lanes::ValueLane`) and [Map Lanes](`lanes::MapLane`) (and the corresponding
/// stores types) will persist their state (where the server has a persistence store). To disable this, the lane
/// field may be marked as transient with an attribute:
///
/// ```no_run
/// use swimos::agent::AgentLaneModel;
/// use swimos::agent::lanes::ValueLane;
///
/// #[derive(AgentLaneModel)]
/// struct TransientAgent {
///     #[item(transient)]
///     value_lane: ValueLane<i32>,
/// }
/// ```
pub trait AgentLaneModel: agent_model::AgentSpec {}

impl<A> AgentLaneModel for A where A: agent_model::AgentSpec {}

pub use swimos_agent::agent_lifecycle;
pub use swimos_agent::downlink_lifecycle;
pub use swimos_agent::event_handler;
pub use swimos_agent::reexport;

/// Configuration types for downlinks that are started from agent lifecycles.
pub mod config {
    pub use swimos_agent::config::{MapDownlinkConfig, SimpleDownlinkConfig};
}

/// Special model types required from some agent event handlers.
pub mod model {
    pub use swimos_agent::model::{HttpLaneRequest, MapMessage, Text};
}

/// Defines the [agent specification](`agent_model::AgentSpec`) trait used to specify the structure of an agent it terms of lanes
/// and stores.
pub mod agent_model {
    pub use swimos_agent::agent_model::{
        AgentModel, AgentSpec, ItemDescriptor, ItemFlags, ItemInitializer, ItemKind, ItemSpec,
        MapLaneInitializer, MapStoreInitializer, ValueLaneInitializer, ValueStoreInitializer,
        WriteResult,
    };
    pub use swimos_api::agent::{LaneKind, StoreKind, WarpLaneKind};

    /// Support for executing downlink lifecycles within agents.
    pub mod downlink {
        pub use swimos_agent::agent_model::downlink::{
            EventDownlinkHandle, MapDownlinkHandle, ValueDownlinkHandle,
        };
    }
}

pub use swimos_agent::AgentItem;

/// Defines the lane types that can be included in agent specifications. Lanes are exposed externally by the runtime,
/// using the WARP protocol (or HTTP in the case of [HTTP lanes](`lanes::HttpLane`)). The states of lanes may be stored
/// persistently by the runtime.
pub mod lanes {

    pub use swimos_agent::lanes::{
        CommandLane, DemandLane, DemandMapLane, HttpLane, JoinMapLane, JoinValueLane, LaneItem,
        LinkClosedResponse, MapLane, SimpleHttpLane, SupplyLane, ValueLane,
    };

    #[doc(hidden)]
    pub mod command {

        pub use swimos_agent::lanes::command::{decode_and_command, DecodeAndCommand};
        pub mod lifecycle {
            pub use swimos_agent::lanes::command::lifecycle::StatefulCommandLaneLifecycle;
        }
    }

    #[doc(hidden)]
    pub mod demand {
        pub use swimos_agent::lanes::demand::DemandLaneSync;

        pub mod lifecycle {
            pub use swimos_agent::lanes::demand::lifecycle::StatefulDemandLaneLifecycle;
        }
    }

    #[doc(hidden)]
    pub mod demand_map {
        pub use swimos_agent::lanes::demand_map::DemandMapLaneSync;

        pub mod lifecycle {
            pub use swimos_agent::lanes::demand_map::lifecycle::StatefulDemandMapLaneLifecycle;
        }
    }

    #[doc(hidden)]
    pub mod value {
        pub use swimos_agent::lanes::value::{decode_and_set, DecodeAndSet, ValueLaneSync};
        pub mod lifecycle {
            pub use swimos_agent::lanes::value::lifecycle::StatefulValueLaneLifecycle;
        }
    }

    #[doc(hidden)]
    pub mod map {
        pub use swimos_agent::lanes::map::{decode_and_apply, DecodeAndApply, MapLaneSync};
        pub mod lifecycle {
            pub use swimos_agent::lanes::map::lifecycle::StatefulMapLaneLifecycle;
        }
    }

    #[doc(hidden)]
    pub mod join_map {
        pub use swimos_agent::lanes::join_map::JoinMapLaneSync;
        pub mod lifecycle {
            pub use swimos_agent::lanes::join_map::lifecycle::JoinMapLaneLifecycle;
        }
    }

    #[doc(hidden)]
    pub mod join_value {
        pub use swimos_agent::lanes::join_value::JoinValueLaneSync;
        pub mod lifecycle {
            pub use swimos_agent::lanes::join_value::lifecycle::JoinValueLaneLifecycle;
        }
    }

    /// HTTP request/response model and codecs for HTTP lanes.
    pub mod http {
        pub use swimos_agent::lanes::http::{
            CodecError, DefaultCodec, HttpLaneCodec, HttpLaneCodecSupport, HttpRequestContext,
            Recon, Response, UnitResponse,
        };

        #[doc(hidden)]
        pub use swimos_agent::lanes::http::HttpLaneAccept;

        #[cfg(feature = "json")]
        pub use swimos_agent::lanes::http::Json;

        #[doc(hidden)]
        pub mod lifecycle {
            pub use swimos_agent::lanes::http::lifecycle::StatefulHttpLaneLifecycle;
        }
    }

    #[doc(hidden)]
    pub mod supply {
        pub use swimos_agent::lanes::supply::SupplyLaneSync;
    }
}

/// Utility types for building stateful agent lifecycles.
pub mod state {
    pub use swimos_agent::state::{History, State};
}

/// Defines the store types that can be included in agent specifications. These are not exposed externally but their states
/// may be stored persistently by the runtime.
pub mod stores {
    pub use swimos_agent::stores::{MapStore, StoreItem, ValueStore};
}
