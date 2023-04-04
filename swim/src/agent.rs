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

pub use swim_agent_derive::{lifecycle, projections, AgentLaneModel};

/// This trait allows for the definition of an [`crate::api::Agent`] with fixed items (lanes and stores)
/// that are all registered at startup. Particularly, it defines the names, types and any flags (such as
/// whether the state is transient) for those items. It merely describes the structure and does not
/// attach any behaviour to the items (such as event handlers). To create an [`crate::api::Agent`]
/// from a type implementing this trait, it must be combined with an implementation of
/// [`crate::agent::agent_lifecycle::AgentLifecycle`] using [`crate::agent::agent_model::AgentModel`].
///
/// Implementing this trait should generally be provided by he associated derive macro. The macro can be
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
///
/// For [`crate::agent::lanes::ValueLane`] and [`crate::agent::lanes::CommandLane`], the type parameter
/// must implement that [`crate::form::Form`] trait (used for serialization and deserialization). For
/// [`crate::agent::lanes::MapLane`] and [`crate::agent::lanes::JoinValueLane`], both parameters must
/// implement [`crate::form::Form`] and additionally, the key type `K` must satisfy
/// `K: Hash + Eq + Ord + Clone + Form`.
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
/// use swim::agent::lanes::{ValueLane, CommandLane, MapLane, JoinValueLane};
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
    pub use swim_agent::model::{MapMessage, Text};
}

pub mod agent_model {
    pub use swim_agent::agent_model::{
        AgentModel, AgentSpec, ItemInitializer, ItemKind, ItemSpec, JoinValueInitializer,
        LaneFlags, MapLaneInitializer, MapStoreInitializer, ValueLaneInitializer,
        ValueStoreInitializer, WriteResult,
    };
    pub use swim_api::meta::lane::LaneKind;
    pub use swim_api::store::StoreKind;
}

pub mod item {
    pub use swim_agent::item::AgentItem;
}

pub mod lanes {
    pub use swim_agent::lanes::{CommandLane, JoinValueLane, Lane, MapLane, ValueLane};

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

    pub mod join_value {
        pub use swim_agent::lanes::join_value::{JoinValueLaneSync, LinkClosedResponse};
        pub mod lifecycle {
            pub use swim_agent::lanes::join_value::lifecycle::JoinValueLaneLifecycle;
        }
    }
}

pub mod stores {
    pub use swim_agent::stores::{MapStore, Store, ValueStore};
}
