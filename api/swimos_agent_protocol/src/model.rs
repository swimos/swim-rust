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

use swimos_api::address::Address;
use swimos_form::Form;
use swimos_model::Text;
use swimos_utilities::encoding::BytesStr;
use uuid::Uuid;

/// Message type for communication between the agent runtime and agent implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneRequest<T> {
    /// A command to alter the state of the lane.
    Command(T),
    /// Indicates that the lane initialization phase is complete.
    InitComplete,
    /// Request a synchronization with the lane (responses will be tagged with the provided ID).
    Sync(Uuid),
}

/// Message type for communication from the agent implementation to the agent runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneResponse<T> {
    /// An event to be broadcast to all uplinks.
    StandardEvent(T),
    /// Indicates that the lane has been initialized.
    Initialized,
    /// An event to be sent to a specific uplink.
    SyncEvent(Uuid, T),
    /// Signal that an uplink has a consistent view of a lane.
    Synced(Uuid),
}

impl<T> LaneResponse<T> {
    pub fn synced(id: Uuid) -> Self {
        LaneResponse::Synced(id)
    }

    pub fn event(body: T) -> Self {
        LaneResponse::StandardEvent(body)
    }

    pub fn sync_event(id: Uuid, body: T) -> Self {
        LaneResponse::SyncEvent(id, body)
    }
}

/// An operation that can be applied to a map lane. This type is used by map uplinks and downlinks
/// to describe alterations to the lane.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Form)]
pub enum MapOperation<K, V> {
    /// Update the value associated with a key in the map (or insert an entry if they key does not exist).
    #[form(tag = "update")]
    Update {
        key: K,
        #[form(body)]
        value: V,
    },
    /// Remove an entry from the map, by key (does nothing if there is no such entry).
    #[form(tag = "remove")]
    Remove {
        #[form(header)]
        key: K,
    },
    /// Remove all entries in the map.
    #[form(tag = "clear")]
    Clear,
}

/// Representation of map lane messages (used to form the body of Recon messages when operating)
/// on downlinks. This extends [`MapOperation`] with `Take` (retain the first `n` items) and `Drop`
/// (remove the first `n` items).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Form, Hash)]
pub enum MapMessage<K, V> {
    /// Update the value associated with a key in the map (or insert an entry if they key does not exist).
    #[form(tag = "update")]
    Update {
        key: K,
        #[form(body)]
        value: V,
    },
    /// Remove an entry from the map, by key (does nothing if there is no such entry).
    #[form(tag = "remove")]
    Remove {
        #[form(header)]
        key: K,
    },
    /// Remove all entries in the map.
    #[form(tag = "clear")]
    Clear,
    /// Retain only the first `n` entries in the map, the remainder are removed. The ordering
    /// used to determine 'first' is the Recon order of the keys. If there are fewer than `n`
    /// entries in the map, this does nothing.
    #[form(tag = "take")]
    Take(#[form(header_body)] u64),
    /// Remove the first `n` entries in the map. The ordering used to determine 'first' is the
    /// Recon order of the keys. If there are fewer than `n` entries in the map, it is cleared.
    #[form(tag = "drop")]
    Drop(#[form(header_body)] u64),
}

pub type MapLaneResponse<K, V> = LaneResponse<MapOperation<K, V>>;

/// Message type used by the runtime the initialize the state of a store when an agent starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreInitMessage<T> {
    /// A command to alter the state of the lane.
    Command(T),
    /// Indicates that the lane initialization phase is complete.
    InitComplete,
}

/// The message sent to the runtime by a store when it has been successfully initialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreInitialized;

/// Message type for an agent to notify the runtime of a change to the state of store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreResponse<T> {
    pub message: T,
}

impl<T> StoreResponse<T> {
    pub fn new(message: T) -> Self {
        StoreResponse { message }
    }
}

pub type MapStoreResponse<K, V> = StoreResponse<MapOperation<K, V>>;

impl<T> From<StoreResponse<T>> for LaneResponse<T> {
    fn from(response: StoreResponse<T>) -> Self {
        let StoreResponse { message } = response;
        LaneResponse::StandardEvent(message)
    }
}

/// Target endpoint for a command message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandMessageTarget<S> {
    /// Explicit address for an ad hoc message.
    Addressed(Address<S>),
    /// ID of a registered point to point connection.
    Registered(u16),
}

impl PartialEq<CommandMessageTarget<&str>> for CommandMessageTarget<Text> {
    fn eq(&self, other: &CommandMessageTarget<&str>) -> bool {
        match (self, other) {
            (CommandMessageTarget::Registered(id1), CommandMessageTarget::Registered(id2)) => id1 == id2,
            (CommandMessageTarget::Addressed(addr1), CommandMessageTarget::Addressed(addr2)) => addr1 == addr2,
            _ => false,
        }
    }
}

impl PartialEq<CommandMessageTarget<&str>> for CommandMessageTarget<BytesStr> {
    fn eq(&self, other: &CommandMessageTarget<&str>) -> bool {
        match (self, other) {
            (CommandMessageTarget::Registered(id1), CommandMessageTarget::Registered(id2)) => id1 == id2,
            (CommandMessageTarget::Addressed(addr1), CommandMessageTarget::Addressed(addr2)) => addr1 == addr2,
            _ => false,
        }
    }
}

/// Message type for agents to send commands to the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommandMessage<S, T> {
    pub target: CommandMessageTarget<S>,
    pub command: T,
    pub overwrite_permitted: bool,
}

impl<S, T> CommandMessage<S, T> {
    /// # Arguments
    /// * `address` - The target lane for the command.
    /// * `command` - The body of the command message.
    /// * `overwrite_permitted` - Controls the behaviour of command handling in the case of back-pressure.
    ///   If this is true, the command maybe be overwritten by a subsequent command to the same target (and so
    ///   will never be sent). If false, the command will be queued instead. This is a user specifiable parameter
    ///   in the API.
    pub fn ad_hoc(address: Address<S>, command: T, overwrite_permitted: bool) -> Self {
        CommandMessage {
            target: CommandMessageTarget::Addressed(address),
            command,
            overwrite_permitted,
        }
    }

    pub fn new(target: CommandMessageTarget<S>, command: T, overwrite_permitted: bool) -> Self {
        CommandMessage {
            target,
            command,
            overwrite_permitted,
        }
    }
}

impl<K, V> From<MapOperation<K, V>> for MapMessage<K, V> {
    fn from(op: MapOperation<K, V>) -> Self {
        match op {
            MapOperation::Update { key, value } => MapMessage::Update { key, value },
            MapOperation::Remove { key } => MapMessage::Remove { key },
            MapOperation::Clear => MapMessage::Clear,
        }
    }
}

impl<T1, T2> PartialEq<CommandMessage<&str, T1>> for CommandMessage<Text, T2>
where
    T2: PartialEq<T1>,
{
    fn eq(&self, other: &CommandMessage<&str, T1>) -> bool {
        self.target == other.target
            && self.command == other.command
            && self.overwrite_permitted == other.overwrite_permitted
    }
}

impl<T1, T2> PartialEq<CommandMessage<&str, T1>> for CommandMessage<BytesStr, T2>
where
    T2: PartialEq<T1>,
{
    fn eq(&self, other: &CommandMessage<&str, T1>) -> bool {
        self.target == other.target
            && self.command == other.command
            && self.overwrite_permitted == other.overwrite_permitted
    }
}

/// Message type for communication from the runtime to a downlink subscriber.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DownlinkNotification<T> {
    Linked,
    Synced,
    Event { body: T },
    Unlinked,
}

/// Message type for communication from a downlink subscriber to the runtime.
#[derive(Debug, PartialEq, Eq)]
pub struct DownlinkOperation<T> {
    pub body: T,
}

impl<T> DownlinkOperation<T> {
    pub fn new(body: T) -> Self {
        DownlinkOperation { body }
    }
}
