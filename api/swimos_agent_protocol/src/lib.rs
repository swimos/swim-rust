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

//! # SwimOS Agent Protocol
//!
//! This crate defines the protocol used to communicate between the Swim runtime and implementations of agents.
//!
//! The root module contains types describing the messages that can be sent on the various types of
//! channel between the runtime and the agent. Each message type has encoders and decoders in the [`encoding`]
//! module. Encoders/decoders whose name starts with 'Raw' send and receive raw arrays of bytes as the content
//! of the message. Otherwise, the body should be any type that can be serialized to Recon, using the
//! [`swimos_form::Form`] trait.

mod ad_hoc;
mod downlink;
mod lane;
mod map;
mod store;

mod model;

pub use model::{
    AdHocCommand, DownlinkNotification, DownlinkOperation, LaneRequest, LaneResponse,
    MapLaneResponse, MapMessage, MapOperation, MapStoreResponse, StoreInitMessage,
    StoreInitialized, StoreResponse,
};

/// Tokio encoders and decoders for the agent protocols.
pub mod encoding {

    /// # The protocol used by the runtime to communicate with downlinks.
    ///
    /// 1) [`crate::DownlinkNotification`] messages are sent by the runtime to downlinks to inform them that the state
    /// of the link has changed or a new event has been received.
    /// 2) [`crate::DownlinkOperation`] messages are sent to the runtime by the downlink to instruct it to send a
    /// command to the remote lane.
    pub mod downlink {
        pub use crate::downlink::{
            DownlinkNotificationEncoder, DownlinkOperationDecoder, DownlinkOperationEncoder,
            MapNotificationDecoder, ValueNotificationDecoder,
        };
    }

    /// # The protocol used by the runtime to communicate with lanes.
    ///
    /// There are two phases to the communication between the runtime and the agent.
    ///
    /// 1. Initialization
    /// 2. Agent running.
    ///
    /// During the initialization phase:
    ///
    /// 1. The runtime sends one or more [`crate::LaneRequest`] commands which transmit
    /// the state of the lane to the agent.
    /// 2. The runtime sends a single [`crate::LaneRequest::InitComplete`] message.
    /// 3. The lane responds with the [`crate::LaneResponse::Initialized`] message.
    /// 4. Both parties switch to the protocol for the Agent Running phase.
    ///
    /// During the agent running phase:
    /// 1) [`crate::LaneRequest::Command`] messages are sent by the runtime to lane to inform the lane of commands
    /// received, addressed to that lane. The lane is not require to respond.
    /// 2) Each time the state of the lane changes (whether in response to a received command or otherwise) it must
    /// notify the runtime of the change using [`crate::LaneResponse::StandardEvent`] message.
    /// 3) [`crate::LaneRequest::Sync`] messages are sent by the runtime to the lane to request its state. The lane
    /// must respond with 0 or more [`crate::LaneResponse::SyncEvent`] messages, labelled with the same ID as provided
    /// in the request. After all such messages are sent, it must send a [`crate::LaneResponse::Synced`] message with
    /// the same ID.
    pub mod lane {
        pub use crate::lane::{
            MapLaneRequestDecoder, MapLaneRequestEncoder, MapLaneResponseDecoder,
            MapLaneResponseEncoder, RawMapLaneRequestDecoder, RawMapLaneRequestEncoder,
            RawMapLaneResponseDecoder, RawMapLaneResponseEncoder, RawValueLaneRequestDecoder,
            RawValueLaneRequestEncoder, RawValueLaneResponseDecoder, RawValueLaneResponseEncoder,
            ValueLaneRequestDecoder, ValueLaneRequestEncoder, ValueLaneResponseDecoder,
            ValueLaneResponseEncoder,
        };
    }

    /// # The encoding used for map like lanes and stores, shared between the other protocols in this module.
    pub mod map {
        pub use crate::map::{
            MapMessageDecoder, MapMessageEncoder, MapOperationDecoder, MapOperationEncoder,
            RawMapMessageDecoder, RawMapMessageEncoder, RawMapOperationDecoder,
            RawMapOperationEncoder,
        };
    }

    /// # The protocol used by the agents to send ad hoc messages to lanes on other agents.
    ///
    /// [`crate::AdHocCommand`] messages are sent by agents to the runtime to instruct it to send an ad hoc
    /// command to an arbitrary lane endpoint.
    pub mod ad_hoc {
        pub use crate::ad_hoc::{
            AdHocCommandDecoder, AdHocCommandEncoder, RawAdHocCommandDecoder,
            RawAdHocCommandEncoder,
        };
    }

    /// # The protocol used by the runtime to communicate with stores.
    ///
    /// There are two phases to the communication between the runtime and the agent.
    ///
    /// 1. Initialization
    /// 2. Agent running.
    ///
    /// The initialization phase occurs when the agent is starting. After all items
    /// complete this phase, the agent running phase starts.
    ///
    /// During the initialization phase:
    ///
    /// 1. The runtime sends one or more [`crate::StoreInitMessage`] commands which transmit
    /// the state of the item to the agent.
    /// 2. The runtime sends a single [`crate::StoreInitMessage`] `InitComplete` message.
    /// 3. The store or lane responds with the [`crate::StoreInitialized`] message.
    /// 4. Both parties switch to the protocol for the Agent Running phase.
    ///
    /// During the agent running phase:
    ///
    /// 1. The runtime does not send messages to the agent and may drop the channel.
    /// 2. The store or land sends [`crate::StoreResponse`] messages each time its state
    /// changes which are persisted by the runtime.
    pub mod store {

        // TODO Non-transient lanes also implicitly contain a store. They should
        // ultimately use the initialization component of this protocol. Currently,
        // they have initialization messages built into the lane protocol.

        pub use crate::store::{
            MapStoreInitDecoder, MapStoreResponseEncoder, RawMapStoreInitDecoder,
            RawMapStoreInitEncoder, RawMapStoreResponseDecoder, RawValueStoreInitDecoder,
            RawValueStoreInitEncoder, RawValueStoreResponseDecoder, StoreInitializedCodec,
            ValueStoreInitDecoder, ValueStoreResponseEncoder,
        };
    }
}

/// Utility functions to parse the header of the Recon representation of a [`MapMessage`].
pub mod peeling {
    pub use crate::map::{extract_header, extract_header_str};
}

#[cfg(test)]
mod tests;

const TAG_SIZE: usize = std::mem::size_of::<u8>();
const LEN_SIZE: usize = std::mem::size_of::<u64>();

const COMMAND: u8 = 0;
const SYNC: u8 = 1;
const SYNC_COMPLETE: u8 = 2;
const EVENT: u8 = 3;
const INIT_DONE: u8 = 4;
const INITIALIZED: u8 = 5;

const TAG_LEN: usize = 1;
const ID_LEN: usize = std::mem::size_of::<u128>();
