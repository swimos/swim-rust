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
pub mod encoding {

    pub mod downlink {
        pub use crate::downlink::{
            DownlinkNotificationEncoder, DownlinkOperationDecoder, DownlinkOperationEncoder,
            MapNotificationDecoder, ValueNotificationDecoder,
        };
    }

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

    pub mod map {
        pub use crate::map::{
            MapMessageDecoder, MapMessageEncoder, MapOperationDecoder, MapOperationEncoder,
            RawMapMessageDecoder, RawMapMessageEncoder, RawMapOperationDecoder,
            RawMapOperationEncoder,
        };
    }

    pub mod ad_hoc {
        pub use crate::ad_hoc::{
            AdHocCommandDecoder, AdHocCommandEncoder, RawAdHocCommandDecoder,
            RawAdHocCommandEncoder,
        };
    }

    pub mod store {
        pub use crate::store::{
            MapInitDecoder, MapStoreResponseEncoder, RawMapInitDecoder, RawMapInitEncoder,
            RawMapStoreResponseDecoder, RawValueInitDecoder, RawValueInitEncoder,
            RawValueStoreResponseDecoder, StoreInitializedCodec, ValueInitDecoder,
            ValueStoreResponseEncoder,
        };
    }
}

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
