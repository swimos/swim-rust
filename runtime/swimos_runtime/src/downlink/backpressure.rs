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

use swimos_agent_protocol::encoding::{
    downlink::DownlinkOperationDecoder, map::RawMapOperationDecoder,
};
use tokio_util::codec::Decoder;

use crate::backpressure::{BackpressureStrategy, MapBackpressure, ValueBackpressure};

pub trait DownlinkBackpressure: BackpressureStrategy {
    /// Decoder for operations received from the downlink implementation.
    type Dec: Decoder<Item = Self::Operation>;

    fn make_decoder() -> Self::Dec;
}

impl DownlinkBackpressure for ValueBackpressure {
    type Dec = DownlinkOperationDecoder;

    fn make_decoder() -> Self::Dec {
        Default::default()
    }
}

impl DownlinkBackpressure for MapBackpressure {
    type Dec = RawMapOperationDecoder;
    fn make_decoder() -> Self::Dec {
        Default::default()
    }
}
