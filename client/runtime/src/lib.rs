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

pub mod downlink;
mod runtime;

use ratchet::WebSocketConfig;
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
pub use swim_api::downlink::DownlinkKind;
pub use swim_api::error::DownlinkTaskError;
use swim_utilities::non_zero_usize;

#[non_exhaustive]
pub struct ClientConfig {
    pub websocket: WebSocketConfig,
    pub remote_buffer_size: NonZeroUsize,
}

impl Debug for ClientConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ClientConfig {
            websocket,
            remote_buffer_size,
        } = self;
        f.debug_struct("ClientConfig")
            .field("websocket", websocket)
            .field("remote_buffer_size", remote_buffer_size)
            .finish()
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            websocket: WebSocketConfig::default(),
            remote_buffer_size: non_zero_usize!(4096),
        }
    }
}
