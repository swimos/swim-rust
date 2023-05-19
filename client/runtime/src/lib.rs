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

#[cfg(test)]
mod tests;

mod error;
mod models;
mod pending;
mod runtime;
mod transport;

pub use crate::error::{DownlinkErrorKind, DownlinkRuntimeError, TimeoutElapsed};
pub use crate::models::RemotePath;
pub use crate::runtime::{start_runtime, RawHandle};
pub use crate::transport::{Transport, TransportRequest};
#[cfg(feature = "deflate")]
use ratchet::deflate::DeflateConfig;
use std::fmt::Debug;
use std::num::NonZeroUsize;
pub use swim_api::downlink::DownlinkKind;
pub use swim_api::error::DownlinkTaskError;
use swim_utilities::non_zero_usize;

const DEFAULT_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(32);

#[derive(Debug)]
pub struct WebSocketConfig {
    pub max_message_size: usize,
    #[cfg(feature = "deflate")]
    pub deflate_config: Option<DeflateConfig>,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        WebSocketConfig {
            max_message_size: 64 << 20,
            #[cfg(feature = "deflate")]
            deflate_config: None,
        }
    }
}

#[non_exhaustive]
#[derive(Debug)]
pub struct ClientConfig {
    pub websocket: WebSocketConfig,
    #[cfg(feature = "deflate")]
    pub deflate: Option<DeflateConfig>,
    pub remote_buffer_size: NonZeroUsize,
    pub transport_buffer_size: NonZeroUsize,
    pub registration_buffer_size: NonZeroUsize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            websocket: WebSocketConfig::default(),
            #[cfg(feature = "deflate")]
            deflate: None,
            remote_buffer_size: non_zero_usize!(4096),
            transport_buffer_size: DEFAULT_BUFFER_SIZE,
            registration_buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }
}
