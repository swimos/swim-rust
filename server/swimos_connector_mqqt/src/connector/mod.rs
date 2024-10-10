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

mod egress;
mod ingress;

use std::time::Duration;

pub use ingress::MqttIngressConnector;
use rumqttc::{AsyncClient, EventLoop, MqttOptions};

use crate::MqttConnectorError;

const DEFAULT_CHAN_SIZE: usize = 16;

fn open_client(
    url: &str,
    keep_alive_secs: Option<u64>,
    max_packet_size: Option<usize>,
    client_channel_size: Option<usize>,
    max_inflight: Option<u32>,
) -> Result<(AsyncClient, EventLoop), MqttConnectorError> {
    let mut opts = MqttOptions::parse_url(url)?;
    if let Some(t) = keep_alive_secs {
        opts.set_keep_alive(Duration::from_secs(t));
    }
    if let Some(n) = max_packet_size {
        opts.set_max_packet_size(n, n);
    }
    if let Some(n) = max_inflight {
        let max = u16::try_from(n).unwrap_or(u16::MAX);
        opts.set_inflight(max);
    }
    let cap = client_channel_size.unwrap_or(DEFAULT_CHAN_SIZE);
    Ok(AsyncClient::new(opts, cap))
}
