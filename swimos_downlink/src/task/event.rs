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

use std::fmt::Display;

use crate::model::lifecycle::EventDownlinkLifecycle;
use futures::StreamExt;
use swimos_agent_protocol::{encoding::downlink::ValueNotificationDecoder, DownlinkNotification};
use swimos_api::{address::Address, error::DownlinkTaskError};
use swimos_client_api::DownlinkConfig;
use swimos_form::Form;
use swimos_model::Text;
use swimos_recon::print_recon;
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};
use tokio_util::codec::FramedRead;
use tracing::{info_span, trace, Instrument};

use crate::EventDownlinkModel;

/// Task to drive an event downlink, calling lifecyle events at appropriate points.
///
/// # Arguments
///
/// * `model` - The downlink model, providing the lifecycle and a stream of values to set.
/// * `path` - The path of the lane to which the downlink is attached.
/// * `config` - Configuration parameters to the downlink.
/// * `input` - Input stream for messages to the downlink from the runtime.
/// * `_output` - Output stream for messages from the downlink to the runtime.
pub async fn event_downlink_task<T, LC>(
    model: EventDownlinkModel<T, LC>,
    path: Address<Text>,
    config: DownlinkConfig,
    input: ByteReader,
    _output: ByteWriter,
) -> Result<(), DownlinkTaskError>
where
    T: Form + Send + Sync + 'static,
    LC: EventDownlinkLifecycle<T>,
{
    let EventDownlinkModel { lifecycle, .. } = model;

    read_task(config, input, lifecycle)
        .instrument(info_span!("Downlink read task.", %path))
        .await
}

enum State {
    Unlinked,
    Linked,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Unlinked => f.write_str("Unlinked"),
            State::Linked => f.write_str("Linked"),
        }
    }
}

async fn read_task<T, LC>(
    config: DownlinkConfig,
    input: ByteReader,
    mut lifecycle: LC,
) -> Result<(), DownlinkTaskError>
where
    T: Form + Send + Sync + 'static,
    LC: EventDownlinkLifecycle<T>,
{
    let DownlinkConfig {
        terminate_on_unlinked,
        ..
    } = config;
    let mut state = State::Unlinked;
    let mut framed_read = FramedRead::new(input, ValueNotificationDecoder::default());

    while let Some(result) = framed_read.next().await {
        match result? {
            DownlinkNotification::Linked | DownlinkNotification::Synced => {
                trace!("Received Linked or Synced in state {state}", state = &state);
                if matches!(&state, State::Unlinked) {
                    lifecycle.on_linked().await;
                    state = State::Linked;
                }
            }
            DownlinkNotification::Event { body } => {
                trace!(
                    "Received Event with body '{body}' in state {state}",
                    body = print_recon(&body),
                    state = &state
                );
                if matches!(state, State::Linked) {
                    lifecycle.on_event(&body).await;
                }
            }
            DownlinkNotification::Unlinked => {
                trace!("Received Unlinked in state {state}", state = &state);
                lifecycle.on_unlinked().await;
                if terminate_on_unlinked {
                    trace!("Terminating on Unlinked.");
                    break;
                } else {
                    state = State::Unlinked;
                }
            }
        }
    }
    Ok(())
}
