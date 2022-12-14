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

use std::fmt::Display;

use crate::model::lifecycle::ValueDownlinkLifecycle;
use futures::future::join;
use futures::{Sink, SinkExt, StreamExt};
use swim_api::downlink::DownlinkConfig;
use swim_api::error::DownlinkTaskError;
use swim_api::protocol::downlink::{
    DownlinkNotification, DownlinkOperation, DownlinkOperationEncoder, ValueNotificationDecoder,
};
use swim_form::structural::write::StructuralWritable;
use swim_form::Form;
use swim_model::address::Address;
use swim_model::Text;
use swim_recon::printer::print_recon;
use swim_utilities::future::immediate_or_join;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{info_span, trace};
use tracing_futures::Instrument;

use crate::ValueDownlinkModel;

/// Task to drive a value downlink, calling lifecyle events at appropriate points.
///
/// #Arguments
///
/// * `model` - The downlink model, providing the lifecycle and a stream of values to set.
/// * `path` - The path of the lane to which the downlink is attached.
/// * `config` - Configuration parameters to the downlink.
/// * `input` - Input stream for messages to the downlink from the runtime.
/// * `output` - Output stream for messages from the downlink to the runtime.
pub async fn value_dowinlink_task<T, LC>(
    model: ValueDownlinkModel<T, LC>,
    path: Address<Text>,
    config: DownlinkConfig,
    input: ByteReader,
    output: ByteWriter,
) -> Result<(), DownlinkTaskError>
where
    T: Form + Send + Sync + 'static,
    LC: ValueDownlinkLifecycle<T>,
{
    let ValueDownlinkModel {
        set_value,
        lifecycle,
    } = model;
    let (stop_tx, stop_rx) = trigger::trigger();
    let read = async move {
        let result = read_task(config, input, lifecycle).await;
        let _ = stop_tx.trigger();
        result
    }
    .instrument(info_span!("Downlink read task.", %path));
    let framed_writer = FramedWrite::new(output, DownlinkOperationEncoder);
    let write = write_task(framed_writer, set_value, stop_rx)
        .instrument(info_span!("Downlink write task.", %path));
    let (read_result, _) = join(read, write).await;
    read_result
}

enum State<T> {
    Unlinked,
    Linked(Option<T>),
    Synced(T),
}

struct ShowState<'a, T>(&'a State<T>);

impl<'a, T: StructuralWritable> Display for ShowState<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ShowState(inner) = self;
        match *inner {
            State::Unlinked => f.write_str("Unlinked"),
            State::Linked(Some(v)) => write!(f, "Linked({})", print_recon(v)),
            State::Linked(_) => f.write_str("Linked"),
            State::Synced(v) => write!(f, "Synced({})", print_recon(v)),
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
    LC: ValueDownlinkLifecycle<T>,
{
    let DownlinkConfig {
        events_when_not_synced,
        terminate_on_unlinked,
        ..
    } = config;
    let mut state: State<T> = State::Unlinked;
    let mut framed_read = FramedRead::new(input, ValueNotificationDecoder::default());

    while let Some(result) = framed_read.next().await {
        match result? {
            DownlinkNotification::Linked => {
                trace!(
                    "Received Linked in state {state}",
                    state = ShowState(&state)
                );
                if matches!(&state, State::Unlinked) {
                    lifecycle.on_linked().await;
                    state = State::Linked(None);
                }
            }
            DownlinkNotification::Synced => {
                trace!(
                    "Received Synced in state {state}",
                    state = ShowState(&state)
                );
                match state {
                    State::Linked(Some(value)) => {
                        lifecycle.on_synced(&value).await;
                        state = State::Synced(value);
                    }
                    _ => {
                        return Err(DownlinkTaskError::SyncedWithNoValue);
                    }
                }
            }
            DownlinkNotification::Event { body } => {
                trace!(
                    "Received Event with body '{body}' in state {state}",
                    body = print_recon(&body),
                    state = ShowState(&state)
                );
                match &mut state {
                    State::Linked(value) => {
                        if events_when_not_synced {
                            lifecycle.on_event(&body).await;
                            lifecycle.on_set(value.as_ref(), &body).await;
                        }
                        *value = Some(body);
                    }
                    State::Synced(value) => {
                        lifecycle.on_event(&body).await;
                        lifecycle.on_set(Some(value), &body).await;
                        *value = body;
                    }
                    _ => {}
                }
            }
            DownlinkNotification::Unlinked => {
                trace!(
                    "Received Unlinked in state {state}",
                    state = ShowState(&state)
                );
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

async fn write_task<Snk, T>(
    mut framed: Snk,
    set_value: mpsc::Receiver<T>,
    stop_trigger: trigger::Receiver,
) where
    Snk: Sink<DownlinkOperation<T>> + Unpin,
    T: Form + Send + 'static,
{
    let mut set_stream = ReceiverStream::new(set_value).take_until(stop_trigger);
    while let (Some(value), Some(Ok(_)) | None) =
        immediate_or_join(set_stream.next(), framed.flush()).await
    {
        trace!("Sending command '{cmd}'.", cmd = print_recon(&value));
        let op = DownlinkOperation::new(value);
        if framed.feed(op).await.is_err() {
            break;
        }
    }
}
