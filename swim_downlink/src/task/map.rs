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

use std::collections::hash_map::RandomState;
use std::fmt::Display;
use std::hash::Hash;

use crate::model::lifecycle::MapDownlinkLifecycle;
use futures::future::join;
use futures::{Sink, SinkExt, StreamExt};
use im::hashmap::Entry;
use swim_api::downlink::DownlinkConfig;
use swim_api::error::DownlinkTaskError;
use swim_api::protocol::downlink::{
    DownlinkNotification, DownlinkOperation, DownlinkOperationEncoder, MapNotificationDecoder,
    ValueNotificationDecoder,
};
use swim_api::protocol::map::MapMessage;
use swim_form::structural::write::StructuralWritable;
use swim_form::Form;
use swim_model::path::Path;
use swim_recon::printer::print_recon;
use swim_utilities::future::immediate_or_join;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{info_span, trace};
use tracing_futures::Instrument;

use crate::MapDownlinkModel;

/// Task to drive a map downlink, calling lifecycle events at appropriate points.
///
/// # Arguments
///
/// * `model` - The downlink model, providing the lifecycle and a stream of events to process.
/// * `path` - The path of the lane to which the downlink is attached.
/// * `config` - Configuration parameters to the downlink.
/// * `input` - Input stream for messages to the downlink from the runtime.
/// * `output` - Output stream for messages from the downlink to the runtime.
pub async fn map_downlink_task<K, V, LC>(
    model: MapDownlinkModel<K, V, LC>,
    path: Path,
    config: DownlinkConfig,
    input: ByteReader,
    output: ByteWriter,
) -> Result<(), DownlinkTaskError>
where
    K: Form + Hash + Eq + Clone + Send + Sync + 'static,
    V: Form + Clone + Send + Sync + 'static,
    LC: MapDownlinkLifecycle<K, V>,
{
    let MapDownlinkModel {
        event_stream,
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
    let write = write_task(framed_writer, event_stream, stop_rx)
        .instrument(info_span!("Downlink write task.", %path));
    let (read_result, _) = join(read, write).await;
    read_result
}

enum State<K, V> {
    Unlinked,
    Linked(im::HashMap<K, V>),
    Synced(im::HashMap<K, V>),
}

struct ShowState<'a, K, V>(&'a State<K, V>);

//Todo change print
impl<'a, K: StructuralWritable, V: StructuralWritable> Display for ShowState<'a, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ShowState(inner) = self;
        match *inner {
            State::Unlinked => f.write_str("Unlinked"),
            State::Linked(map) => write!(f, "Linked({})", "unimplemented"),
            // State::Linked(_) => f.write_str("Linked"),
            State::Synced(map) => write!(f, "Synced({})", "unimplemented"),
        }
    }
}

async fn read_task<K, V, LC>(
    config: DownlinkConfig,
    input: ByteReader,
    mut lifecycle: LC,
) -> Result<(), DownlinkTaskError>
where
    K: Form + Hash + Eq + Clone + Send + Sync + 'static,
    V: Form + Clone + Send + Sync + 'static,
    LC: MapDownlinkLifecycle<K, V>,
{
    let DownlinkConfig {
        events_when_not_synced,
        terminate_on_unlinked,
    } = config;
    let mut state: State<K, V> = State::Unlinked;
    let mut framed_read = FramedRead::new(input, MapNotificationDecoder::<K, V>::default());

    while let Some(result) = framed_read.next().await {
        match result? {
            DownlinkNotification::Linked => {
                trace!(
                    "Received Linked in state {state}",
                    state = ShowState(&state)
                );
                if matches!(&state, State::Unlinked) {
                    lifecycle.on_linked().await;
                    state = State::Linked(im::HashMap::new());
                }
            }
            DownlinkNotification::Synced => {
                trace!(
                    "Received Synced in state {state}",
                    state = ShowState(&state)
                );
                match state {
                    State::Linked(value) => {
                        lifecycle.on_synced(&value).await;
                        state = State::Synced(value);
                    }
                    _ => {
                        return Err(DownlinkTaskError::SyncedWithNoValue);
                    }
                }
            }
            DownlinkNotification::Event { body } => {
                //     trace!(
                //     "Received Event with body '{body}' in state {state}",
                //     body = print_recon(&body),
                //     state = ShowState(&state)
                // );
                match &mut state {
                    State::Linked(map) => {
                        if events_when_not_synced {
                            lifecycle.on_event(&body).await;
                            match body {
                                MapMessage::Update { .. } => {}
                                MapMessage::Remove { .. } => {}
                                MapMessage::Clear => {}
                                MapMessage::Take(_) => {}
                                MapMessage::Drop(_) => {}
                            }

                            // lifecycle.on_set(value.as_ref(), &body).await;
                        }
                        // *value = Some(body);
                    }
                    State::Synced(map) => {
                        lifecycle.on_event(&body).await;

                        match body {
                            MapMessage::Update { key, value } => {
                                let prev_value = map.get(&key);
                                lifecycle.on_updated(&key, prev_value, &value);
                                map.insert(key, value);
                            }
                            MapMessage::Remove { key } => {
                                let prev_value = map.remove(&key);
                                lifecycle.on_removed(&key, prev_value.as_ref());
                            }
                            MapMessage::Clear => {
                                //Todo replace with ref
                                lifecycle.on_cleared(map.clone());
                                map.clear();
                            }
                            MapMessage::Take(n) => {
                                unimplemented!()
                            }
                            MapMessage::Drop(n) => {
                                unimplemented!()
                            }
                        }
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

async fn write_task<Snk, K, V>(
    mut framed: Snk,
    event_stream: mpsc::Receiver<MapMessage<K, V>>,
    stop_trigger: trigger::Receiver,
) where
    Snk: Sink<DownlinkOperation<MapMessage<K, V>>> + Unpin,
    K: Form + Send + 'static,
    V: Form + Send + 'static,
{
    let mut event_stream = ReceiverStream::new(event_stream).take_until(stop_trigger);
    while let (Some(value), Some(Ok(_)) | None) =
        immediate_or_join(event_stream.next(), framed.flush()).await
    {
        trace!("Sending command '{cmd}'.", cmd = print_recon(&value));
        let op = DownlinkOperation::new(value);
        if framed.feed(op).await.is_err() {
            break;
        }
    }
}
