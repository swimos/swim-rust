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

use crate::model::lifecycle::MapDownlinkLifecycle;
use crate::model::MapDownlinkModel;
use futures::future::join;
use futures::{Sink, SinkExt, StreamExt};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::hash::Hash;
use std::mem::take;
use swim_api::downlink::DownlinkConfig;
use swim_api::error::DownlinkTaskError;
use swim_api::protocol::downlink::{
    DownlinkNotification, DownlinkOperation, DownlinkOperationEncoder, RecNotificationDecoder,
};
use swim_api::protocol::map::MapMessage;
use swim_form::structural::read::recognizer::RecognizerReadable;
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
use tracing::{info_span, trace, Instrument};

#[derive(Debug)]
pub struct MapSender<K, V> {
    inner: mpsc::Sender<MapMessage<K, V>>,
}

impl<K, V> MapSender<K, V> {
    pub fn new(inner: mpsc::Sender<MapMessage<K, V>>) -> MapSender<K, V> {
        MapSender { inner }
    }

    pub async fn update(&self, key: K, value: V) -> Result<(), (K, V)> {
        self.inner
            .send(MapMessage::Update { key, value })
            .await
            .map_err(|e| match e.0 {
                MapMessage::Update { key, value } => (key, value),
                _ => unreachable!(),
            })
    }

    pub async fn remove(&self, key: K) -> Result<(), K> {
        self.inner
            .send(MapMessage::Remove { key })
            .await
            .map_err(|e| match e.0 {
                MapMessage::Remove { key } => key,
                _ => unreachable!(),
            })
    }

    pub async fn clear(&self) -> Result<(), ()> {
        self.inner.send(MapMessage::Clear).await.map_err(|_| ())
    }

    pub async fn take(&self, n: u64) -> Result<(), ()> {
        self.inner.send(MapMessage::Take(n)).await.map_err(|_| ())
    }

    pub async fn drop(&self, n: u64) -> Result<(), ()> {
        self.inner.send(MapMessage::Drop(n)).await.map_err(|_| ())
    }
}

/// Task to drive a map downlink, calling lifecycle events at appropriate points.
///
/// #Arguments
///
/// * `model` - The downlink model, providing the lifecycle and a stream of actions.
/// * `path` - The path of the lane to which the downlink is attached.
/// * `config` - Configuration parameters to the downlink.
/// * `input` - Input stream for messages to the downlink from the runtime.
/// * `output` - Output stream for messages from the downlink to the runtime.
pub async fn map_downlink_task<K, V, LC>(
    model: MapDownlinkModel<K, V, LC>,
    path: Address<Text>,
    config: DownlinkConfig,
    input: ByteReader,
    output: ByteWriter,
) -> Result<(), DownlinkTaskError>
where
    K: Clone + Form + Send + Sync + Eq + Hash + Ord + 'static,
    V: Clone + Form + Send + Sync + 'static,
    <V as RecognizerReadable>::Rec: Send,
    LC: MapDownlinkLifecycle<K, V>,
{
    let MapDownlinkModel { actions, lifecycle } = model;
    let (stop_tx, stop_rx) = trigger::trigger();
    let read = async move {
        let result = read_task(config, input, lifecycle).await;
        let _ = stop_tx.trigger();
        result
    }
    .instrument(info_span!("Downlink read task.", %path));
    let framed_writer = FramedWrite::new(output, DownlinkOperationEncoder);
    let write = write_task(framed_writer, actions, stop_rx)
        .instrument(info_span!("Downlink write task.", %path));
    let (read_result, _) = join(read, write).await;
    read_result
}

enum State<K, V> {
    Unlinked,
    Linked(BTreeMap<K, V>),
    Synced(BTreeMap<K, V>),
}

struct ShowState<'a, K, V>(&'a State<K, V>);

impl<'a, K, V> Display for ShowState<'a, K, V>
where
    K: StructuralWritable,
    V: StructuralWritable,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ShowState(inner) = self;
        match *inner {
            State::Unlinked => f.write_str("Unlinked"),
            State::Linked(map) => write!(f, "Linked({})", print_recon(map)),
            State::Synced(map) => write!(f, "Synced({})", print_recon(map)),
        }
    }
}

async fn read_task<K, V, LC>(
    config: DownlinkConfig,
    input: ByteReader,
    mut lifecycle: LC,
) -> Result<(), DownlinkTaskError>
where
    K: Ord + Clone + Form + Send + Sync + Eq + Hash + 'static,
    V: Clone + Form + Send + Sync + 'static,
    <V as RecognizerReadable>::Rec: Send,
    LC: MapDownlinkLifecycle<K, V>,
{
    let DownlinkConfig {
        events_when_not_synced,
        terminate_on_unlinked,
        ..
    } = config;

    let mut state: State<K, V> = State::Unlinked;
    let mut framed_read = FramedRead::new(input, RecNotificationDecoder::default());

    while let Some(result) = framed_read.next().await {
        match result? {
            DownlinkNotification::Linked => {
                trace!(
                    "Received Linked in state {state}",
                    state = ShowState(&state)
                );
                if matches!(&state, State::Unlinked) {
                    lifecycle.on_linked().await;
                    state = State::Linked(BTreeMap::new());
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
                    _ => {}
                }
            }
            DownlinkNotification::Event { body } => {
                trace!(
                    "Received Event with body '{body}' in state {state}",
                    body = print_recon(&body),
                    state = ShowState(&state)
                );

                match &mut state {
                    State::Unlinked => {}
                    State::Linked(map) => {
                        on_event(map, &mut lifecycle, body, events_when_not_synced).await
                    }
                    State::Synced(map) => on_event(map, &mut lifecycle, body, true).await,
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
    actions: mpsc::Receiver<MapMessage<K, V>>,
    stop_trigger: trigger::Receiver,
) where
    Snk: Sink<DownlinkOperation<MapMessage<K, V>>> + Unpin,
    K: Form + Send + 'static,
    V: Form + Send + 'static,
{
    let mut set_stream = ReceiverStream::new(actions).take_until(stop_trigger);
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
async fn on_event<K, V, LC>(
    map: &mut BTreeMap<K, V>,
    lifecycle: &mut LC,
    event: MapMessage<K, V>,
    fire_lifecycle: bool,
) where
    LC: MapDownlinkLifecycle<K, V>,
    K: Clone + Ord,
    V: Clone,
{
    match event {
        MapMessage::Update { key, value } => {
            let old = map.insert(key.clone(), value.clone());
            if fire_lifecycle {
                lifecycle.on_update(key, map, old, &value).await;
            }
        }
        MapMessage::Remove { key } => {
            if fire_lifecycle {
                if let Some(value) = map.remove(&key) {
                    lifecycle.on_remove(key, map, value).await;
                }
            }
        }
        MapMessage::Clear => {
            if fire_lifecycle {
                let old_map = take(map);
                lifecycle.on_clear(old_map.into()).await;
            }
        }
        MapMessage::Take(cnt) => {
            let mut it = take(map).into_iter();

            for (key, value) in (&mut it).take(cnt as usize) {
                map.insert(key, value);
            }
            for (key, value) in it {
                lifecycle.on_remove(key, map, value).await;
            }
        }
        MapMessage::Drop(cnt) => {
            let mut it = take(map).into_iter();

            for (key, value) in (&mut it).take(cnt as usize) {
                lifecycle.on_remove(key, map, value).await;
            }
            for (key, value) in it {
                map.insert(key, value);
            }
        }
    }
}
