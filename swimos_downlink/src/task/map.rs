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

use crate::model::lifecycle::MapDownlinkLifecycle;
use crate::model::MapDownlinkModel;
use crate::task::{MapKey, MapValue};
use futures::{FutureExt, Sink, SinkExt, StreamExt};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use std::mem;
use swimos_agent_protocol::encoding::downlink::{DownlinkOperationEncoder, MapNotificationDecoder};
use swimos_agent_protocol::MapMessage;
use swimos_agent_protocol::{DownlinkNotification, DownlinkOperation};
use swimos_api::error::DownlinkTaskError;
use swimos_client_api::DownlinkConfig;
use swimos_form::structural::write::StructuralWritable;
use swimos_model::address::Address;
use swimos_model::Text;
use swimos_recon::print_recon;
use swimos_utilities::future::immediate_or_join;
use swimos_utilities::io::byte_channel::{ByteReader, ByteWriter};
use tokio::select;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Decoder, FramedRead, FramedWrite};
use tracing::{error, info_span, trace, Instrument};

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
    K: MapKey,
    V: MapValue,
    V::Rec: Send,
    LC: MapDownlinkLifecycle<K, V>,
{
    let MapDownlinkModel { actions, lifecycle } = model;

    run_io(
        config,
        input,
        lifecycle,
        FramedWrite::new(output, DownlinkOperationEncoder::default()),
        actions,
        MapNotificationDecoder::default(),
    )
    .instrument(info_span!("Downlink IO task.", %path))
    .await
}

/// The current state of the downlink.
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

enum IoEvent<K, V> {
    Read(DownlinkNotification<MapMessage<K, V>>),
    Write(Option<MapMessage<K, V>>),
}

/// The current IO mode. Defaults to read/write and once the writer channel is dropped, the IO loop
/// switches to a simplified block.
enum Mode {
    ReadWrite,
    Read,
}

async fn run_io<K, V, LC, Snk, D, E>(
    config: DownlinkConfig,
    input: ByteReader,
    mut lifecycle: LC,
    mut framed: Snk,
    actions: mpsc::Receiver<MapMessage<K, V>>,
    decoder: D,
) -> Result<(), DownlinkTaskError>
where
    K: MapKey,
    V: MapValue,
    V::Rec: Send,
    LC: MapDownlinkLifecycle<K, V>,
    Snk: Sink<DownlinkOperation<MapMessage<K, V>>> + Unpin,
    Snk::Error: Debug,
    D: Decoder<Item = DownlinkNotification<MapMessage<K, V>>, Error = E>,
    DownlinkTaskError: From<E>,
    E: Debug,
{
    let mut state: State<K, V> = State::Unlinked;
    let mut mode = Mode::ReadWrite;
    let mut framed_read = FramedRead::new(input, decoder);
    let mut set_stream = ReceiverStream::new(actions);

    loop {
        match mode {
            Mode::ReadWrite => {
                let mut write_fut =
                    immediate_or_join(set_stream.next(), framed.flush()).map(|(msg, _)| msg);
                let event = select! {
                    write = (&mut write_fut) => IoEvent::Write(write),
                    read_event = framed_read.next() => match read_event {
                        Some(Ok(notification)) => IoEvent::Read(notification),
                        Some(Err(e)) => break Err(e.into()),
                        None => break Ok(()),
                    }
                };

                match event {
                    IoEvent::Write(Some(message)) => {
                        trace!("Sending command '{cmd}'.", cmd = print_recon(&message));

                        match &mut state {
                            State::Synced(map) | State::Linked(map) => match &message {
                                MapMessage::Update { key, value } => {
                                    map.insert(K::clone(key), V::clone(value));
                                }
                                MapMessage::Remove { key } => {
                                    map.remove(key);
                                }
                                MapMessage::Clear => map.clear(),
                                MapMessage::Take(cnt) => {
                                    let mut it = mem::take(map).into_iter();
                                    for (key, value) in (&mut it).take(*cnt as usize) {
                                        map.insert(key, value);
                                    }
                                }
                                MapMessage::Drop(cnt) => {
                                    let it = mem::take(map).into_iter().skip(*cnt as usize);
                                    for (key, value) in it {
                                        map.insert(key, value);
                                    }
                                }
                            },
                            State::Unlinked => {}
                        }

                        let op = DownlinkOperation::new(message);
                        if let Err(e) = framed.feed(op).await {
                            error!(error = ?e, "Failed to feed downlink frame. Transitioning to read-only mode");
                        }
                    }
                    IoEvent::Write(None) => mode = Mode::Read,
                    IoEvent::Read(notification) => {
                        match on_read(state, &mut lifecycle, notification, config).await {
                            Step::Cont(new_state) => {
                                state = new_state;
                            }
                            Step::Terminate => break Ok(()),
                        }
                    }
                }
            }
            Mode::Read => {
                while let Some(result) = framed_read.next().await {
                    match on_read(state, &mut lifecycle, result?, config).await {
                        Step::Cont(new_state) => {
                            state = new_state;
                        }
                        Step::Terminate => break,
                    }
                }
                break Ok(());
            }
        }
    }
}

async fn on_read<K, V, LC>(
    mut state: State<K, V>,
    lifecycle: &mut LC,
    notification: DownlinkNotification<MapMessage<K, V>>,
    config: DownlinkConfig,
) -> Step<K, V>
where
    K: MapKey,
    V: MapValue,
    V::Rec: Send,
    LC: MapDownlinkLifecycle<K, V>,
{
    let DownlinkConfig {
        events_when_not_synced,
        terminate_on_unlinked,
        ..
    } = config;

    match notification {
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

            if let State::Linked(value) = state {
                lifecycle.on_synced(&value).await;
                state = State::Synced(value);
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
                State::Linked(map) => on_event(map, lifecycle, body, events_when_not_synced).await,
                State::Synced(map) => on_event(map, lifecycle, body, true).await,
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
                return Step::Terminate;
            } else {
                state = State::Unlinked;
            }
        }
    }

    Step::Cont(state)
}

/// The next step that the IO loop should take.
enum Step<K, V> {
    /// The IO loop should continue and update its state.
    Cont(State<K, V>),
    /// The IO loop should terminate.
    Terminate,
}

async fn on_event<K, V, LC>(
    map: &mut BTreeMap<K, V>,
    lifecycle: &mut LC,
    event: MapMessage<K, V>,
    dispatch: bool,
) where
    LC: MapDownlinkLifecycle<K, V>,
    K: Clone + Ord,
    V: Clone,
{
    match event {
        MapMessage::Update { key, value } => {
            let old = map.insert(key.clone(), value.clone());
            if dispatch {
                lifecycle.on_update(key, map, old, &value).await;
            }
        }
        MapMessage::Remove { key } => {
            if let Some(value) = map.remove(&key) {
                if dispatch {
                    lifecycle.on_remove(key, map, value).await;
                }
            }
        }
        MapMessage::Clear => {
            if dispatch {
                let old_map = mem::take(map);
                lifecycle.on_clear(old_map).await;
            }
        }
        MapMessage::Take(cnt) => {
            let mut it = mem::take(map).into_iter();

            for (key, value) in (&mut it).take(cnt as usize) {
                map.insert(key, value);
            }
            for (key, value) in it {
                lifecycle.on_remove(key, map, value).await;
            }
        }
        MapMessage::Drop(cnt) => {
            let mut it = mem::take(map).into_iter();

            for (key, value) in (&mut it).take(cnt as usize) {
                lifecycle.on_remove(key, map, value).await;
            }
            for (key, value) in it {
                map.insert(key, value);
            }
        }
    }
}
