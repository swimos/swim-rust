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

use std::{
    collections::{HashMap, VecDeque},
    str::Utf8Error,
};

use bytes::{BufMut, Bytes, BytesMut};
use futures::Future;
use swim_api::{agent::UplinkKind, protocol::map::MapOperation};
use swim_model::Text;
use swim_utilities::io::byte_channel::ByteWriter;
use tokio_util::codec::Encoder;

use crate::{
    pressure::{
        recon::MapOperationReconEncoder, BackpressureStrategy, MapBackpressure, ValueBackpressure,
    },
    routing::RoutingAddr,
};

mod remote_sender;
#[cfg(test)]
mod tests;

pub use remote_sender::RemoteSender;

pub type WriteResult = (RemoteSender, BytesMut, Result<(), std::io::Error>);

#[derive(Debug)]
pub struct Uplinks<F> {
    writer: Option<(RemoteSender, BytesMut)>,
    value_uplinks: HashMap<u64, Uplink<ValueBackpressure>>,
    map_uplinks: HashMap<u64, Uplink<MapBackpressure>>,
    write_queue: VecDeque<(UplinkKind, u64)>,
    special_queue: VecDeque<SpecialUplinkAction>,
    write_op: F,
}

#[derive(Debug, Clone)]
pub enum SpecialUplinkAction {
    Linked(u64),
    Unlinked(u64, Text),
    LaneNotFound,
}

pub enum WriteAction {
    Event,
    EventAndSynced,
    MapSynced(Option<MapBackpressure>),
    Special(SpecialUplinkAction),
}

#[derive(Debug, Clone)]
pub enum UplinkResponse {
    SyncedValue(Bytes),
    SyncedMap,
    Value(Bytes),
    Map(MapOperation<Bytes, Bytes>),
}

impl<F, W> Uplinks<F>
where
    F: Fn(RemoteSender, BytesMut, WriteAction) -> W + Clone,
    W: Future<Output = WriteResult> + Send + 'static,
{
    pub fn new(node: Text, identity: RoutingAddr, writer: ByteWriter, write_op: F) -> Self {
        let sender = RemoteSender::new(writer, identity, node);
        Uplinks {
            writer: Some((sender, Default::default())),
            value_uplinks: Default::default(),
            map_uplinks: Default::default(),
            write_queue: Default::default(),
            special_queue: Default::default(),
            write_op,
        }
    }

    pub fn push_special(&mut self, action: SpecialUplinkAction, lane_name: &str) -> Option<W> {
        let Uplinks {
            writer,
            value_uplinks,
            map_uplinks,
            special_queue,
            write_op,
            ..
        } = self;
        if let Some((mut writer, buffer)) = writer.take() {
            writer.update_lane(lane_name);
            Some(write_op(writer, buffer, WriteAction::Special(action)))
        } else {
            if let SpecialUplinkAction::Unlinked(id, _) = &action {
                value_uplinks.remove(id);
                map_uplinks.remove(id);
            }
            special_queue.push_back(action);
            None
        }
    }

    pub fn push(
        &mut self,
        lane_id: u64,
        event: UplinkResponse,
        lane_name: &str,
    ) -> Result<Option<W>, Utf8Error> {
        let Uplinks {
            writer,
            value_uplinks,
            map_uplinks,
            write_queue,
            write_op,
            ..
        } = self;
        if let Some((mut writer, mut buffer)) = writer.take() {
            let action = write_to_buffer(event, &mut buffer);
            writer.update_lane(lane_name);
            Ok(Some(write_op(writer, buffer, action)))
        } else {
            match event {
                UplinkResponse::Value(body) => {
                    let Uplink {
                        queued,
                        backpressure,
                        ..
                    } = value_uplinks.entry(lane_id).or_default();
                    backpressure.push_bytes(body);
                    if !*queued {
                        write_queue.push_back((UplinkKind::Value, lane_id));
                        *queued = true;
                    }
                }
                UplinkResponse::Map(operation) => {
                    let Uplink {
                        queued,
                        backpressure,
                        ..
                    } = map_uplinks.entry(lane_id).or_default();
                    backpressure.push(operation)?;
                    if !*queued {
                        write_queue.push_back((UplinkKind::Map, lane_id));
                        *queued = true;
                    }
                }
                UplinkResponse::SyncedValue(body) => {
                    let Uplink {
                        queued,
                        send_synced,
                        backpressure,
                        ..
                    } = value_uplinks.entry(lane_id).or_default();
                    backpressure.push_bytes(body);
                    *send_synced = true;
                    if !*queued {
                        write_queue.push_back((UplinkKind::Value, lane_id));
                        *queued = true;
                    }
                }
                UplinkResponse::SyncedMap => {
                    let Uplink {
                        queued,
                        send_synced,
                        ..
                    } = map_uplinks.entry(lane_id).or_default();
                    *send_synced = true;
                    if !*queued {
                        write_queue.push_back((UplinkKind::Map, lane_id));
                        *queued = true;
                    }
                }
            }
            Ok(None)
        }
    }

    pub fn replace_and_pop(&mut self, sender: RemoteSender, mut buffer: BytesMut) -> Option<W> {
        let Uplinks {
            writer,
            value_uplinks,
            map_uplinks,
            write_queue,
            special_queue,
            write_op,
        } = self;
        debug_assert!(writer.is_none());
        if let Some(special) = special_queue.pop_front() {
            Some(write_op(sender, buffer, WriteAction::Special(special)))
        } else if let Some((kind, lane_id)) = write_queue.pop_front() {
            match kind {
                UplinkKind::Value => value_uplinks.get_mut(&lane_id).map(
                    |Uplink {
                         queued,
                         send_synced,
                         backpressure,
                     }| {
                        *queued = false;
                        let synced = std::mem::replace(send_synced, false);
                        backpressure.prepare_write(&mut buffer);
                        let action = if synced {
                            WriteAction::EventAndSynced
                        } else {
                            WriteAction::Event
                        };
                        write_op(sender, buffer, action)
                    },
                ),
                UplinkKind::Map => map_uplinks.get_mut(&lane_id).map(
                    |Uplink {
                         queued,
                         send_synced,
                         backpressure,
                     }| {
                        let synced = std::mem::replace(send_synced, false);
                        if synced {
                            *queued = false;
                            write_op(
                                sender,
                                buffer,
                                WriteAction::MapSynced(Some(std::mem::take(backpressure))),
                            )
                        } else {
                            backpressure.prepare_write(&mut buffer);
                            if backpressure.has_data() {
                                write_queue.push_back((UplinkKind::Map, lane_id));
                            } else {
                                *queued = false;
                            }
                            write_op(sender, buffer, WriteAction::Event)
                        }
                    },
                ),
            }
        } else {
            *writer = Some((sender, buffer));
            None
        }
    }
}

#[derive(Debug, Default)]
struct Uplink<B> {
    queued: bool,
    send_synced: bool,
    backpressure: B,
}

fn write_to_buffer(response: UplinkResponse, buffer: &mut BytesMut) -> WriteAction {
    match response {
        UplinkResponse::SyncedValue(body) => {
            buffer.clear();
            buffer.reserve(body.len());
            buffer.put(body);
            WriteAction::EventAndSynced
        }
        UplinkResponse::SyncedMap => WriteAction::MapSynced(None),
        UplinkResponse::Value(body) => {
            buffer.clear();
            buffer.reserve(body.len());
            buffer.put(body);
            WriteAction::Event
        }
        UplinkResponse::Map(operation) => {
            let mut encoder = MapOperationReconEncoder;
            encoder
                .encode(operation, buffer)
                .expect("Wiritng map operations is infallible.");
            WriteAction::Event
        }
    }
}
