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

use std::collections::{HashMap, VecDeque};

use bytes::{BufMut, Bytes, BytesMut};
use swim_api::{agent::UplinkKind, protocol::map::MapOperation};
use swim_model::Text;
use swim_utilities::io::byte_channel::ByteWriter;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use crate::{
    agent::task::write_fut::{SpecialAction, WriteAction, WriteTask},
    error::InvalidKey,
    pressure::{
        recon::MapOperationReconEncoder, BackpressureStrategy, MapBackpressure, ValueBackpressure,
    },
    routing::RoutingAddr,
};

#[cfg(test)]
mod tests;

use super::{LaneRegistry, RemoteSender};

#[derive(Debug)]
pub struct Uplinks {
    writer: Option<(RemoteSender, BytesMut)>,
    value_uplinks: HashMap<u64, Uplink<ValueBackpressure>>,
    map_uplinks: HashMap<u64, Uplink<MapBackpressure>>,
    write_queue: VecDeque<(UplinkKind, u64)>,
    special_queue: VecDeque<SpecialAction>,
}

#[derive(Debug, Clone)]
pub enum UplinkResponse {
    SyncedValue(Bytes),
    SyncedMap,
    Value(Bytes),
    Map(MapOperation<Bytes, Bytes>),
}

const UNREGISTERED_LANE: &str = "Unregistered lane ID.";

impl Uplinks {
    pub fn new(node: Text, identity: RoutingAddr, remote_id: Uuid, writer: ByteWriter) -> Self {
        let sender = RemoteSender::new(writer, identity, remote_id, node);
        Uplinks {
            writer: Some((sender, Default::default())),
            value_uplinks: Default::default(),
            map_uplinks: Default::default(),
            write_queue: Default::default(),
            special_queue: Default::default(),
        }
    }

    pub fn push_special(
        &mut self,
        action: SpecialAction,
        registry: &LaneRegistry,
    ) -> Option<WriteTask> {
        let Uplinks {
            writer,
            value_uplinks,
            map_uplinks,
            special_queue,
            ..
        } = self;
        if let Some((mut writer, buffer)) = writer.take() {
            let lane_name = action.lane_name(registry);
            writer.update_lane(lane_name);
            Some(WriteTask::new(writer, buffer, WriteAction::Special(action)))
        } else {
            if let SpecialAction::Unlinked { lane_id, .. } = &action {
                value_uplinks.remove(lane_id);
                map_uplinks.remove(lane_id);
            }
            special_queue.push_back(action);
            None
        }
    }

    pub fn push(
        &mut self,
        lane_id: u64,
        event: UplinkResponse,
        registry: &LaneRegistry,
    ) -> Result<Option<WriteTask>, InvalidKey> {
        let Uplinks {
            writer,
            value_uplinks,
            map_uplinks,
            write_queue,
            ..
        } = self;
        if let Some((mut writer, mut buffer)) = writer.take() {
            let action = write_to_buffer(event, &mut buffer)?;
            let lane_name = registry.name_for(lane_id).expect(UNREGISTERED_LANE);
            writer.update_lane(lane_name);
            Ok(Some(WriteTask::new(writer, buffer, action)))
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

    pub fn replace_and_pop(
        &mut self,
        mut sender: RemoteSender,
        mut buffer: BytesMut,
        registry: &LaneRegistry,
    ) -> Option<WriteTask> {
        let Uplinks {
            writer,
            value_uplinks,
            map_uplinks,
            write_queue,
            special_queue,
        } = self;
        debug_assert!(writer.is_none());
        if let Some(special) = special_queue.pop_front() {
            sender.update_lane(special.lane_name(registry));
            Some(WriteTask::new(
                sender,
                buffer,
                WriteAction::Special(special),
            ))
        } else {
            loop {
                if let Some((kind, lane_id)) = write_queue.pop_front() {
                    match kind {
                        UplinkKind::Value => {
                            if let Some(Uplink {
                                queued,
                                send_synced,
                                backpressure,
                            }) = value_uplinks.get_mut(&lane_id)
                            {
                                *queued = false;
                                let synced = std::mem::replace(send_synced, false);
                                backpressure.prepare_write(&mut buffer);
                                let action = if synced {
                                    WriteAction::EventAndSynced
                                } else {
                                    WriteAction::Event
                                };
                                let lane_name =
                                    registry.name_for(lane_id).expect(UNREGISTERED_LANE);
                                sender.update_lane(lane_name);
                                break Some(WriteTask::new(sender, buffer, action));
                            }
                        }
                        UplinkKind::Map => {
                            if let Some(Uplink {
                                queued,
                                send_synced,
                                backpressure,
                            }) = map_uplinks.get_mut(&lane_id)
                            {
                                let synced = std::mem::replace(send_synced, false);
                                let write = if synced {
                                    *queued = false;
                                    let lane_name =
                                        registry.name_for(lane_id).expect(UNREGISTERED_LANE);
                                    sender.update_lane(lane_name);
                                    WriteTask::new(
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
                                    let lane_name =
                                        registry.name_for(lane_id).expect(UNREGISTERED_LANE);
                                    sender.update_lane(lane_name);
                                    WriteTask::new(sender, buffer, WriteAction::Event)
                                };
                                break Some(write);
                            }
                        }
                    }
                } else {
                    *writer = Some((sender, buffer));
                    break None;
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct Uplink<B> {
    queued: bool,
    send_synced: bool,
    backpressure: B,
}

fn write_to_buffer(
    response: UplinkResponse,
    buffer: &mut BytesMut,
) -> Result<WriteAction, InvalidKey> {
    let action = match response {
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
            //Validating the key is valid UTF8 for consistency with the backpressure relief case.
            match &operation {
                MapOperation::Update { key, .. } | MapOperation::Remove { key } => {
                    if let Err(e) = std::str::from_utf8(key.as_ref()) {
                        return Err(InvalidKey::new(key.clone(), e));
                    }
                }
                _ => {}
            }
            let mut encoder = MapOperationReconEncoder;
            encoder
                .encode(operation, buffer)
                .expect("Wiritng map operations is infallible.");
            WriteAction::Event
        }
    };
    Ok(action)
}
