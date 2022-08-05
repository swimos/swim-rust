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
use swim_utilities::{io::byte_channel::ByteWriter, trigger::promise};
use tokio_util::codec::Encoder;
use uuid::Uuid;

use crate::{
    agent::{
        task::write_fut::{SpecialAction, WriteAction, WriteTask},
        DisconnectionReason,
    },
    error::InvalidKey,
    pressure::{
        recon::MapOperationReconEncoder, BackpressureStrategy, MapBackpressure, ValueBackpressure,
    },
    routing::RoutingAddr,
};

#[cfg(test)]
mod tests;

use super::{LaneRegistry, RemoteSender};

/// Keeps track of the state of uplinks from lanes to remotes. In effect, this is a specialized queue of futures
/// representing a write to be performed on the remote. When attempting to push a new entry into the queue,
/// if the writer is present, a write will be produced immediately. If the writer is absent, the new entry
/// will either be enqueued (link and unlink messages for example) or will be pushed into the appropriate
/// backpressure relief mechanism for the lane. To pop from the queue, the writer is returned. If there is
/// more work to be done, it will be popped and returned as a new future (once again removing the writer). If
/// no work is pending, the writer is stored within the queue and nothing is returned.
#[derive(Debug)]
pub struct Uplinks {
    writer: Option<(RemoteSender, BytesMut)>, //Holds the sender and associated buffer when it has not been leant out.
    value_uplinks: HashMap<u64, Uplink<ValueBackpressure>>, //Uplinks for value lanes.
    map_uplinks: HashMap<u64, Uplink<MapBackpressure>>, //Uplinks for map lanes.
    write_queue: VecDeque<(UplinkKind, u64)>, //Queue tracking which uplink should be written next.
    special_queue: VecDeque<SpecialAction>, //Queue of special actions (primarily link/unlink messages) which take precedence over uplinks.
    completion: promise::Sender<DisconnectionReason>, //Promise to be satisfied when the remote is closed.
}

/// The type of entries that can be pushed into the queue.
#[derive(Debug, Clone)]
pub enum UplinkResponse {
    /// A synced message for value type lane.
    SyncedValue(Bytes),
    /// A synced message for a map type lane.
    SyncedMap,
    /// An event message for a value type lane.
    Value(Bytes),
    /// An event message for a map type lane.
    Map(MapOperation<Bytes, Bytes>),
}

impl UplinkResponse {
    pub fn is_synced(&self) -> bool {
        matches!(
            self,
            UplinkResponse::SyncedValue(_) | UplinkResponse::SyncedMap
        )
    }
}

const UNREGISTERED_LANE: &str = "Unregistered lane ID.";

impl Uplinks {
    /// #Arguments
    /// * `node` - The node URI to attach to the outgoing messages.
    /// * `identity` - The routing address of the agent to add to the outgoing messages.
    /// * `remote_id` - The ID of the target remote.
    /// * `writer` - Byte chanel connected to the remote.
    /// * `completion` - A promise to be completed when the remote is closed.
    pub fn new(
        node: Text,
        identity: RoutingAddr,
        remote_id: Uuid,
        writer: ByteWriter,
        completion: promise::Sender<DisconnectionReason>,
    ) -> Self {
        let sender = RemoteSender::new(writer, identity, remote_id, node);
        Uplinks {
            writer: Some((sender, Default::default())),
            value_uplinks: Default::default(),
            map_uplinks: Default::default(),
            write_queue: Default::default(),
            special_queue: Default::default(),
            completion,
        }
    }

    /// Push a special action into the queue. Special actions are not subject to backpressure relief and
    /// are always popped before other entries.
    /// #Arguments
    /// * `actions` - The special action.
    /// * `registry` - Registry mapping lane IDs to lane names.
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

    /// Push an event into the queue.
    /// #Arguments
    /// * `lane_id` - ID of the lane to which the event refers.
    /// * `event` - The event.
    /// * `registry` - Registry mapping lane IDs to lane names.
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

    /// Return the remote sender (and its associated buffer) and pop the next write future (if there is
    /// more work).
    /// #Arguments
    /// * `sender` - The sender to return.
    /// * `buffer` - The buffer associated with the sender.
    /// * `registry` - Registry mapping lane IDs to lane names.
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
            ..
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
                                        WriteAction::MapSynced(Some(Box::new(std::mem::take(
                                            backpressure,
                                        )))),
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

    /// Dispose of the uplinks, providing the specified reason.
    pub fn complete(self, reason: DisconnectionReason) {
        let _ = self.completion.provide(reason);
    }
}

/// The state of a single uplink within an [`Uplinks`] instance for a remote.
#[derive(Debug, Default)]
struct Uplink<B> {
    queued: bool,      //Indicates that this uplink is currently in the queue.
    send_synced: bool, //Indicates that a synced message needs to be emitted for this uplink.
    backpressure: B,   //Backpressure relief queue (varying implementation based on uplink kind).
}

/// Writ the body of a response directly into a buffer (used when backpressure relief is not
/// required).
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
            buffer.clear();
            encoder
                .encode(operation, buffer)
                .expect("Wiritng map operations is infallible.");
            WriteAction::Event
        }
    };
    Ok(action)
}
