// Copyright 2015-2021 SWIM.AI inc.
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

use crate::configuration::tags::{
    BACK_PRESSURE_TAG, BRIDGE_BUFFER_SIZE_TAG, BUFFER_SIZE_TAG, CONFIG_TAG, DEFAULT_TAG,
    DL_REQ_BUFFER_SIZE_TAG, DOWNLINKS_TAG, DOWNLINK_CONFIG_TAG, DOWNLINK_CONNECTIONS_TAG, HOST_TAG,
    IDLE_TIMEOUT_TAG, IGNORE_TAG, INPUT_BUFFER_SIZE_TAG, LANE_TAG, MAX_ACTIVE_KEYS_TAG,
    ON_INVALID_TAG, PROPAGATE_TAG, RELEASE_TAG, RETRY_STRATEGY_TAG, TERMINATE_TAG, YIELD_AFTER_TAG,
};
use crate::configuration::{
    BackpressureMode, ClientDownlinksConfig, DownlinkConfig, DownlinkConnectionsConfig,
    OnInvalidMessage, SwimClientConfig,
};
use swim_common::form::structural::write::{
    BodyWriter, HeaderWriter, RecordBodyKind, StructuralWritable, StructuralWriter,
};

impl StructuralWritable for SwimClientConfig {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr(CONFIG_TAG)?
            .complete_header(RecordBodyKind::ArrayLike, 4)?;

        body_writer = body_writer.write_value(&self.downlink_connections_config)?;
        body_writer = body_writer.write_value(&self.remote_connections_config)?;
        body_writer = body_writer.write_value(&self.websocket_config)?;
        body_writer = body_writer.write_value(&self.downlinks_config)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr(CONFIG_TAG)?
            .complete_header(RecordBodyKind::ArrayLike, 4)?;

        body_writer = body_writer.write_value_into(self.downlink_connections_config)?;
        body_writer = body_writer.write_value_into(self.remote_connections_config)?;
        body_writer = body_writer.write_value_into(self.websocket_config)?;
        body_writer = body_writer.write_value_into(self.downlinks_config)?;

        body_writer.done()
    }
}

impl StructuralWritable for DownlinkConnectionsConfig {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr(DOWNLINK_CONNECTIONS_TAG)?
            .complete_header(RecordBodyKind::MapLike, 4)?;

        body_writer = body_writer.write_slot(&DL_REQ_BUFFER_SIZE_TAG, &self.dl_req_buffer_size)?;
        body_writer = body_writer.write_slot(&BUFFER_SIZE_TAG, &self.buffer_size)?;
        body_writer = body_writer.write_slot(&YIELD_AFTER_TAG, &self.yield_after)?;
        body_writer = body_writer.write_slot(&RETRY_STRATEGY_TAG, &self.retry_strategy)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr(DOWNLINK_CONNECTIONS_TAG)?
            .complete_header(RecordBodyKind::MapLike, 4)?;

        body_writer =
            body_writer.write_slot_into(DL_REQ_BUFFER_SIZE_TAG, self.dl_req_buffer_size)?;
        body_writer = body_writer.write_slot_into(BUFFER_SIZE_TAG, self.buffer_size)?;
        body_writer = body_writer.write_slot_into(YIELD_AFTER_TAG, self.yield_after)?;
        body_writer = body_writer.write_slot_into(RETRY_STRATEGY_TAG, self.retry_strategy)?;

        body_writer.done()
    }
}

impl StructuralWritable for ClientDownlinksConfig {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut num_items = 1;
        if !self.by_host.is_empty() {
            num_items += 1;
        }

        if !self.by_lane.is_empty() {
            num_items += 1;
        }

        let mut body_writer = header_writer
            .write_extant_attr(DOWNLINKS_TAG)?
            .complete_header(RecordBodyKind::MapLike, num_items)?;

        body_writer = body_writer.write_slot(&DEFAULT_TAG, &self.default)?;

        if !self.by_host.is_empty() {
            body_writer = body_writer.write_slot(&HOST_TAG, &self.by_host)?;
        }

        if !self.by_lane.is_empty() {
            body_writer = body_writer.write_slot(&LANE_TAG, &self.by_lane)?;
        }

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut num_items = 1;
        if !self.by_host.is_empty() {
            num_items += 1;
        }

        if !self.by_lane.is_empty() {
            num_items += 1;
        }

        let mut body_writer = header_writer
            .write_extant_attr(DOWNLINKS_TAG)?
            .complete_header(RecordBodyKind::MapLike, num_items)?;

        body_writer = body_writer.write_slot_into(DEFAULT_TAG, self.default)?;

        if !self.by_host.is_empty() {
            body_writer = body_writer.write_slot_into(HOST_TAG, self.by_host)?;
        }

        if !self.by_lane.is_empty() {
            body_writer = body_writer.write_slot_into(LANE_TAG, self.by_lane)?;
        }

        body_writer.done()
    }
}

impl StructuralWritable for DownlinkConfig {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut body_writer = header_writer
            .write_extant_attr(DOWNLINK_CONFIG_TAG)?
            .complete_header(RecordBodyKind::MapLike, 5)?;

        body_writer = body_writer.write_slot(&BACK_PRESSURE_TAG, &self.back_pressure)?;
        body_writer = body_writer.write_slot(&IDLE_TIMEOUT_TAG, &self.idle_timeout)?;
        body_writer = body_writer.write_slot(&BUFFER_SIZE_TAG, &self.buffer_size)?;
        body_writer = body_writer.write_slot(&ON_INVALID_TAG, &self.on_invalid)?;
        body_writer = body_writer.write_slot(&YIELD_AFTER_TAG, &self.yield_after)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut body_writer = header_writer
            .write_extant_attr(DOWNLINK_CONFIG_TAG)?
            .complete_header(RecordBodyKind::MapLike, 5)?;

        body_writer = body_writer.write_slot_into(BACK_PRESSURE_TAG, self.back_pressure)?;
        body_writer = body_writer.write_slot_into(IDLE_TIMEOUT_TAG, self.idle_timeout)?;
        body_writer = body_writer.write_slot_into(BUFFER_SIZE_TAG, self.buffer_size)?;
        body_writer = body_writer.write_slot_into(ON_INVALID_TAG, self.on_invalid)?;
        body_writer = body_writer.write_slot_into(YIELD_AFTER_TAG, self.yield_after)?;

        body_writer.done()
    }
}

impl StructuralWritable for BackpressureMode {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            BackpressureMode::Propagate => {
                let header_writer = writer.record(1)?;
                header_writer
                    .write_extant_attr(PROPAGATE_TAG)?
                    .complete_header(RecordBodyKind::Mixed, 0)?
                    .done()
            }
            BackpressureMode::Release {
                input_buffer_size,
                bridge_buffer_size,
                max_active_keys,
                yield_after,
            } => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr(RELEASE_TAG)?
                    .complete_header(RecordBodyKind::MapLike, 4)?;

                body_writer = body_writer.write_slot(&INPUT_BUFFER_SIZE_TAG, input_buffer_size)?;
                body_writer =
                    body_writer.write_slot(&BRIDGE_BUFFER_SIZE_TAG, bridge_buffer_size)?;
                body_writer = body_writer.write_slot(&MAX_ACTIVE_KEYS_TAG, max_active_keys)?;
                body_writer = body_writer.write_slot(&YIELD_AFTER_TAG, yield_after)?;

                body_writer.done()
            }
        }
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            BackpressureMode::Propagate => {
                let header_writer = writer.record(1)?;
                header_writer
                    .write_extant_attr(PROPAGATE_TAG)?
                    .complete_header(RecordBodyKind::Mixed, 0)?
                    .done()
            }
            BackpressureMode::Release {
                input_buffer_size,
                bridge_buffer_size,
                max_active_keys,
                yield_after,
            } => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr(RELEASE_TAG)?
                    .complete_header(RecordBodyKind::MapLike, 4)?;

                body_writer =
                    body_writer.write_slot_into(INPUT_BUFFER_SIZE_TAG, input_buffer_size)?;
                body_writer =
                    body_writer.write_slot_into(BRIDGE_BUFFER_SIZE_TAG, bridge_buffer_size)?;
                body_writer = body_writer.write_slot_into(MAX_ACTIVE_KEYS_TAG, max_active_keys)?;
                body_writer = body_writer.write_slot_into(YIELD_AFTER_TAG, yield_after)?;

                body_writer.done()
            }
        }
    }
}

impl StructuralWritable for OnInvalidMessage {
    fn num_attributes(&self) -> usize {
        0
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        match self {
            OnInvalidMessage::Ignore => header_writer.write_extant_attr(IGNORE_TAG)?,
            OnInvalidMessage::Terminate => header_writer.write_extant_attr(TERMINATE_TAG)?,
        }
        .complete_header(RecordBodyKind::Mixed, 0)?
        .done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        match self {
            OnInvalidMessage::Ignore => header_writer.write_extant_attr(IGNORE_TAG)?,
            OnInvalidMessage::Terminate => header_writer.write_extant_attr(TERMINATE_TAG)?,
        }
        .complete_header(RecordBodyKind::Mixed, 0)?
        .done()
    }
}
