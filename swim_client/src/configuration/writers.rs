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
            .write_extant_attr("config")?
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
            .write_extant_attr("config")?
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
            .write_extant_attr("downlink_connections")?
            .complete_header(RecordBodyKind::MapLike, 4)?;

        body_writer = body_writer.write_slot(&"dl_req_buffer_size", &self.dl_req_buffer_size)?;
        body_writer = body_writer.write_slot(&"buffer_size", &self.buffer_size)?;
        body_writer = body_writer.write_slot(&"yield_after", &self.yield_after)?;
        body_writer = body_writer.write_slot(&"retry_strategy", &self.retry_strategy)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr("downlink_connections")?
            .complete_header(RecordBodyKind::MapLike, 4)?;

        body_writer = body_writer.write_slot_into("dl_req_buffer_size", self.dl_req_buffer_size)?;
        body_writer = body_writer.write_slot_into("buffer_size", self.buffer_size)?;
        body_writer = body_writer.write_slot_into("yield_after", self.yield_after)?;
        body_writer = body_writer.write_slot_into("retry_strategy", self.retry_strategy)?;

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
            .write_extant_attr("downlinks")?
            .complete_header(RecordBodyKind::MapLike, num_items)?;

        body_writer = body_writer.write_slot(&"default", &self.default)?;

        if !self.by_host.is_empty() {
            body_writer = body_writer.write_slot(&"host", &self.by_host)?;
        }

        if !self.by_lane.is_empty() {
            body_writer = body_writer.write_slot(&"lane", &self.by_lane)?;
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
            .write_extant_attr("downlinks")?
            .complete_header(RecordBodyKind::MapLike, num_items)?;

        body_writer = body_writer.write_slot_into("default", self.default)?;

        if !self.by_host.is_empty() {
            body_writer = body_writer.write_slot_into("host", self.by_host)?;
        }

        if !self.by_lane.is_empty() {
            body_writer = body_writer.write_slot_into("lane", self.by_lane)?;
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
            .write_extant_attr("downlink_config")?
            .complete_header(RecordBodyKind::MapLike, 5)?;

        body_writer = body_writer.write_slot(&"back_pressure", &self.back_pressure)?;
        body_writer = body_writer.write_slot(&"idle_timeout", &self.idle_timeout)?;
        body_writer = body_writer.write_slot(&"buffer_size", &self.buffer_size)?;
        body_writer = body_writer.write_slot(&"on_invalid", &self.on_invalid)?;
        body_writer = body_writer.write_slot(&"yield_after", &self.yield_after)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut body_writer = header_writer
            .write_extant_attr("downlink_config")?
            .complete_header(RecordBodyKind::MapLike, 5)?;

        body_writer = body_writer.write_slot_into("back_pressure", self.back_pressure)?;
        body_writer = body_writer.write_slot_into("idle_timeout", self.idle_timeout)?;
        body_writer = body_writer.write_slot_into("buffer_size", self.buffer_size)?;
        body_writer = body_writer.write_slot_into("on_invalid", self.on_invalid)?;
        body_writer = body_writer.write_slot_into("yield_after", self.yield_after)?;

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
                    .write_extant_attr("propagate")?
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
                    .write_extant_attr("release")?
                    .complete_header(RecordBodyKind::MapLike, 4)?;

                body_writer = body_writer.write_slot(&"input_buffer_size", input_buffer_size)?;
                body_writer = body_writer.write_slot(&"bridge_buffer_size", bridge_buffer_size)?;
                body_writer = body_writer.write_slot(&"max_active_keys", max_active_keys)?;
                body_writer = body_writer.write_slot(&"yield_after", yield_after)?;

                body_writer.done()
            }
        }
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            BackpressureMode::Propagate => {
                let header_writer = writer.record(1)?;
                header_writer
                    .write_extant_attr("propagate")?
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
                    .write_extant_attr("release")?
                    .complete_header(RecordBodyKind::MapLike, 4)?;

                body_writer =
                    body_writer.write_slot_into("input_buffer_size", input_buffer_size)?;
                body_writer =
                    body_writer.write_slot_into("bridge_buffer_size", bridge_buffer_size)?;
                body_writer = body_writer.write_slot_into("max_active_keys", max_active_keys)?;
                body_writer = body_writer.write_slot_into("yield_after", yield_after)?;

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
            OnInvalidMessage::Ignore => header_writer.write_extant_attr("ignore")?,
            OnInvalidMessage::Terminate => header_writer.write_extant_attr("terminate")?,
        }
        .complete_header(RecordBodyKind::Mixed, 0)?
        .done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            OnInvalidMessage::Ignore => writer.write_text("ignore"),
            OnInvalidMessage::Terminate => writer.write_text("terminate"),
        }
    }
}
