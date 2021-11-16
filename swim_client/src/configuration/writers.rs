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

use crate::configuration::tags::{CONFIG_TAG, DEFAULT_TAG, DOWNLINKS_TAG, HOST_TAG, LANE_TAG};
use crate::configuration::{ClientDownlinksConfig, SwimClientConfig};
use swim_form::structural::write::{
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
        body_writer = body_writer.write_value(&self.websocket_config.clone())?;
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
