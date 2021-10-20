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

use crate::form::structural::write::{
    BodyWriter, HeaderWriter, RecordBodyKind, StructuralWritable, StructuralWriter,
};
use swim_utilities::future::retryable::RetryStrategy;
use tokio_tungstenite::tungstenite::extensions::compression::WsCompression;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

impl StructuralWritable for RetryStrategy {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            RetryStrategy::Immediate(strat) => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr("immediate")?
                    .complete_header(RecordBodyKind::MapLike, 1)?;
                body_writer = body_writer.write_slot(&"retries", &strat.retry)?;
                body_writer.done()
            }
            RetryStrategy::Interval(strat) => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr("interval")?
                    .complete_header(RecordBodyKind::MapLike, 2)?;
                body_writer = body_writer.write_slot(&"delay", &strat.delay)?;
                body_writer = body_writer.write_slot(&"retries", &strat.retry)?;
                body_writer.done()
            }
            RetryStrategy::Exponential(strat) => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr("exponential")?
                    .complete_header(RecordBodyKind::MapLike, 2)?;
                body_writer = body_writer.write_slot(&"max_interval", &strat.max_interval)?;
                body_writer = body_writer.write_slot(&"max_backoff", &strat.max_backoff)?;
                body_writer.done()
            }
            RetryStrategy::None(_) => writer
                .record(1)?
                .write_extant_attr("none")?
                .complete_header(RecordBodyKind::Mixed, 0)?
                .done(),
        }
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            RetryStrategy::Immediate(strat) => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr("immediate")?
                    .complete_header(RecordBodyKind::MapLike, 1)?;
                body_writer = body_writer.write_slot_into("retries", strat.retry)?;
                body_writer.done()
            }
            RetryStrategy::Interval(strat) => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr("interval")?
                    .complete_header(RecordBodyKind::MapLike, 2)?;
                body_writer = body_writer.write_slot_into("delay", strat.delay)?;
                body_writer = body_writer.write_slot_into("retries", strat.retry)?;
                body_writer.done()
            }
            RetryStrategy::Exponential(strat) => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr("exponential")?
                    .complete_header(RecordBodyKind::MapLike, 2)?;
                body_writer = body_writer.write_slot_into("max_interval", strat.max_interval)?;
                body_writer = body_writer.write_slot_into("max_backoff", strat.max_backoff)?;
                body_writer.done()
            }
            RetryStrategy::None(_) => writer
                .record(1)?
                .write_extant_attr("none")?
                .complete_header(RecordBodyKind::Mixed, 0)?
                .done(),
        }
    }
}

impl StructuralWritable for WebSocketConfig {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut num_items = 2;

        if self.max_send_queue.is_some() {
            num_items += 1
        }

        if self.max_message_size.is_some() {
            num_items += 1
        }

        if self.max_frame_size.is_some() {
            num_items += 1
        }

        let mut body_writer = header_writer
            .write_extant_attr("websocket_connections")?
            .complete_header(RecordBodyKind::MapLike, num_items)?;

        if let Some(val) = &self.max_send_queue {
            body_writer = body_writer.write_slot(&"max_send_queue", val)?
        }
        if let Some(val) = &self.max_message_size {
            body_writer = body_writer.write_slot(&"max_message_size", val)?
        }
        if let Some(val) = &self.max_frame_size {
            body_writer = body_writer.write_slot(&"max_frame_size", val)?
        }
        body_writer =
            body_writer.write_slot(&"accept_unmasked_frames", &self.accept_unmasked_frames)?;

        body_writer = body_writer.write_slot(&"compression", &self.compression)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut num_items = 2;

        if self.max_send_queue.is_some() {
            num_items += 1
        }

        if self.max_message_size.is_some() {
            num_items += 1
        }

        if self.max_frame_size.is_some() {
            num_items += 1
        }

        let mut body_writer = header_writer
            .write_extant_attr("websocket_connections")?
            .complete_header(RecordBodyKind::MapLike, num_items)?;

        if let Some(val) = self.max_send_queue {
            body_writer = body_writer.write_slot_into("max_send_queue", val)?
        }
        if let Some(val) = self.max_message_size {
            body_writer = body_writer.write_slot_into("max_message_size", val)?
        }
        if let Some(val) = self.max_frame_size {
            body_writer = body_writer.write_slot_into("max_frame_size", val)?
        }
        body_writer =
            body_writer.write_slot_into("accept_unmasked_frames", self.accept_unmasked_frames)?;

        body_writer = body_writer.write_slot_into("compression", self.compression)?;

        body_writer.done()
    }
}

impl StructuralWritable for WsCompression {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            WsCompression::None(Some(val)) => {
                let header_writer = writer.record(1)?;

                let mut body_writer = header_writer
                    .write_extant_attr("none")?
                    .complete_header(RecordBodyKind::ArrayLike, 1)?;

                body_writer = body_writer.write_value(val)?;
                body_writer.done()
            }
            WsCompression::None(None) => {
                let header_writer = writer.record(1)?;

                header_writer
                    .write_extant_attr("none")?
                    .complete_header(RecordBodyKind::Mixed, 0)?
                    .done()
            }
            WsCompression::Deflate(deflate) => {
                let header_writer = writer.record(1)?;

                let mut body_writer = header_writer
                    .write_extant_attr("deflate")?
                    .complete_header(RecordBodyKind::ArrayLike, 1)?;

                body_writer = body_writer.write_value(&deflate.compression_level().level())?;
                body_writer.done()
            }
        }
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            WsCompression::None(Some(val)) => {
                let header_writer = writer.record(1)?;

                let mut body_writer = header_writer
                    .write_extant_attr("none")?
                    .complete_header(RecordBodyKind::ArrayLike, 1)?;

                body_writer = body_writer.write_value_into(val)?;
                body_writer.done()
            }
            WsCompression::None(None) => {
                let header_writer = writer.record(1)?;

                header_writer
                    .write_extant_attr("none")?
                    .complete_header(RecordBodyKind::Mixed, 0)?
                    .done()
            }
            WsCompression::Deflate(deflate) => {
                let header_writer = writer.record(1)?;

                let mut body_writer = header_writer
                    .write_extant_attr("deflate")?
                    .complete_header(RecordBodyKind::ArrayLike, 1)?;

                body_writer = body_writer.write_value_into(deflate.compression_level().level())?;
                body_writer.done()
            }
        }
    }
}
