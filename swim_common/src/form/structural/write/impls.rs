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

use crate::form::structural::tags::{
    ACCEPT_UNMASKED_FRAMES_TAG, COMPRESSION_TAG, DEFLATE_TAG, DELAY_TAG, EXPONENTIAL_TAG,
    IMMEDIATE_TAG, INTERVAL_TAG, MAX_BACKOFF_TAG, MAX_FRAME_SIZE_TAG, MAX_INTERVAL_TAG,
    MAX_MESSAGE_SIZE_TAG, MAX_SEND_QUEUE_TAG, NOTE_TAG, RETRIES_TAG, WEBSOCKET_CONNECTIONS_TAG,
};
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
                    .write_extant_attr(IMMEDIATE_TAG)?
                    .complete_header(RecordBodyKind::MapLike, 1)?;
                body_writer = body_writer.write_slot(&RETRIES_TAG, &strat.retry)?;
                body_writer.done()
            }
            RetryStrategy::Interval(strat) => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr(INTERVAL_TAG)?
                    .complete_header(RecordBodyKind::MapLike, 2)?;
                body_writer = body_writer.write_slot(&DELAY_TAG, &strat.delay)?;
                body_writer = body_writer.write_slot(&RETRIES_TAG, &strat.retry)?;
                body_writer.done()
            }
            RetryStrategy::Exponential(strat) => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr(EXPONENTIAL_TAG)?
                    .complete_header(RecordBodyKind::MapLike, 2)?;
                body_writer =
                    body_writer.write_slot(&MAX_INTERVAL_TAG, &strat.get_max_interval())?;
                body_writer = body_writer.write_slot(&MAX_BACKOFF_TAG, &strat.get_max_backoff())?;
                body_writer.done()
            }
            RetryStrategy::None(_) => writer
                .record(1)?
                .write_extant_attr(NOTE_TAG)?
                .complete_header(RecordBodyKind::Mixed, 0)?
                .done(),
        }
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            RetryStrategy::Immediate(strat) => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr(IMMEDIATE_TAG)?
                    .complete_header(RecordBodyKind::MapLike, 1)?;
                body_writer = body_writer.write_slot_into(RETRIES_TAG, strat.retry)?;
                body_writer.done()
            }
            RetryStrategy::Interval(strat) => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr(INTERVAL_TAG)?
                    .complete_header(RecordBodyKind::MapLike, 2)?;
                body_writer = body_writer.write_slot_into(DELAY_TAG, strat.delay)?;
                body_writer = body_writer.write_slot_into(RETRIES_TAG, strat.retry)?;
                body_writer.done()
            }
            RetryStrategy::Exponential(strat) => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr(EXPONENTIAL_TAG)?
                    .complete_header(RecordBodyKind::MapLike, 2)?;
                body_writer =
                    body_writer.write_slot_into(MAX_INTERVAL_TAG, strat.get_max_interval())?;
                body_writer =
                    body_writer.write_slot_into(MAX_BACKOFF_TAG, strat.get_max_backoff())?;
                body_writer.done()
            }
            RetryStrategy::None(_) => writer
                .record(1)?
                .write_extant_attr(NOTE_TAG)?
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
            .write_extant_attr(WEBSOCKET_CONNECTIONS_TAG)?
            .complete_header(RecordBodyKind::MapLike, num_items)?;

        if let Some(val) = &self.max_send_queue {
            body_writer = body_writer.write_slot(&MAX_SEND_QUEUE_TAG, val)?
        }
        if let Some(val) = &self.max_message_size {
            body_writer = body_writer.write_slot(&MAX_MESSAGE_SIZE_TAG, val)?
        }
        if let Some(val) = &self.max_frame_size {
            body_writer = body_writer.write_slot(&MAX_FRAME_SIZE_TAG, val)?
        }
        body_writer =
            body_writer.write_slot(&ACCEPT_UNMASKED_FRAMES_TAG, &self.accept_unmasked_frames)?;

        body_writer = body_writer.write_slot(&COMPRESSION_TAG, &self.compression)?;

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
            .write_extant_attr(WEBSOCKET_CONNECTIONS_TAG)?
            .complete_header(RecordBodyKind::MapLike, num_items)?;

        if let Some(val) = self.max_send_queue {
            body_writer = body_writer.write_slot_into(MAX_SEND_QUEUE_TAG, val)?
        }
        if let Some(val) = self.max_message_size {
            body_writer = body_writer.write_slot_into(MAX_MESSAGE_SIZE_TAG, val)?
        }
        if let Some(val) = self.max_frame_size {
            body_writer = body_writer.write_slot_into(MAX_FRAME_SIZE_TAG, val)?
        }
        body_writer =
            body_writer.write_slot_into(ACCEPT_UNMASKED_FRAMES_TAG, self.accept_unmasked_frames)?;

        body_writer = body_writer.write_slot_into(COMPRESSION_TAG, self.compression)?;

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
                    .write_extant_attr(NOTE_TAG)?
                    .complete_header(RecordBodyKind::ArrayLike, 1)?;

                body_writer = body_writer.write_value(val)?;
                body_writer.done()
            }
            WsCompression::None(None) => {
                let header_writer = writer.record(1)?;

                header_writer
                    .write_extant_attr(NOTE_TAG)?
                    .complete_header(RecordBodyKind::Mixed, 0)?
                    .done()
            }
            WsCompression::Deflate(deflate) => {
                let header_writer = writer.record(1)?;

                let mut body_writer = header_writer
                    .write_extant_attr(DEFLATE_TAG)?
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
                    .write_extant_attr(NOTE_TAG)?
                    .complete_header(RecordBodyKind::ArrayLike, 1)?;

                body_writer = body_writer.write_value_into(val)?;
                body_writer.done()
            }
            WsCompression::None(None) => {
                let header_writer = writer.record(1)?;

                header_writer
                    .write_extant_attr(NOTE_TAG)?
                    .complete_header(RecordBodyKind::Mixed, 0)?
                    .done()
            }
            WsCompression::Deflate(deflate) => {
                let header_writer = writer.record(1)?;

                let mut body_writer = header_writer
                    .write_extant_attr(DEFLATE_TAG)?
                    .complete_header(RecordBodyKind::ArrayLike, 1)?;

                body_writer = body_writer.write_value_into(deflate.compression_level().level())?;
                body_writer.done()
            }
        }
    }
}
