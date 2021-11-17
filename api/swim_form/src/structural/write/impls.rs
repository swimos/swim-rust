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

use crate::structural::tags::{
    DELAY_TAG, EXPONENTIAL_TAG, IMMEDIATE_TAG, INTERVAL_TAG, MAX_BACKOFF_TAG, MAX_INTERVAL_TAG,
    RETRIES_TAG,
};

const NONE_TAG: &str = "none";

use crate::structural::write::{
    BodyWriter, HeaderWriter, RecordBodyKind, StructuralWritable, StructuralWriter,
};
use swim_utilities::future::retryable::RetryStrategy;

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
                .write_extant_attr(NONE_TAG)?
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
                .write_extant_attr(NONE_TAG)?
                .complete_header(RecordBodyKind::Mixed, 0)?
                .done(),
        }
    }
}
